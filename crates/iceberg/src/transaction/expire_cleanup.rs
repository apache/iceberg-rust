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

//! ExpireSnapshots FILE CLEANUP — the physical half of Java's `ExpireSnapshots`
//! (`cleanExpiredFiles(true)`), Increment B2 on top of the metadata-only
//! [`ExpireSnapshotsAction`](crate::transaction::ExpireSnapshotsAction) (B1).
//!
//! **THIS MODULE DELETES FILES.** Deleting a file still reachable from any retained snapshot
//! destroys data unrecoverably, so every design choice here biases toward UNDER-deletion, and
//! the deletion set is computed exclusively by the conservative set algebra below.
//!
//! # What is ported
//!
//! Java 1.10.0 `ReachableFileCleanup` (every step bytecode-verified against
//! `iceberg-core-1.10.0.jar`) — the general-correct cleanup strategy Java selects whenever the
//! expiry is anything but a pure linear main-ancestry trim. Given the table metadata BEFORE and
//! AFTER a successful expire-snapshots commit (`cleanFiles(beforeExpiration, afterExpiration)`):
//!
//! 1. **Expired snapshots** = `before.snapshots() − after.snapshots()` (Java diffs `Snapshot`
//!    sets; `BaseSnapshot.equals` compares id/parent/sequence-number/timestamp/schema-id —
//!    all immutable once a snapshot is written, so within one table's (before, after) pair an
//!    id diff is equivalent).
//! 2. **Manifest lists:** every expired snapshot's `manifest_list` location is deleted.
//!    *Rust safety divergence:* a location also referenced by a RETAINED snapshot is spared.
//!    Java 1.10.0 deletes expired manifest-list locations unconditionally — safe there only
//!    because every Java-written snapshot owns a unique manifest-list file. Sparing a shared one
//!    is strictly under-deleting and is pinned by a test.
//! 3. **Candidate manifests** = the union of all `ManifestFile`s listed by expired snapshots'
//!    manifest lists, MINUS every manifest listed by ANY retained (post-expiry) snapshot —
//!    compared **by path** (`GenericManifestFile.equals`/`hashCode` use `manifestPath` only,
//!    bytecode-verified). This subtraction is what protects carried-forward manifests: a fast
//!    append re-lists every prior manifest, so an expired snapshot's manifests are usually still
//!    referenced by its retained descendants.
//! 4. **Content files** (only when candidate manifests survive step 3): the union of the
//!    **live** entry paths of every candidate manifest, minus the live entry paths of every
//!    retained manifest. "Live" = entry status `ADDED` or `EXISTING` — `ManifestReader
//!    .liveEntries()` filters `status != DELETED` (`isLiveEntry`, bytecode-verified) on BOTH
//!    sides of the subtraction. Both DATA and DELETE manifests are walked: Java's cleanup
//!    projection (`FileCleanupStrategy.MANIFEST_PROJECTION` = path/length/spec-id/snapshot-id/
//!    deleted-files-count) omits the `content` field and the avro-read
//!    `GenericManifestFile` constructor defaults it to `DATA` (bytecode-verified), so
//!    `ManifestFiles.readPaths` reads delete manifests through the data-manifest reader — the
//!    entry schemas share the `file_path` field, so the paths come out correctly. The Rust port
//!    reads both manifest contents through the typed reader instead, with identical results.
//!    A deletion-vector entry's `file_path` is its PUFFIN location, so a puffin shared by
//!    several DVs is naturally deduplicated by the path-set, and one retained DV in the puffin
//!    protects the whole file.
//! 5. **Statistics files:** `statistics`/`partition-statistics` locations present in `before`
//!    but absent from `after` (`FileCleanupStrategy.expiredStatisticsFilesLocations`).
//!
//! Deletion order mirrors Java: content files → manifests → manifest lists → statistics files.
//!
//! # The post-commit seam
//!
//! Java `RemoveSnapshots.commit()` (bytecode): run the metadata commit in the retry loop
//! (`internalApply(); ops.commit(base, updated)`), and ONLY after it returns successfully run
//! `cleanExpiredSnapshots()`, which re-reads the POST-commit metadata (`ops.refresh()`) and
//! calls `cleanFiles(base, current)` with the PRE-commit base. File deletion never runs inside
//! the retry loop and never on a failed commit.
//!
//! The Rust action seam ([`TransactionAction`](crate::transaction::TransactionAction)) has no
//! post-commit hook, so the cleanup lives OUTSIDE the action as this standalone entry point:
//!
//! - [`ExpireSnapshotsCleanup::clean_expired_files`] — the two-state core (`cleanFiles`):
//!   takes the pre-commit and post-commit [`TableMetadata`], plans the deletion set, and sweeps
//!   it through the injectable delete function. **`after` must be the table's CURRENT committed
//!   metadata** — passing a stale "after" state would delete files the real table still
//!   reaches.
//! - [`ExpireSnapshotsCleanup::commit_and_clean`] — the recommended wrapper mirroring Java's
//!   `commit()` ordering: capture the pre-commit metadata, `Transaction::commit`, and run the
//!   cleanup only on the successfully returned table. The `?` on the commit makes the deletion
//!   path structurally unreachable on a failed commit (pinned by a test). The captured "before"
//!   may be staler than the final retry base (the transaction re-bases internally).
//!   Decomposing `before − after`: snapshots a concurrent commit ADDED are absent from
//!   `before` (never candidates — the expired set shrinks), while snapshots a concurrent
//!   EXPIRE removed do ENTER the expired set — but they are absent from the committed `after`
//!   too, so the sweep still deletes only files unreachable from the table's current metadata.
//!   Java's `cleanFiles(base, current)` has the identical property (`base` is the
//!   construction-time metadata); the grown case is the benign double-delete race with the
//!   other expirer's own cleanup — the second sweep sees the other's wins per file (an
//!   idempotent `Ok` or a collected failure, backend-dependent) or aborts at planning when
//!   the expired lists are already gone (pinned by the re-run test), and never over-deletes.
//!
//! Java's `current = ops.refresh()` may observe commits that landed after the expiry; this port
//! uses the committed table returned by `Transaction::commit` instead. Equivalent safety: a
//! later snapshot can only reference files reachable from the committed state (already
//! protected by the retained-side subtraction) or files it newly added (never deletion
//! candidates).
//!
//! **The inherited B1 ref-resurrection window (not widened by B2).** A ref created or rolled
//! back concurrently AT a to-be-expired snapshot id is not guardable by the expiry's
//! `RefSnapshotIdMatch` guards (recorded at B1; Java-REST shares it — its `UpdateRequirements`
//! derives no requirement for snapshot removal): if the racing ref commit lands first
//! unguarded, the expiry's apply-side sweep drops the now-dangling ref, and the COMMITTED
//! metadata no longer reaches that snapshot. This cleanup derives strictly from that
//! already-committed metadata, so it deletes the files the dropped ref pointed at — making the
//! metadata-level loss physical. The window belongs to the metadata commit, not to this
//! module: full-CAS catalogs (Java's primary path) reject the racing ref commit, and a ref
//! commit attempted AFTER the expiry committed fails validation (its snapshot no longer
//! exists).
//!
//! **Default posture divergence:** Java's `cleanExpiredFiles` defaults to TRUE — `commit()`
//! deletes files unless told otherwise. The Rust [`ExpireSnapshotsAction`]
//! (crate::transaction::ExpireSnapshotsAction) commits metadata only; file cleanup happens only
//! when the caller explicitly invokes this module (the `cleanExpiredFiles(false)` posture by
//! default). Rationale: the action seam cannot express Java's post-commit step, and a library
//! port of an irreversible file-deletion path must be opt-in, not ambient.
//!
//! # Failure posture
//!
//! Java logs-and-continues through cleanup failures (`Tasks.suppressFailureWhenFinished` +
//! SLF4J warnings). The `iceberg` crate has no logging-facade dependency (adding one needs
//! approval), and silent swallowing is unacceptable for a deletion sweep — so failures are
//! **collected and returned** in the [`CleanupReport`] instead:
//!
//! - A **manifest-list read failure** (either side) aborts with `Err` BEFORE any deletion —
//!   Java's `readManifests`/`pruneReferencedManifests` use `throwFailureWhenFinished` and run
//!   before the first delete, so the abort-clean semantics match. (Rust is marginally
//!   stricter: Java never reads the RETAINED lists when no candidate manifest exists and its
//!   prune walk early-exits once the candidate set empties, so a corrupt retained list can
//!   escape Java's walk; Rust always reads both sides — a hard `Err` in strictly more cases,
//!   all of them pre-deletion.)
//! - A **candidate-manifest read failure** records a [`CleanupFailureKind::ReadCandidateManifest`]
//!   and skips that manifest's content files (Java suppresses it) — under-deletes only. The
//!   unreadable manifest itself is still deleted (it is referenced by no retained snapshot).
//! - A **retained-manifest read failure** records a [`CleanupFailureKind::ReadRetainedManifest`]
//!   and clears the ENTIRE content-file deletion set — Java's `findFilesToDelete` catches any
//!   `Throwable` while enumerating live files and returns the empty set: when the live set
//!   cannot be proven, no content file may die. Manifests and manifest lists are still swept
//!   (their subtraction needed no manifest reads).
//! - A **per-file delete failure** records the path + funnel kind and the sweep continues —
//!   never a panic, never an abort that leaves an unreported partial sweep.
//!
//! # Deferred (loudly)
//!
//! - **`IncrementalFileCleanup`** — Java's second strategy, picked only when NO explicit
//!   snapshot ids were expired, NO snapshot outside the main ancestry was removed, and NO
//!   non-main snapshots remain (`RemoveSnapshots.cleanExpiredSnapshots`, bytecode-verified; an
//!   explicit `incrementalCleanup(true)` outside those bounds throws). It walks
//!   manifest provenance (`added_snapshot_id`, `hasDeletedFiles`, DELETED entries of expired
//!   ancestor snapshots, cherry-pick `source-snapshot-id` protection) to delete files
//!   logically removed by expired main-line history — an OPTIMIZATION that avoids reading
//!   retained manifests on huge tables, not a correctness requirement: `ReachableFileCleanup`
//!   is the strategy Java itself falls back to for every non-linear history. The reachable
//!   strategy's deletion set is correct for every eligible incremental case; porting the
//!   incremental walk adds a second, more intricate deletion-set derivation for performance
//!   only — deferred until profiling demands it (B3/never).
//! - `cleanExpiredMetadata` (unreachable spec/schema pruning) — same deferral as B1.
//! - Java interop evidence for the cleanup (the GAP_MATRIX row stays 🟡).

use std::collections::{BTreeMap, BTreeSet, HashSet};

use futures::future::BoxFuture;

use crate::error::Result;
use crate::io::FileIO;
use crate::spec::{ManifestFile, SnapshotRef, TableMetadata, TableProperties};
use crate::table::Table;
use crate::transaction::Transaction;
use crate::transaction::expire_snapshots::parse_property;
use crate::{Catalog, Error, ErrorKind};

/// The injectable delete function: receives the absolute file location, resolves to the
/// deletion outcome. The default deletes through [`FileIO::delete`].
pub type DeleteFunction = dyn Fn(String) -> BoxFuture<'static, Result<()>> + Send + Sync;

/// Which cleanup step a [`CleanupFailure`] belongs to — the four delete funnels (Java
/// `FileCleanupStrategy.deleteFiles(set, fileType)`) plus the two manifest-read planning steps
/// whose failures Java suppresses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CleanupFailureKind {
    /// Deleting a content file (a data file, a position/equality delete file, or a
    /// deletion-vector puffin) failed — Java's `"data"` funnel.
    DeleteContentFile,
    /// Deleting a manifest file failed — Java's `"manifest"` funnel.
    DeleteManifest,
    /// Deleting a manifest-list file failed — Java's `"manifest list"` funnel.
    DeleteManifestList,
    /// Deleting a statistics / partition-statistics file failed — Java's `"statistics files"`
    /// funnel.
    DeleteStatisticsFile,
    /// A candidate (expired-only) manifest could not be read while enumerating content files;
    /// its content files were skipped (under-deletion only — the manifest itself is still
    /// referenced by no retained snapshot and is still deleted).
    ReadCandidateManifest,
    /// A RETAINED manifest could not be read while enumerating live content files; the entire
    /// content-file deletion set was cleared (Java's catch-`Throwable` → empty set): when the
    /// live set cannot be proven, no content file may die.
    ReadRetainedManifest,
}

/// One collected, non-aborting cleanup failure (the Rust replacement for Java's
/// log-and-continue posture — see the [module docs](self)).
#[derive(Debug)]
pub struct CleanupFailure {
    /// The file the failed step was operating on.
    pub path: String,
    /// Which cleanup step failed.
    pub kind: CleanupFailureKind,
    /// The underlying error.
    pub error: Error,
}

/// The outcome of one cleanup sweep: every successfully deleted path, per delete funnel
/// (mirroring Java's four `deleteFiles` calls), plus every collected failure. Vectors are
/// deterministic: paths sorted within each funnel, funnels in Java's deletion order.
#[derive(Debug, Default)]
pub struct CleanupReport {
    /// Deleted content files: data files, position/equality delete files, deletion-vector
    /// puffins (Java's `"data"` funnel — its `readPaths` walk emits all three classes).
    pub deleted_content_files: Vec<String>,
    /// Deleted manifest files (data and delete manifests).
    pub deleted_manifests: Vec<String>,
    /// Deleted manifest-list files.
    pub deleted_manifest_lists: Vec<String>,
    /// Deleted statistics / partition-statistics files.
    pub deleted_statistics_files: Vec<String>,
    /// Collected failures, in plan-then-sweep order. Empty means a fully clean sweep.
    pub failures: Vec<CleanupFailure>,
}

impl CleanupReport {
    /// True when the sweep deleted nothing and collected no failures.
    pub fn is_empty(&self) -> bool {
        self.deleted_content_files.is_empty()
            && self.deleted_manifests.is_empty()
            && self.deleted_manifest_lists.is_empty()
            && self.deleted_statistics_files.is_empty()
            && self.failures.is_empty()
    }
}

/// Post-commit physical file cleanup for snapshot expiry — the Rust port of Java 1.10.0
/// `ReachableFileCleanup` (see the [module docs](self) for the exact set algebra, the seam
/// design, and the failure posture). **This deletes files**; construct it with the table's
/// [`FileIO`] and run it only against a successfully committed expiry, preferably through
/// [`Self::commit_and_clean`].
pub struct ExpireSnapshotsCleanup {
    file_io: FileIO,
    delete_function: Box<DeleteFunction>,
}

impl ExpireSnapshotsCleanup {
    /// Creates a cleanup with the default delete function ([`FileIO::delete`]).
    pub fn new(file_io: FileIO) -> Self {
        let delete_io = file_io.clone();
        ExpireSnapshotsCleanup {
            file_io,
            delete_function: Box::new(move |path: String| {
                let delete_io = delete_io.clone();
                Box::pin(async move { delete_io.delete(&path).await })
            }),
        }
    }

    /// Replaces the delete function (Java `ExpireSnapshots.deleteWith`) — the testability /
    /// dry-run seam: inject a recorder that returns `Ok` without deleting to compute the
    /// would-be deletion set without touching storage, or route deletions through an external
    /// queue. The planning reads still go through the construction-time [`FileIO`].
    pub fn delete_with(
        mut self,
        delete_function: impl Fn(String) -> BoxFuture<'static, Result<()>> + Send + Sync + 'static,
    ) -> Self {
        self.delete_function = Box::new(delete_function);
        self
    }

    /// Commit `transaction` through `catalog` and, ONLY if the commit succeeds, clean the files
    /// expired by it — the Java `RemoveSnapshots.commit()` ordering (commit retry loop first,
    /// `cleanExpiredSnapshots()` strictly after success; bytecode-verified). On a failed commit
    /// the error propagates before any deletion is planned or executed.
    ///
    /// The pre-commit state is captured from the transaction's table; the post-commit state is
    /// the committed table the catalog returned. Mirroring Java's `commit()` gate
    /// (`cleanExpiredFiles && !base.snapshots().isEmpty()`), a pre-commit table with no
    /// snapshots commits but skips cleanup.
    pub async fn commit_and_clean(
        &self,
        transaction: Transaction,
        catalog: &dyn Catalog,
    ) -> Result<(Table, CleanupReport)> {
        let before = transaction.table.metadata_ref();
        // The `?` is the safety gate: a failed/uncertain commit returns here, making the
        // deletion path below structurally unreachable (pinned by
        // `test_failed_commit_performs_zero_deletions`).
        let committed = transaction.commit(catalog).await?;
        if before.snapshots().len() == 0 {
            return Ok((committed, CleanupReport::default()));
        }
        let report = self
            .clean_expired_files(&before, committed.metadata())
            .await?;
        Ok((committed, report))
    }

    /// The two-state cleanup core (Java `ReachableFileCleanup.cleanFiles(before, after)`):
    /// delete every file reachable from `before` but unreachable from `after`, per the set
    /// algebra in the [module docs](self).
    ///
    /// **`after` MUST be the table's current committed metadata** (the state a successful
    /// expire-snapshots commit produced). Passing anything staler deletes files the live table
    /// still reaches — use [`Self::commit_and_clean`] unless you are wiring your own commit.
    ///
    /// Returns `Err` without deleting anything when a manifest LIST cannot be read or the
    /// table's `gc.enabled` gate refuses; per-file problems after planning are collected in the
    /// returned [`CleanupReport`] instead (see the failure posture in the module docs).
    pub async fn clean_expired_files(
        &self,
        before: &TableMetadata,
        after: &TableMetadata,
    ) -> Result<CleanupReport> {
        // Re-honor the GC gate at the cleanup door. Java's gate sits in the RemoveSnapshots
        // CONSTRUCTOR (bytecode: `PropertyUtil.propertyAsBoolean(base.properties, "gc.enabled",
        // true)` + verbatim message), which covers cleanup because cleanup runs inside the same
        // object; this standalone entry point must re-check, or a direct call would bypass the
        // gate the action enforced at commit.
        let gc_enabled = parse_property(
            before.properties(),
            TableProperties::PROPERTY_GC_ENABLED,
            TableProperties::PROPERTY_GC_ENABLED_DEFAULT,
        )?;
        if !gc_enabled {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Cannot expire snapshots: GC is disabled (deleting files may corrupt other tables)"
                    .to_string(),
            ));
        }

        // 1. Expired snapshots (before − after, by id) and their manifest lists. The
        //    retained-shared filter on the lists is the documented Rust-only under-deletion
        //    guard (Java deletes expired manifest-list locations unconditionally).
        let retained_ids: HashSet<i64> = after.snapshots().map(|s| s.snapshot_id()).collect();
        let expired_snapshots: Vec<&SnapshotRef> = before
            .snapshots()
            .filter(|snapshot| !retained_ids.contains(&snapshot.snapshot_id()))
            .collect();
        let retained_list_locations: HashSet<&str> =
            after.snapshots().map(|s| s.manifest_list()).collect();
        let manifest_lists_to_delete: BTreeSet<String> = expired_snapshots
            .iter()
            .map(|snapshot| snapshot.manifest_list())
            .filter(|location| !retained_list_locations.contains(location))
            .map(str::to_string)
            .collect();

        // 2 + 3. Candidate manifests (from expired snapshots) minus retained manifests, BY PATH
        //    (GenericManifestFile path equality). A manifest-list read failure on either side
        //    aborts here, before any deletion (Java `throwFailureWhenFinished`).
        let candidate_manifests = self
            .manifests_by_path(expired_snapshots.iter().copied(), before)
            .await?;
        let retained_manifests = self.manifests_by_path(after.snapshots(), after).await?;
        let manifests_to_delete: BTreeMap<String, ManifestFile> = candidate_manifests
            .into_iter()
            .filter(|(path, _)| !retained_manifests.contains_key(path))
            .collect();

        let mut report = CleanupReport::default();

        // 4. Content files: live entries of the deleted manifests minus live entries of every
        //    retained manifest (Java `findFilesToDelete`; "live" = status != DELETED on BOTH
        //    sides). Skipped entirely when no manifest dies (Java's `!manifestsToDelete
        //    .isEmpty()` gate).
        let mut content_files_to_delete: BTreeSet<String> = BTreeSet::new();
        if !manifests_to_delete.is_empty() {
            for (path, manifest_file) in &manifests_to_delete {
                match manifest_file.load_manifest(&self.file_io).await {
                    Ok(manifest) => {
                        for entry in manifest.entries() {
                            if entry.is_alive() {
                                content_files_to_delete.insert(entry.file_path().to_string());
                            }
                        }
                    }
                    Err(error) => report.failures.push(CleanupFailure {
                        path: path.clone(),
                        kind: CleanupFailureKind::ReadCandidateManifest,
                        error,
                    }),
                }
            }
            if !content_files_to_delete.is_empty() {
                for (path, manifest_file) in &retained_manifests {
                    match manifest_file.load_manifest(&self.file_io).await {
                        Ok(manifest) => {
                            for entry in manifest.entries() {
                                if entry.is_alive() {
                                    content_files_to_delete.remove(entry.file_path());
                                }
                            }
                        }
                        Err(error) => {
                            // Java catches any Throwable here and returns the EMPTY set: when
                            // the live file set cannot be proven, no content file may die.
                            report.failures.push(CleanupFailure {
                                path: path.clone(),
                                kind: CleanupFailureKind::ReadRetainedManifest,
                                error,
                            });
                            content_files_to_delete.clear();
                            break;
                        }
                    }
                }
            }
        }

        // 5. Statistics files present before but not after (Java
        //    `expiredStatisticsFilesLocations` — plain location-set difference).
        let statistics_to_delete: BTreeSet<String> = statistics_locations(before)
            .difference(&statistics_locations(after))
            .cloned()
            .collect();

        // 6. The sweep, in Java's deletion order: content files → manifests → manifest lists →
        //    statistics. Per-file failures are collected; the sweep never aborts.
        report.deleted_content_files = self
            .delete_all(
                content_files_to_delete,
                CleanupFailureKind::DeleteContentFile,
                &mut report.failures,
            )
            .await;
        report.deleted_manifests = self
            .delete_all(
                manifests_to_delete.into_keys().collect(),
                CleanupFailureKind::DeleteManifest,
                &mut report.failures,
            )
            .await;
        report.deleted_manifest_lists = self
            .delete_all(
                manifest_lists_to_delete,
                CleanupFailureKind::DeleteManifestList,
                &mut report.failures,
            )
            .await;
        report.deleted_statistics_files = self
            .delete_all(
                statistics_to_delete,
                CleanupFailureKind::DeleteStatisticsFile,
                &mut report.failures,
            )
            .await;

        Ok(report)
    }

    /// Read the manifest lists of `snapshots` and collect every listed [`ManifestFile`] keyed
    /// by path (Java `readManifests(Set<Snapshot>)` / the prune walk — both keyed on
    /// `GenericManifestFile`'s path equality). DATA and DELETE manifests are both collected. A
    /// manifest-list read failure is a hard error (Java `throwFailureWhenFinished`; the caller
    /// runs this before any deletion).
    async fn manifests_by_path<'a>(
        &self,
        snapshots: impl Iterator<Item = &'a SnapshotRef>,
        metadata: &TableMetadata,
    ) -> Result<BTreeMap<String, ManifestFile>> {
        let mut manifests = BTreeMap::new();
        for snapshot in snapshots {
            let manifest_list = snapshot
                .load_manifest_list(&self.file_io, metadata)
                .await
                .map_err(|error| {
                    error.with_context(
                        "manifest_list",
                        format!(
                            "failed to read manifest list of snapshot {} during expire-snapshots \
                             cleanup planning (no files were deleted)",
                            snapshot.snapshot_id()
                        ),
                    )
                })?;
            for manifest_file in manifest_list.entries() {
                manifests.insert(manifest_file.manifest_path.clone(), manifest_file.clone());
            }
        }
        Ok(manifests)
    }

    /// Delete every path in `paths` through the injected delete function, returning the
    /// successfully deleted paths (in `paths`' sorted order) and recording failures tagged
    /// `kind` — the per-funnel sweep (Java `FileCleanupStrategy.deleteFiles(set, fileType)`,
    /// with collect-and-return replacing log-and-continue). Never aborts.
    async fn delete_all(
        &self,
        paths: BTreeSet<String>,
        kind: CleanupFailureKind,
        failures: &mut Vec<CleanupFailure>,
    ) -> Vec<String> {
        let mut deleted = Vec::with_capacity(paths.len());
        for path in paths {
            match (self.delete_function)(path.clone()).await {
                Ok(()) => deleted.push(path),
                Err(error) => failures.push(CleanupFailure { path, kind, error }),
            }
        }
        deleted
    }
}

/// All statistics + partition-statistics file locations of `metadata` (Java
/// `FileCleanupStrategy.statsFileLocations`).
fn statistics_locations(metadata: &TableMetadata) -> BTreeSet<String> {
    metadata
        .statistics_iter()
        .map(|statistics| statistics.statistics_path.clone())
        .chain(
            metadata
                .partition_statistics_iter()
                .map(|statistics| statistics.statistics_path.clone()),
        )
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    use bytes::Bytes;
    use futures::future::BoxFuture;

    use super::{CleanupFailureKind, ExpireSnapshotsCleanup};
    use crate::error::Result;
    use crate::memory::tests::new_memory_catalog;
    use crate::spec::{
        DataContentType, DataFile, DataFileBuilder, DataFileFormat, Literal, ManifestContentType,
        ManifestStatus, Operation, Snapshot, StatisticsFile, Struct, Summary, TableProperties,
    };
    use crate::table::Table;
    use crate::transaction::{ApplyTransactionAction, Transaction};
    use crate::{Catalog, Error, ErrorKind};

    /// =======================================================================
    /// Fixture helpers — real tables in a memory catalog, real manifest /
    /// manifest-list files in the table's FileIO
    /// =======================================================================
    /// A synthetic data file routed to partition `x = 0` (metadata-only; the parquet bytes never
    /// exist — content-file deletions are asserted on the REPORT and the injected delete fn).
    fn synthetic_data_file(path: &str) -> DataFile {
        DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(path.to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(0)
            .partition(Struct::from_iter([Some(Literal::long(0))]))
            .build()
            .expect("build synthetic data file")
    }

    /// A synthetic deletion vector: PUFFIN-format position delete with the DV-required
    /// `referenced_data_file` / `content_offset` / `content_size_in_bytes`. Two DVs sharing one
    /// puffin differ only in referenced file + offset — the real multi-blob shape.
    fn synthetic_dv_file(puffin_path: &str, referenced_data_file: &str, offset: i64) -> DataFile {
        DataFileBuilder::default()
            .content(DataContentType::PositionDeletes)
            .file_path(puffin_path.to_string())
            .file_format(DataFileFormat::Puffin)
            .file_size_in_bytes(200)
            .record_count(1)
            .partition_spec_id(0)
            .partition(Struct::from_iter([Some(Literal::long(0))]))
            .referenced_data_file(Some(referenced_data_file.to_string()))
            .content_offset(Some(offset))
            .content_size_in_bytes(Some(40))
            .build()
            .expect("build synthetic dv file")
    }

    async fn append(catalog: &impl Catalog, table: &Table, files: Vec<DataFile>) -> Table {
        let tx = Transaction::new(table);
        let tx = tx
            .fast_append()
            .add_data_files(files)
            .apply(tx)
            .expect("apply fast append");
        tx.commit(catalog).await.expect("commit fast append")
    }

    /// The manifest-list location of `snapshot_id`.
    fn list_path(table: &Table, snapshot_id: i64) -> String {
        table
            .metadata()
            .snapshot_by_id(snapshot_id)
            .expect("snapshot present")
            .manifest_list()
            .to_string()
    }

    /// `(manifest_path, content)` pairs listed by `snapshot_id`'s manifest list.
    async fn manifests_of(table: &Table, snapshot_id: i64) -> Vec<(String, ManifestContentType)> {
        let snapshot = table
            .metadata()
            .snapshot_by_id(snapshot_id)
            .expect("snapshot present");
        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .expect("load manifest list");
        manifest_list
            .entries()
            .iter()
            .map(|manifest| (manifest.manifest_path.clone(), manifest.content))
            .collect()
    }

    async fn exists(table: &Table, path: &str) -> bool {
        table.file_io().exists(path).await.expect("exists check")
    }

    /// A recording delete fn that "succeeds" without touching storage — the dry-run /
    /// zero-deletion-proof seam.
    #[allow(clippy::type_complexity)] // the tuple IS the seam: (recorded paths, injectable fn)
    fn recording_delete_fn() -> (
        Arc<Mutex<Vec<String>>>,
        impl Fn(String) -> BoxFuture<'static, Result<()>> + Send + Sync + 'static,
    ) {
        let recorded = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::clone(&recorded);
        let delete_fn = move |path: String| -> BoxFuture<'static, Result<()>> {
            sink.lock().expect("recorder lock").push(path);
            Box::pin(async { Ok(()) })
        };
        (recorded, delete_fn)
    }

    /// Expire everything age-eligible keeping each branch head (`expire_older_than(i64::MAX)` +
    /// `retain_last(1)`), committed + cleaned through the `commit_and_clean` wrapper.
    async fn expire_and_clean(
        catalog: &impl Catalog,
        table: &Table,
        cleanup: &ExpireSnapshotsCleanup,
    ) -> (Table, super::CleanupReport) {
        let tx = Transaction::new(table);
        let tx = tx
            .expire_snapshots()
            .expire_older_than(i64::MAX)
            .retain_last(1)
            .apply(tx)
            .expect("apply expire");
        cleanup
            .commit_and_clean(tx, catalog)
            .await
            .expect("commit and clean")
    }

    /// Commit the same expiry WITHOUT cleanup, returning (pre-commit table, post-commit table) —
    /// for tests that need to intervene (corrupt a file) between commit and cleanup.
    async fn expire_metadata_only(catalog: &impl Catalog, table: &Table) -> (Table, Table) {
        let tx = Transaction::new(table);
        let tx = tx
            .expire_snapshots()
            .expire_older_than(i64::MAX)
            .retain_last(1)
            .apply(tx)
            .expect("apply expire");
        let committed = tx.commit(catalog).await.expect("commit expire");
        (table.clone(), committed)
    }

    /// =======================================================================
    /// The deletion-set pins — every class, both directions
    /// =======================================================================
    /// Risk pinned (THE most important pin): fast-append chains CARRY manifests forward, so an
    /// expired snapshot's manifest is almost always still listed by its retained descendants. A
    /// shared manifest must SURVIVE (deleting it destroys the retained snapshot's data
    /// unrecoverably — the data-loss class the candidates-minus-retained subtraction exists
    /// for), while the expired snapshot's now-unreferenced manifest LIST must die.
    #[tokio::test]
    async fn test_carried_forward_shared_manifest_survives_expired_list_dies() {
        let catalog = new_memory_catalog().await;
        let table = make_table(&catalog).await;
        let table1 = append(&catalog, &table, vec![synthetic_data_file(
            "test/wtB2/a.parquet",
        )])
        .await;
        let s1 = table1.metadata().current_snapshot_id().expect("s1");
        let table2 = append(&catalog, &table1, vec![synthetic_data_file(
            "test/wtB2/b.parquet",
        )])
        .await;
        let s2 = table2.metadata().current_snapshot_id().expect("s2");

        let s1_list = list_path(&table2, s1);
        let s2_list = list_path(&table2, s2);
        let m1 = manifests_of(&table2, s1).await[0].0.clone();
        // Pre-flight: the fixture really shares — S2's list carries M1 forward.
        let s2_manifest_paths: Vec<String> = manifests_of(&table2, s2)
            .await
            .into_iter()
            .map(|(path, _)| path)
            .collect();
        assert!(
            s2_manifest_paths.contains(&m1),
            "fixture must carry M1 forward: {s2_manifest_paths:?}"
        );

        let cleanup = ExpireSnapshotsCleanup::new(table2.file_io().clone());
        let (expired_table, report) = expire_and_clean(&catalog, &table2, &cleanup).await;
        assert!(expired_table.metadata().snapshot_by_id(s1).is_none());

        assert_eq!(report.deleted_manifest_lists, vec![s1_list.clone()]);
        assert!(
            report.deleted_manifests.is_empty(),
            "the SHARED manifest must survive: {:?}",
            report.deleted_manifests
        );
        assert!(report.deleted_content_files.is_empty());
        assert!(report.failures.is_empty());
        assert!(!exists(&table2, &s1_list).await, "expired list must die");
        assert!(exists(&table2, &m1).await, "shared manifest must survive");
        assert!(exists(&table2, &s2_list).await);
    }

    /// Risk pinned: a data file REWRITTEN into a new manifest (rewrite_manifests) but still LIVE
    /// in the retained state must survive even though its original manifest dies — the
    /// retained-side live-file subtraction in (c). Skipping that check deletes live data.
    #[tokio::test]
    async fn test_rewritten_but_live_data_file_survives_its_old_manifest_dies() {
        let catalog = new_memory_catalog().await;
        let table = make_table(&catalog).await;
        let data_path = "test/wtB2/rewritten-live.parquet";
        let table1 = append(&catalog, &table, vec![synthetic_data_file(data_path)]).await;
        let s1 = table1.metadata().current_snapshot_id().expect("s1");
        let m1 = manifests_of(&table1, s1).await[0].0.clone();

        // S2: cluster every data manifest into a fresh one — the entry for `data_path` is
        // carried as EXISTING in a NEW manifest file.
        let tx = Transaction::new(&table1);
        let tx = tx
            .rewrite_manifests()
            .cluster_by(|_file| "all".to_string())
            .apply(tx)
            .expect("apply rewrite manifests");
        let table2 = tx.commit(&catalog).await.expect("commit rewrite manifests");
        let s2 = table2.metadata().current_snapshot_id().expect("s2");
        let s2_manifests = manifests_of(&table2, s2).await;
        assert!(
            !s2_manifests.iter().any(|(path, _)| path == &m1),
            "pre-flight: the rewrite must have replaced M1: {s2_manifests:?}"
        );

        let cleanup = ExpireSnapshotsCleanup::new(table2.file_io().clone());
        let (_, report) = expire_and_clean(&catalog, &table2, &cleanup).await;

        assert_eq!(report.deleted_manifests, vec![m1.clone()]);
        assert!(
            report.deleted_content_files.is_empty(),
            "the still-live data file must NOT die: {:?}",
            report.deleted_content_files
        );
        assert!(report.failures.is_empty());
        assert!(!exists(&table2, &m1).await);
        for (path, _) in &s2_manifests {
            assert!(
                exists(&table2, path).await,
                "retained manifest {path} must survive"
            );
        }
    }

    /// Risk pinned (the other direction of (c)): a data file live ONLY in expired snapshots —
    /// removed by a retained `delete_files` commit — must DIE exactly once, and the DELETED
    /// tombstone the retained rewritten manifest carries for it must NOT protect it ("live" =
    /// status != DELETED on the retained side too, Java `isLiveEntry`).
    #[tokio::test]
    async fn test_data_file_only_in_expired_snapshots_dies_tombstone_does_not_protect() {
        let catalog = new_memory_catalog().await;
        let table = make_table(&catalog).await;
        let data_path = "test/wtB2/expired-only.parquet";
        let table1 = append(&catalog, &table, vec![synthetic_data_file(data_path)]).await;
        let s1 = table1.metadata().current_snapshot_id().expect("s1");
        let m1 = manifests_of(&table1, s1).await[0].0.clone();

        // S2: delete the data file (copy-on-write tombstone in a rewritten manifest).
        let tx = Transaction::new(&table1);
        let tx = tx
            .delete_files()
            .delete_file(data_path)
            .apply(tx)
            .expect("apply delete files");
        let table2 = tx.commit(&catalog).await.expect("commit delete files");
        let s2 = table2.metadata().current_snapshot_id().expect("s2");
        // Pre-flight: the retained state really carries the DELETED tombstone for the file (the
        // suppression-fixture rule — prove the case reaches the path under test).
        let s2_manifests = manifests_of(&table2, s2).await;
        let mut tombstone_seen = false;
        for (path, _) in &s2_manifests {
            assert_ne!(path, &m1, "pre-flight: M1 must have been rewritten");
            let manifest_file = table2
                .metadata()
                .snapshot_by_id(s2)
                .expect("s2 snapshot")
                .load_manifest_list(table2.file_io(), table2.metadata())
                .await
                .expect("s2 list")
                .entries()
                .iter()
                .find(|m| &m.manifest_path == path)
                .expect("listed manifest")
                .clone();
            let manifest = manifest_file
                .load_manifest(table2.file_io())
                .await
                .expect("load retained manifest");
            tombstone_seen |= manifest.entries().iter().any(|entry| {
                entry.status() == ManifestStatus::Deleted && entry.file_path() == data_path
            });
        }
        assert!(
            tombstone_seen,
            "pre-flight: tombstone for {data_path} must exist"
        );

        let cleanup = ExpireSnapshotsCleanup::new(table2.file_io().clone());
        let (_, report) = expire_and_clean(&catalog, &table2, &cleanup).await;

        assert_eq!(
            report.deleted_content_files,
            vec![data_path.to_string()],
            "the expired-only data file must die, exactly once"
        );
        assert_eq!(report.deleted_manifests, vec![m1.clone()]);
        assert!(report.failures.is_empty());
        for (path, _) in &s2_manifests {
            assert!(
                exists(&table2, path).await,
                "retained manifest {path} must survive"
            );
        }
    }

    /// Risk pinned: a puffin path referenced by BOTH a dying delete manifest and a retained one
    /// must SURVIVE, while a puffin referenced only by the dying manifest dies — the path-set
    /// dedup + retained-side subtraction, both directions in one fixture. DM1 holds DV-A@P and
    /// DV-C@P3; replacing DV-C rewrites DM1 into a retained manifest carrying DV-A as EXISTING
    /// (same puffin P), so when DM1 dies with its snapshots, P is still live (survives) and P3
    /// is not (dies).
    ///
    /// Note the shape deliberately avoids "two DVs in one puffin, remove one": delete-file
    /// removal is BY PATH in Java too (1.10.0 `ManifestFilterManager.delete(F)` adds
    /// `file.location()` to the `deletePaths` CharSequenceSet — bytecode-verified), so removing
    /// one DV of a shared puffin tombstones every same-path entry and the puffin genuinely
    /// becomes unreachable. The cross-MANIFEST share below is the real-world shared-puffin
    /// shape the cleanup must protect.
    #[tokio::test]
    async fn test_shared_puffin_with_one_retained_dv_survives() {
        let catalog = new_memory_catalog().await;
        let table = crate::transaction::tests::make_v3_minimal_table_in_catalog(&catalog).await;
        let (a_path, c_path) = ("test/wtB2/dv-a.parquet", "test/wtB2/dv-c.parquet");
        let table1 = append(&catalog, &table, vec![
            synthetic_data_file(a_path),
            synthetic_data_file(c_path),
        ])
        .await;

        // S2: one delete manifest holding DV-A in the shared puffin and DV-C in its own.
        let shared_puffin = "test/wtB2/shared.puffin";
        let replaced_puffin = "test/wtB2/replaced.puffin";
        let dv_a = synthetic_dv_file(shared_puffin, a_path, 4);
        let dv_c = synthetic_dv_file(replaced_puffin, c_path, 4);
        let tx = Transaction::new(&table1);
        let tx = tx
            .row_delta()
            .add_deletes(vec![dv_a, dv_c.clone()])
            .apply(tx)
            .expect("apply row delta dvs");
        let table2 = tx.commit(&catalog).await.expect("commit row delta dvs");
        let s2 = table2.metadata().current_snapshot_id().expect("s2");
        let dm1 = manifests_of(&table2, s2)
            .await
            .into_iter()
            .find(|(_, content)| *content == ManifestContentType::Deletes)
            .expect("S2 delete manifest")
            .0;

        // S3: replace DV-C (remove + successor puffin). The rewrite carries DV-A as EXISTING —
        // still at the shared puffin path — into the retained rewritten delete manifest.
        let dv_c2 = synthetic_dv_file("test/wtB2/successor.puffin", c_path, 4);
        let tx = Transaction::new(&table2);
        let tx = tx
            .row_delta()
            .remove_deletes(dv_c)
            .add_deletes(vec![dv_c2])
            .apply(tx)
            .expect("apply dv replacement");
        let table3 = tx.commit(&catalog).await.expect("commit dv replacement");
        let s3 = table3.metadata().current_snapshot_id().expect("s3");
        let s3_manifests = manifests_of(&table3, s3).await;
        assert!(
            !s3_manifests.iter().any(|(path, _)| path == &dm1),
            "pre-flight: DM1 must have been rewritten by the removal: {s3_manifests:?}"
        );

        let cleanup = ExpireSnapshotsCleanup::new(table3.file_io().clone());
        let (_, report) = expire_and_clean(&catalog, &table3, &cleanup).await;

        assert_eq!(report.deleted_manifests, vec![dm1]);
        assert_eq!(
            report.deleted_content_files,
            vec![replaced_puffin.to_string()],
            "the SHARED puffin must survive (DV-A lives EXISTING in the retained rewrite); \
             only the fully-replaced puffin dies"
        );
        assert!(report.failures.is_empty());
    }

    /// The kill direction of the puffin class: a puffin whose EVERY DV expired (the lone DV was
    /// replaced) is referenced by no retained manifest and must DIE; the successor puffin and
    /// the data files stay.
    #[tokio::test]
    async fn test_expired_only_dv_puffin_dies() {
        let catalog = new_memory_catalog().await;
        let table = crate::transaction::tests::make_v3_minimal_table_in_catalog(&catalog).await;
        let a_path = "test/wtB2/dv-only.parquet";
        let table1 = append(&catalog, &table, vec![synthetic_data_file(a_path)]).await;

        let old_puffin = "test/wtB2/old.puffin";
        let dv_a = synthetic_dv_file(old_puffin, a_path, 4);
        let tx = Transaction::new(&table1);
        let tx = tx
            .row_delta()
            .add_deletes(vec![dv_a.clone()])
            .apply(tx)
            .expect("apply row delta dv");
        let table2 = tx.commit(&catalog).await.expect("commit row delta dv");

        let dv_a2 = synthetic_dv_file("test/wtB2/new.puffin", a_path, 4);
        let tx = Transaction::new(&table2);
        let tx = tx
            .row_delta()
            .remove_deletes(dv_a)
            .add_deletes(vec![dv_a2])
            .apply(tx)
            .expect("apply dv replacement");
        let table3 = tx.commit(&catalog).await.expect("commit dv replacement");

        let cleanup = ExpireSnapshotsCleanup::new(table3.file_io().clone());
        let (_, report) = expire_and_clean(&catalog, &table3, &cleanup).await;

        assert_eq!(
            report.deleted_content_files,
            vec![old_puffin.to_string()],
            "the fully-expired puffin must die — and ONLY it (data file + successor survive)"
        );
        assert!(report.failures.is_empty());
    }

    /// Risk pinned: the documented Rust-only SAFETY DIVERGENCE — Java 1.10.0 deletes every
    /// expired snapshot's manifest-list location unconditionally; this port spares a location a
    /// RETAINED snapshot also references (a grafted/cloned-metadata shape no Java writer
    /// produces, but unconditional deletion would destroy the retained snapshot). A
    /// documented-but-unpinned divergence is indistinguishable from an accidental one.
    #[tokio::test]
    async fn test_manifest_list_shared_with_retained_snapshot_survives() {
        let catalog = new_memory_catalog().await;
        let table = make_table(&catalog).await;
        let table1 = append(&catalog, &table, vec![synthetic_data_file(
            "test/wtB2/shared-list.parquet",
        )])
        .await;
        let s1 = table1.metadata().current_snapshot_id().expect("s1");
        let s1_list = list_path(&table1, s1);

        // "before" = the real table plus a grafted dangling snapshot X whose manifest_list
        // points at S1's list file; "after" = the real table (X expired, S1 retained).
        let grafted = Snapshot::builder()
            .with_snapshot_id(999_001)
            .with_parent_snapshot_id(Some(s1))
            .with_sequence_number(table1.metadata().last_sequence_number() + 1)
            .with_timestamp_ms(table1.metadata().last_updated_ms())
            .with_manifest_list(s1_list.clone())
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: HashMap::new(),
            })
            .with_schema_id(0)
            .build();
        let before = table1
            .metadata()
            .clone()
            .into_builder(None)
            .add_snapshot(grafted)
            .expect("graft snapshot")
            .build()
            .expect("build grafted metadata")
            .metadata;

        let cleanup = ExpireSnapshotsCleanup::new(table1.file_io().clone());
        let report = cleanup
            .clean_expired_files(&before, table1.metadata())
            .await
            .expect("clean expired files");

        assert!(
            report.is_empty(),
            "the shared manifest list (and everything under it) must survive: {report:?}"
        );
        assert!(exists(&table1, &s1_list).await);
    }

    /// Risk pinned: statistics files of expired snapshots die; a retained snapshot's statistics
    /// survive (the before-minus-after location diff, both directions in one fixture).
    #[tokio::test]
    async fn test_expired_snapshot_statistics_file_dies_retained_one_survives() {
        let catalog = new_memory_catalog().await;
        let table = make_table(&catalog).await;
        let table1 = append(&catalog, &table, vec![synthetic_data_file(
            "test/wtB2/stats-a.parquet",
        )])
        .await;
        let s1 = table1.metadata().current_snapshot_id().expect("s1");
        let table2 = append(&catalog, &table1, vec![synthetic_data_file(
            "test/wtB2/stats-b.parquet",
        )])
        .await;
        let s2 = table2.metadata().current_snapshot_id().expect("s2");

        let stats_path = |id: i64| {
            format!(
                "{}/metadata/wtB2-stats-{id}.puffin",
                table2.metadata().location()
            )
        };
        let mut table_with_stats = table2.clone();
        for id in [s1, s2] {
            let path = stats_path(id);
            table_with_stats
                .file_io()
                .new_output(&path)
                .expect("stats output")
                .write(Bytes::from_static(b"wtB2 stats fixture"))
                .await
                .expect("write stats fixture");
            let tx = Transaction::new(&table_with_stats);
            let tx = tx
                .update_statistics()
                .set_statistics(StatisticsFile {
                    snapshot_id: id,
                    statistics_path: path,
                    file_size_in_bytes: 18,
                    file_footer_size_in_bytes: 4,
                    key_metadata: None,
                    blob_metadata: vec![],
                })
                .apply(tx)
                .expect("apply set statistics");
            table_with_stats = tx.commit(&catalog).await.expect("commit statistics");
        }

        let cleanup = ExpireSnapshotsCleanup::new(table_with_stats.file_io().clone());
        let (_, report) = expire_and_clean(&catalog, &table_with_stats, &cleanup).await;

        assert_eq!(report.deleted_statistics_files, vec![stats_path(s1)]);
        assert!(report.failures.is_empty());
        assert!(!exists(&table_with_stats, &stats_path(s1)).await);
        assert!(
            exists(&table_with_stats, &stats_path(s2)).await,
            "the retained snapshot's statistics must survive"
        );
    }

    /// =======================================================================
    /// The seam pins — commit ordering, dry-run, failure posture, gates
    /// =======================================================================
    /// Risk pinned (THE structural safety pin): file deletion must be IMPOSSIBLE when the
    /// commit fails — Java's `commit()` runs `cleanExpiredSnapshots()` strictly after the
    /// successful CAS. A failing catalog must propagate the error with ZERO delete-fn
    /// invocations and storage untouched.
    #[tokio::test]
    async fn test_failed_commit_performs_zero_deletions() {
        let catalog = new_memory_catalog().await;
        let table = make_table(&catalog).await;
        let table1 = append(&catalog, &table, vec![synthetic_data_file(
            "test/wtB2/failed-commit.parquet",
        )])
        .await;
        let s1 = table1.metadata().current_snapshot_id().expect("s1");
        let table2 = append(&catalog, &table1, vec![synthetic_data_file(
            "test/wtB2/failed-commit-2.parquet",
        )])
        .await;
        let s1_list = list_path(&table2, s1);

        // A catalog that loads the real table but refuses the commit, non-retryably.
        let mut failing_catalog = crate::catalog::MockCatalog::new();
        let loaded = table2.clone();
        failing_catalog.expect_load_table().returning_st(move |_| {
            let table = loaded.clone();
            Box::pin(async move { Ok(table) })
        });
        failing_catalog
            .expect_update_table()
            .times(1)
            .returning_st(|_| {
                Box::pin(async {
                    Err(Error::new(ErrorKind::Unexpected, "injected commit failure")
                        .with_retryable(false))
                })
            });

        let (recorded, delete_fn) = recording_delete_fn();
        let cleanup = ExpireSnapshotsCleanup::new(table2.file_io().clone()).delete_with(delete_fn);
        let tx = Transaction::new(&table2);
        let tx = tx
            .expire_snapshots()
            .expire_older_than(i64::MAX)
            .retain_last(1)
            .apply(tx)
            .expect("apply expire");

        let result = cleanup.commit_and_clean(tx, &failing_catalog).await;
        assert!(result.is_err(), "the failed commit must propagate");
        assert!(
            recorded.lock().expect("recorder lock").is_empty(),
            "NO deletion may run on a failed commit"
        );
        assert!(exists(&table2, &s1_list).await, "storage must be untouched");
    }

    /// Risk pinned: dry-run by injection — a recording delete fn that never touches storage
    /// computes the full would-be deletion set (report + recorder agree) while every file
    /// remains on storage.
    #[tokio::test]
    async fn test_dry_run_by_injection_leaves_storage_untouched() {
        let catalog = new_memory_catalog().await;
        let table = make_table(&catalog).await;
        let table1 = append(&catalog, &table, vec![synthetic_data_file(
            "test/wtB2/dry-run.parquet",
        )])
        .await;
        let s1 = table1.metadata().current_snapshot_id().expect("s1");
        let table2 = append(&catalog, &table1, vec![synthetic_data_file(
            "test/wtB2/dry-run-2.parquet",
        )])
        .await;
        let s1_list = list_path(&table2, s1);

        let (recorded, delete_fn) = recording_delete_fn();
        let cleanup = ExpireSnapshotsCleanup::new(table2.file_io().clone()).delete_with(delete_fn);
        let (_, report) = expire_and_clean(&catalog, &table2, &cleanup).await;

        assert_eq!(report.deleted_manifest_lists, vec![s1_list.clone()]);
        assert_eq!(
            *recorded.lock().expect("recorder lock"),
            vec![s1_list.clone()],
            "the recorder must see exactly the would-be deletion set"
        );
        assert!(
            exists(&table2, &s1_list).await,
            "dry-run must leave storage untouched"
        );
    }

    /// Risk pinned: a delete failure is COLLECTED and the sweep CONTINUES — never an abort
    /// leaving the rest of the sweep undone and unreported, never a silent swallow (the
    /// documented divergence from Java's log-and-continue).
    #[tokio::test]
    async fn test_injected_delete_failure_is_collected_and_sweep_continues() {
        let catalog = new_memory_catalog().await;
        let table = make_table(&catalog).await;
        let table1 = append(&catalog, &table, vec![synthetic_data_file(
            "test/wtB2/fail-collect-1.parquet",
        )])
        .await;
        let s1 = table1.metadata().current_snapshot_id().expect("s1");
        let table2 = append(&catalog, &table1, vec![synthetic_data_file(
            "test/wtB2/fail-collect-2.parquet",
        )])
        .await;
        let s2 = table2.metadata().current_snapshot_id().expect("s2");
        let table3 = append(&catalog, &table2, vec![synthetic_data_file(
            "test/wtB2/fail-collect-3.parquet",
        )])
        .await;
        let s1_list = list_path(&table3, s1);
        let s2_list = list_path(&table3, s2);

        // Fail exactly S1's list; delete everything else for real.
        let io = table3.file_io().clone();
        let fail_path = s1_list.clone();
        let cleanup = ExpireSnapshotsCleanup::new(table3.file_io().clone()).delete_with(
            move |path: String| -> BoxFuture<'static, Result<()>> {
                let io = io.clone();
                let fail_path = fail_path.clone();
                Box::pin(async move {
                    if path == fail_path {
                        Err(Error::new(ErrorKind::Unexpected, "injected delete failure"))
                    } else {
                        io.delete(&path).await
                    }
                })
            },
        );
        let (_, report) = expire_and_clean(&catalog, &table3, &cleanup).await;

        assert_eq!(
            report.deleted_manifest_lists,
            vec![s2_list.clone()],
            "the sweep must continue past the failure"
        );
        assert_eq!(report.failures.len(), 1, "failures: {:?}", report.failures);
        assert_eq!(report.failures[0].path, s1_list);
        assert_eq!(
            report.failures[0].kind,
            CleanupFailureKind::DeleteManifestList
        );
        assert!(exists(&table3, &s1_list).await);
        assert!(!exists(&table3, &s2_list).await);
    }

    /// Risk pinned: an UNREADABLE RETAINED manifest must clear the ENTIRE content-file deletion
    /// set (Java's catch-`Throwable` → empty set): when liveness cannot be proven, no content
    /// file may die — while manifests/lists (whose subtraction needed no manifest reads) are
    /// still swept and the failure is reported.
    #[tokio::test]
    async fn test_unreadable_retained_manifest_spares_all_content_files() {
        let catalog = new_memory_catalog().await;
        let table = make_table(&catalog).await;
        let table1 = append(&catalog, &table, vec![synthetic_data_file(
            "test/wtB2/spared.parquet",
        )])
        .await;
        let s1 = table1.metadata().current_snapshot_id().expect("s1");
        let m1 = manifests_of(&table1, s1).await[0].0.clone();
        let tx = Transaction::new(&table1);
        let tx = tx
            .rewrite_manifests()
            .cluster_by(|_file| "all".to_string())
            .apply(tx)
            .expect("apply rewrite manifests");
        let table2 = tx.commit(&catalog).await.expect("commit rewrite manifests");
        let s2 = table2.metadata().current_snapshot_id().expect("s2");
        let m2 = manifests_of(&table2, s2).await[0].0.clone();

        let (before, after) = expire_metadata_only(&catalog, &table2).await;
        // Corrupt the RETAINED manifest after the commit, before the cleanup.
        table2
            .file_io()
            .new_output(&m2)
            .expect("corrupt output")
            .write(Bytes::from_static(b"wtB2 corrupted avro"))
            .await
            .expect("corrupt retained manifest");

        let cleanup = ExpireSnapshotsCleanup::new(table2.file_io().clone());
        let report = cleanup
            .clean_expired_files(before.metadata(), after.metadata())
            .await
            .expect("clean expired files");

        assert!(
            report.deleted_content_files.is_empty(),
            "no content file may die when a retained manifest is unreadable: {:?}",
            report.deleted_content_files
        );
        assert_eq!(report.deleted_manifests, vec![m1]);
        assert_eq!(report.failures.len(), 1);
        assert_eq!(report.failures[0].path, m2);
        assert_eq!(
            report.failures[0].kind,
            CleanupFailureKind::ReadRetainedManifest
        );
    }

    /// Risk pinned: an unreadable CANDIDATE manifest skips only ITS content files
    /// (under-deletion), is itself still deleted (no retained snapshot references it), and the
    /// failure is reported — Java suppresses the read and proceeds.
    #[tokio::test]
    async fn test_unreadable_candidate_manifest_skips_its_files_but_still_dies() {
        let catalog = new_memory_catalog().await;
        let table = make_table(&catalog).await;
        let table1 = append(&catalog, &table, vec![synthetic_data_file(
            "test/wtB2/skipped.parquet",
        )])
        .await;
        let s1 = table1.metadata().current_snapshot_id().expect("s1");
        let m1 = manifests_of(&table1, s1).await[0].0.clone();
        let tx = Transaction::new(&table1);
        let tx = tx
            .rewrite_manifests()
            .cluster_by(|_file| "all".to_string())
            .apply(tx)
            .expect("apply rewrite manifests");
        let table2 = tx.commit(&catalog).await.expect("commit rewrite manifests");

        let (before, after) = expire_metadata_only(&catalog, &table2).await;
        table2
            .file_io()
            .new_output(&m1)
            .expect("corrupt output")
            .write(Bytes::from_static(b"wtB2 corrupted avro"))
            .await
            .expect("corrupt candidate manifest");

        let cleanup = ExpireSnapshotsCleanup::new(table2.file_io().clone());
        let report = cleanup
            .clean_expired_files(before.metadata(), after.metadata())
            .await
            .expect("clean expired files");

        assert!(report.deleted_content_files.is_empty());
        assert_eq!(
            report.deleted_manifests,
            vec![m1.clone()],
            "the unreadable expired-only manifest itself must still die"
        );
        assert_eq!(report.failures.len(), 1);
        assert_eq!(report.failures[0].path, m1);
        assert_eq!(
            report.failures[0].kind,
            CleanupFailureKind::ReadCandidateManifest
        );
    }

    /// Risk pinned: an unreadable manifest LIST aborts the cleanup with `Err` BEFORE any
    /// deletion — planning happens strictly before the sweep (Java `throwFailureWhenFinished`
    /// in the pre-delete walks).
    #[tokio::test]
    async fn test_unreadable_manifest_list_aborts_before_any_deletion() {
        let catalog = new_memory_catalog().await;
        let table = make_table(&catalog).await;
        let table1 = append(&catalog, &table, vec![synthetic_data_file(
            "test/wtB2/abort.parquet",
        )])
        .await;
        let s1 = table1.metadata().current_snapshot_id().expect("s1");
        let table2 = append(&catalog, &table1, vec![synthetic_data_file(
            "test/wtB2/abort-2.parquet",
        )])
        .await;
        let s1_list = list_path(&table2, s1);
        let m1 = manifests_of(&table2, s1).await[0].0.clone();

        let (before, after) = expire_metadata_only(&catalog, &table2).await;
        table2
            .file_io()
            .new_output(&s1_list)
            .expect("corrupt output")
            .write(Bytes::from_static(b"wtB2 corrupted avro"))
            .await
            .expect("corrupt manifest list");

        let (recorded, delete_fn) = recording_delete_fn();
        let cleanup = ExpireSnapshotsCleanup::new(table2.file_io().clone()).delete_with(delete_fn);
        let error = cleanup
            .clean_expired_files(before.metadata(), after.metadata())
            .await
            .expect_err("unreadable manifest list must abort");
        assert!(
            error.to_string().contains("cleanup planning"),
            "the planning context must name the abort: {error}"
        );
        assert!(
            recorded.lock().expect("recorder lock").is_empty(),
            "nothing may be deleted on a planning abort"
        );
        assert!(exists(&table2, &m1).await);
    }

    /// Risk pinned: an empty expiry (identical before/after) deletes nothing and reports
    /// nothing — scheduled maintenance must be able to run the cleanup unconditionally.
    #[tokio::test]
    async fn test_empty_expiry_is_a_noop() {
        let catalog = new_memory_catalog().await;
        let table = make_table(&catalog).await;
        let table1 = append(&catalog, &table, vec![synthetic_data_file(
            "test/wtB2/noop.parquet",
        )])
        .await;
        let s1 = table1.metadata().current_snapshot_id().expect("s1");
        let s1_list = list_path(&table1, s1);

        let cleanup = ExpireSnapshotsCleanup::new(table1.file_io().clone());
        let report = cleanup
            .clean_expired_files(table1.metadata(), table1.metadata())
            .await
            .expect("clean expired files");
        assert!(
            report.is_empty(),
            "no-op expiry must report nothing: {report:?}"
        );
        assert!(exists(&table1, &s1_list).await);

        // And through the wrapper: an expiry that expires nothing cleans nothing.
        let tx = Transaction::new(&table1);
        let tx = tx
            .expire_snapshots()
            .expire_older_than(0)
            .apply(tx)
            .expect("apply no-op expire");
        let (_, report) = cleanup
            .commit_and_clean(tx, &catalog)
            .await
            .expect("commit and clean no-op");
        assert!(report.is_empty());
        assert!(exists(&table1, &s1_list).await);
    }

    /// Risk pinned (structural — the crash-resume property): the sweep runs Java's funnel order,
    /// content files → manifests → manifest lists → statistics (`cleanFiles` bytecode: "data" →
    /// "manifest" → "manifest list" → "statistics files"). Leaves die before the structures that
    /// index them, so a crash mid-sweep always leaves the expired manifest LISTS readable until
    /// last and a re-run can still PLAN (and finish) the remainder; a lists-first sweep that
    /// crashed would orphan every manifest/content file beneath it beyond any re-run (only a
    /// future DeleteOrphanFiles could reclaim them). No per-funnel report assertion can see
    /// cross-funnel order, so this pins the recorder's raw invocation sequence.
    #[tokio::test]
    async fn test_sweep_order_content_manifests_lists_statistics() {
        let catalog = new_memory_catalog().await;
        let table = make_table(&catalog).await;
        let data_path = "test/wtB2/order.parquet";
        let table1 = append(&catalog, &table, vec![synthetic_data_file(data_path)]).await;
        let s1 = table1.metadata().current_snapshot_id().expect("s1");
        // Stats on S1 (die with it); the delete_files rewrite then makes S1's manifest and the
        // data file expired-only — all four funnels non-empty in one sweep.
        let stats_path = format!(
            "{}/metadata/wtB2-order-stats.puffin",
            table1.metadata().location()
        );
        table1
            .file_io()
            .new_output(&stats_path)
            .expect("stats output")
            .write(Bytes::from_static(b"wtB2 stats fixture"))
            .await
            .expect("write stats fixture");
        let tx = Transaction::new(&table1);
        let tx = tx
            .update_statistics()
            .set_statistics(StatisticsFile {
                snapshot_id: s1,
                statistics_path: stats_path.clone(),
                file_size_in_bytes: 18,
                file_footer_size_in_bytes: 4,
                key_metadata: None,
                blob_metadata: vec![],
            })
            .apply(tx)
            .expect("apply set statistics");
        let table1 = tx.commit(&catalog).await.expect("commit statistics");
        let tx = Transaction::new(&table1);
        let tx = tx
            .delete_files()
            .delete_file(data_path)
            .apply(tx)
            .expect("apply delete files");
        let table2 = tx.commit(&catalog).await.expect("commit delete files");

        let (recorded, delete_fn) = recording_delete_fn();
        let cleanup = ExpireSnapshotsCleanup::new(table2.file_io().clone()).delete_with(delete_fn);
        let (_, report) = expire_and_clean(&catalog, &table2, &cleanup).await;

        // Every funnel must be exercised, or the order pin is vacuous.
        assert_eq!(report.deleted_content_files, vec![data_path.to_string()]);
        assert!(!report.deleted_manifests.is_empty(), "fixture: no manifest");
        assert!(
            !report.deleted_manifest_lists.is_empty(),
            "fixture: no list"
        );
        assert_eq!(report.deleted_statistics_files, vec![stats_path]);
        assert!(report.failures.is_empty());
        let expected: Vec<String> = report
            .deleted_content_files
            .iter()
            .chain(&report.deleted_manifests)
            .chain(&report.deleted_manifest_lists)
            .chain(&report.deleted_statistics_files)
            .cloned()
            .collect();
        assert_eq!(
            *recorded.lock().expect("recorder lock"),
            expected,
            "the sweep must run content → manifests → manifest lists → statistics"
        );
    }

    /// Risk pinned (the re-run story): running the SAME (before, after) cleanup twice — a
    /// retried maintenance job, or a race with another process's cleanup of the same expiry —
    /// must never over-delete and never panic. After a complete first sweep the expired
    /// manifest lists are gone, so the second run aborts at PLANNING with a hard `Err` and
    /// ZERO delete calls (Java's `readManifests` throws identically) — the under-deletion
    /// direction. A sweep interrupted EARLIER re-plans from the still-intact lists instead
    /// (the order pin above keeps them alive until last).
    #[tokio::test]
    async fn test_rerun_after_complete_sweep_aborts_at_planning_with_zero_deletions() {
        let catalog = new_memory_catalog().await;
        let table = make_table(&catalog).await;
        let data_path = "test/wtB2/rerun.parquet";
        let table1 = append(&catalog, &table, vec![synthetic_data_file(data_path)]).await;
        let tx = Transaction::new(&table1);
        let tx = tx
            .delete_files()
            .delete_file(data_path)
            .apply(tx)
            .expect("apply delete files");
        let table2 = tx.commit(&catalog).await.expect("commit delete files");

        let cleanup = ExpireSnapshotsCleanup::new(table2.file_io().clone());
        let (before, after) = expire_metadata_only(&catalog, &table2).await;
        let first = cleanup
            .clean_expired_files(before.metadata(), after.metadata())
            .await
            .expect("first sweep");
        assert!(!first.deleted_manifest_lists.is_empty());
        assert!(first.failures.is_empty());

        let (recorded, delete_fn) = recording_delete_fn();
        let rerun = ExpireSnapshotsCleanup::new(table2.file_io().clone()).delete_with(delete_fn);
        let error = rerun
            .clean_expired_files(before.metadata(), after.metadata())
            .await
            .expect_err("re-run must abort at planning: the expired manifest lists are gone");
        assert!(
            error.to_string().contains("cleanup planning"),
            "the planning context must name the abort: {error}"
        );
        assert!(
            recorded.lock().expect("recorder lock").is_empty(),
            "the re-run may delete NOTHING"
        );
    }

    /// Risk pinned: the `gc.enabled` gate is re-honored at the CLEANUP door — the standalone
    /// entry point must refuse with Java's constructor message (the B1 gate runs at the
    /// action's commit; a direct cleanup call must not bypass it).
    #[tokio::test]
    async fn test_gc_disabled_cleanup_refused() {
        let catalog = new_memory_catalog().await;
        let table = make_table(&catalog).await;
        let table1 = append(&catalog, &table, vec![synthetic_data_file(
            "test/wtB2/gc-gate.parquet",
        )])
        .await;
        let disabled = table1
            .metadata()
            .clone()
            .into_builder(None)
            .set_properties(HashMap::from([(
                TableProperties::PROPERTY_GC_ENABLED.to_string(),
                "false".to_string(),
            )]))
            .expect("set gc.enabled")
            .build()
            .expect("build gc-disabled metadata")
            .metadata;

        let cleanup = ExpireSnapshotsCleanup::new(table1.file_io().clone());
        let error = cleanup
            .clean_expired_files(&disabled, &disabled)
            .await
            .expect_err("gc.enabled=false must refuse cleanup");
        assert_eq!(error.kind(), ErrorKind::DataInvalid);
        assert_eq!(
            error.message(),
            "Cannot expire snapshots: GC is disabled (deleting files may corrupt other tables)"
        );

        // The gate reads the PRE-expiry state (Java's gate lives in the `RemoveSnapshots`
        // constructor and consults `base`, never the post-state): before=disabled must refuse
        // even when after=enabled — a gate consulting only `after` would run a cleanup the
        // expiry's own commit-time gate refused.
        let error = cleanup
            .clean_expired_files(&disabled, table1.metadata())
            .await
            .expect_err("before=disabled / after=enabled must still refuse");
        assert_eq!(error.kind(), ErrorKind::DataInvalid);
        assert_eq!(
            error.message(),
            "Cannot expire snapshots: GC is disabled (deleting files may corrupt other tables)"
        );
    }

    /// Shared fixture root: a fresh V2 table in the catalog.
    async fn make_table(catalog: &impl Catalog) -> Table {
        crate::transaction::tests::make_v2_minimal_table_in_catalog(catalog).await
    }
}
