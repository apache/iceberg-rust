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
//! **Concurrent-commit conflicting-DELETE validation (`validateNoConflictingDeletes`, OPT-IN):** when
//! enabled via [`OverwriteFilesAction::validate_no_conflicting_deletes`], the commit is rejected on a
//! concurrent delete conflict, mirroring Java `BaseOverwriteFiles.validate`'s full `validateNewDeletes`
//! block (L167-178) — its TWO sub-branches both live under this one flag:
//! - **Branch A (Java L168-172, row-filter keyed):** when the [`OverwriteFilesAction::overwrite_by_row_filter`]
//!   row filter is set (`rowFilter() != alwaysFalse()`), the commit is rejected if a concurrent commit since
//!   the starting snapshot either ADDED a delete file that can apply to records matching the filter (Java
//!   `validateNoNewDeleteFiles` → shared [`validate_no_conflicting_added_delete_files`]) or DELETED a data
//!   file that can contain such records (Java `validateDeletedDataFiles` → shared
//!   [`validate_deleted_data_files`]). The filter is the explicit
//!   [`OverwriteFilesAction::conflict_detection_filter`] when set, else the row filter. This is the
//!   serializable guard that a concurrent merge-on-read delete does not silently invalidate an
//!   overwrite-by-row-filter.
//! - **Branch B (Java L174-177, removed-data-file keyed):** when this overwrite REMOVES data files supplied
//!   with full metadata via [`OverwriteFilesAction::delete_data_files`], the commit is rejected if a
//!   concurrent commit since the starting snapshot added a DELETE file (position / equality delete) that
//!   APPLIES to one of those removed data files — you cannot drop a data file out from under a concurrent
//!   row-level delete (Java's `!deletedDataFiles.isEmpty()` branch →
//!   `MergingSnapshotProducer.validateNoNewDeletesForDataFiles`). It delegates to the shared
//!   [`validate_no_new_deletes_for_data_files`] helper.
//!
//! INDEPENDENT of `validateNoConflictingData` — enabling one does not enable the other. The delete-file
//! sub-checks are a no-op on a V1 table (delete files do not exist before format version 2).
//!
//! **Delete-by-row-filter mode (`overwriteByRowFilter(Expression)`):** [`OverwriteFilesAction::overwrite_by_row_filter`]
//! stores a row predicate (Java `BaseOverwriteFiles.overwriteByRowFilter` → `deleteByRowFilter`). At apply
//! time the producer resolves every LIVE data file the predicate STRICTLY matches (all of its rows match)
//! via [`SnapshotProducer::resolve_filter_deletes`] — the Rust port of Java
//! `ManifestFilterManager.manifestHasDeletedFiles` + `PartitionAndMetricsEvaluator` (partition pre-filter +
//! strict/inclusive metrics evaluators). Those files feed the SAME [`process_deletes`] rewrite as the
//! explicit by-path deletes, so they drop in the one snapshot alongside any explicit add/delete. A file the
//! predicate matches only PARTIALLY (some-but-not-all rows) is a non-retryable error (Java's "Cannot delete
//! file where some, but not all, rows match filter"). An unpartitioned `alwaysTrue` row filter deletes every
//! file (full replace).
//!
//! **`validateAddedFilesMatchOverwriteFilter` (OPT-IN, Java `BaseOverwriteFiles.validate` block 1, L137-161):**
//! when enabled via [`OverwriteFilesAction::validate_added_files_match_overwrite_filter`], each ADDED data
//! file must have ALL of its rows inside the row filter: `inclusive_partition.eval(partition) &&
//! (strict_partition.eval(partition) || StrictMetricsEvaluator::eval(rowFilter, file))`. On failure → a
//! non-retryable `DataInvalid` ("Cannot append file with rows that do not match filter ..."). Only meaningful
//! WITH `overwrite_by_row_filter` (the filter is `alwaysFalse` when unset).
//!
//! This action now ports the full `BaseOverwriteFiles.validate`: the explicit add + delete core, the
//! delete-by-row-filter mode and its added-file validation, the filter-based `validateNoConflictingData`
//! (with the row-filter conflict-filter default), and the complete `validateNoConflictingDeletes` block —
//! BOTH the row-filter-keyed sub-branch (Java L168-172) and the explicitly-removed-data-file sub-branch
//! (Java L174-177).

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use uuid::Uuid;

use crate::error::Result;
use crate::expr::visitors::expression_evaluator::ExpressionEvaluator;
use crate::expr::visitors::inclusive_projection::InclusiveProjection;
use crate::expr::visitors::strict_metrics_evaluator::StrictMetricsEvaluator;
use crate::expr::visitors::strict_projection::StrictProjection;
use crate::expr::{Bind, BoundPredicate, Predicate};
use crate::spec::{DataFile, ManifestEntry, ManifestFile, Operation, Schema};
use crate::table::Table;
use crate::transaction::snapshot::{
    DefaultManifestProcess, SnapshotProduceOperation, SnapshotProducer,
    validate_deleted_data_files, validate_no_conflicting_added_data_files,
    validate_no_conflicting_added_delete_files, validate_no_new_deletes_for_data_files,
};
use crate::transaction::{ActionCommit, TransactionAction};
use crate::{Error, ErrorKind};

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
    /// The DATA files removed via [`OverwriteFilesAction::delete_data_files`] (the ones supplied with full
    /// [`DataFile`] metadata), mirroring Java `BaseOverwriteFiles.deletedDataFiles`. These — and ONLY these,
    /// not the path-only [`OverwriteFilesAction::delete_file`] entries — are validated by
    /// `validateNoConflictingDeletes`: that check needs each removed file's partition + sequence metadata to
    /// decide whether a concurrent delete applies, which a bare path does not carry. Their paths are ALSO in
    /// `delete_paths` (Java's `deleteFile(DataFile)` adds to both `deletedDataFiles` and the filter set), so
    /// the manifest-rewrite machinery removes them exactly as it removes path-only deletes.
    deleted_data_files: Vec<DataFile>,
    commit_uuid: Option<Uuid>,
    key_metadata: Option<Vec<u8>>,
    snapshot_properties: HashMap<String, String>,
    /// Whether concurrent-commit conflict validation is enabled (Java `validateNoConflictingData`). OFF by
    /// default = snapshot isolation (no validation, current behavior). When ON, the commit is rejected if a
    /// concurrent snapshot added a DATA file that could contain records matching the conflict filter.
    validate_no_conflicting_data: bool,
    /// Whether concurrent-commit conflicting-DELETE validation is enabled (Java
    /// `OverwriteFiles.validateNoConflictingDeletes`). OFF by default = snapshot isolation (no validation,
    /// current behavior). When ON — AND this overwrite REMOVES data files supplied with full metadata via
    /// [`OverwriteFilesAction::delete_data_files`] — the commit is rejected if a concurrent snapshot added a
    /// DELETE file (position / equality delete) that APPLIES to one of those removed data files (you cannot
    /// drop a data file out from under a concurrent row-level delete). INDEPENDENT of
    /// [`Self::validate_no_conflicting_data`] — enabling one does NOT enable the other (Java exposes them as
    /// two separate methods setting two separate flags).
    validate_no_conflicting_deletes: bool,
    /// The conflict-detection filter (Java `conflictDetectionFilter`). When `Some`, only concurrently-added
    /// files whose metrics COULD match this predicate are conflicts. When `None`, the filter defaults to
    /// `AlwaysTrue` (any concurrently-added DATA file is a conflict — the most conservative serializable
    /// check), mirroring Java `BaseOverwriteFiles.dataConflictDetectionFilter()` when no filter and no row
    /// filter are set.
    conflict_detection_filter: Option<Predicate>,
    /// An explicit starting snapshot for conflict validation (Java `validateFromSnapshot`). When `None`, the
    /// validation uses the transaction's starting snapshot (the table head when the transaction was created).
    validate_from_snapshot: Option<i64>,
    /// The row predicate of the delete-by-row-filter mode (Java `BaseOverwriteFiles.overwriteByRowFilter` →
    /// `deleteByRowFilter`, stored as `deleteExpression`). When `Some`, every LIVE data file the predicate
    /// STRICTLY matches is removed at apply time via [`SnapshotProducer::resolve_filter_deletes`]. When
    /// `None`, the row filter is Java `alwaysFalse()` (no by-filter delete) — used as such in the
    /// conflict-filter default ([`dataConflictDetectionFilter`](Self::validate)) and the added-file
    /// validation.
    row_filter: Option<Predicate>,
    /// Whether to assert that every ADDED data file lies entirely inside the `row_filter` (Java
    /// `OverwriteFiles.validateAddedFilesMatchOverwriteFilter`). OFF by default. Only meaningful together with
    /// [`Self::row_filter`].
    validate_added_files_match_overwrite_filter: bool,
}

impl OverwriteFilesAction {
    pub(crate) fn new() -> Self {
        Self {
            added_data_files: vec![],
            delete_paths: HashSet::default(),
            deleted_data_files: vec![],
            commit_uuid: None,
            key_metadata: None,
            snapshot_properties: HashMap::default(),
            validate_no_conflicting_data: false,
            validate_no_conflicting_deletes: false,
            conflict_detection_filter: None,
            validate_from_snapshot: None,
            row_filter: None,
            validate_added_files_match_overwrite_filter: false,
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

    /// Delete multiple files supplied as full [`DataFile`]s (Java `OverwriteFiles.deleteFile(DataFile)`).
    ///
    /// Each file's path is added to the delete set that drives the manifest rewrite (exactly like
    /// [`Self::delete_file`]), AND the full [`DataFile`] is retained in [`Self::deleted_data_files`] so
    /// `validateNoConflictingDeletes` (when enabled via [`Self::validate_no_conflicting_deletes`]) can test
    /// it against concurrently-added deletes — a check that needs the file's partition + metrics, which a
    /// bare path does not carry. Mirrors Java's `deleteFile(DataFile)` populating both `deletedDataFiles` and
    /// the filter set.
    pub fn delete_data_files(mut self, files: impl IntoIterator<Item = DataFile>) -> Self {
        for file in files {
            self.delete_paths.insert(file.file_path().to_string());
            self.deleted_data_files.push(file);
        }
        self
    }

    /// DELETE every current data file the `predicate` STRICTLY matches (Java
    /// `OverwriteFiles.overwriteByRowFilter(Expression)` → `deleteByRowFilter`). At apply time the producer
    /// resolves the live data files whose ALL rows match the predicate (via
    /// [`SnapshotProducer::resolve_filter_deletes`]) and removes them in the SAME `Overwrite` snapshot as any
    /// explicit [`Self::add_file`] / [`Self::delete_file`]. A file the predicate matches only PARTIALLY
    /// (some-but-not-all rows) makes the commit fail with a non-retryable error. An unpartitioned
    /// `Predicate::AlwaysTrue` filter deletes EVERY data file (full replace).
    ///
    /// This is independent of the path-based [`Self::delete_file`] / [`Self::delete_data_files`] removals —
    /// all of them are resolved and dropped in the one snapshot. The recorded operation stays `Overwrite`
    /// (the row filter requests a delete, so an add+row-filter overwrite is a true overwrite).
    pub fn overwrite_by_row_filter(mut self, predicate: Predicate) -> Self {
        self.row_filter = Some(predicate);
        self
    }

    /// ENABLE the assertion that every ADDED data file lies entirely inside the [`Self::overwrite_by_row_filter`]
    /// predicate (Java `OverwriteFiles.validateAddedFilesMatchOverwriteFilter`): at validate time each added
    /// data file must have ALL of its rows match the row filter, or the commit is rejected with a non-retryable
    /// `ValidationException` ("Cannot append file with rows that do not match filter"). This guards a
    /// replace-by-predicate from re-introducing rows outside the predicate it just deleted.
    ///
    /// Only meaningful together with [`Self::overwrite_by_row_filter`] — with no row filter the predicate is
    /// `alwaysFalse` and no added file could match it. Default (this method NOT called) = no assertion.
    pub fn validate_added_files_match_overwrite_filter(mut self) -> Self {
        self.validate_added_files_match_overwrite_filter = true;
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

    /// ENABLE concurrent-commit conflicting-DELETE validation (Java
    /// `OverwriteFiles.validateNoConflictingDeletes`): the commit is rejected with a non-retryable
    /// `ValidationException` if a concurrent snapshot since the starting snapshot added a DELETE file
    /// (position / equality delete) that APPLIES to one of the DATA files this overwrite REMOVES (the data
    /// files supplied with full metadata via [`Self::delete_data_files`]). Under serializable isolation you
    /// cannot drop a data file out from under a concurrent row-level delete.
    ///
    /// This is INDEPENDENT of [`Self::validate_no_conflicting_data`] — enabling one does NOT enable the other
    /// (Java's two `validateNoConflicting*` methods set two separate flags). Only the
    /// [`Self::delete_data_files`] entries are validated; path-only [`Self::delete_file`] /
    /// [`Self::delete_files`] removals are not (a bare path lacks the partition + metrics the check needs),
    /// mirroring Java which validates only the `deletedDataFiles` `DataFile` objects. A no-op on a V1 table
    /// (delete files do not exist before format version 2).
    ///
    /// Default (this method NOT called) = snapshot isolation = no validation (current behavior unchanged).
    pub fn validate_no_conflicting_deletes(mut self) -> Self {
        self.validate_no_conflicting_deletes = true;
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

    /// The row filter the validations operate on (Java `MergingSnapshotProducer.rowFilter()` =
    /// `deleteExpression`): the [`Self::overwrite_by_row_filter`] predicate, or `AlwaysFalse` (Java
    /// `Expressions.alwaysFalse()`) when unset.
    fn row_filter(&self) -> Predicate {
        self.row_filter.clone().unwrap_or(Predicate::AlwaysFalse)
    }

    /// The conflict-detection filter for `validateNoConflictingData`, mirroring Java
    /// `BaseOverwriteFiles.dataConflictDetectionFilter()` (L181-188):
    /// - an explicit [`Self::conflict_detection_filter`] when set; ELSE
    /// - the [`Self::overwrite_by_row_filter`] row filter when it is set (`rowFilter != alwaysFalse`) AND there
    ///   are no explicitly-removed data files ([`Self::deleted_data_files`] empty); ELSE
    /// - `None` (the shared helper binds `None` as `AlwaysTrue` — Java `Expressions.alwaysTrue()`).
    ///
    /// Returning `None` for the `alwaysTrue` case keeps the existing explicit-filter + None-default behavior
    /// for the non-row-filter path byte-for-byte (the helper treats `None` as `AlwaysTrue`), while a
    /// row-filter overwrite with no explicit deletes now uses the row filter as the conflict filter.
    fn data_conflict_detection_filter(&self) -> Option<&Predicate> {
        if self.conflict_detection_filter.is_some() {
            return self.conflict_detection_filter.as_ref();
        }
        match &self.row_filter {
            Some(row_filter) if self.deleted_data_files.is_empty() => Some(row_filter),
            // `None` row filter (alwaysFalse) OR explicit deletes present ⇒ Java `alwaysTrue()`; the shared
            // helper binds `None` as `AlwaysTrue`, so return `None` to preserve the existing default exactly.
            _ => None,
        }
    }

    /// Assert every ADDED data file lies entirely inside the row filter (Java
    /// `BaseOverwriteFiles.validate` L137-161). For each added file:
    /// `inclusive_partition.eval(file.partition()) && (strict_partition.eval(file.partition()) ||
    /// StrictMetricsEvaluator::eval(rowFilter, file))`, where `inclusive_partition` / `strict_partition` are
    /// the [`InclusiveProjection`] / [`StrictProjection`] of the row filter evaluated on the partition (Java
    /// `Projections.inclusive/strict(spec).project(rowFilter)` + an `Evaluator`). On failure → non-retryable
    /// `DataInvalid` (Java L154-159).
    fn check_added_files_match_overwrite_filter(&self, current: &Table) -> Result<()> {
        // With no added files there is nothing to assert; with an `alwaysFalse` row filter (none set) the
        // assertion is vacuous-but-strict — every non-empty added file would fail. Java only reaches this
        // block when the flag is on, which is only meaningful with a row filter; we still evaluate faithfully.
        if self.added_data_files.is_empty() {
            return Ok(());
        }

        let row_filter = self.row_filter();
        let schema = current.metadata().current_schema().clone();
        // Strict METRICS evaluator runs on the full row filter against the table schema (Java
        // `new StrictMetricsEvaluator(base.schema(), rowFilter, isCaseSensitive())`). `rewrite_not` so the
        // visitor never sees a `Not`.
        let bound_row_filter: BoundPredicate = row_filter
            .clone()
            .rewrite_not()
            .bind(schema.clone(), true)?;

        // The added files all share the table default partition spec (validated in `commit` via
        // `validate_added_data_files`); build the inclusive/strict PARTITION evaluators for that spec.
        let spec_id = current.metadata().default_partition_spec_id();
        let inclusive_partition =
            self.build_partition_evaluator(current, &bound_row_filter, spec_id, true)?;
        let strict_partition =
            self.build_partition_evaluator(current, &bound_row_filter, spec_id, false)?;

        for file in &self.added_data_files {
            // The real test: strict-partition OR strict-metrics proves ALL rows match; inclusive-partition
            // avoids testing metrics for a file the partition already excludes (Java L151-156).
            let all_rows_match = inclusive_partition.eval(file)?
                && (strict_partition.eval(file)?
                    || StrictMetricsEvaluator::eval(&bound_row_filter, file)?);
            if !all_rows_match {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Cannot append file with rows that do not match filter {row_filter}: {}",
                        file.file_path()
                    ),
                ));
            }
        }

        Ok(())
    }

    /// Build the partition [`ExpressionEvaluator`] for `spec_id` from the row filter: when `inclusive`,
    /// project via [`InclusiveProjection`] (Java `Projections.inclusive(spec).project(rowFilter)`), else via
    /// [`StrictProjection`] (Java `Projections.strict(spec)`), then bind the projected predicate to the
    /// partition schema. The result evaluates a [`DataFile`]'s partition tuple (Java
    /// `new Evaluator(spec.partitionType(), expr).eval(file.partition())`).
    fn build_partition_evaluator(
        &self,
        current: &Table,
        bound_row_filter: &BoundPredicate,
        spec_id: i32,
        inclusive: bool,
    ) -> Result<ExpressionEvaluator> {
        let schema = current.metadata().current_schema();
        let partition_spec = current
            .metadata()
            .partition_spec_by_id(spec_id)
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!("Cannot validate added files: unknown partition spec id {spec_id}"),
                )
            })?;

        let partition_type = partition_spec.partition_type(schema)?;
        let partition_schema = Arc::new(
            Schema::builder()
                .with_schema_id(partition_spec.spec_id())
                .with_fields(partition_type.fields().to_owned())
                .build()?,
        );

        let projected = if inclusive {
            InclusiveProjection::new(partition_spec.clone()).project(bound_row_filter)?
        } else {
            StrictProjection::new(partition_spec.clone()).strict_project(bound_row_filter)?
        };

        let partition_filter = projected.rewrite_not().bind(partition_schema, true)?;
        Ok(ExpressionEvaluator::new(partition_filter))
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
                    // The delete-by-row-filter predicate (Java `deleteExpression`). When `Some`, the producer
                    // also resolves the live data files this predicate strictly matches.
                    row_filter: self.row_filter.clone(),
                    // The recorded operation is classified on the REQUESTED sets (Java `addsDataFiles()` =
                    // requested added files non-empty), evaluated before the deletes are resolved.
                    adds_data_files: !self.added_data_files.is_empty(),
                },
                DefaultManifestProcess,
            )
            .await
    }

    /// Serializable-isolation conflict validation (Java `BaseOverwriteFiles.validate`). Runs two INDEPENDENT
    /// opt-in checks against the refreshed base; with neither enabled it is a no-op (snapshot isolation,
    /// current behavior unchanged). Both share the same effective starting snapshot
    /// ([`Self::validate_from_snapshot`] if set, else the transaction-provided `starting_snapshot_id`) and a
    /// failure of EITHER rejects the commit with a NON-retryable `DataInvalid` (Java's non-retryable
    /// `ValidationException`), so the commit retry loop stops and the error propagates.
    ///
    /// 1. **`validateAddedFilesMatchOverwriteFilter`** (Java L137-161, when
    ///    [`Self::validate_added_files_match_overwrite_filter`] is enabled): assert every ADDED data file lies
    ///    entirely inside the [`Self::overwrite_by_row_filter`] predicate (`rowFilter`), via
    ///    [`Self::check_added_files_match_overwrite_filter`] — `inclusive_partition.eval(file.partition()) &&
    ///    (strict_partition.eval(file.partition()) || StrictMetricsEvaluator::eval(rowFilter, file))`. On
    ///    failure → non-retryable `DataInvalid` ("Cannot append file with rows that do not match filter ...").
    /// 2. **`validateNoConflictingData`** (Java L163-165 → `validateNewDataFiles` →
    ///    `MergingSnapshotProducer.validateAddedDataFiles`, when [`Self::validate_no_conflicting_data`] is
    ///    enabled): enumerate every DATA file ADDED to the refreshed base by snapshots committed since the
    ///    start (Java `addedDataFiles`) and reject if ANY COULD contain records matching the conflict-detection
    ///    filter (the existing `InclusiveMetricsEvaluator`). The filter is the
    ///    [`Self::data_conflict_detection_filter`] derived per Java `dataConflictDetectionFilter()` (L181-188):
    ///    the explicit [`Self::conflict_detection_filter`] when set; else the [`Self::overwrite_by_row_filter`]
    ///    `rowFilter` when it is set (`!= alwaysFalse`) AND there are no explicitly-removed data files; else
    ///    `AlwaysTrue`. Delegates to the SHARED [`validate_no_conflicting_added_data_files`] helper (also used
    ///    by `RowDelta`).
    /// 3. **`validateNoConflictingDeletes`** (Java `validateNewDeletes`, L167-178, when
    ///    [`Self::validate_no_conflicting_deletes`] is enabled) — the two delete-conflict sub-branches, both
    ///    under one guard, mirroring Java's structure:
    ///    - **Branch A** (Java L168-172, when the [`Self::overwrite_by_row_filter`] row filter is set, i.e.
    ///      `rowFilter() != alwaysFalse()`): reject if a concurrent commit since the start either ADDED a
    ///      delete file that can apply to records matching the filter (`validateNoNewDeleteFiles` → shared
    ///      [`validate_no_conflicting_added_delete_files`]) or DELETED a data file that can contain such
    ///      records (`validateDeletedDataFiles` → shared [`validate_deleted_data_files`]). The filter is the
    ///      explicit [`Self::conflict_detection_filter`] when set, else the row filter (Java's local
    ///      `filter`). This is the merge-on-read serializable guard for an overwrite-by-row-filter against a
    ///      concurrent delete it would otherwise silently invalidate.
    ///    - **Branch B** (Java L174-177 → `validateNoNewDeletesForDataFiles`, when this overwrite removes data
    ///      files supplied with full metadata via [`Self::delete_data_files`]): enumerate the DELETE files
    ///      added by concurrent commits since the start and reject if ANY APPLIES to one of those removed data
    ///      files (you cannot drop a data file out from under a concurrent row-level delete). Delegates to
    ///      [`validate_no_new_deletes_for_data_files`] with `ignore_equality_deletes = false` (Java passes the
    ///      full `deletedDataFiles` and does not ignore equality deletes here). Only the
    ///      [`Self::delete_data_files`] entries (Java's `deletedDataFiles`) are checked — path-only
    ///      [`Self::delete_file`] / [`Self::delete_files`] removals are not. A no-op on a V1 table.
    ///
    /// **Case sensitivity:** Java binds the conflict filter with `isCaseSensitive()`. This action has no such
    /// field, so the filter is bound case-sensitive (`true`) — the Iceberg/Java default for column resolution.
    async fn validate(
        self: Arc<Self>,
        starting_snapshot_id: Option<i64>,
        current: &Table,
    ) -> Result<()> {
        // Java `BaseOverwriteFiles` uses `startingSnapshotId` (the `validateFromSnapshot` override) when set,
        // else the operation's starting snapshot. CRITICAL: `starting_snapshot_id` is the TRANSACTION-captured
        // base (the head when `Transaction::new` ran), NOT the refreshed head — re-reading the refreshed head
        // would make the concurrent set empty and silently pass. Both checks share this start.
        let effective_start = self.validate_from_snapshot.or(starting_snapshot_id);

        // 1. validateAddedFilesMatchOverwriteFilter (Java L137-161): every ADDED data file must lie entirely
        //    inside the row filter. Runs FIRST, matching Java's block order.
        if self.validate_added_files_match_overwrite_filter {
            self.check_added_files_match_overwrite_filter(current)?;
        }

        // 2. Concurrent-added DATA-file conflict (Java `validateNewDataFiles` branch). The walk + bind +
        //    per-file inclusive-metrics evaluation + non-retryable-conflict error are the shared helper. The
        //    conflict filter is derived per Java `dataConflictDetectionFilter()` (the row-filter default).
        if self.validate_no_conflicting_data {
            let conflict_filter = self.data_conflict_detection_filter();
            validate_no_conflicting_added_data_files(
                current,
                effective_start,
                conflict_filter,
                true,
            )
            .await?;
        }

        // 3. validateNoConflictingDeletes (Java `validateNewDeletes`, L167-178) — the two delete-conflict
        //    sub-branches under one guard, mirroring Java's structure exactly.
        if self.validate_no_conflicting_deletes {
            // 3a. Branch A (Java L168-172): when the delete-by-row-filter is set (`rowFilter() !=
            //     alwaysFalse()`), reject if a concurrent commit either ADDED a delete file that can apply to
            //     records matching the filter (`validateNoNewDeleteFiles`) or DELETED a data file that can
            //     contain such records (`validateDeletedDataFiles`). Java's local `filter` is the explicit
            //     `conflictDetectionFilter` when set, else the row filter. These are the merge-on-read
            //     conflicting-delete checks keyed on the row filter — the serializable-isolation guard for an
            //     overwrite-by-row-filter against a concurrent delete the overwrite would otherwise silently
            //     invalidate.
            let row_filter = self.row_filter();
            if row_filter != Predicate::AlwaysFalse {
                let filter = self.conflict_detection_filter.clone().unwrap_or(row_filter);
                validate_no_conflicting_added_delete_files(
                    current,
                    effective_start,
                    Some(&filter),
                    true,
                )
                .await?;
                validate_deleted_data_files(current, effective_start, Some(&filter), true).await?;
            }

            // 3b. Branch B (Java L174-177): when this overwrite removes data files supplied with full metadata
            //     via [`Self::delete_data_files`], reject if a concurrent commit ADDED a DELETE file that
            //     APPLIES to one of those removed data files (you cannot drop a data file out from under a
            //     concurrent row-level delete). Only the `delete_data_files` (full-metadata) removals are
            //     validated — matching Java's `deletedDataFiles`. `ignore_equality_deletes = false` (Java
            //     passes the full set with equality deletes counted). UNCHANGED from the prior increment.
            if !self.deleted_data_files.is_empty() {
                validate_no_new_deletes_for_data_files(
                    current,
                    effective_start,
                    self.conflict_detection_filter.as_ref(),
                    &self.deleted_data_files,
                    false,
                )
                .await?;
            }
        }

        Ok(())
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
    /// The delete-by-row-filter predicate (Java `deleteExpression`). When `Some`, `delete_files` also resolves
    /// every live data file this predicate STRICTLY matches (via `resolve_filter_deletes`), unioned with the
    /// path-resolved deletes. `None` ⇒ Java `alwaysFalse` (no by-filter delete).
    row_filter: Option<Predicate>,
    /// Whether this overwrite requested any added data files. Combined with whether any delete was requested
    /// (a non-empty `delete_paths` OR a set `row_filter`, mirroring Java `containsDeletes()` =
    /// `deleteExpression != alwaysFalse`), this classifies the recorded operation the way Java
    /// `BaseOverwriteFiles.operation()` does.
    adds_data_files: bool,
}

impl SnapshotProduceOperation for OverwriteFilesOperation {
    /// Classify the recorded operation exactly as Java `BaseOverwriteFiles.operation()` does, on the
    /// REQUESTED add/delete sets: delete-only → [`Operation::Delete`], add-only → [`Operation::Append`],
    /// both → [`Operation::Overwrite`]. An empty overwrite (neither) is rejected before this is read, so
    /// the both-empty case here would fall through to `Overwrite` and never commit.
    fn operation(&self) -> Operation {
        // Java `containsDeletes()` (L198-203): a delete is requested if any path was requested OR the
        // delete-by-row-filter predicate is set (`deleteExpression != alwaysFalse`). A set row filter
        // therefore counts as "deletes data files" even before the matched files are resolved — matching
        // Java's REQUESTED-set classification.
        let deletes_data_files = !self.delete_paths.is_empty() || self.row_filter.is_some();
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
        let mut resolved = snapshot_produce
            .resolve_delete_paths(&self.delete_paths)
            .await?;

        // Union the delete-by-row-filter matches (Java `deleteByRowFilter` — every live data file the
        // predicate strictly matches; a partial match is a non-retryable error inside `resolve_filter_deletes`).
        // De-dupe by path so a file removed by BOTH an explicit path and the row filter is not deleted twice
        // (the producer's `process_deletes` matches by path, so a duplicate would be harmless, but the summary
        // counts must stay accurate — Java's `DataFileSet` likewise dedupes).
        if let Some(row_filter) = &self.row_filter {
            let filter_deletes = snapshot_produce.resolve_filter_deletes(row_filter).await?;
            let mut seen: HashSet<String> = resolved
                .iter()
                .map(|df| df.file_path().to_string())
                .collect();
            for data_file in filter_deletes {
                if seen.insert(data_file.file_path().to_string()) {
                    resolved.push(data_file);
                }
            }
        }

        Ok(resolved)
    }

    async fn existing_manifest(
        &self,
        snapshot_produce: &SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestFile>> {
        // Expose EVERY current manifest — DATA and DELETE — via the shared
        // [`SnapshotProducer::current_manifests`]. The producer's `process_deletes` decides per DATA manifest
        // whether to rewrite (to drop deleted files), carry forward unchanged, or drop it; every DELETE
        // manifest carries forward UNCHANGED (its entries are delete-file paths, never in the data-file
        // `delete_paths`), so an overwrite on a merge-on-read table preserves all outstanding position /
        // equality deletes instead of silently dropping them and resurrecting deleted rows. The conservative
        // dangling-delete posture (no pruning) is documented on the helper.
        snapshot_produce.current_manifests().await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::sync::Arc;

    use arrow_array::{ArrayRef, Int64Array, RecordBatch};
    use futures::TryStreamExt;

    use crate::expr::{Predicate, Reference};
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

    /// Like [`data_file_with_y_bounds`] but with COMPLETE `y` (field id 2) stats: lower/upper bounds plus
    /// `value_counts`, `null_value_counts = 0`, `nan_value_counts = 0`. The zero null/nan counts let the
    /// [`StrictMetricsEvaluator`] fully classify the file against a `y` predicate (the strict evaluator
    /// conservatively returns "might not match" when a column might contain a null/nan — Java
    /// `StrictMetricsEvaluator`). This is the realistic shape a real Parquet writer produces, and it is what
    /// the delete-by-row-filter path needs to decide DELETE vs KEEP without a spurious partial-match error.
    fn data_file_with_y_stats(path: &str, part_value: i64, y_lower: i64, y_upper: i64) -> DataFile {
        DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(path.to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(0)
            .partition(Struct::from_iter([Some(Literal::long(part_value))]))
            .value_counts(HashMap::from([(2, 1)]))
            .null_value_counts(HashMap::from([(2, 0)]))
            .nan_value_counts(HashMap::from([(2, 0)]))
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

    // ============================================================================================
    // Conflicting-DELETE validation (Java `validateNoConflictingDeletes` — serializable isolation).
    // Java `BaseOverwriteFiles.validate` → the `!deletedDataFiles.isEmpty()` branch →
    // `MergingSnapshotProducer.validateNoNewDeletesForDataFiles` (L519-551): for the DATA files this
    // overwrite REMOVES (the `delete_data_files` full-metadata set), reject the commit if a concurrent
    // commit since the start added a DELETE file that APPLIES to one of them — you cannot drop a data file
    // out from under a concurrent row-level delete.
    //
    // The race: an `overwrite_files` is BUILT against head S0 deleting data file A (via `delete_data_files`,
    // so A is in the validated `deletedDataFiles` set). BEFORE it commits, a concurrent `row_delta` lands a
    // POSITION delete (seq > start) in A's partition. With `validate_no_conflicting_deletes()` enabled the
    // overwrite must FAIL (non-retryable). A delete in a DIFFERENT partition, a delete at seq <= start, the
    // validation OFF, or A removed only by PATH (not `delete_data_files`) must all COMMIT.
    // ============================================================================================

    /// A synthetic POSITION-delete file routed to partition `x = part_value` (spec id 0), with a unique
    /// path — manifest-only (not a real parquet file).
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

    /// A synthetic EQUALITY-delete file routed to partition `x = part_value` (spec id 0), equality on
    /// field id 1 (`x`), with a unique path — manifest-only.
    fn equality_delete_file(path: &str, part_value: i64) -> DataFile {
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

    /// Concurrently add the given DELETE files in a single `row_delta` commit (records `Operation::Delete`,
    /// which `validateNoConflictingDeletes` inspects) and return the updated table.
    async fn add_deletes(catalog: &impl Catalog, table: &Table, deletes: Vec<DataFile>) -> Table {
        let tx = Transaction::new(table);
        let action = tx.row_delta().add_deletes(deletes);
        let tx = action.apply(tx).unwrap();
        tx.commit(catalog).await.unwrap()
    }

    /// THE HEADLINE TEST. Append A (delete-data-files target). Build an `overwrite_files` that REMOVES A via
    /// `delete_data_files` (full metadata) with `.validate_no_conflicting_deletes()`. Then a CONCURRENT
    /// `row_delta` lands a POSITION delete in A's partition (x=0, seq > start). The overwrite commit must FAIL
    /// with a NON-retryable `DataInvalid` naming A (Java "Cannot commit, found new delete for replaced data
    /// file: <path>").
    ///
    /// Risk pinned: dropping A out from under a concurrent row-level delete = lost delete under serializable
    /// isolation. Without the check the overwrite would commit and silently discard the concurrent delete.
    #[tokio::test]
    async fn test_overwrite_rejects_concurrent_delete_for_removed_data_file() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let a = data_file("test/a.parquet", 0);
        let (table, s0) = append_and_snapshot_id(&catalog, &table, vec![a.clone()]).await;

        // Build the overwrite removing A via delete_data_files (full metadata ⇒ validated), validation on,
        // pinned to S0.
        let tx = Transaction::new(&table);
        let action = tx
            .overwrite_files()
            .delete_data_files(vec![a])
            .add_file(data_file("test/b.parquet", 0))
            .validate_from_snapshot(s0)
            .validate_no_conflicting_deletes();
        let tx = action.apply(tx).unwrap();

        // CONCURRENT commit (S1): a position delete in A's partition (x=0), seq > start.
        let _concurrent = add_deletes(&catalog, &table, vec![position_delete_file(
            "test/pos-del.parquet",
            0,
        )])
        .await;

        let err = tx.commit(&catalog).await.expect_err(
            "overwrite must fail: a concurrent delete applies to the removed data file A",
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
            "the error must name the replaced data file, got: {}",
            err.message()
        );

        // The catalog head is still S1 (the concurrent delete) — the overwrite did NOT commit over it.
        let reloaded = catalog.load_table(table.identifier()).await.unwrap();
        let live = live_file_paths(&reloaded).await;
        assert!(
            !live.contains("test/b.parquet"),
            "the rejected overwrite's added file must NOT be in the table"
        );
    }

    /// NO-FALSE-CONFLICT (different partition). Same setup, but the concurrent position delete is in a
    /// DIFFERENT partition (x=1) than the removed file A (x=0) — it does NOT apply to A. The overwrite must
    /// COMMIT. Risk pinned: a partition-blind check that rejects ANY concurrent delete (a false positive).
    #[tokio::test]
    async fn test_overwrite_allows_concurrent_delete_in_other_partition() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let a = data_file("test/a.parquet", 0);
        let (table, s0) = append_and_snapshot_id(&catalog, &table, vec![a.clone()]).await;

        let tx = Transaction::new(&table);
        let action = tx
            .overwrite_files()
            .delete_data_files(vec![a])
            .add_file(data_file("test/b.parquet", 0))
            .validate_from_snapshot(s0)
            .validate_no_conflicting_deletes();
        let tx = action.apply(tx).unwrap();

        // CONCURRENT commit (S1): a position delete in a DIFFERENT partition (x=1) — cannot apply to A (x=0).
        let _concurrent = add_deletes(&catalog, &table, vec![position_delete_file(
            "test/pos-del-other.parquet",
            1,
        )])
        .await;

        let table = tx
            .commit(&catalog)
            .await
            .expect("overwrite must commit: the concurrent delete is in a different partition");
        let live = live_file_paths(&table).await;
        assert!(
            live.contains("test/b.parquet"),
            "the overwrite's added file must be in the table (commit succeeded)"
        );
        assert!(
            !live.contains("test/a.parquet"),
            "A was removed by the overwrite"
        );
    }

    /// NO-CONFLICT (delete at/before the start). The concurrent delete lands at seq <= the starting sequence
    /// number, so it is part of the base, not a concurrent commit. Here we set `validate_from_snapshot` to the
    /// CURRENT head (after the delete already landed) so the delete is NOT in the concurrent window — the
    /// overwrite must COMMIT. Risk pinned: a check that ignores the sequence-number / starting-snapshot
    /// boundary and flags a pre-start delete.
    #[tokio::test]
    async fn test_overwrite_allows_delete_at_or_before_start() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let a = data_file("test/a.parquet", 0);
        let table = append_files(&catalog, &table, vec![a.clone()]).await;

        // A delete lands in A's partition BEFORE the transaction's validation window (it becomes the head S1).
        let table = add_deletes(&catalog, &table, vec![position_delete_file(
            "test/pos-del.parquet",
            0,
        )])
        .await;
        let s1 = table.metadata().current_snapshot().unwrap().snapshot_id();

        // Build the overwrite pinned to S1 (the current head) — the pre-existing delete is NOT concurrent.
        let tx = Transaction::new(&table);
        let action = tx
            .overwrite_files()
            .delete_data_files(vec![a])
            .add_file(data_file("test/b.parquet", 0))
            .validate_from_snapshot(s1)
            .validate_no_conflicting_deletes();
        let tx = action.apply(tx).unwrap();
        let table = tx
            .commit(&catalog)
            .await
            .expect("with start = current head, the pre-existing delete is not concurrent ⇒ commit succeeds");
        assert!(live_file_paths(&table).await.contains("test/b.parquet"));
    }

    /// IGNORE-EQUALITY semantics (OverwriteFiles passes `ignore_equality_deletes = false`). An EQUALITY delete
    /// in A's partition added concurrently IS a conflict for the removed file A (Java's else-branch counts ANY
    /// applicable delete). Pins that equality deletes are NOT silently ignored by the overwrite check.
    #[tokio::test]
    async fn test_overwrite_rejects_concurrent_equality_delete_for_removed_data_file() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let a = data_file("test/a.parquet", 0);
        let (table, s0) = append_and_snapshot_id(&catalog, &table, vec![a.clone()]).await;

        let tx = Transaction::new(&table);
        let action = tx
            .overwrite_files()
            .delete_data_files(vec![a])
            .validate_from_snapshot(s0)
            .validate_no_conflicting_deletes();
        let tx = action.apply(tx).unwrap();

        // CONCURRENT commit (S1): an EQUALITY delete in A's partition (x=0).
        let _concurrent = add_deletes(&catalog, &table, vec![equality_delete_file(
            "test/eq-del.parquet",
            0,
        )])
        .await;

        let err = tx.commit(&catalog).await.expect_err(
            "overwrite must fail: an equality delete applies to the removed data file A",
        );
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(!err.retryable());
        assert!(
            err.message()
                .contains("found new delete for replaced data file"),
            "got: {}",
            err.message()
        );
    }

    /// FLAG-OFF CONTROL. With `validate_no_conflicting_deletes()` NOT called, a concurrent delete applying to
    /// the removed file does NOT fail the commit — snapshot isolation, the DEFAULT, unchanged. Risk pinned:
    /// the check must be OPT-IN.
    #[tokio::test]
    async fn test_overwrite_without_deletes_validation_allows_conflicting_delete() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let a = data_file("test/a.parquet", 0);
        let table = append_files(&catalog, &table, vec![a.clone()]).await;

        // Build the overwrite WITHOUT enabling the deletes validation (default = snapshot isolation).
        let tx = Transaction::new(&table);
        let action = tx
            .overwrite_files()
            .delete_data_files(vec![a])
            .add_file(data_file("test/b.parquet", 0));
        let tx = action.apply(tx).unwrap();

        // CONCURRENT commit (S1): a position delete applying to A.
        let _concurrent = add_deletes(&catalog, &table, vec![position_delete_file(
            "test/pos-del.parquet",
            0,
        )])
        .await;

        let table = tx.commit(&catalog).await.expect(
            "with the deletes-validation OFF, a conflicting concurrent delete must not block the commit",
        );
        assert!(
            live_file_paths(&table).await.contains("test/b.parquet"),
            "the overwrite committed (snapshot isolation, no conflicting-delete check)"
        );
    }

    /// PATH-ONLY REMOVAL IS NOT VALIDATED. The removed file A is supplied via `delete_file(path)` (path-only,
    /// NOT `delete_data_files`), so it is NOT in the validated `deletedDataFiles` set. Even with the deletes
    /// validation enabled and a concurrent delete applying to A, the overwrite COMMITS — matching Java, which
    /// validates only the `DataFile` objects in `deletedDataFiles`. Risk pinned: validating path-only removals
    /// (which lack the partition/metrics the check needs) would over-reject beyond Java's contract.
    #[tokio::test]
    async fn test_overwrite_path_only_removal_is_not_validated_for_deletes() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let table = append_files(&catalog, &table, vec![data_file("test/a.parquet", 0)]).await;

        // Remove A by PATH only (not delete_data_files) — so it is not in the validated set.
        let tx = Transaction::new(&table);
        let action = tx
            .overwrite_files()
            .delete_file("test/a.parquet")
            .add_file(data_file("test/b.parquet", 0))
            .validate_no_conflicting_deletes();
        let tx = action.apply(tx).unwrap();

        // CONCURRENT commit (S1): a position delete in A's partition — would conflict IF A were validated.
        let _concurrent = add_deletes(&catalog, &table, vec![position_delete_file(
            "test/pos-del.parquet",
            0,
        )])
        .await;

        let table = tx.commit(&catalog).await.expect(
            "a path-only removal is not in the validated deletedDataFiles set ⇒ no conflict ⇒ commit succeeds",
        );
        assert!(live_file_paths(&table).await.contains("test/b.parquet"));
    }

    /// TX-CAPTURED START PIN (the recurring gap). The conflicting-delete check works WITHOUT an explicit
    /// `validate_from_snapshot`, relying solely on the transaction-captured starting snapshot surviving
    /// `do_commit`'s re-base. The action calls ONLY `.validate_no_conflicting_deletes()`. The start is the one
    /// captured in `Transaction::new` (= S0); `do_commit` overwrites `self.table` with the refreshed base (S1,
    /// the concurrent delete), but `starting_snapshot_id` must SURVIVE — so the concurrent delete is still
    /// enumerated and the commit rejected.
    ///
    /// Risk pinned: if the start were re-read from the refreshed head at validation time, start == current
    /// head ⇒ the concurrent set is empty ⇒ the check silently always passes (a serializable-isolation hole).
    /// All the other enabled deletes-tests pin `validate_from_snapshot`, so this is the only guard that the
    /// `Transaction::new` capture survives the re-base for the deletes check.
    #[tokio::test]
    async fn test_overwrite_rejects_concurrent_delete_using_tx_captured_starting_snapshot() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let a = data_file("test/a.parquet", 0);
        let table = append_files(&catalog, &table, vec![a.clone()]).await;

        // Build the overwrite with the deletes validation enabled but WITHOUT validate_from_snapshot — the
        // start is the tx-captured head (S0).
        let tx = Transaction::new(&table);
        let action = tx
            .overwrite_files()
            .delete_data_files(vec![a])
            .add_file(data_file("test/b.parquet", 0))
            .validate_no_conflicting_deletes();
        let tx = action.apply(tx).unwrap();

        // CONCURRENT commit (S1): a position delete applying to A.
        let _concurrent = add_deletes(&catalog, &table, vec![position_delete_file(
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

    /// INDEPENDENCE. Enabling ONLY `validate_no_conflicting_deletes()` does NOT enable the DATA-file check:
    /// a concurrent APPEND of a data file (which the data-file check would flag under AlwaysTrue) does NOT
    /// fail the commit when only the deletes check is on. Pins that the two flags are independent (enabling
    /// the deletes check must not silently turn on `validate_no_conflicting_data`).
    #[tokio::test]
    async fn test_overwrite_deletes_validation_does_not_enable_data_validation() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let a = data_file("test/a.parquet", 0);
        let (table, s0) = append_and_snapshot_id(&catalog, &table, vec![a.clone()]).await;

        // ONLY the deletes check is enabled (no validate_no_conflicting_data).
        let tx = Transaction::new(&table);
        let action = tx
            .overwrite_files()
            .delete_data_files(vec![a])
            .add_file(data_file("test/b.parquet", 0))
            .validate_from_snapshot(s0)
            .validate_no_conflicting_deletes();
        let tx = action.apply(tx).unwrap();

        // CONCURRENT commit (S1): a plain data APPEND — a DATA-file conflict under AlwaysTrue, NOT a delete.
        let _concurrent = append_files(&catalog, &table, vec![data_file(
            "test/concurrent.parquet",
            0,
        )])
        .await;

        // The deletes check ignores added data files ⇒ the commit succeeds (the data check is OFF).
        let table = tx.commit(&catalog).await.expect(
            "only the deletes check is on; a concurrent data APPEND is not a conflicting-delete ⇒ commit",
        );
        assert!(live_file_paths(&table).await.contains("test/b.parquet"));
    }

    // ============================================================================================
    // Delete-by-row-filter mode (Java `BaseOverwriteFiles.overwriteByRowFilter` → `deleteByRowFilter`).
    // `resolve_filter_deletes` (`snapshot.rs`) ports Java `ManifestFilterManager.manifestHasDeletedFiles`
    // + `PartitionAndMetricsEvaluator`: per live data file, reduce the predicate to its per-partition
    // residual, then DELETE if strict-metrics says all rows match, KEEP if inclusive says none match, and
    // ERROR ("some, but not all, rows match") on a partial match.
    // ============================================================================================

    /// THE KEY DELETE-BY-ROW-FILTER TEST. Append files in partitions x=0 (a, b) and x=1 (c). An
    /// `overwrite_by_row_filter(x == 0)` STRICTLY matches the whole x=0 partition (identity(x) residual is
    /// `alwaysTrue` there) → a and b are deleted; c (x=1) is kept. An added file d (x=0) lands. Post-commit
    /// SCAN = {c, d}. Pins the partition-residual delete: a file with NO `x` column bounds must still be
    /// deleted because the partition value satisfies the predicate.
    #[tokio::test]
    async fn test_overwrite_by_row_filter_deletes_strictly_matching_partition() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let table = append_files(&catalog, &table, vec![
            data_file("test/a.parquet", 0),
            data_file("test/b.parquet", 0),
            data_file("test/c.parquet", 1),
        ])
        .await;

        let tx = Transaction::new(&table);
        let action = tx
            .overwrite_files()
            .overwrite_by_row_filter(Reference::new("x").equal_to(Datum::long(0)))
            .add_file(data_file("test/d.parquet", 0));
        let tx = action.apply(tx).unwrap();
        let table = tx
            .commit(&catalog)
            .await
            .expect("overwrite_by_row_filter(x == 0) must delete the x=0 files and add d");

        assert_eq!(
            live_file_paths(&table).await,
            HashSet::from(["test/c.parquet".to_string(), "test/d.parquet".to_string(),]),
            "x=0 files (a, b) deleted by the row filter; c (x=1) kept; d added"
        );
        // Both an add and a (row-filter) delete ⇒ the operation is Overwrite.
        assert_eq!(
            table
                .metadata()
                .current_snapshot()
                .unwrap()
                .summary()
                .operation,
            Operation::Overwrite
        );
    }

    /// FULL REPLACE. `overwrite_by_row_filter(AlwaysTrue)` deletes EVERY live data file (Java
    /// `deleteByRowFilter(alwaysTrue)`), and the added file is all that remains. Pins the unpartitioned
    /// `alwaysTrue` full-replace shape (the residual is the whole `alwaysTrue`, strict-matched by every file).
    #[tokio::test]
    async fn test_overwrite_by_row_filter_always_true_replaces_all() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let table = append_files(&catalog, &table, vec![
            data_file("test/a.parquet", 0),
            data_file("test/b.parquet", 1),
        ])
        .await;

        let tx = Transaction::new(&table);
        let action = tx
            .overwrite_files()
            .overwrite_by_row_filter(Predicate::AlwaysTrue)
            .add_file(data_file("test/new.parquet", 0));
        let tx = action.apply(tx).unwrap();
        let table = tx
            .commit(&catalog)
            .await
            .expect("overwrite_by_row_filter(AlwaysTrue) must replace every file");

        assert_eq!(
            live_file_paths(&table).await,
            HashSet::from(["test/new.parquet".to_string()]),
            "AlwaysTrue deletes every live file; only the added file remains"
        );
    }

    /// PARTIAL MATCH ⇒ ERROR. A file whose `y` bounds `[0,10]` STRADDLE the predicate `y == 5` matches only
    /// SOME rows (inclusive yes, strict no). The commit must ERROR with Java's exact message ("Cannot delete
    /// file where some, but not all, rows match filter"). Pins that a partial match is a hard non-retryable
    /// error, never a silent partial delete.
    #[tokio::test]
    async fn test_overwrite_by_row_filter_partial_match_errors() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        // y bounds [0,10] straddle `y == 5` ⇒ some-but-not-all rows match.
        let table = append_files(&catalog, &table, vec![data_file_with_y_bounds(
            "test/straddle.parquet",
            0,
            0,
            10,
        )])
        .await;

        let tx = Transaction::new(&table);
        let action = tx
            .overwrite_files()
            .overwrite_by_row_filter(Reference::new("y").equal_to(Datum::long(5)));
        let tx = action.apply(tx).unwrap();
        let err = tx
            .commit(&catalog)
            .await
            .expect_err("a partial (some-but-not-all) row match must error");

        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(!err.retryable(), "a partial-match delete is non-retryable");
        assert!(
            err.message()
                .contains("Cannot delete file where some, but not all, rows match filter"),
            "must match Java's message, got: {}",
            err.message()
        );
        assert!(
            err.message().contains("test/straddle.parquet"),
            "the error must name the offending file, got: {}",
            err.message()
        );

        // The table is unchanged (the failed overwrite committed nothing).
        let reloaded = catalog.load_table(table.identifier()).await.unwrap();
        assert_eq!(
            live_file_paths(&reloaded).await,
            HashSet::from(["test/straddle.parquet".to_string()])
        );
    }

    /// NON-MATCHING FILE IS KEPT. The predicate `y == 5` cannot match a file whose `y` bounds `[60,70]` lie
    /// entirely outside it (inclusive says no rows match). The file survives an `overwrite_by_row_filter`
    /// (the row filter deletes nothing). Pins that a non-matching file is neither deleted nor an error.
    #[tokio::test]
    async fn test_overwrite_by_row_filter_keeps_non_matching_file() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        // y bounds [60,70] are entirely above `y == 5` ⇒ no rows match ⇒ KEEP.
        let table = append_files(&catalog, &table, vec![data_file_with_y_bounds(
            "test/high.parquet",
            0,
            60,
            70,
        )])
        .await;

        let tx = Transaction::new(&table);
        let action = tx
            .overwrite_files()
            .overwrite_by_row_filter(Reference::new("y").equal_to(Datum::long(5)))
            .add_file(data_file("test/added.parquet", 0));
        let tx = action.apply(tx).unwrap();
        let table = tx
            .commit(&catalog)
            .await
            .expect("a non-matching file is kept; the row filter deletes nothing");

        assert_eq!(
            live_file_paths(&table).await,
            HashSet::from([
                "test/high.parquet".to_string(),
                "test/added.parquet".to_string(),
            ]),
            "the non-matching file survives; the added file lands"
        );
    }

    // ============================================================================================
    // validateAddedFilesMatchOverwriteFilter (Java `BaseOverwriteFiles.validate` L137-161). Every ADDED
    // data file must lie entirely inside the row filter:
    //   inclusive_partition.eval(partition) && (strict_partition.eval(partition) || strictMetrics(rowFilter))
    // ============================================================================================

    /// ADDED FILE INSIDE THE FILTER ⇒ OK. With `validate_added_files_match_overwrite_filter` on and row
    /// filter `x == 0`, an added file routed to partition x=0 has ALL rows matching (strict-partition on
    /// identity(x) proves it) ⇒ the commit succeeds.
    #[tokio::test]
    async fn test_validate_added_files_match_filter_accepts_in_filter_file() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let table = append_files(&catalog, &table, vec![data_file("test/a.parquet", 0)]).await;

        let tx = Transaction::new(&table);
        let action = tx
            .overwrite_files()
            .overwrite_by_row_filter(Reference::new("x").equal_to(Datum::long(0)))
            .add_file(data_file("test/in-filter.parquet", 0))
            .validate_added_files_match_overwrite_filter();
        let tx = action.apply(tx).unwrap();
        let table = tx
            .commit(&catalog)
            .await
            .expect("an added file whose rows all match the filter must be accepted");

        assert!(
            live_file_paths(&table)
                .await
                .contains("test/in-filter.parquet"),
            "the in-filter added file is present (commit succeeded)"
        );
    }

    /// ADDED FILE OUTSIDE THE FILTER ⇒ REJECTED. With the validation on and row filter `x == 0`, an added
    /// file routed to partition x=1 has rows OUTSIDE the filter (strict-partition fails AND strict-metrics
    /// fails — no `x` bounds). The commit must be rejected with Java's "Cannot append file with rows that do
    /// not match filter" message.
    #[tokio::test]
    async fn test_validate_added_files_match_filter_rejects_out_of_filter_file() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let table = append_files(&catalog, &table, vec![data_file("test/a.parquet", 0)]).await;

        let tx = Transaction::new(&table);
        let action = tx
            .overwrite_files()
            .overwrite_by_row_filter(Reference::new("x").equal_to(Datum::long(0)))
            // Added file in partition x=1 — its rows are OUTSIDE `x == 0`.
            .add_file(data_file("test/out-of-filter.parquet", 1))
            .validate_added_files_match_overwrite_filter();
        let tx = action.apply(tx).unwrap();
        let err = tx
            .commit(&catalog)
            .await
            .expect_err("an added file with rows outside the filter must be rejected");

        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(!err.retryable(), "the validation failure is non-retryable");
        assert!(
            err.message()
                .contains("Cannot append file with rows that do not match filter"),
            "must match Java's message, got: {}",
            err.message()
        );
        assert!(
            err.message().contains("test/out-of-filter.parquet"),
            "the error must name the offending file, got: {}",
            err.message()
        );
    }

    // ============================================================================================
    // Conflict-filter default with overwrite_by_row_filter (Java `dataConflictDetectionFilter()` L181-188):
    // with NO explicit conflict filter, NO explicitly-removed data files, and a set row filter, the row
    // filter becomes the default conflict filter for `validateNoConflictingData`.
    // ============================================================================================

    /// ROW FILTER BECOMES THE DEFAULT CONFLICT FILTER (MATCH ⇒ REJECT). An `overwrite_by_row_filter(y >= 50)`
    /// with `validate_no_conflicting_data()` and NO explicit conflict filter (and no explicit deletes). A
    /// concurrent append of a file whose `y` bounds `[60,70]` MATCH `y >= 50` must conflict — proving the row
    /// filter (not AlwaysTrue) became the conflict filter.
    #[tokio::test]
    async fn test_row_filter_is_default_conflict_filter_matching_add_conflicts() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        // Seed lies OUTSIDE the row filter `y >= 50` (bounds [0,10]) so the row filter KEEPS it (no
        // partial-match ambiguity from possibly-null metrics) — it is only the base; the conflict is about
        // the concurrent / added files.
        let (table, s0) = append_and_snapshot_id(&catalog, &table, vec![data_file_with_y_bounds(
            "test/seed.parquet",
            0,
            0,
            10,
        )])
        .await;

        // overwrite_by_row_filter(y >= 50), no explicit deletes, no explicit conflict filter, validation on.
        let tx = Transaction::new(&table);
        let action = tx
            .overwrite_files()
            .overwrite_by_row_filter(Reference::new("y").greater_than_or_equal_to(Datum::long(50)))
            .add_file(data_file_with_y_bounds("test/added.parquet", 0, 80, 90))
            .validate_from_snapshot(s0)
            .validate_no_conflicting_data();
        let tx = action.apply(tx).unwrap();

        // CONCURRENT add: y bounds [60,70] MATCH `y >= 50` ⇒ a conflict under the row-filter default.
        let _concurrent = append_files(&catalog, &table, vec![data_file_with_y_bounds(
            "test/concurrent.parquet",
            0,
            60,
            70,
        )])
        .await;

        let err = tx.commit(&catalog).await.expect_err(
            "a concurrent add matching the row filter conflicts (row filter is the default conflict filter)",
        );
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(!err.retryable());
        assert!(
            err.message().contains("test/concurrent.parquet"),
            "the conflict must name the concurrent file, got: {}",
            err.message()
        );
    }

    /// ROW FILTER AS DEFAULT CONFLICT FILTER (OUTSIDE ⇒ NO CONFLICT). Same setup, but the concurrent file's
    /// `y` bounds `[10,20]` lie ENTIRELY BELOW the row filter `y >= 50` — it does NOT match, so it is not a
    /// conflict and the overwrite COMMITS. This is the discriminating half: under an AlwaysTrue default this
    /// concurrent add WOULD have conflicted, so a successful commit proves the row filter is the conflict
    /// filter (not AlwaysTrue).
    #[tokio::test]
    async fn test_row_filter_is_default_conflict_filter_outside_add_does_not_conflict() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        // Seed lies OUTSIDE the row filter `y >= 50` (bounds [0,10]) so the row filter KEEPS it (no
        // partial-match ambiguity from possibly-null metrics) — it is only the base; the conflict is about
        // the concurrent / added files.
        let (table, s0) = append_and_snapshot_id(&catalog, &table, vec![data_file_with_y_bounds(
            "test/seed.parquet",
            0,
            0,
            10,
        )])
        .await;

        let tx = Transaction::new(&table);
        let action = tx
            .overwrite_files()
            .overwrite_by_row_filter(Reference::new("y").greater_than_or_equal_to(Datum::long(50)))
            .add_file(data_file_with_y_bounds("test/added.parquet", 0, 80, 90))
            .validate_from_snapshot(s0)
            .validate_no_conflicting_data();
        let tx = action.apply(tx).unwrap();

        // CONCURRENT add: y bounds [10,20] are entirely BELOW `y >= 50` ⇒ NOT a conflict.
        let _concurrent = append_files(&catalog, &table, vec![data_file_with_y_bounds(
            "test/concurrent.parquet",
            0,
            10,
            20,
        )])
        .await;

        let table = tx.commit(&catalog).await.expect(
            "a concurrent add OUTSIDE the row filter is not a conflict (proves row filter is the default)",
        );
        assert!(
            live_file_paths(&table).await.contains("test/added.parquet"),
            "the overwrite committed (no conflict under the row-filter default)"
        );
        assert!(
            live_file_paths(&table)
                .await
                .contains("test/concurrent.parquet"),
            "the non-conflicting concurrent add survives the re-based overwrite"
        );
    }

    /// EXPLICIT DELETES DISABLE THE ROW-FILTER DEFAULT (Java L184 `&& deletedDataFiles.isEmpty()`). With an
    /// explicitly-removed data file present, `dataConflictDetectionFilter()` falls through to AlwaysTrue (NOT
    /// the row filter), so a concurrent add that lies OUTSIDE the row filter STILL conflicts. Pins the
    /// `deletedDataFiles.isEmpty()` guard on the row-filter default.
    #[tokio::test]
    async fn test_row_filter_default_disabled_when_explicit_deletes_present() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        // Seed lies OUTSIDE the row filter `y >= 50` (bounds [0,10]) so the row filter KEEPS it; it is
        // removed by the EXPLICIT delete_data_files below, which is what disables the row-filter default.
        let seed = data_file_with_y_bounds("test/seed.parquet", 0, 0, 10);
        let (table, s0) = append_and_snapshot_id(&catalog, &table, vec![seed.clone()]).await;

        // overwrite_by_row_filter(y >= 50) BUT ALSO an explicit delete_data_files(seed) ⇒ the row-filter
        // conflict-default is disabled, so the conflict filter falls back to AlwaysTrue.
        let tx = Transaction::new(&table);
        let action = tx
            .overwrite_files()
            .overwrite_by_row_filter(Reference::new("y").greater_than_or_equal_to(Datum::long(50)))
            .delete_data_files(vec![seed])
            .add_file(data_file_with_y_bounds("test/added.parquet", 0, 80, 90))
            .validate_from_snapshot(s0)
            .validate_no_conflicting_data();
        let tx = action.apply(tx).unwrap();

        // CONCURRENT add OUTSIDE the row filter (y bounds [10,20] < 50). Under AlwaysTrue it STILL conflicts.
        let _concurrent = append_files(&catalog, &table, vec![data_file_with_y_bounds(
            "test/concurrent.parquet",
            0,
            10,
            20,
        )])
        .await;

        let err = tx.commit(&catalog).await.expect_err(
            "with explicit deletes present the conflict filter is AlwaysTrue ⇒ even an outside add conflicts",
        );
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(!err.retryable());
        assert!(err.message().contains("test/concurrent.parquet"));
    }

    /// EXPLICIT CONFLICT FILTER STILL WINS (Java L182). When the caller sets BOTH an explicit
    /// `conflict_detection_filter` and an `overwrite_by_row_filter`, the EXPLICIT filter is used (not the row
    /// filter). Here the explicit filter `y >= 100` excludes a concurrent add at `[60,70]` that the row filter
    /// `y >= 50` WOULD have caught — so the commit succeeds, proving the explicit filter takes precedence.
    #[tokio::test]
    async fn test_explicit_conflict_filter_takes_precedence_over_row_filter() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        // Seed lies OUTSIDE the row filter `y >= 50` (bounds [0,10]) so the row filter KEEPS it (no
        // partial-match ambiguity from possibly-null metrics) — it is only the base; the conflict is about
        // the concurrent / added files.
        let (table, s0) = append_and_snapshot_id(&catalog, &table, vec![data_file_with_y_bounds(
            "test/seed.parquet",
            0,
            0,
            10,
        )])
        .await;

        let tx = Transaction::new(&table);
        let action = tx
            .overwrite_files()
            .overwrite_by_row_filter(Reference::new("y").greater_than_or_equal_to(Datum::long(50)))
            .conflict_detection_filter(
                Reference::new("y").greater_than_or_equal_to(Datum::long(100)),
            )
            .add_file(data_file_with_y_bounds("test/added.parquet", 0, 120, 130))
            .validate_from_snapshot(s0)
            .validate_no_conflicting_data();
        let tx = action.apply(tx).unwrap();

        // CONCURRENT add [60,70] with COMPLETE stats: matches the row filter (y >= 50) but NOT the explicit
        // filter (y >= 100). Full stats (0 nulls/nans) let the re-based row-filter delete cleanly classify it
        // as a FULL match (deleted), so the test isolates the conflict-filter precedence (no partial-match
        // noise). Under an AlwaysTrue or row-filter conflict default this add would conflict; the explicit
        // y >= 100 filter excludes it ⇒ the commit succeeds, proving the explicit filter wins.
        let _concurrent = append_files(&catalog, &table, vec![data_file_with_y_stats(
            "test/concurrent.parquet",
            0,
            60,
            70,
        )])
        .await;

        let table = tx.commit(&catalog).await.expect(
            "the explicit conflict filter (y >= 100) excludes the [60,70] add ⇒ commit (explicit wins)",
        );
        assert!(live_file_paths(&table).await.contains("test/added.parquet"));
    }

    // ============================================================================================
    // validateNewDeletes — BRANCH A (the row-filter-keyed delete-conflict checks). Java
    // `BaseOverwriteFiles.validate` L168-172: `if rowFilter() != alwaysFalse()` ⇒
    //   filter = conflictDetectionFilter != null ? conflictDetectionFilter : rowFilter()
    //   validateNoNewDeleteFiles(base, start, filter, parent)   // a concurrent ADDED delete file
    //   validateDeletedDataFiles(base, start, filter, parent)   // a concurrent DELETED data file
    // These guard an overwrite-by-row-filter against a concurrent merge-on-read delete that the
    // overwrite would otherwise silently invalidate. Both run under `validate_no_conflicting_deletes()`,
    // and BOTH require the row filter to be set (`overwrite_by_row_filter`) — branch A is gated on
    // `rowFilter() != alwaysFalse()`.
    //
    // The race these tests simulate: an `overwrite_files().overwrite_by_row_filter(P)` is BUILT against
    // head S0; before it commits a concurrent commit lands (S1) that EITHER adds a delete file matching P
    // OR removes a data file whose metrics match P. On re-base the overwrite's `validate` runs against S1
    // and must REJECT (non-retryable).
    // ============================================================================================

    /// A synthetic POSITION-delete file routed to partition `x = part_value` (spec id 0) whose column `y`
    /// (field id 2) carries `[y_lower, y_upper]` value bounds — so the [`InclusiveMetricsEvaluator`] inside
    /// `validateNoNewDeleteFiles` can include/exclude it against a row filter on `y`. A real position-delete
    /// file does not normally carry data-column bounds; these are synthetic to drive the metrics test.
    fn position_delete_file_with_y_bounds(
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

    /// BRANCH A — `validateNoNewDeleteFiles` POSITIVE (Java L170). `overwrite_by_row_filter(y >= 50)` with
    /// `.validate_no_conflicting_deletes()`. A concurrent `row_delta` ADDS a DELETE file whose `y` bounds
    /// `[60,70]` match the row filter `y >= 50`. The overwrite commit must be REJECTED with a NON-retryable
    /// `DataInvalid` carrying Java's "Found new conflicting delete files that can apply to records matching"
    /// message naming the conflicting delete file.
    ///
    /// Risk pinned: WITHOUT branch A, a concurrent merge-on-read delete that targets the same rows this
    /// overwrite-by-row-filter rewrites is silently invalidated — a lost delete under serializable isolation.
    /// (Failure mode caught: branch A missing, or gated wrongly, or the added-delete walk not run.)
    #[tokio::test]
    async fn test_overwrite_row_filter_rejects_concurrent_added_delete_file_matching_filter() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        // Seed `a` lies OUTSIDE the row filter (y bounds [0,10] < 50) so the row filter keeps it — the
        // conflict is entirely about the CONCURRENT delete file, not the base.
        let (table, s0) = append_and_snapshot_id(&catalog, &table, vec![data_file_with_y_bounds(
            "test/a.parquet",
            0,
            0,
            10,
        )])
        .await;

        // Build the overwrite: row filter y >= 50, deletes validation on, pinned to S0.
        let tx = Transaction::new(&table);
        let action = tx
            .overwrite_files()
            .overwrite_by_row_filter(Reference::new("y").greater_than_or_equal_to(Datum::long(50)))
            .add_file(data_file_with_y_bounds("test/b.parquet", 0, 80, 90))
            .validate_from_snapshot(s0)
            .validate_no_conflicting_deletes();
        let tx = action.apply(tx).unwrap();

        // CONCURRENT commit (S1): ADD a position-delete file whose y bounds [60,70] match `y >= 50`.
        let _concurrent = add_deletes(&catalog, &table, vec![position_delete_file_with_y_bounds(
            "test/concurrent-del.parquet",
            0,
            60,
            70,
        )])
        .await;

        let err = tx.commit(&catalog).await.expect_err(
            "overwrite must fail: a concurrent ADDED delete file matches the row filter (branch A)",
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
                .contains("Found new conflicting delete files that can apply to records matching"),
            "must match Java validateNoNewDeleteFiles message, got: {}",
            err.message()
        );
        assert!(
            err.message().contains("test/concurrent-del.parquet"),
            "the error must name the conflicting delete file, got: {}",
            err.message()
        );

        // The overwrite did NOT commit over the concurrent delete.
        let reloaded = catalog.load_table(table.identifier()).await.unwrap();
        assert!(
            !live_file_paths(&reloaded).await.contains("test/b.parquet"),
            "the rejected overwrite's added file must NOT be in the table"
        );
    }

    /// BRANCH A — `validateDeletedDataFiles` POSITIVE (Java L171). `overwrite_by_row_filter(y >= 50)` with
    /// `.validate_no_conflicting_deletes()`. A concurrent `overwrite_files().delete_file` REMOVES a data file
    /// whose `y` bounds `[60,70]` match the row filter. The overwrite commit must be REJECTED with Java's
    /// "Found conflicting deleted files that can contain records matching" message.
    ///
    /// This pins the SECOND of branch A's two checks (the added-delete walk in the test above finds nothing —
    /// the concurrent commit added no delete file — so this rejection is solely `validate_deleted_data_files`).
    ///
    /// Risk pinned: WITHOUT `validate_deleted_data_files`, a concurrent commit that already deleted the rows
    /// this overwrite-by-row-filter targets is silently re-overwritten — a write-write conflict lost under
    /// serializable isolation.
    #[tokio::test]
    async fn test_overwrite_row_filter_rejects_concurrent_deleted_data_file_matching_filter() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        // Seed `a` carries y bounds [60,70] — INSIDE the row filter `y >= 50`. When a concurrent commit
        // deletes it, its tombstone (with these bounds) is what `validate_deleted_data_files` flags.
        let (table, s0) = append_and_snapshot_id(&catalog, &table, vec![
            data_file_with_y_bounds("test/a.parquet", 0, 60, 70),
            // A second file the overwrite can keep, so the base is non-trivial.
            data_file_with_y_bounds("test/keep.parquet", 1, 0, 10),
        ])
        .await;

        // Build the overwrite: row filter y >= 50, deletes validation on, pinned to S0. (No explicit
        // delete_data_files — branch B is inert; only branch A's checks run.)
        let tx = Transaction::new(&table);
        let action = tx
            .overwrite_files()
            .overwrite_by_row_filter(Reference::new("y").greater_than_or_equal_to(Datum::long(50)))
            .add_file(data_file_with_y_bounds("test/b.parquet", 0, 80, 90))
            .validate_from_snapshot(s0)
            .validate_no_conflicting_deletes();
        let tx = action.apply(tx).unwrap();

        // CONCURRENT commit (S1): DELETE the data file `a` (records Operation::Delete, leaving a tombstone
        // for `a` carrying its y bounds [60,70]).
        let concurrent_tx = Transaction::new(&table);
        let concurrent_action = concurrent_tx
            .overwrite_files()
            .delete_file("test/a.parquet");
        let concurrent_tx = concurrent_action.apply(concurrent_tx).unwrap();
        let _concurrent = concurrent_tx.commit(&catalog).await.unwrap();

        let err = tx.commit(&catalog).await.expect_err(
            "overwrite must fail: a concurrent DELETED data file matches the row filter (branch A)",
        );
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(!err.retryable());
        assert!(
            err.message()
                .contains("Found conflicting deleted files that can contain records matching"),
            "must match Java validateDeletedDataFiles message, got: {}",
            err.message()
        );
        assert!(
            err.message().contains("test/a.parquet"),
            "the error must name the concurrently-deleted data file, got: {}",
            err.message()
        );

        // The overwrite did NOT commit over the concurrent delete.
        let reloaded = catalog.load_table(table.identifier()).await.unwrap();
        assert!(
            !live_file_paths(&reloaded).await.contains("test/b.parquet"),
            "the rejected overwrite's added file must NOT be in the table"
        );
    }

    /// BRANCH A — TX-CAPTURED START PIN (MANDATORY, the recurring gap). Branch A rejects WITHOUT an explicit
    /// `validate_from_snapshot` — relying solely on the transaction-captured starting snapshot surviving
    /// `do_commit`'s re-base. The action calls ONLY `.overwrite_by_row_filter(y >= 50)` +
    /// `.validate_no_conflicting_deletes()`. The start is the one captured in `Transaction::new` (= S0);
    /// `do_commit` overwrites `self.table` with the refreshed base (S1, the concurrent delete file), but
    /// `starting_snapshot_id` must SURVIVE — so the concurrent S1 delete file is still enumerated and rejected.
    ///
    /// Risk pinned: if `effective_start` were re-read from the REFRESHED head at validation time, start ==
    /// current head ⇒ the concurrent set is empty ⇒ branch A silently always passes (a serializable hole).
    /// MUTATION PROOF: changing `effective_start` in `validate()` to
    /// `current.metadata().current_snapshot_id()` (the refreshed head) makes EXACTLY this test fail
    /// (the other branch-A tests pin `validate_from_snapshot(s0)`, so they survive the mutation). Confirmed
    /// locally, then reverted.
    #[tokio::test]
    async fn test_overwrite_row_filter_rejects_concurrent_delete_using_tx_captured_starting_snapshot()
     {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        // Seed `a` outside the row filter so the base does not self-conflict.
        let table = append_files(&catalog, &table, vec![data_file_with_y_bounds(
            "test/a.parquet",
            0,
            0,
            10,
        )])
        .await;

        // Build the overwrite WITHOUT validate_from_snapshot — the start is the tx-captured head (S0).
        let tx = Transaction::new(&table);
        let action = tx
            .overwrite_files()
            .overwrite_by_row_filter(Reference::new("y").greater_than_or_equal_to(Datum::long(50)))
            .add_file(data_file_with_y_bounds("test/b.parquet", 0, 80, 90))
            .validate_no_conflicting_deletes();
        let tx = action.apply(tx).unwrap();

        // CONCURRENT commit (S1): ADD a delete file whose y bounds [60,70] match `y >= 50`.
        let _concurrent = add_deletes(&catalog, &table, vec![position_delete_file_with_y_bounds(
            "test/concurrent-del.parquet",
            0,
            60,
            70,
        )])
        .await;

        let err = tx.commit(&catalog).await.expect_err(
            "branch A conflict must be detected via the tx-captured starting snapshot (no override)",
        );
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(!err.retryable());
        assert!(
            err.message()
                .contains("Found new conflicting delete files that can apply to records matching"),
            "got: {}",
            err.message()
        );
        assert!(err.message().contains("test/concurrent-del.parquet"));
    }

    /// BRANCH A — FLAG-OFF CONTROL. The SAME concurrent conflict as the `validateNoNewDeleteFiles` positive
    /// test, but WITHOUT `.validate_no_conflicting_deletes()` ⇒ the overwrite COMMITS cleanly (snapshot
    /// isolation, the default). Proves branch A is genuinely OPT-IN.
    ///
    /// Risk pinned: branch A firing by default would change existing behavior and break callers relying on
    /// snapshot isolation for overwrite-by-row-filter.
    #[tokio::test]
    async fn test_overwrite_row_filter_without_validation_allows_concurrent_added_delete() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let table = append_files(&catalog, &table, vec![data_file_with_y_bounds(
            "test/a.parquet",
            0,
            0,
            10,
        )])
        .await;

        // Build the overwrite WITHOUT enabling the deletes validation (default = snapshot isolation).
        let tx = Transaction::new(&table);
        let action = tx
            .overwrite_files()
            .overwrite_by_row_filter(Reference::new("y").greater_than_or_equal_to(Datum::long(50)))
            .add_file(data_file_with_y_bounds("test/b.parquet", 0, 80, 90));
        let tx = action.apply(tx).unwrap();

        // CONCURRENT commit (S1): ADD a delete file whose y bounds [60,70] WOULD match if branch A ran.
        let _concurrent = add_deletes(&catalog, &table, vec![position_delete_file_with_y_bounds(
            "test/concurrent-del.parquet",
            0,
            60,
            70,
        )])
        .await;

        // With the deletes validation OFF, the overwrite COMMITS (branch A did not run).
        let table = tx.commit(&catalog).await.expect(
            "with validation OFF, a conflicting concurrent delete must not block the commit",
        );
        assert!(
            live_file_paths(&table).await.contains("test/b.parquet"),
            "the overwrite committed (snapshot isolation, branch A not run)"
        );
    }

    /// BRANCH A — ROW-FILTER GATE (Java L168 `if rowFilter() != alwaysFalse()`). With
    /// `.validate_no_conflicting_deletes()` set but NO `overwrite_by_row_filter` (so `row_filter()` is
    /// `AlwaysFalse`) and NO `delete_data_files`, branch A does NOT run: a concurrent ADDED delete file that
    /// branch A WOULD catch does not cause a rejection. The overwrite COMMITS.
    ///
    /// Risk pinned: dropping the `rowFilter() != alwaysFalse()` gate would run the row-filter delete checks
    /// (with an `AlwaysFalse` filter) on EVERY `validate_no_conflicting_deletes()` overwrite — `AlwaysFalse`
    /// bound by `first_conflicting_file` matches nothing, so it would not over-reject here, but the gate is
    /// the structural contract: branch A is keyed on a set row filter. This test pins that the row-filter
    /// checks are not reached when there is no row filter (the add-only/path-delete overwrite path is
    /// unaffected by branch A). To make the concurrent delete observable as a non-conflict, the overwrite
    /// adds a file and the commit must succeed.
    #[tokio::test]
    async fn test_overwrite_no_row_filter_skips_branch_a() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let table = append_files(&catalog, &table, vec![data_file_with_y_bounds(
            "test/a.parquet",
            0,
            0,
            10,
        )])
        .await;

        // Deletes validation ON, but NO overwrite_by_row_filter ⇒ row_filter() == AlwaysFalse ⇒ branch A is
        // gated OFF. No delete_data_files either ⇒ branch B is also inert. An add-only overwrite.
        let tx = Transaction::new(&table);
        let action = tx
            .overwrite_files()
            .add_file(data_file_with_y_bounds("test/b.parquet", 0, 80, 90))
            .validate_no_conflicting_deletes();
        let tx = action.apply(tx).unwrap();

        // CONCURRENT commit (S1): ADD a delete file whose y bounds [60,70] WOULD match a `y >= 50` row filter
        // IF branch A ran — but with no row filter the gate skips branch A entirely.
        let _concurrent = add_deletes(&catalog, &table, vec![position_delete_file_with_y_bounds(
            "test/concurrent-del.parquet",
            0,
            60,
            70,
        )])
        .await;

        // Branch A does not run (no row filter) ⇒ the overwrite COMMITS despite the concurrent delete file.
        let table = tx.commit(&catalog).await.expect(
            "with no row filter, branch A is gated off ⇒ a concurrent added delete does not reject",
        );
        assert!(
            live_file_paths(&table).await.contains("test/b.parquet"),
            "the add-only overwrite committed (branch A skipped, gate held)"
        );
    }

    /// BRANCH A — GATE IS ON `rowFilter()`, NOT the conflict filter (Java L168 `if rowFilter() !=
    /// alwaysFalse()`; inside, `filter = conflictDetectionFilter != null ? conflictDetectionFilter :
    /// rowFilter()`). The SUBTLE case the prior gate test omits: `.conflict_detection_filter(y >= 50)` IS set
    /// but there is NO `.overwrite_by_row_filter` (so `row_filter()` is `AlwaysFalse`) and NO
    /// `delete_data_files`. Java does NOT run branch A — the gate is keyed on the ROW filter, not the
    /// conflict-detection filter. A concurrent ADDED delete file whose `y` bounds `[60,70]` WOULD match the
    /// conflict filter `y >= 50` (so it would reject IF branch A ran with that filter) does NOT cause a
    /// rejection. The overwrite COMMITS.
    ///
    /// Risk pinned: a buggy gate keyed on `conflict_detection_filter` (or running branch A unconditionally
    /// under `validate_no_conflicting_deletes`) would OVER-REJECT here — a serializable-isolation false
    /// positive that diverges from Java, blocking a legal overwrite. MUTATION PROOF: changing the branch-A
    /// gate to `if row_filter != Predicate::AlwaysFalse || self.conflict_detection_filter.is_some()` makes
    /// EXACTLY this test fail (the existing `..._skips_branch_a` test has no conflict filter, so it survives);
    /// confirmed locally, then reverted.
    #[tokio::test]
    async fn test_overwrite_conflict_filter_without_row_filter_skips_branch_a() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let table = append_files(&catalog, &table, vec![data_file_with_y_bounds(
            "test/a.parquet",
            0,
            0,
            10,
        )])
        .await;

        // Deletes validation ON and a conflict_detection_filter IS set, but NO overwrite_by_row_filter ⇒
        // row_filter() == AlwaysFalse ⇒ branch A is gated OFF (Java gates on rowFilter(), not the conflict
        // filter). No delete_data_files ⇒ branch B is also inert. An add-only overwrite.
        let tx = Transaction::new(&table);
        let action = tx
            .overwrite_files()
            .add_file(data_file_with_y_bounds("test/b.parquet", 0, 80, 90))
            .conflict_detection_filter(
                Reference::new("y").greater_than_or_equal_to(Datum::long(50)),
            )
            .validate_no_conflicting_deletes();
        let tx = action.apply(tx).unwrap();

        // CONCURRENT commit (S1): ADD a delete file whose y bounds [60,70] DO match the conflict filter
        // `y >= 50` — branch A WOULD reject this IF it ran with that filter, but the gate (keyed on the
        // absent row filter) skips branch A entirely.
        let _concurrent = add_deletes(&catalog, &table, vec![position_delete_file_with_y_bounds(
            "test/concurrent-del.parquet",
            0,
            60,
            70,
        )])
        .await;

        // Branch A does not run (no row filter, despite the conflict filter) ⇒ the overwrite COMMITS.
        let table = tx.commit(&catalog).await.expect(
            "gate is on rowFilter(); with no row filter branch A is skipped even when a conflict filter \
             is set, so a concurrent added delete does not reject",
        );
        assert!(
            live_file_paths(&table).await.contains("test/b.parquet"),
            "the add-only overwrite committed (branch A skipped — gate keyed on the absent row filter)"
        );
    }

    // ============================================================================================
    // Merge-on-read DELETE-MANIFEST CARRY (Increment 2b — the silent-resurrection bug fix).
    //
    // `existing_manifest` now returns the FULL manifest list (DATA + DELETE) via the shared
    // `SnapshotProducer::current_manifests`, so an `overwrite_files` commit on a table that already carries
    // outstanding position/equality deletes preserves those delete manifests instead of dropping them
    // table-wide. This test uses the row_delta crown-jewel fixture (real parquet + a REAL position-delete
    // file written by the production writer + a production scan), so the resurrection physics is proven
    // end-to-end, not just at the manifest-metadata level.
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
    /// signal, independent of the read path). An overwrite must carry outstanding delete manifests forward,
    /// so this count must NOT drop to 0 across the commit.
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

    /// THE CROWN JEWEL (risk: an `overwrite_files` on a merge-on-read table silently DROPS every outstanding
    /// delete manifest, resurrecting deleted rows table-wide). Data file X (partition 0) carries a real
    /// position delete masking its row y=20; data file Y lives in partition 1. An overwrite that ADDS G
    /// (partition 1) and DELETES Y must keep X's delete applying — the scan after the commit is exactly
    /// {10, 80} (X's masked y=20 stays absent, Y's rows are gone, G's y=80 present).
    ///
    /// MUTATION (run manually, then restore): in `OverwriteFilesOperation::existing_manifest`, filter the
    /// `current_manifests()` result to DATA manifests only (the old data-only behavior) ⇒ this test FAILS
    /// with y=20 resurrected (the scan returns {10, 20, 80}) AND the structural delete-manifest count drops
    /// to 0.
    #[tokio::test]
    async fn test_overwrite_files_preserves_outstanding_delete_manifests_no_resurrection() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        // X in partition 0 with rows y = [10, 20]; Y in partition 1 with rows y = [60, 70].
        let x = write_data_file(&table, "x.parquet", 0, &[(0, 10, 100), (0, 20, 200)]).await;
        let x_path = x.file_path().to_string();
        let y = write_data_file(&table, "y.parquet", 1, &[(1, 60, 600), (1, 70, 700)]).await;
        let y_path = y.file_path().to_string();
        let table = append_files(&catalog, &table, vec![x, y]).await;

        // RowDelta a REAL position delete masking X's row at position 1 (y=20).
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

        // Sanity: before the overwrite, the scan drops y=20 (X's masked row) and shows Y's rows.
        assert_eq!(
            scan_y_values(&table).await,
            HashSet::from([10, 60, 70]),
            "the position delete masks y=20 from X; Y's rows are present"
        );

        // Overwrite: add G (partition 1, y=80) AND delete Y. This must NOT drop X's outstanding delete.
        let g = write_data_file(&table, "g.parquet", 1, &[(1, 80, 800)]).await;
        let tx = Transaction::new(&table);
        let action = tx.overwrite_files().add_file(g).delete_file(&y_path);
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

        // SCAN PIN: X's masked y=20 STILL ABSENT, Y gone, G's y=80 present ⇒ exactly {10, 80}.
        assert_eq!(
            scan_y_values(&table).await,
            HashSet::from([10, 80]),
            "Y replaced by G AND X's masked y=20 stays absent — no resurrection"
        );

        // STRUCTURAL PIN: the delete manifest survived the commit (count must not drop to 0).
        assert_eq!(
            count_delete_manifests(&table).await,
            1,
            "the overwrite_files commit must carry the outstanding delete manifest forward (not drop it)"
        );
    }
}
