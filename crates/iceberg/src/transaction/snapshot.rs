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

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::ops::RangeFrom;

use uuid::Uuid;

use crate::error::Result;
use crate::expr::visitors::inclusive_metrics_evaluator::InclusiveMetricsEvaluator;
use crate::expr::visitors::residual_evaluator::ResidualEvaluator;
use crate::expr::visitors::strict_metrics_evaluator::StrictMetricsEvaluator;
use crate::expr::{Bind, BoundPredicate, Predicate};
use crate::spec::{
    DataFile, DataFileFormat, FormatVersion, MAIN_BRANCH, Manifest, ManifestContentType,
    ManifestEntry, ManifestFile, ManifestListWriter, ManifestStatus, ManifestWriter,
    ManifestWriterBuilder, Operation, Schema, Snapshot, SnapshotReference, SnapshotRetention,
    SnapshotSummaryCollector, Struct, StructType, Summary, TableProperties,
    update_snapshot_summaries,
};
use crate::table::Table;
use crate::transaction::ActionCommit;
use crate::{Error, ErrorKind, TableRequirement, TableUpdate};

const META_ROOT_PATH: &str = "metadata";

/// A trait that defines how different table operations produce new snapshots.
///
/// `SnapshotProduceOperation` is used by [`SnapshotProducer`] to customize snapshot creation
/// based on the type of operation being performed (e.g., `Append`, `Overwrite`, `Delete`, etc.).
/// Each operation type implements this trait to specify:
/// - Which operation type to record in the snapshot summary
/// - Which existing manifest files should be included in the new snapshot
/// - Which manifest entries should be marked as deleted
///
/// # When it accomplishes
///
/// This trait is used during the snapshot creation process in [`SnapshotProducer::commit()`]:
///
/// 1. **Operation Type Recording**: The `operation()` method determines which operation type
///    (e.g., `Operation::Append`, `Operation::Overwrite`) is recorded in the snapshot summary.
///    This metadata helps track what kind of change was made to the table.
///
/// 2. **Manifest File Selection**: The `existing_manifest()` method determines which existing
///    manifest files from the current snapshot should be carried forward to the new snapshot.
///    For example:
///    - An `Append` operation typically includes all existing manifests plus new ones
///    - An `Overwrite` operation might exclude manifests for partitions being overwritten
///
/// 3. **Delete Entry Processing**: The `delete_entries()` method is intended for future delete
///    operations to specify which manifest entries should be marked as deleted.
pub(crate) trait SnapshotProduceOperation: Send + Sync {
    /// Returns the operation type that will be recorded in the snapshot summary.
    ///
    /// This determines what kind of operation is being performed (e.g., `Append`, `Overwrite`),
    /// which is stored in the snapshot metadata for tracking and auditing purposes.
    fn operation(&self) -> Operation;

    /// Returns manifest entries that should be marked as deleted in the new snapshot.
    #[allow(unused)]
    fn delete_entries(
        &self,
        snapshot_produce: &SnapshotProducer,
    ) -> impl Future<Output = Result<Vec<ManifestEntry>>> + Send;

    /// Returns the data files this operation wants to remove from the table.
    ///
    /// The producer resolves these against the current snapshot's manifests at commit time: every
    /// existing manifest that contains a live entry for one of these files is rewritten with the
    /// matching entries marked `Deleted` (mirroring Java `ManifestFilterManager.filterManifest`).
    /// Operations that only add files (e.g. fast append) return an empty vector.
    fn delete_files(
        &self,
        snapshot_produce: &SnapshotProducer<'_>,
    ) -> impl Future<Output = Result<Vec<DataFile>>> + Send;

    /// Returns existing manifest files that should be included in the new snapshot.
    ///
    /// This method determines which manifest files from the current snapshot should be
    /// carried forward to the new snapshot. The selection depends on the operation type:
    ///
    /// - **Append operations**: Typically include all existing manifests
    /// - **Overwrite operations**: May exclude manifests for partitions being overwritten
    /// - **Delete operations**: May exclude manifests for partitions being deleted
    fn existing_manifest(
        &self,
        snapshot_produce: &SnapshotProducer<'_>,
    ) -> impl Future<Output = Result<Vec<ManifestFile>>> + Send;
}

pub(crate) struct DefaultManifestProcess;

impl ManifestProcess for DefaultManifestProcess {
    async fn process_manifests(
        &self,
        _snapshot_produce: &mut SnapshotProducer<'_>,
        manifests: Vec<ManifestFile>,
    ) -> Result<Vec<ManifestFile>> {
        // Pass the manifest list through unchanged — the fast-append / single-manifest path. This MUST
        // stay a no-op so `FastAppend` behavior is byte-identical to the pre-seam-change producer.
        Ok(manifests)
    }
}

/// Post-process the manifest list a snapshot is about to commit, after the producer has written the
/// added DATA/DELETE manifests and rewritten any delete-bearing manifests (Java
/// `MergingSnapshotProducer.apply`'s `mergeManager.mergeManifests(...)` step). The default
/// ([`DefaultManifestProcess`]) returns the list untouched (fast append); the merge-append manager
/// ([`crate::transaction::merge_append::MergeManifestProcess`]) bin-packs and merges them.
///
/// Takes `&mut SnapshotProducer` because a manager that MERGES manifests needs the producer's writer
/// factory ([`SnapshotProducer::new_cluster_manifest_writer`]) — which advances the manifest-name
/// counter — to write the merged manifests. It is async + `Result` because merging reads the input
/// manifests back from object storage and writes new ones.
pub(crate) trait ManifestProcess: Send + Sync {
    fn process_manifests(
        &self,
        snapshot_produce: &mut SnapshotProducer<'_>,
        manifests: Vec<ManifestFile>,
    ) -> impl Future<Output = Result<Vec<ManifestFile>>> + Send;
}

pub(crate) struct SnapshotProducer<'a> {
    pub(crate) table: &'a Table,
    snapshot_id: i64,
    commit_uuid: Uuid,
    key_metadata: Option<Vec<u8>>,
    snapshot_properties: HashMap<String, String>,
    added_data_files: Vec<DataFile>,
    // DELETE files (position / equality) this snapshot adds, written into a DELETE manifest alongside
    // the DATA manifest (Java `MergingSnapshotProducer.add(DeleteFile)`). The merge-on-read write path
    // (`RowDelta`) populates this; add-only data operations (fast append, overwrite-by-files) leave it
    // empty. Their entries inherit the new snapshot's sequence number at read time, exactly like added
    // data files (so a delete added now applies to earlier data: `data_seq <= delete_seq`).
    added_delete_files: Vec<DataFile>,
    // An explicit DATA sequence number to stamp on every ADDED data file (Java
    // `MergingSnapshotProducer.newDataFilesDataSequenceNumber`). When `Some(seq)`, each added data
    // entry is written with this explicit data seq instead of inheriting the new snapshot's seq at
    // read time — the `RewriteFiles.dataSequenceNumber` preservation path that keeps outstanding
    // equality deletes applying to rewritten data (`data_seq < delete_seq`). `None` (the default for
    // every other operation) ⇒ the added files inherit the new snapshot's sequence number as usual.
    new_data_files_data_sequence_number: Option<i64>,
    // Data files removed by this snapshot, resolved against the current snapshot at commit time. Held
    // so the snapshot summary can reflect the deleted file/record counts (Java overwrite/delete summary).
    // Empty for add-only operations such as fast append.
    removed_data_files: Vec<DataFile>,
    // DELETE files (position / equality / DV) removed by this snapshot — the merge-on-read apply-side
    // removal of superseded delete files (Java `MergingSnapshotProducer.delete(DeleteFile)` →
    // `deleteFilterManager.delete(file)`). Resolved against the current snapshot's DELETE manifests by
    // path in `commit()`, then fed to the SAME `process_deletes` rewrite path (which matches by path
    // across the full manifest list, so a removed delete file's tombstone lands in the rewritten DELETE
    // manifest) and to the summary's `remove_file` (DV → `removed-dvs`, parquet position →
    // `removed-position-delete-files`, equality → `removed-equality-delete-files`). Populated by
    // `RowDelta.removeDeletes`; empty for every operation that does not remove delete files.
    removed_delete_files: Vec<DataFile>,
    // A counter used to generate unique manifest file names.
    // It starts from 0 and increments for each new manifest file.
    // Note: This counter is limited to the range of (0..u64::MAX).
    manifest_counter: RangeFrom<u64>,
}

impl<'a> SnapshotProducer<'a> {
    pub(crate) fn new(
        table: &'a Table,
        commit_uuid: Uuid,
        key_metadata: Option<Vec<u8>>,
        snapshot_properties: HashMap<String, String>,
        added_data_files: Vec<DataFile>,
    ) -> Self {
        Self {
            table,
            snapshot_id: Self::generate_unique_snapshot_id(table),
            commit_uuid,
            key_metadata,
            snapshot_properties,
            added_data_files,
            added_delete_files: vec![],
            new_data_files_data_sequence_number: None,
            removed_data_files: vec![],
            removed_delete_files: vec![],
            manifest_counter: (0..),
        }
    }

    /// Attach the DELETE files (position / equality) this snapshot adds. They are written into a
    /// DELETE manifest alongside the DATA manifest in the same snapshot (Java
    /// `MergingSnapshotProducer.add(DeleteFile)`). Used by the merge-on-read write path (`RowDelta`).
    pub(crate) fn with_added_delete_files(mut self, added_delete_files: Vec<DataFile>) -> Self {
        self.added_delete_files = added_delete_files;
        self
    }

    /// Attach the DELETE files (position / equality / DV) this snapshot REMOVES — the apply-side
    /// removal of superseded merge-on-read delete files (Java `MergingSnapshotProducer.delete(DeleteFile)`
    /// → `deleteFilterManager.delete(file)`, the delete-side sibling of `delete(DataFile)`). Used by the
    /// merge-on-read write path (`RowDelta.removeDeletes`) to drop a delete file the new delete supersedes
    /// — e.g. removing the OLD deletion vector when a merged super-set DV replaces it.
    ///
    /// The supplied files are resolved against the current snapshot's DELETE manifests by path in
    /// [`SnapshotProducer::commit`] (a missing path fails loud), then fed to the SAME `process_deletes`
    /// rewrite path as removed DATA files: `process_deletes` matches each removed file's path against EVERY
    /// existing manifest (DATA and DELETE), so a removed delete file's tombstone lands in the rewritten
    /// DELETE manifest while DATA manifests are untouched. The resolved files also reach the summary's
    /// `remove_file` (DV → `removed-dvs`, parquet position → `removed-position-delete-files`, equality →
    /// `removed-equality-delete-files`).
    pub(crate) fn with_removed_delete_files(mut self, removed_delete_files: Vec<DataFile>) -> Self {
        self.removed_delete_files = removed_delete_files;
        self
    }

    /// Stamp every ADDED data file with an explicit DATA sequence number instead of inheriting the new
    /// snapshot's sequence number (Java `MergingSnapshotProducer.setNewDataFilesDataSequenceNumber` /
    /// `RewriteFiles.dataSequenceNumber`). Used by the compaction write path (`RewriteFiles`) to preserve
    /// the replaced files' data sequence number so any outstanding merge-on-read EQUALITY delete still
    /// applies to the rewritten data (`data_seq < delete_seq`) — without this, the added files would take a
    /// fresh, higher sequence number and the old deletes would stop applying, resurrecting deleted rows.
    /// `seq` must be non-negative (the manifest writer silently strips a negative one back into
    /// re-inheritance — the caller validates this before calling).
    pub(crate) fn with_new_data_files_data_sequence_number(mut self, sequence_number: i64) -> Self {
        self.new_data_files_data_sequence_number = Some(sequence_number);
        self
    }

    /// The id of the snapshot this producer is creating. Exposed so an action that pre-computes its
    /// own manifest list (e.g. `RewriteManifests`) can stamp externally-added manifests with the new
    /// snapshot id before they reach the manifest-list writer (Java `withSnapshotId`,
    /// `BaseRewriteManifests.apply` L184-187 — required by
    /// [`ManifestListWriter::add_manifests`]'s `assign_sequence_numbers` precondition).
    pub(crate) fn snapshot_id(&self) -> i64 {
        self.snapshot_id
    }

    /// Merge additional snapshot summary properties computed AFTER construction (Java
    /// `RewriteManifests.summary()` sets `manifests-created` / `-kept` / `-replaced` /
    /// `entries-processed` only once the rewrite has run). [`SnapshotProducer::new`] takes the
    /// user-supplied properties up front; this additive setter lets the rewrite inject the counts it
    /// can only know post-rewrite. These non-empty properties also satisfy the empty-commit
    /// precondition in [`SnapshotProducer::manifest_file`] for an action that adds no data files.
    pub(crate) fn extend_snapshot_properties(
        &mut self,
        properties: impl IntoIterator<Item = (String, String)>,
    ) {
        self.snapshot_properties.extend(properties);
    }

    /// Build a manifest writer for a brand-new (non-filtered) manifest of `content` under
    /// `partition_spec_id` — the cluster-writer factory shared by [`crate::transaction::rewrite_manifests`],
    /// [`crate::transaction::merge_append`], and the producer's own per-spec ADDED-manifest grouping
    /// ([`SnapshotProducer::write_added_manifests`] / [`SnapshotProducer::write_added_delete_manifests`]).
    ///
    /// Keyed by the partition-spec id directly (a cluster writer is created per
    /// `(cluster_key, partition_spec_id)`, Java `BaseRewriteManifests.getWriter` keyed on `Pair.of(key,
    /// partitionSpecId)`) rather than off a source [`ManifestFile`] — that is the difference from
    /// [`SnapshotProducer::new_filtering_manifest_writer`], which keys off a source manifest. The `content`
    /// axis mirrors Java's `writeDataManifests(files, spec)` vs `writeDeleteManifests(files, spec)`: a DATA
    /// content yields a data writer, a DELETE content a delete writer. The rewrite/merge callers pass
    /// `Data` (their entries are pre-existing data entries copied forward via `add_existing_entry`,
    /// provenance preserved); the producer's added-delete grouping passes `Deletes`.
    pub(crate) fn new_cluster_manifest_writer(
        &mut self,
        partition_spec_id: i32,
        content: ManifestContentType,
    ) -> Result<ManifestWriter> {
        let partition_spec = self
            .table
            .metadata()
            .partition_spec_by_id(partition_spec_id)
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Cannot rewrite manifests: unknown partition spec id {partition_spec_id}"
                    ),
                )
            })?
            .as_ref()
            .clone();

        let new_manifest_path = format!(
            "{}/{}/{}-m{}.{}",
            self.table.metadata().location(),
            META_ROOT_PATH,
            self.commit_uuid,
            self.manifest_counter.next().ok_or_else(|| {
                Error::new(
                    ErrorKind::Unexpected,
                    "Exhausted manifest file name counter",
                )
            })?,
            DataFileFormat::Avro
        );
        let output_file = self.table.file_io().new_output(new_manifest_path)?;
        let builder = ManifestWriterBuilder::new(
            output_file,
            Some(self.snapshot_id),
            self.key_metadata.clone(),
            self.table.metadata().current_schema().clone(),
            partition_spec,
        );
        match self.table.metadata().format_version() {
            FormatVersion::V1 => Ok(builder.build_v1()),
            FormatVersion::V2 => match content {
                ManifestContentType::Data => Ok(builder.build_v2_data()),
                ManifestContentType::Deletes => Ok(builder.build_v2_deletes()),
            },
            FormatVersion::V3 => match content {
                ManifestContentType::Data => Ok(builder.build_v3_data()),
                ManifestContentType::Deletes => Ok(builder.build_v3_deletes()),
            },
        }
    }

    /// Validate the added DELETE files (Java `RowDelta.addDeletes` / `MergingSnapshotProducer.add`):
    /// each must be a `PositionDeletes` or `EqualityDeletes` content file (a `Data` file is rejected —
    /// it must be added as a row, not a delete), must pass the FORMAT-VERSION gate (see
    /// [`validate_delete_file_for_version`]), and its partition spec must EXIST in the table's specs.
    ///
    /// **Per-spec (Java parity, NOT default-spec-only):** Java `MergingSnapshotProducer.addInternal`
    /// (1.10.0 bytecode) resolves `spec(file.specId())` for EACH added delete file and rejects only when
    /// no such spec exists — `Preconditions.checkArgument(spec != null, "Cannot find partition spec %s
    /// for delete file: %s", file.specId(), file.location())`. A delete file under an OLDER (non-default)
    /// partition spec is therefore accepted; the producer writes per-spec DELETE manifest groups
    /// ([`SnapshotProducer::manifest_file`]). The partition-value compatibility check runs against THAT
    /// file's own spec's partition type, not the table default's.
    ///
    /// **Placement (the format-version gate):** Java 1.10.0 runs `validateNewDeleteFile` →
    /// `validateDeleteFileForVersion` inside `MergingSnapshotProducer.add(DeleteFile)` (bytecode:
    /// `addInternal` calls `validateNewDeleteFile` first; `apply` does NOT re-validate — the buffered
    /// re-validation `validateDeleteFilesForVersion(base.formatVersion())` at the top of `apply` is a
    /// post-1.10.0 MAIN addition guarding a concurrent format upgrade). In this Rust model the action
    /// builder has no table access at `add_deletes` time, so the gate runs HERE — in the action's
    /// `commit()` against the REFRESHED base (`do_commit` re-bases before calling `commit`). That is
    /// exactly MAIN's stronger apply-time placement and subsumes 1.10.0's add-time check: a row delta
    /// built before a concurrent `upgrade_format_version` commit is re-gated against the upgraded
    /// version on every retry.
    pub(crate) fn validate_added_delete_files(&self) -> Result<()> {
        let format_version = self.table.metadata().format_version();
        for delete_file in &self.added_delete_files {
            match delete_file.content_type() {
                crate::spec::DataContentType::PositionDeletes
                | crate::spec::DataContentType::EqualityDeletes => {}
                crate::spec::DataContentType::Data => {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        "Only position-delete or equality-delete content is allowed for added delete files",
                    ));
                }
            }
            validate_delete_file_for_version(delete_file, format_version)?;
            // Java `addInternal`: `spec(file.specId())` must exist (any table spec, not just the default).
            let partition_type = self.partition_type_for_added_file(delete_file, true)?;
            Self::validate_partition_value(delete_file.partition(), &partition_type)?;
        }

        Ok(())
    }

    pub(crate) fn validate_added_data_files(&self) -> Result<()> {
        for data_file in &self.added_data_files {
            if data_file.content_type() != crate::spec::DataContentType::Data {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    "Only data content type is allowed for fast append",
                ));
            }
            // Java `MergingSnapshotProducer.add(DataFile)` / `FastAppend.appendFile`: `spec(file.specId())`
            // must exist (ANY table spec, not just the default). A file under an older spec is accepted and
            // routed into its own per-spec manifest group; only an UNKNOWN spec id is rejected.
            let partition_type = self.partition_type_for_added_file(data_file, false)?;
            Self::validate_partition_value(data_file.partition(), &partition_type)?;
        }

        Ok(())
    }

    /// Resolve the partition TYPE of the spec a freshly-added file claims (`data_file.partition_spec_id`),
    /// or return Java's exact "Cannot find partition spec %s for {data,delete} file: %s" error when no such
    /// spec exists — the Rust port of Java's `spec(file.specId())` + `Preconditions.checkArgument(spec !=
    /// null, ...)` in `MergingSnapshotProducer.add` / `FastAppend.appendFile`.
    ///
    /// `is_delete_file` selects the message noun ("delete file" vs "data file"), matching Java's two
    /// distinct precondition messages (`add(DataFile)` L242-247 vs `addInternal(DeleteFile)` L273-278).
    /// The partition type is computed from that spec against the current schema (Java
    /// `spec.partitionType()`); it is the type the partition-value compatibility check binds against.
    fn partition_type_for_added_file(
        &self,
        file: &DataFile,
        is_delete_file: bool,
    ) -> Result<StructType> {
        let metadata = self.table.metadata();
        let spec = metadata
            .partition_spec_by_id(file.partition_spec_id)
            .ok_or_else(|| {
                let noun = if is_delete_file {
                    "delete file"
                } else {
                    "data file"
                };
                Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Cannot find partition spec {} for {}: {}",
                        file.partition_spec_id,
                        noun,
                        file.file_path()
                    ),
                )
            })?;
        spec.partition_type(metadata.current_schema())
    }

    pub(crate) async fn validate_duplicate_files(&self) -> Result<()> {
        let new_files: HashSet<&str> = self
            .added_data_files
            .iter()
            .map(|df| df.file_path.as_str())
            .collect();

        let mut referenced_files = Vec::new();
        if let Some(current_snapshot) = self.table.metadata().current_snapshot() {
            let manifest_list = current_snapshot
                .load_manifest_list(self.table.file_io(), &self.table.metadata_ref())
                .await?;
            for manifest_list_entry in manifest_list.entries() {
                let manifest = manifest_list_entry
                    .load_manifest(self.table.file_io())
                    .await?;
                for entry in manifest.entries() {
                    let file_path = entry.file_path();
                    if new_files.contains(file_path) && entry.is_alive() {
                        referenced_files.push(file_path.to_string());
                    }
                }
            }
        }

        if !referenced_files.is_empty() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Cannot add files that are already referenced by table, files: {}",
                    referenced_files.join(", ")
                ),
            ));
        }

        Ok(())
    }

    /// Return EVERY current manifest — DATA **and** DELETE — from the current snapshot's manifest list,
    /// the complete candidate set a delete-bearing operation's `existing_manifest` hands to the producer.
    ///
    /// Shared by every delete-bearing operation (`DeleteFiles`, `OverwriteFiles`, `ReplacePartitions`,
    /// `RewriteFiles`): each exposes the FULL manifest list so the producer's `process_deletes` can decide
    /// per DATA manifest whether to rewrite (drop the removed/replaced files), carry forward unchanged, or
    /// drop it, while every DELETE manifest is carried forward UNCHANGED — a delete manifest's entries are
    /// delete-file paths, which can never appear in a DATA `delete_paths` set, so `process_deletes` leaves
    /// them alone.
    ///
    /// Carrying delete manifests forward is REQUIRED FOR CORRECTNESS, not an optimization. This mirrors Java
    /// `MergingSnapshotProducer.apply` (`core/MergingSnapshotProducer.java` L973-1011), which composes BOTH
    /// `filterManager.filterManifests(dataManifests)` AND `deleteFilterManager.filterManifests(deleteManifests)`
    /// into the new manifest list. If an action returned DATA manifests only (the old
    /// `current_data_manifests`), the new snapshot's manifest list would OMIT every delete manifest the
    /// current snapshot carried — on a merge-on-read table (Java- or Rust-written) that silently drops all
    /// outstanding position / equality deletes table-wide, resurrecting every deleted row. This helper exists
    /// to make that whole bug class unrepresentable: all four delete-bearing actions carry the full set.
    ///
    /// **Conservative dangling-delete posture (documented divergence from Java):** Java's `apply` also drops
    /// delete files older than the surviving data's minimum sequence number and removes DVs orphaned by the
    /// data files it deleted (L982-993, `dropDeleteFilesOlderThan` / `removeDanglingDeletesFor`). This port
    /// deliberately does NOT port that pruning — it carries every delete manifest forward UNCHANGED. That is
    /// the conservative-safe direction: keeping a delete that no longer applies is harmless (it matches no
    /// live row), whereas dropping one that still applies resurrects deleted rows. Dangling-delete cleanup is
    /// a maintenance concern for a future `RemoveDanglingDeleteFiles` action, not a commit-path obligation.
    ///
    /// Returns an empty list when the table has no current snapshot.
    pub(crate) async fn current_manifests(&self) -> Result<Vec<ManifestFile>> {
        let Some(snapshot) = self.table.metadata().current_snapshot() else {
            return Ok(vec![]);
        };

        let manifest_list = snapshot
            .load_manifest_list(self.table.file_io(), &self.table.metadata_ref())
            .await?;

        Ok(manifest_list.entries().to_vec())
    }

    /// Resolve `delete_paths` against the current snapshot's live data entries, returning the matching
    /// [`DataFile`]s, and fail if any requested path matched no live entry.
    ///
    /// Shared by `DeleteFiles`, `OverwriteFiles`, and `RowDelta` (`removeRows`). The requested path set
    /// is only known to the calling operation (the producer downstream sees just the resolved
    /// `DataFile`s), so the missing-path check (Java `failMissingDeletePaths`) must happen here during
    /// resolution: a present-and-absent mix errors rather than silently dropping the present file.
    ///
    /// **Per-caller faithfulness of the missing-path FAIL:** `StreamingDelete` (DeleteFiles) and
    /// `BaseOverwriteFiles` both call `failMissingDeletePaths()` (1.10.0 bytecode), so the unconditional
    /// loud failure is Java-faithful for them. Java's `BaseRowDelta` does NOT set the flag for
    /// `removeRows` (its sole `failMissingDeletePaths()` sits behind `if (validateDeletes)` in
    /// `validate()`, gating the unrelated `validateDataFilesExist` walk), so for the `RowDelta.removeRows`
    /// caller this Rust path is STRICTER than Java's silent best-effort default — the same conservative
    /// posture documented on [`Self::resolve_delete_file_paths`] (the `removeDeletes` sibling) and on
    /// `transaction/row_delta.rs`'s apply-side note. Returns an empty vector when `delete_paths` is empty.
    pub(crate) async fn resolve_delete_paths(
        &self,
        delete_paths: &HashSet<String>,
    ) -> Result<Vec<DataFile>> {
        if delete_paths.is_empty() {
            return Ok(vec![]);
        }

        let mut resolved = Vec::new();
        let mut found_paths: HashSet<String> = HashSet::new();
        if let Some(snapshot) = self.table.metadata().current_snapshot() {
            let manifest_list = snapshot
                .load_manifest_list(self.table.file_io(), &self.table.metadata_ref())
                .await?;

            for manifest_file in manifest_list.entries() {
                if manifest_file.content != ManifestContentType::Data {
                    continue;
                }
                let manifest = manifest_file.load_manifest(self.table.file_io()).await?;
                for entry in manifest.entries() {
                    if entry.is_alive() && delete_paths.contains(entry.file_path()) {
                        found_paths.insert(entry.file_path().to_string());
                        resolved.push(entry.data_file().clone());
                    }
                }
            }
        }

        let missing: Vec<&str> = delete_paths
            .iter()
            .map(String::as_str)
            .filter(|path| !found_paths.contains(*path))
            .collect();
        if !missing.is_empty() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!("Missing required files to delete: {}", missing.join(", ")),
            ));
        }

        Ok(resolved)
    }

    /// Resolve `delete_paths` against the current snapshot's live DELETE entries, returning the matching
    /// [`DataFile`]s, and fail if any requested path matched no live delete entry — the DELETE-manifest
    /// sibling of [`SnapshotProducer::resolve_delete_paths`] (Java
    /// `MergingSnapshotProducer.delete(DeleteFile)` → `deleteFilterManager.delete(file)` resolved at
    /// `filterManifests` time).
    ///
    /// Scans every current DELETE manifest (NOT data manifests — a removed delete file's path lives in a
    /// `ManifestContentType::Deletes` manifest) and collects each live entry whose path is in
    /// `delete_paths`. The missing-path check mirrors `resolve_delete_paths`' present-and-absent semantics
    /// (Java `failMissingDeletePaths` / `validateRequiredDeletes`, "Missing required files to delete: %s").
    ///
    /// **Posture note (slightly stricter than Java's `RowDelta.removeDeletes` DEFAULT):** Java's
    /// `validateRequiredDeletes` only fails on a missing path when `failMissingDeletePaths` is set, which
    /// `RowDelta` does NOT set (only `StreamingDelete`/overwrite call `failMissingDeletePaths()`). This port
    /// fails loud on a missing removal path unconditionally — the same conservative posture the Rust
    /// `process_deletes` already takes for removed DATA files (it never models the `failMissingDeletePaths`
    /// flag): removing a delete file that is not live is a caller error worth surfacing, not silently
    /// dropping. One consequence: a commit RETRY whose target delete file was concurrently removed fails
    /// loud (non-retryable) where Java's silent-ignore default would converge — the safe (loud) direction,
    /// accepted. Returns an empty vector when `delete_paths` is empty.
    pub(crate) async fn resolve_delete_file_paths(
        &self,
        delete_paths: &HashSet<String>,
    ) -> Result<Vec<DataFile>> {
        if delete_paths.is_empty() {
            return Ok(vec![]);
        }

        let mut resolved = Vec::new();
        let mut found_paths: HashSet<String> = HashSet::new();
        if let Some(snapshot) = self.table.metadata().current_snapshot() {
            let manifest_list = snapshot
                .load_manifest_list(self.table.file_io(), &self.table.metadata_ref())
                .await?;

            for manifest_file in manifest_list.entries() {
                if manifest_file.content != ManifestContentType::Deletes {
                    continue;
                }
                let manifest = manifest_file.load_manifest(self.table.file_io()).await?;
                for entry in manifest.entries() {
                    if entry.is_alive() && delete_paths.contains(entry.file_path()) {
                        found_paths.insert(entry.file_path().to_string());
                        resolved.push(entry.data_file().clone());
                    }
                }
            }
        }

        let missing: Vec<&str> = delete_paths
            .iter()
            .map(String::as_str)
            .filter(|path| !found_paths.contains(*path))
            .collect();
        if !missing.is_empty() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!("Missing required files to delete: {}", missing.join(", ")),
            ));
        }

        Ok(resolved)
    }

    /// Resolve a set of `(partition_spec_id, partition)` tuples against the current snapshot's live data
    /// entries, returning every matching [`DataFile`] (the ones a partition-scoped replace removes).
    ///
    /// This is the by-PARTITION delete-resolution path used by `ReplacePartitions` (dynamic partition
    /// overwrite), the sibling of the by-PATH [`SnapshotProducer::resolve_delete_paths`]. It scans every
    /// current data manifest and collects each live entry whose `(partition_spec_id, partition)` is in
    /// `drop_partitions` — mirroring Java `ManifestFilterManager`'s `dropPartitions.contains(file.specId(),
    /// file.partition())` test (`filterManifestWithDeletedFiles`). The resolved [`DataFile`]s are then fed
    /// to the SAME producer rewrite machinery (`process_deletes`, which matches by path), so the
    /// rewrite/keep/drop + provenance-preservation logic is reused unchanged.
    ///
    /// Unlike `resolve_delete_paths`, there is NO missing-target validation: Java's `failMissingDeletePaths`
    /// guards only path/file deletes (`validateRequiredDeletes`), never partition drops. Replacing a
    /// partition that has no existing files is therefore a pure add (no spurious delete, no error). Returns
    /// an empty vector when `drop_partitions` is empty or the table has no current snapshot.
    pub(crate) async fn resolve_partition_deletes(
        &self,
        drop_partitions: &HashSet<(i32, Struct)>,
    ) -> Result<Vec<DataFile>> {
        if drop_partitions.is_empty() {
            return Ok(vec![]);
        }

        let mut resolved = Vec::new();
        if let Some(snapshot) = self.table.metadata().current_snapshot() {
            let manifest_list = snapshot
                .load_manifest_list(self.table.file_io(), &self.table.metadata_ref())
                .await?;

            for manifest_file in manifest_list.entries() {
                if manifest_file.content != ManifestContentType::Data {
                    continue;
                }
                let manifest = manifest_file.load_manifest(self.table.file_io()).await?;
                for entry in manifest.entries() {
                    if !entry.is_alive() {
                        continue;
                    }
                    let data_file = entry.data_file();
                    let key = (data_file.partition_spec_id, data_file.partition().clone());
                    if drop_partitions.contains(&key) {
                        resolved.push(data_file.clone());
                    }
                }
            }
        }

        Ok(resolved)
    }

    /// Resolve the LIVE data files this overwrite removes BY ROW PREDICATE, returning every file the
    /// `predicate` STRICTLY matches (all of its rows match) — the Rust port of Java
    /// `ManifestFilterManager.manifestHasDeletedFiles` + `PartitionAndMetricsEvaluator`
    /// (`core/ManifestFilterManager.java` L450-491, L583-627) for the `overwriteByRowFilter` /
    /// `deleteByRowFilter` mode. The resolved [`DataFile`]s feed the SAME [`process_deletes`] rewrite path
    /// as [`resolve_partition_deletes`] / [`resolve_delete_paths`], so the matched files drop in the one
    /// `Operation::Overwrite` snapshot alongside any explicit add/delete.
    ///
    /// **`PartitionAndMetricsEvaluator` faithfully (Java L604-626):** for each LIVE data file the predicate is
    /// reduced to its per-partition RESIDUAL via [`ResidualEvaluator::residual_for`] (Java
    /// `residualEvaluator.residualFor(partition)` — predicates the partition tuple already decides are folded
    /// to `true`/`false`), then the strict / inclusive METRICS evaluators run on THAT residual against the
    /// file's column metrics. This is what makes a partition-column predicate (e.g. `x == 0` on `identity(x)`)
    /// delete a file with no `x` column bounds: for partition `x = 0` the residual is `alwaysTrue`, which the
    /// strict-metrics evaluator trivially satisfies. Running the metrics on the FULL predicate instead would
    /// wrongly classify such a file as a partial match (no bounds ⇒ strict false, inclusive true).
    ///
    /// **Decision tree per LIVE data file (mirrors Java `manifestHasDeletedFiles` L458-487, with
    /// `markedForDelete == false` because the by-path / by-partition deletes are resolved separately):**
    /// 1. **`rowsMightMatch` (KEEP-fast):** [`InclusiveMetricsEvaluator::eval`] on the residual (Java L470,
    ///    L592-596). If NO rows can match (residual `alwaysFalse`, or metrics exclude) → **KEEP**.
    /// 2. **`rowsMustMatch` (DELETE):** [`StrictMetricsEvaluator::eval`] on the residual (Java L471,
    ///    L598-602). If ALL rows must match (`ROWS_MUST_MATCH`, residual `alwaysTrue` is trivially strict) →
    ///    **DELETE**.
    /// 3. **PARTIAL ⇒ ERROR:** might-match but NOT strictly all (Java L472-477:
    ///    `ValidationException.check(allRowsMatch || isDelete, "Cannot delete file where some, but not all,
    ///    rows match filter %s: %s", deleteExpression, file.location())` — throws for a DATA manifest where
    ///    `isDelete == false`) → return a NON-retryable [`ErrorKind::DataInvalid`] with that exact message.
    ///
    /// **Unpartitioned `alwaysTrue` ⇒ full replace:** with no partition fields the residual is the whole
    /// `alwaysTrue` filter, which strictly matches every file ⇒ every live data file is deleted (Java
    /// `deleteByRowFilter(alwaysTrue)` full-replace).
    ///
    /// The predicate is bound case-sensitive (`true`, the Iceberg/Java default; this mode has no
    /// case-sensitivity field). The residual evaluator is cached per partition-spec id (different manifests
    /// can carry different spec ids), mirroring Java's per-spec `PartitionAndMetricsEvaluator`.
    pub(crate) async fn resolve_filter_deletes(
        &self,
        predicate: &Predicate,
    ) -> Result<Vec<DataFile>> {
        let Some(snapshot) = self.table.metadata().current_snapshot() else {
            return Ok(vec![]);
        };

        let schema = self.table.metadata().current_schema().clone();
        // Bind the row predicate to the table schema once (Java `deleteExpression`). `rewrite_not` first so
        // the projection / residual visitors never see a `Not` (they reject it).
        let bound_predicate = predicate.clone().rewrite_not().bind(schema.clone(), true)?;

        // Per-partition-spec cache of the residual evaluator (Java's per-spec `PartitionAndMetricsEvaluator`).
        let mut residual_evaluators: HashMap<i32, ResidualEvaluator> = HashMap::new();

        let manifest_list = snapshot
            .load_manifest_list(self.table.file_io(), &self.table.metadata_ref())
            .await?;

        let mut resolved = Vec::new();
        for manifest_file in manifest_list.entries() {
            if manifest_file.content != ManifestContentType::Data {
                continue;
            }
            let manifest = manifest_file.load_manifest(self.table.file_io()).await?;
            for entry in manifest.entries() {
                if !entry.is_alive() {
                    continue;
                }
                let data_file = entry.data_file();

                // Reduce the predicate to its residual for this file's partition (Java
                // `residualEvaluator.residualFor(partition)`), then bind the residual to the table schema for
                // the metrics evaluators. A new spec id builds (and caches) its residual evaluator.
                let spec_id = data_file.partition_spec_id;
                let residual_evaluator = match residual_evaluators.entry(spec_id) {
                    std::collections::hash_map::Entry::Occupied(e) => e.into_mut(),
                    std::collections::hash_map::Entry::Vacant(e) => {
                        let evaluator = Self::build_residual_evaluator(
                            self.table,
                            &bound_predicate,
                            &schema,
                            spec_id,
                        )?;
                        e.insert(evaluator)
                    }
                };
                let residual = residual_evaluator
                    .residual_for(data_file.partition())?
                    .rewrite_not()
                    .bind(schema.clone(), true)?;

                // 1. `rowsMightMatch` (Java L470, L592-596): no rows can match ⇒ KEEP.
                if !InclusiveMetricsEvaluator::eval(&residual, data_file, true)? {
                    continue;
                }

                // 2. `rowsMustMatch` (Java L471, L598-602): all rows match ⇒ DELETE.
                if StrictMetricsEvaluator::eval(&residual, data_file)? {
                    resolved.push(data_file.clone());
                    continue;
                }

                // 3. PARTIAL match: might-match but NOT strictly all ⇒ non-retryable error (Java L472-477).
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Cannot delete file where some, but not all, rows match filter {predicate}: {}",
                        data_file.file_path()
                    ),
                ));
            }
        }

        Ok(resolved)
    }

    /// Build the [`ResidualEvaluator`] for `spec_id` from the bound row predicate (Java
    /// `ResidualEvaluator.of(spec, deleteExpression, caseSensitive)` inside `PartitionAndMetricsEvaluator`).
    /// An unpartitioned spec degrades to `ResidualEvaluator::unpartitioned` (every residual is the whole
    /// filter).
    fn build_residual_evaluator(
        table: &Table,
        bound_predicate: &BoundPredicate,
        schema: &Schema,
        spec_id: i32,
    ) -> Result<ResidualEvaluator> {
        let partition_spec = table
            .metadata()
            .partition_spec_by_id(spec_id)
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!("Cannot resolve filter deletes: unknown partition spec id {spec_id}"),
                )
            })?;

        ResidualEvaluator::of(
            partition_spec.clone(),
            schema,
            bound_predicate.clone(),
            true,
        )
    }

    fn generate_unique_snapshot_id(table: &Table) -> i64 {
        let generate_random_id = || -> i64 {
            let (lhs, rhs) = Uuid::new_v4().as_u64_pair();
            let snapshot_id = (lhs ^ rhs) as i64;
            if snapshot_id < 0 {
                -snapshot_id
            } else {
                snapshot_id
            }
        };
        let mut snapshot_id = generate_random_id();

        while table
            .metadata()
            .snapshots()
            .any(|s| s.snapshot_id() == snapshot_id)
        {
            snapshot_id = generate_random_id();
        }
        snapshot_id
    }

    // Check if the partition value is compatible with the partition type.
    fn validate_partition_value(
        partition_value: &Struct,
        partition_type: &StructType,
    ) -> Result<()> {
        if partition_value.fields().len() != partition_type.fields().len() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Partition value is not compatible with partition type",
            ));
        }

        for (value, field) in partition_value.fields().iter().zip(partition_type.fields()) {
            let field = field.field_type.as_primitive_type().ok_or_else(|| {
                Error::new(
                    ErrorKind::Unexpected,
                    "Partition field should only be primitive type.",
                )
            })?;
            if let Some(value) = value
                && !field.compatible(&value.as_primitive_literal().unwrap())
            {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    "Partition value is not compatible partition type",
                ));
            }
        }
        Ok(())
    }

    /// Group the added files by their own `partition_spec_id`, returning the groups in a DETERMINISTIC
    /// order — spec id DESCENDING (the established Rust per-spec convention, Java `groupBySpec` uses a
    /// `TreeMap(Comparator.reverseOrder())` for manifest merging, and `merge_append` / `rewrite_manifests`
    /// already reverse-sort their spec groups).
    ///
    /// Java groups added files into `newDataFilesBySpec` / `newDeleteFilesBySpec` (`HashMap<Integer, ...>`)
    /// and `forEach`-iterates to write per-spec manifests; HashMap iteration order is undefined, and the
    /// group order never appears in the spec-canonical metadata view (a manifest list is a SET of manifest
    /// files, compared by content, not position — same reasoning as the cluster-key lesson 2026-06-10). We
    /// pick the stable reverse-spec-id order so the on-disk manifest list is reproducible across runs.
    fn group_files_by_spec(files: Vec<DataFile>) -> Vec<(i32, Vec<DataFile>)> {
        let mut groups: HashMap<i32, Vec<DataFile>> = HashMap::new();
        for file in files {
            groups.entry(file.partition_spec_id).or_default().push(file);
        }
        let mut groups: Vec<(i32, Vec<DataFile>)> = groups.into_iter().collect();
        // Reverse spec-id order (Java `groupBySpec` TreeMap with `Comparator.reverseOrder()`).
        groups.sort_unstable_by(|(left, _), (right, _)| right.cmp(left));
        groups
    }

    // Write one DATA manifest per partition-spec group of the added data files, returning the
    // ManifestFiles for the ManifestList (Java `MergingSnapshotProducer.newDataFilesAsManifests`:
    // `newDataFilesBySpec.forEach((specId, files) -> writeDataManifests(files, ..., spec(specId)))`).
    // Each group's entries are written by a writer built under THAT group's spec, so a file added under an
    // older spec keeps its own spec id / partition type instead of being stamped under the default.
    async fn write_added_manifests(&mut self) -> Result<Vec<ManifestFile>> {
        let added_data_files = std::mem::take(&mut self.added_data_files);
        if added_data_files.is_empty() {
            return Err(Error::new(
                ErrorKind::PreconditionFailed,
                "No added data files found when write an added manifest file",
            ));
        }

        let snapshot_id = self.snapshot_id;
        let format_version = self.table.metadata().format_version();
        // When set (the `RewriteFiles.dataSequenceNumber` preservation path, Java
        // `newDataFilesDataSequenceNumber`), every added data entry carries this EXPLICIT data sequence
        // number so the manifest writer keeps it (mirrors Java `writeDataFileGroup` calling
        // `writer.add(file, dataSeq)` instead of `writer.add(file)`). V2/V3 only — V1 manifests carry no
        // sequence numbers, so on V1 this is ignored and the added entry just stamps the snapshot id.
        let new_data_seq = self.new_data_files_data_sequence_number;

        let mut manifest_files = Vec::new();
        for (partition_spec_id, group) in Self::group_files_by_spec(added_data_files) {
            let mut writer =
                self.new_cluster_manifest_writer(partition_spec_id, ManifestContentType::Data)?;
            for data_file in group {
                let builder = ManifestEntry::builder()
                    .status(crate::spec::ManifestStatus::Added)
                    .data_file(data_file);
                let entry = if format_version == FormatVersion::V1 {
                    builder.snapshot_id(snapshot_id).build()
                } else if let Some(sequence_number) = new_data_seq {
                    // Preserve the explicit data sequence number on the added entry (Java
                    // `writeDataFileGroup` with a non-null `dataSeq`). The writer keeps a non-negative
                    // explicit data seq and lets the FILE sequence number inherit at read time — matching
                    // Java `wrapAppend(snapshotId, dataSeq, file)` with a null file seq.
                    builder.sequence_number(sequence_number).build()
                } else {
                    // For format version > 1, set the snapshot id at inherited time to avoid rewriting
                    // the manifest file when a commit fails.
                    builder.build()
                };
                writer.add_entry(entry)?;
            }
            manifest_files.push(writer.write_manifest_file().await?);
        }
        Ok(manifest_files)
    }

    /// Write one DELETE manifest per partition-spec group of the added delete files, returning the
    /// [`ManifestFile`]s for the manifest list. Mirrors [`write_added_manifests`](Self::write_added_manifests)
    /// but uses the `Deletes` cluster writer (Java `MergingSnapshotProducer.newDeleteFilesAsManifests`:
    /// `newDeleteFilesBySpec.forEach((specId, files) -> writeDeleteManifests(files, spec(specId)))`). The
    /// entries are `Added` with no sequence number for V2/V3 so they inherit the new snapshot's sequence
    /// number at read time — exactly the mechanism added data files use — which makes the delete apply to
    /// earlier data (`data_seq <= delete_seq`). Each group's writer is built under THAT group's spec, so a
    /// delete file under an older spec keeps its own spec id / partition type.
    async fn write_added_delete_manifests(&mut self) -> Result<Vec<ManifestFile>> {
        let added_delete_files = std::mem::take(&mut self.added_delete_files);
        if added_delete_files.is_empty() {
            return Err(Error::new(
                ErrorKind::PreconditionFailed,
                "No added delete files found when writing an added delete manifest file",
            ));
        }

        let snapshot_id = self.snapshot_id;
        let format_version = self.table.metadata().format_version();

        let mut manifest_files = Vec::new();
        for (partition_spec_id, group) in Self::group_files_by_spec(added_delete_files) {
            let mut writer =
                self.new_cluster_manifest_writer(partition_spec_id, ManifestContentType::Deletes)?;
            for delete_file in group {
                let builder = ManifestEntry::builder()
                    .status(crate::spec::ManifestStatus::Added)
                    .data_file(delete_file);
                let entry = if format_version == FormatVersion::V1 {
                    // Position/equality deletes are V2+ concepts; a V1 table has no delete manifests.
                    builder.snapshot_id(snapshot_id).build()
                } else {
                    // For format version > 1, set the snapshot id + sequence number at inherited time so
                    // the manifest does not need rewriting on a commit retry (same as added data files).
                    builder.build()
                };
                writer.add_entry(entry)?;
            }
            manifest_files.push(writer.write_manifest_file().await?);
        }
        Ok(manifest_files)
    }

    async fn manifest_file<OP: SnapshotProduceOperation, MP: ManifestProcess>(
        &mut self,
        snapshot_produce_operation: &OP,
        manifest_process: &MP,
    ) -> Result<Vec<ManifestFile>> {
        // The files to remove were resolved in `commit()` (before `summary()`, so the summary can reflect
        // the deletes) and stored in `self.removed_data_files` / `self.removed_delete_files`. Take both
        // here and pass them as ONE set to `process_deletes`, which matches by path across the full
        // manifest list (DATA and DELETE manifests): a removed DATA file's tombstone lands in the
        // rewritten DATA manifest, a removed DELETE file's in the rewritten DELETE manifest — the Rust
        // analogue of Java composing `filterManager.filterManifests(dataManifests)` AND
        // `deleteFilterManager.filterManifests(deleteManifests)` (`MergingSnapshotProducer.apply` L977-1000).
        let mut delete_files = std::mem::take(&mut self.removed_data_files);
        let removed_delete_files = std::mem::take(&mut self.removed_delete_files);
        delete_files.extend(removed_delete_files);

        // Assert the new snapshot contributes content: added data files, added DELETE files, removed
        // (deleted) data or delete files, or added snapshot properties. An add-deletes-only commit (delete
        // files, no data files) is allowed (the merge-on-read `RowDelta` path); a delete-only data commit
        // (rewrite data manifests, no adds) is allowed; a remove-deletes-only commit (drop a superseded
        // delete file, no adds) is allowed; a truly-empty commit is not.
        //
        // TODO: Allowing snapshot property setup with no added data files is a workaround.
        // We should clean it up after all necessary actions are supported.
        // For details, please refer to https://github.com/apache/iceberg-rust/issues/1548
        if self.added_data_files.is_empty()
            && self.added_delete_files.is_empty()
            && delete_files.is_empty()
            && self.snapshot_properties.is_empty()
        {
            return Err(Error::new(
                ErrorKind::PreconditionFailed,
                "No added data files, added delete files, deleted data files, or added snapshot properties found when write a manifest file",
            ));
        }

        let existing_manifests = snapshot_produce_operation.existing_manifest(self).await?;

        // Rewrite existing manifests to remove the requested deletes (Java
        // `ManifestFilterManager.filterManifests`). Manifests that contain none of the target files are
        // carried forward unchanged.
        let mut manifest_files = self
            .process_deletes(existing_manifests, &delete_files)
            .await?;

        // Process added data entries — ONE DATA manifest per partition-spec group (Java
        // `newDataFilesBySpec.forEach(writeDataManifests)`), so a file under an older spec keeps its own
        // spec id rather than being stamped under the table default.
        if !self.added_data_files.is_empty() {
            let added_manifests = self.write_added_manifests().await?;
            manifest_files.extend(added_manifests);
        }

        // Process added DELETE entries — ONE DELETE manifest per partition-spec group (Java
        // `newDeleteFilesBySpec.forEach(writeDeleteManifests)`) for merge-on-read deletes added now.
        if !self.added_delete_files.is_empty() {
            let added_delete_manifests = self.write_added_delete_manifests().await?;
            manifest_files.extend(added_delete_manifests);
        }

        let manifest_files = manifest_process
            .process_manifests(self, manifest_files)
            .await?;
        Ok(manifest_files)
    }

    /// Rewrite the existing manifests to remove `delete_files`, mirroring Java
    /// `ManifestFilterManager.filterManifests` + `MergingSnapshotProducer.apply`'s keep rule.
    ///
    /// For each existing manifest:
    /// - if it contains at least one live entry whose path is in `delete_files`, it is rewritten:
    ///   matching live entries are marked `Deleted` (carrying their existing data file and data/file
    ///   sequence numbers; the new snapshot id is stamped), every other live entry is copied forward as
    ///   `Existing` (preserving its snapshot id and both sequence numbers — V2/V3 inheritance);
    /// - otherwise it is carried forward unchanged (efficiency + fewer files).
    ///
    /// A rewritten manifest is kept even when every live entry became `Deleted` (its `added_snapshot_id`
    /// is the new snapshot id — Java's `snapshotId() == snapshotId()` keep rule). An unrewritten manifest
    /// with no live files is dropped.
    ///
    /// Errors if any requested delete path matched no live entry in the table (mirrors Java
    /// `failMissingDeletePaths` / `validateRequiredDeletes`).
    async fn process_deletes(
        &mut self,
        existing_manifests: Vec<ManifestFile>,
        delete_files: &[DataFile],
    ) -> Result<Vec<ManifestFile>> {
        if delete_files.is_empty() {
            return Ok(existing_manifests);
        }

        let delete_paths: HashSet<&str> = delete_files
            .iter()
            .map(|df| df.file_path.as_str())
            .collect();

        // Track which requested paths were actually removed, to validate that none was missing.
        let mut deleted_paths: HashSet<String> = HashSet::new();
        let mut result_manifests = Vec::with_capacity(existing_manifests.len());

        for manifest_file in existing_manifests {
            let manifest = manifest_file.load_manifest(self.table.file_io()).await?;

            // Does any live entry in this manifest target one of the files to delete?
            let has_matching_delete = manifest
                .entries()
                .iter()
                .any(|entry| entry.is_alive() && delete_paths.contains(entry.file_path()));

            if !has_matching_delete {
                // Carry the manifest forward unchanged unless it has no live files at all.
                if manifest_file.has_added_files() || manifest_file.has_existing_files() {
                    result_manifests.push(manifest_file);
                }
                continue;
            }

            let rewritten = self
                .rewrite_manifest_with_deletes(
                    &manifest_file,
                    &manifest,
                    &delete_paths,
                    &mut deleted_paths,
                )
                .await?;
            result_manifests.push(rewritten);
        }

        // Validate that every requested delete path was found in a live entry (Java
        // `failMissingDeletePaths`).
        let missing: Vec<&str> = delete_paths
            .iter()
            .filter(|path| !deleted_paths.contains(**path))
            .copied()
            .collect();
        if !missing.is_empty() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!("Missing required files to delete: {}", missing.join(", ")),
            ));
        }

        Ok(result_manifests)
    }

    /// Write a rewritten copy of `manifest` with the entries in `delete_paths` marked `Deleted` and the
    /// rest copied forward as `Existing`. Records each removed path in `deleted_paths`.
    async fn rewrite_manifest_with_deletes(
        &mut self,
        manifest_file: &ManifestFile,
        manifest: &Manifest,
        delete_paths: &HashSet<&str>,
        deleted_paths: &mut HashSet<String>,
    ) -> Result<ManifestFile> {
        // Rewrite with the source manifest's own partition spec so the spec id / partition type of the
        // copied-forward entries is preserved (Java writes with `reader.spec()`).
        let mut writer = self.new_filtering_manifest_writer(manifest_file)?;

        for entry in manifest.entries() {
            // Already-deleted entries are informational only and are not carried forward.
            if !entry.is_alive() {
                continue;
            }

            let entry = entry.as_ref().clone();
            if delete_paths.contains(entry.file_path()) {
                deleted_paths.insert(entry.file_path().to_string());
                writer.add_delete_entry(entry)?;
            } else {
                writer.add_existing_entry(entry)?;
            }
        }

        writer.write_manifest_file().await
    }

    /// Build a manifest writer for a rewritten (filtered) manifest, using the partition spec of the
    /// source manifest so existing entries keep their spec id and partition type.
    ///
    /// **Content-keyed (the delete-side extension — read this if you touch it):** the writer's CONTENT
    /// matches the SOURCE manifest's content. A rewritten DATA manifest gets a DATA writer
    /// (`build_v2_data`/`build_v3_data`); a rewritten DELETE manifest gets a DELETE writer
    /// (`build_v2_deletes`/`build_v3_deletes`). This is REQUIRED for the merge-on-read apply-side delete
    /// removal (`RowDelta.removeDeletes`): when `process_deletes` rewrites a DELETE manifest to tombstone a
    /// superseded delete file, the rewritten manifest MUST stay a DELETE manifest (content `Deletes`,
    /// `_file_type` 1) or the manifest list misclassifies it and the read path stops applying its
    /// surviving deletes (resurrection). Java keys this on the filter manager: `DataFileFilterManager`'s
    /// `newManifestWriter` calls `newManifestWriter(spec)` (DATA), `DeleteFileFilterManager`'s calls
    /// `newDeleteManifestWriter(spec)` (DELETE) — `MergingSnapshotProducer.java` L1205/L1274. Mirroring it
    /// off `source_manifest.content` keeps a single `process_deletes` path serving both, exactly as Java's
    /// shared `ManifestFilterManager.filterManifest` does. The DATA-side behavior is UNCHANGED (a DATA
    /// source still gets `build_v2_data`/`build_v3_data`) — its existing rewrite tests are the proof.
    fn new_filtering_manifest_writer(
        &mut self,
        source_manifest: &ManifestFile,
    ) -> Result<ManifestWriter> {
        let partition_spec = self
            .table
            .metadata()
            .partition_spec_by_id(source_manifest.partition_spec_id)
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Cannot rewrite manifest: unknown partition spec id {}",
                        source_manifest.partition_spec_id
                    ),
                )
            })?
            .as_ref()
            .clone();

        let new_manifest_path = format!(
            "{}/{}/{}-m{}.{}",
            self.table.metadata().location(),
            META_ROOT_PATH,
            self.commit_uuid,
            self.manifest_counter.next().ok_or_else(|| {
                Error::new(
                    ErrorKind::Unexpected,
                    "Exhausted manifest file name counter",
                )
            })?,
            DataFileFormat::Avro
        );
        let output_file = self.table.file_io().new_output(new_manifest_path)?;
        let builder = ManifestWriterBuilder::new(
            output_file,
            Some(self.snapshot_id),
            self.key_metadata.clone(),
            self.table.metadata().current_schema().clone(),
            partition_spec,
        );
        match self.table.metadata().format_version() {
            FormatVersion::V1 => Ok(builder.build_v1()),
            FormatVersion::V2 => match source_manifest.content {
                ManifestContentType::Data => Ok(builder.build_v2_data()),
                ManifestContentType::Deletes => Ok(builder.build_v2_deletes()),
            },
            FormatVersion::V3 => match source_manifest.content {
                ManifestContentType::Data => Ok(builder.build_v3_data()),
                ManifestContentType::Deletes => Ok(builder.build_v3_deletes()),
            },
        }
    }

    /// Resolve the partition spec a summarized file belongs to (`file.partition_spec_id`), for the
    /// per-file partition-summary path (Java `SnapshotSummary.Builder` uses `spec(file.specId())`). Falls
    /// back to the table default spec only if the file's spec is somehow absent — unreachable for the
    /// add path (the added-file validation already proved every spec exists) but kept so the infallible
    /// summary path never panics.
    fn file_partition_spec(&self, file: &DataFile) -> crate::spec::PartitionSpecRef {
        let metadata = self.table.metadata();
        metadata
            .partition_spec_by_id(file.partition_spec_id)
            .cloned()
            .unwrap_or_else(|| metadata.default_partition_spec().clone())
    }

    // Returns a `Summary` of the current snapshot
    fn summary<OP: SnapshotProduceOperation>(
        &self,
        snapshot_produce_operation: &OP,
    ) -> Result<Summary> {
        let mut summary_collector = SnapshotSummaryCollector::default();
        let table_metadata = self.table.metadata_ref();

        let partition_summary_limit = if let Some(limit) = table_metadata
            .properties()
            .get(TableProperties::PROPERTY_WRITE_PARTITION_SUMMARY_LIMIT)
        {
            if let Ok(limit) = limit.parse::<u64>() {
                limit
            } else {
                TableProperties::PROPERTY_WRITE_PARTITION_SUMMARY_LIMIT_DEFAULT
            }
        } else {
            TableProperties::PROPERTY_WRITE_PARTITION_SUMMARY_LIMIT_DEFAULT
        };

        summary_collector.set_partition_summary_limit(partition_summary_limit);

        // Each file is summarized under its OWN partition spec, not the table default (Java
        // `SnapshotSummary.Builder.addedFile(spec(file.specId()), file)` →
        // `updatePartitions(spec, file)` computes the partition path with the file's own spec). On a
        // multi-spec commit (a file added under an older spec) using the default spec would compute the
        // WRONG partition path and corrupt the changed-partition summaries. `file_partition_spec` falls
        // back to the default only for the unreachable "spec vanished" case (validation already proved
        // every added file's spec exists; the summary path must stay infallible).
        for data_file in &self.added_data_files {
            summary_collector.add_file(
                data_file,
                table_metadata.current_schema().clone(),
                self.file_partition_spec(data_file),
            );
        }

        // Reflect added DELETE files (position / equality) in the summary. `add_file` branches on the
        // file's content type and increments the added-delete-file + added-position/equality-delete
        // counters (Java `MergingSnapshotProducer.add(DeleteFile)` → the delete-file summary). Empty for
        // operations that add no delete files. Summarized under each delete file's OWN spec.
        for delete_file in &self.added_delete_files {
            summary_collector.add_file(
                delete_file,
                table_metadata.current_schema().clone(),
                self.file_partition_spec(delete_file),
            );
        }

        // Reflect deleted files/records in the summary (Java overwrite/delete summary). `removed_data_files`
        // is populated in `commit()` (the resolved delete set) before `summary()` is called; it is empty
        // for add-only operations such as fast append, so this loop is a no-op there. Summarized under each
        // removed file's OWN spec (a removed file may belong to an older spec than the table default).
        for data_file in &self.removed_data_files {
            summary_collector.remove_file(
                data_file,
                table_metadata.current_schema().clone(),
                self.file_partition_spec(data_file),
            );
        }

        // Reflect removed DELETE files (position / equality / DV) in the summary. `remove_file` branches
        // on content type: a removed DV increments `removed-dvs` (D3's reachable-end-to-end branch — this
        // is the path that makes it live), a removed parquet position delete increments
        // `removed-position-delete-files`, an equality delete `removed-equality-delete-files` (Java
        // `SnapshotSummary.UpdateMetrics.removedFile`). `removed_delete_files` is populated in `commit()`
        // (the resolved removal set) before `summary()`; empty for every operation that removes no delete
        // files, so this loop is a no-op there. Summarized under each removed delete file's OWN spec.
        for delete_file in &self.removed_delete_files {
            summary_collector.remove_file(
                delete_file,
                table_metadata.current_schema().clone(),
                self.file_partition_spec(delete_file),
            );
        }

        // The previous snapshot is the current branch head (the parent of the snapshot being produced):
        // at summary time the new snapshot is not yet in `table_metadata`, so its totals are seeded from
        // the current snapshot's summary. Mirrors Java `SnapshotProducer.summary(previous)` which reads
        // `previous.snapshot(previousBranchHead.snapshotId()).summary()`. (Looking up `self.snapshot_id`
        // here would always miss — the new snapshot does not exist yet — leaving totals seeded from zero,
        // which underflows the moment an operation removes more files than it adds.)
        let previous_snapshot = table_metadata.current_snapshot();

        let mut additional_properties = summary_collector.build();
        additional_properties.extend(self.snapshot_properties.clone());

        let summary = Summary {
            operation: snapshot_produce_operation.operation(),
            additional_properties,
        };

        // Compute totals as previous + added - removed for ALL operations, mirroring Java
        // `SnapshotProducer.summary(previous)` (which calls `updateTotal` unconditionally and has NO
        // full-table-truncate branch). `OverwriteFilesAction` is a PARTIAL overwrite (delete some, add
        // some), so its totals must NOT be reset to zero. The Rust-specific `truncate_full_table` path
        // (which zeroes totals + reports every prior file as deleted) is for a future full-table
        // replace/truncate action, not for a partial overwrite — so pass `false` here.
        update_snapshot_summaries(summary, previous_snapshot.map(|s| s.summary()), false)
    }

    fn generate_manifest_list_file_path(&self, attempt: i64) -> String {
        format!(
            "{}/{}/snap-{}-{}-{}.{}",
            self.table.metadata().location(),
            META_ROOT_PATH,
            self.snapshot_id,
            attempt,
            self.commit_uuid,
            DataFileFormat::Avro
        )
    }

    /// Finished building the action and return the [`ActionCommit`] to the transaction.
    pub(crate) async fn commit<OP: SnapshotProduceOperation, MP: ManifestProcess>(
        mut self,
        snapshot_produce_operation: OP,
        process: MP,
    ) -> Result<ActionCommit> {
        // Resolve the data files this operation removes up front (before `summary()`), so the snapshot
        // summary can reflect the deleted file/record counts and `manifest_file()` can reuse the result
        // without re-resolving. Empty for add-only operations (e.g. fast append).
        self.removed_data_files = snapshot_produce_operation.delete_files(&self).await?;

        // Resolve the DELETE files this operation removes against the current snapshot's DELETE manifests by
        // path (the apply-side `RowDelta.removeDeletes` path). Re-binding `self.removed_delete_files` to the
        // RESOLVED set (a) validates every requested removal is a live delete file (missing path fails loud)
        // and (b) replaces the caller-supplied (possibly-stale) `DataFile`s with the ON-DISK entries, so the
        // summary's `remove_file` reads the committed metadata. Empty when no delete files are removed.
        if !self.removed_delete_files.is_empty() {
            let requested_paths: HashSet<String> = self
                .removed_delete_files
                .iter()
                .map(|file| file.file_path().to_string())
                .collect();
            self.removed_delete_files = self.resolve_delete_file_paths(&requested_paths).await?;
        }

        let manifest_list_path = self.generate_manifest_list_file_path(0);
        let next_seq_num = self.table.metadata().next_sequence_number();
        let first_row_id = self.table.metadata().next_row_id();
        let mut manifest_list_writer = match self.table.metadata().format_version() {
            FormatVersion::V1 => ManifestListWriter::v1(
                self.table
                    .file_io()
                    .new_output(manifest_list_path.clone())?,
                self.snapshot_id,
                self.table.metadata().current_snapshot_id(),
            ),
            FormatVersion::V2 => ManifestListWriter::v2(
                self.table
                    .file_io()
                    .new_output(manifest_list_path.clone())?,
                self.snapshot_id,
                self.table.metadata().current_snapshot_id(),
                next_seq_num,
            ),
            FormatVersion::V3 => ManifestListWriter::v3(
                self.table
                    .file_io()
                    .new_output(manifest_list_path.clone())?,
                self.snapshot_id,
                self.table.metadata().current_snapshot_id(),
                next_seq_num,
                Some(first_row_id),
            ),
        };

        // Calling self.summary() before self.manifest_file() is important because self.added_data_files
        // will be set to an empty vec after self.manifest_file() returns, resulting in an empty summary
        // being generated.
        let summary = self.summary(&snapshot_produce_operation).map_err(|err| {
            Error::new(ErrorKind::Unexpected, "Failed to create snapshot summary.").with_source(err)
        })?;

        let new_manifests = self
            .manifest_file(&snapshot_produce_operation, &process)
            .await?;

        manifest_list_writer.add_manifests(new_manifests.into_iter())?;
        let writer_next_row_id = manifest_list_writer.next_row_id();
        manifest_list_writer.close().await?;

        let commit_ts = chrono::Utc::now().timestamp_millis();
        let new_snapshot = Snapshot::builder()
            .with_manifest_list(manifest_list_path)
            .with_snapshot_id(self.snapshot_id)
            .with_parent_snapshot_id(self.table.metadata().current_snapshot_id())
            .with_sequence_number(next_seq_num)
            .with_summary(summary)
            .with_schema_id(self.table.metadata().current_schema_id())
            .with_timestamp_ms(commit_ts);

        let new_snapshot = if let Some(writer_next_row_id) = writer_next_row_id {
            let assigned_rows = writer_next_row_id - self.table.metadata().next_row_id();
            new_snapshot
                .with_row_range(first_row_id, assigned_rows)
                .build()
        } else {
            new_snapshot.build()
        };

        let updates = vec![
            TableUpdate::AddSnapshot {
                snapshot: new_snapshot,
            },
            TableUpdate::SetSnapshotRef {
                ref_name: MAIN_BRANCH.to_string(),
                reference: SnapshotReference::new(
                    self.snapshot_id,
                    SnapshotRetention::branch(None, None, None),
                ),
            },
        ];

        let requirements = vec![
            TableRequirement::UuidMatch {
                uuid: self.table.metadata().uuid(),
            },
            TableRequirement::RefSnapshotIdMatch {
                r#ref: MAIN_BRANCH.to_string(),
                snapshot_id: self.table.metadata().current_snapshot_id(),
            },
        ];

        Ok(ActionCommit::new(updates, requirements))
    }
}

/// Render a delete file in Java's deletion-vector description format — the Rust port of
/// `ContentFileUtil.dvDesc` (`core/.../util/ContentFileUtil.java` L150-157, 1.10.0-bytecode-verified):
/// `DV{location=%s, offset=%s, length=%s, referencedDataFile=%s}`. Java formats the nullable
/// `contentOffset` / `contentSizeInBytes` / `referencedDataFile` with `%s`, which renders a missing
/// value as `null` — mirrored here (NOT Rust's `Some(..)`/`None` debug rendering), so the
/// gate/validation messages are byte-identical to Java's.
pub(crate) fn dv_desc(delete_file: &DataFile) -> String {
    fn opt_to_java<T: std::fmt::Display>(value: Option<T>) -> String {
        value.map_or_else(|| "null".to_string(), |v| v.to_string())
    }
    format!(
        "DV{{location={}, offset={}, length={}, referencedDataFile={}}}",
        delete_file.file_path(),
        opt_to_java(delete_file.content_offset()),
        opt_to_java(delete_file.content_size_in_bytes()),
        opt_to_java(delete_file.referenced_data_file()),
    )
}

/// The format-version gate for an added DELETE file — the Rust port of Java
/// `MergingSnapshotProducer.validateDeleteFileForVersion` (`core/MergingSnapshotProducer.java`
/// L295-316; verified against the 1.10.0 BYTECODE, where the switch is inlined into
/// `validateNewDeleteFile` with cases 1-4):
///
/// - **V1:** delete files do not exist — `"Deletes are supported in V2 and above"`.
/// - **V2:** equality deletes OK; a position delete must NOT be a deletion vector
///   (`!ContentFileUtil.isDV`, i.e. not Puffin format) — `"Must not use DVs for position deletes
///   in V2: %s"` with [`dv_desc`].
/// - **V3 (and Java's V4):** equality deletes OK; a position delete MUST be a deletion vector —
///   `"Must use DVs for position deletes in V%s: %s"` with the format version + the file location.
///
/// Equality deletes are exempt at EVERY version ≥ 2 (both arms test
/// `content() == EQUALITY_DELETES` first). A wrongly-gated DV commit corrupts merge-on-read tables
/// for every engine: a V2 reader cannot load a Puffin DV, and a V3 table mixing fresh parquet
/// position deletes with DVs breaks the DV-supersedes-position-deletes read precedence.
///
/// Returns a NON-retryable [`ErrorKind::DataInvalid`] (Java throws `IllegalArgumentException` from
/// `Preconditions.checkArgument` — also non-retryable), so the commit retry loop stops.
fn validate_delete_file_for_version(
    delete_file: &DataFile,
    format_version: FormatVersion,
) -> Result<()> {
    use crate::delete_file_index::is_deletion_vector;
    use crate::spec::DataContentType;

    let is_equality_delete = delete_file.content_type() == DataContentType::EqualityDeletes;
    match format_version {
        FormatVersion::V1 => Err(Error::new(
            ErrorKind::DataInvalid,
            "Deletes are supported in V2 and above",
        )),
        FormatVersion::V2 => {
            if is_equality_delete || !is_deletion_vector(delete_file) {
                Ok(())
            } else {
                Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Must not use DVs for position deletes in V2: {}",
                        dv_desc(delete_file)
                    ),
                ))
            }
        }
        FormatVersion::V3 => {
            if is_equality_delete || is_deletion_vector(delete_file) {
                Ok(())
            } else {
                Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Must use DVs for position deletes in V{}: {}",
                        format_version as u8,
                        delete_file.file_path()
                    ),
                ))
            }
        }
    }
}

/// Operations whose snapshots can ADD data files — the only ones a "no conflicting data" validation needs
/// to inspect (Java `MergingSnapshotProducer.VALIDATE_ADDED_FILES_OPERATIONS = {APPEND, OVERWRITE}`). A
/// `Delete` / `Replace` snapshot never introduces brand-new conflicting rows.
fn operation_adds_data_files(operation: &Operation) -> bool {
    matches!(operation, Operation::Append | Operation::Overwrite)
}

/// Operations whose snapshots can ADD delete files — the only ones a "no conflicting delete" validation
/// needs to inspect (Java `MergingSnapshotProducer.VALIDATE_ADDED_DELETE_FILES_OPERATIONS = {OVERWRITE,
/// DELETE}`). Note this differs from [`operation_adds_data_files`] (`{APPEND, OVERWRITE}`): an `Append`
/// snapshot never adds delete files, while a `Delete` snapshot (a pure merge-on-read delete commit) does.
fn operation_adds_delete_files(operation: &Operation) -> bool {
    matches!(operation, Operation::Overwrite | Operation::Delete)
}

/// Operations whose snapshots can ADD deletion vectors — the op set the `validateAddedDVs` walk
/// inspects (Java `MergingSnapshotProducer.VALIDATE_ADDED_DVS_OPERATIONS = {OVERWRITE, DELETE,
/// REPLACE}`, L84-85; 1.10.0-bytecode-verified: `ImmutableSet.of("overwrite", "delete",
/// "replace")`).
///
/// NOTE this is STRICTLY WIDER than [`operation_adds_delete_files`] (`{Overwrite, Delete}`): a
/// REPLACE (compaction) snapshot can rewrite deletion vectors — Java's `RewriteDataFiles` writes
/// fresh DVs for the compacted data under `DataOperations.REPLACE` — so the DV conflict check must
/// inspect REPLACE snapshots too. `Operation::Replace` IS representable in Rust (the
/// `rewrite_files` / `rewrite_manifests` actions record it), so dropping it here would silently
/// miss a concurrent Java- or future-Rust-written REPLACE snapshot that added a DV for the same
/// referenced data file.
fn operation_adds_dvs(operation: &Operation) -> bool {
    matches!(
        operation,
        Operation::Overwrite | Operation::Delete | Operation::Replace
    )
}

/// Enumerate the DELETE files ADDED to `table` by snapshots committed AFTER `starting_snapshot_id`,
/// gated to the operations that can add DELETION VECTORS — the walk behind `RowDelta`'s
/// `validateAddedDVs` (Java `MergingSnapshotProducer.validateAddedDVs` L835-841 calling
/// `validationHistory(base, startingSnapshotId, VALIDATE_ADDED_DVS_OPERATIONS,
/// ManifestContent.DELETES, parent)`).
///
/// Same [`files_after`] walk semantics as [`added_delete_files_after`] (DELETE manifests the
/// snapshot itself wrote, `ManifestStatus::Added` entries, inclusive of the current snapshot,
/// exclusive of the starting snapshot) — the ONLY difference is the op set:
/// [`operation_adds_dvs`] = `{Overwrite, Delete, Replace}` instead of `{Overwrite, Delete}`
/// (Java's `VALIDATE_ADDED_DVS_OPERATIONS` vs `VALIDATE_ADDED_DELETE_FILES_OPERATIONS`). The
/// caller filters the result to DVs (`is_deletion_vector`) and applies the conflict test; the
/// non-DV entries a REPLACE snapshot might carry are returned here but never collide (the caller
/// skips non-Puffin files), mirroring Java reading the whole delete manifest and testing
/// `ContentFileUtil.isDV` per entry.
///
/// No format-version guard: Java's `validateAddedDVs` has none (the caller's
/// `dvsByReferencedFile.isEmpty()` self-skip means the walk only ever runs when this operation
/// adds DVs, which the version gate already restricts to V3+ tables).
pub(crate) async fn added_dv_candidate_delete_files_after(
    table: &Table,
    starting_snapshot_id: Option<i64>,
) -> Result<Vec<DataFile>> {
    files_after(
        table,
        starting_snapshot_id,
        ManifestContentType::Deletes,
        operation_adds_dvs,
        ManifestStatus::Added,
    )
    .await
}

/// Operations whose snapshots can REMOVE data files — the only ones a "data files still exist" validation
/// needs to inspect (Java `MergingSnapshotProducer.VALIDATE_DATA_FILES_EXIST_OPERATIONS = {OVERWRITE,
/// REPLACE, DELETE}`). An `Append` snapshot never removes a live data file, so it is not inspected.
///
/// Java's set has THREE members; the Rust [`Operation`] enum has no `Replace` variant (a `ReplacePartitions`
/// commit records `Operation::Overwrite`, and a rewrite/compaction is not yet a distinct operation here), so
/// only `{Overwrite, Delete}` are representable. This is faithful to every operation Rust can currently
/// produce — Rust never records a `REPLACE` snapshot, so there is nothing of that operation to miss.
///
/// This is the `skipDeletes == false` variant; see [`operation_removes_data_files_skip_deletes`] for the
/// `skipDeletes == true` variant Java's `RowDelta` uses by default.
fn operation_removes_data_files(operation: &Operation) -> bool {
    matches!(operation, Operation::Overwrite | Operation::Delete)
}

/// The `skipDeletes == true` variant of [`operation_removes_data_files`] — the operations whose snapshots can
/// remove data files when DELETE-op snapshots are EXCLUDED (Java
/// `MergingSnapshotProducer.VALIDATE_DATA_FILES_EXIST_SKIP_DELETE_OPERATIONS = {OVERWRITE, REPLACE}`).
///
/// Java drops `DELETE` from the set so that a concurrent merge-on-read DELETE-op snapshot (which produces
/// `Deleted` tombstones for the files it removed) does NOT trip the files-exist check — this is what
/// `BaseRowDelta` uses by DEFAULT (its `validateDeletes` flag is `false` unless `validateDeletedFiles()` is
/// called, and it passes `skipDeletes = !validateDeletes = true`). With `REPLACE` unrepresentable in the Rust
/// [`Operation`] enum (a rewrite is not yet a distinct op), only `{Overwrite}` is representable here — faithful
/// to every operation Rust can produce.
fn operation_removes_data_files_skip_deletes(operation: &Operation) -> bool {
    matches!(operation, Operation::Overwrite)
}

/// Enumerate the files of a given manifest `content` that snapshots committed AFTER `starting_snapshot_id`
/// recorded with status `status_to_keep` — the shared walk behind [`added_data_files_after`] /
/// [`added_delete_files_after`] (DATA / DELETE manifests, `ManifestStatus::Added` entries) and
/// [`deleted_data_files_after`] (DATA manifests, `ManifestStatus::Deleted` tombstones).
///
/// This is the Rust port of Java `MergingSnapshotProducer.validationHistory` + the per-check `ManifestGroup`
/// entry filter (`core/MergingSnapshotProducer.java`): it walks the parent chain of `table`'s current
/// snapshot (the refreshed base / Java `parent`) back via `parent_snapshot_id`, INCLUSIVE of the current
/// snapshot and EXCLUSIVE of `starting_snapshot_id` (Java `SnapshotUtil.ancestorsBetween(parent.snapshotId(),
/// startingSnapshotId)`). For each visited snapshot whose operation passes `operation_filter` (Java's
/// per-validation operation set — `VALIDATE_ADDED_FILES_OPERATIONS` for added data,
/// `VALIDATE_ADDED_DELETE_FILES_OPERATIONS` for added deletes, `VALIDATE_DATA_FILES_EXIST_OPERATIONS` for
/// removed data), it loads that snapshot's manifest list, keeps the manifests of `content` that it WROTE
/// (`manifest.added_snapshot_id == snapshot.snapshot_id()`, Java `manifest.snapshotId() ==
/// currentSnapshot.snapshotId()`), and collects every entry whose status equals `status_to_keep`.
///
/// The `status_to_keep` axis selects the per-check entry filter:
/// - `ManifestStatus::Added` ⇒ files ADDED by the concurrent snapshots (Java `ignoreDeleted().ignoreExisting()`
///   keeping `Status.ADDED`) — the data/delete *conflict* checks.
/// - `ManifestStatus::Deleted` ⇒ files DELETED by the concurrent snapshots (Java `deletedDataFiles` keeps
///   `entry.status() == DELETED`, with `ignoreExisting()`) — the `validateDataFilesExist` check.
///
/// A concurrent delete/overwrite records its removals as `Deleted` tombstones in a manifest it itself wrote
/// (`rewrite_manifest_with_deletes` stamps the new snapshot id as `added_snapshot_id`), so the
/// `added_snapshot_id == snapshot_id` manifest filter finds those tombstones — exactly as it finds a
/// snapshot's `Added` entries.
///
/// Both DATA and DELETE files are carried in manifest entries as [`DataFile`]s, distinguished by their
/// content type.
///
/// `starting_snapshot_id == None` means "validate from the beginning of history" — every ancestor of the
/// current snapshot is inspected (Java passes a null starting id to `ancestorsBetween`, which walks to the
/// root). When the current snapshot already IS `starting_snapshot_id` (no concurrent commit landed), the walk
/// yields nothing. A table with no current snapshot likewise yields nothing.
async fn files_after(
    table: &Table,
    starting_snapshot_id: Option<i64>,
    content: ManifestContentType,
    operation_filter: fn(&Operation) -> bool,
    status_to_keep: ManifestStatus,
) -> Result<Vec<DataFile>> {
    let metadata = table.metadata();

    // The "parent" of the operation in Java terms: the current head of the refreshed base. If there is no
    // current snapshot, nothing has been added.
    let Some(mut current) = metadata.current_snapshot().cloned() else {
        return Ok(vec![]);
    };

    let mut collected = Vec::new();

    loop {
        // Java `ancestorsBetween` is EXCLUSIVE of the starting snapshot: stop before re-visiting it (and
        // never inspect the snapshot the operation started from — its files are part of the base, not a
        // concurrent commit).
        if Some(current.snapshot_id()) == starting_snapshot_id {
            break;
        }

        if operation_filter(&current.summary().operation) {
            let manifest_list = current
                .load_manifest_list(table.file_io(), metadata)
                .await?;
            for manifest_file in manifest_list.entries() {
                // Only manifests of the requested `content` that THIS snapshot wrote (Java
                // `manifest.snapshotId() == currentSnapshot.snapshotId()`) — carried-forward manifests
                // belong to older snapshots and their files were not added/removed since the starting
                // snapshot. A delete/overwrite's rewritten manifest (carrying its `Deleted` tombstones)
                // also has `added_snapshot_id == snapshot.snapshot_id()`, so it is included here.
                if manifest_file.content != content
                    || manifest_file.added_snapshot_id != current.snapshot_id()
                {
                    continue;
                }
                let manifest = manifest_file.load_manifest(table.file_io()).await?;
                for entry in manifest.entries() {
                    // Keep only entries of the requested status (the per-check axis): `Added` for the
                    // conflict checks (Java `ignoreDeleted().ignoreExisting()` keeping `Status.ADDED`) or
                    // `Deleted` for the files-exist check (Java `deletedDataFiles` keeping `Status.DELETED`,
                    // with `ignoreExisting()`). An `Existing` entry was added by an earlier snapshot and
                    // copied forward, so it is never the relevant status here.
                    if entry.status() == status_to_keep {
                        collected.push(entry.data_file().clone());
                    }
                }
            }
        }

        // Walk to the parent; stop at the root. A missing parent (dangling id) also terminates the walk,
        // mirroring Java `ancestorsOf` returning when `lookup.apply(parentId)` is null.
        match current.parent_snapshot_id() {
            Some(parent_id) => match metadata.snapshot_by_id(parent_id) {
                Some(parent) => current = parent.clone(),
                None => break,
            },
            None => break,
        }
    }

    Ok(collected)
}

/// Enumerate the DATA files ADDED to `table` by snapshots committed AFTER `starting_snapshot_id` — the
/// concurrent commits a serializable-isolation conflict check must inspect.
///
/// This is the Rust port of Java `MergingSnapshotProducer.addedDataFiles` + `validationHistory`
/// (`core/MergingSnapshotProducer.java`): the shared [`files_after`] walk over DATA manifests, gated
/// to the operations that can add data ([`operation_adds_data_files`] = Java
/// `VALIDATE_ADDED_FILES_OPERATIONS = {APPEND, OVERWRITE}`), keeping `ManifestStatus::Added` entries. See
/// [`files_after`] for the walk semantics (inclusive of the current snapshot, exclusive of the starting
/// snapshot, only manifests the snapshot itself wrote, only entries of the requested status).
///
/// This is the shared foundation the per-action data-file conflict validations (`ReplacePartitions`
/// `validateNoConflictingData`, `OverwriteFiles` / `RowDelta` `validateNoConflictingDataFiles`) build on.
pub(crate) async fn added_data_files_after(
    table: &Table,
    starting_snapshot_id: Option<i64>,
) -> Result<Vec<DataFile>> {
    files_after(
        table,
        starting_snapshot_id,
        ManifestContentType::Data,
        operation_adds_data_files,
        ManifestStatus::Added,
    )
    .await
}

/// Enumerate the DELETE files (position / equality deletes) ADDED to `table` by snapshots committed AFTER
/// `starting_snapshot_id` — the concurrent commits a `validateNoConflictingDeleteFiles` check must inspect.
///
/// This is the Rust port of Java `MergingSnapshotProducer.addedDeleteFiles`
/// (`core/MergingSnapshotProducer.java` L601-625): the shared [`files_after`] walk over DELETE
/// manifests, gated to the operations that can add delete files ([`operation_adds_delete_files`] = Java
/// `VALIDATE_ADDED_DELETE_FILES_OPERATIONS = {OVERWRITE, DELETE}`), keeping `ManifestStatus::Added` entries.
///
/// **V2 guard (Java `base.formatVersion() < 2` ⇒ empty `DeleteFileIndex`):** delete files do not exist
/// before format version 2, so on a V1 table this returns an empty set without walking the history.
///
/// **Over-scan vs Java (documented):** Java's `addedDeleteFiles` additionally builds a `DeleteFileIndex`
/// filtered by the operation's `startingSequenceNumber` (a delete file with `sequence_number <
/// startingSequenceNumber` cannot apply to the rows being committed). This port enumerates the
/// concurrently-added delete files by the snapshot walk alone; the per-file inclusive-metrics filter is
/// applied later in [`validate_no_conflicting_added_delete_files`]. Omitting the sequence-number refinement
/// is a CONSERVATIVE over-scan — it can only consider MORE delete files (over-reject), never fewer
/// (under-reject) — the same class as the manifest-summary pre-filter deferral elsewhere.
pub(crate) async fn added_delete_files_after(
    table: &Table,
    starting_snapshot_id: Option<i64>,
) -> Result<Vec<DataFile>> {
    // V2 guard (Java `addedDeleteFiles`: `base.formatVersion() < 2` ⇒ empty). Delete files don't exist in
    // V1, so there is nothing to enumerate and no history to walk.
    if table.metadata().format_version() < FormatVersion::V2 {
        return Ok(vec![]);
    }

    files_after(
        table,
        starting_snapshot_id,
        ManifestContentType::Deletes,
        operation_adds_delete_files,
        ManifestStatus::Added,
    )
    .await
}

/// Enumerate the DELETE files (position / equality deletes) ADDED to `table` by snapshots committed AFTER
/// `starting_snapshot_id`, PAIRED with each entry's data sequence number — the sequence-preserving sibling
/// of [`added_delete_files_after`].
///
/// [`added_delete_files_after`] deliberately strips the manifest entry's sequence number (returning bare
/// [`DataFile`]s), which is all the metrics-only conflict checks need. But Java
/// `MergingSnapshotProducer.validateNoNewDeletesForDataFiles` builds a `DeleteFileIndex` whose
/// `forDataFile(startingSequenceNumber, dataFile)` compares each delete's DATA sequence number against the
/// operation's `startingSequenceNumber` (`DeleteFileIndex.PositionDeletes.filter`/`EqualityDeletes.filter`
/// keep `data_seq >= startingSequenceNumber`). That comparison needs the sequence number, so this variant
/// preserves it (`entry.sequence_number()`).
///
/// Same walk semantics as [`added_delete_files_after`]: the V2 guard (delete files do not exist before
/// format version 2 — Java `addedDeleteFiles`), the DELETE-manifest walk gated to the operations that can
/// add delete files ([`operation_adds_delete_files`] = Java `VALIDATE_ADDED_DELETE_FILES_OPERATIONS =
/// {OVERWRITE, DELETE}`), keeping `ManifestStatus::Added` entries, inclusive of the current snapshot and
/// exclusive of the starting snapshot. The per-entry `Option<i64>` is the data sequence number a V2/V3
/// added delete inherits from its committing snapshot (always strictly greater than any pre-start data file's
/// sequence number, so in practice the partition match is the load-bearing test — but the comparison is
/// preserved for faithfulness to Java).
async fn added_delete_files_with_seq_after(
    table: &Table,
    starting_snapshot_id: Option<i64>,
) -> Result<Vec<(DataFile, Option<i64>)>> {
    let metadata = table.metadata();

    // V2 guard (Java `addedDeleteFiles`: `base.formatVersion() < 2` ⇒ empty `DeleteFileIndex`).
    if metadata.format_version() < FormatVersion::V2 {
        return Ok(vec![]);
    }

    // The "parent" of the operation in Java terms: the current head of the refreshed base.
    let Some(mut current) = metadata.current_snapshot().cloned() else {
        return Ok(vec![]);
    };

    let mut collected = Vec::new();

    loop {
        // Java `ancestorsBetween` is EXCLUSIVE of the starting snapshot (mirrors [`files_after`]).
        if Some(current.snapshot_id()) == starting_snapshot_id {
            break;
        }

        if operation_adds_delete_files(&current.summary().operation) {
            let manifest_list = current
                .load_manifest_list(table.file_io(), metadata)
                .await?;
            for manifest_file in manifest_list.entries() {
                // Only DELETE manifests THIS snapshot wrote (Java `manifest.snapshotId() ==
                // currentSnapshot.snapshotId()`) — mirrors the manifest filter in [`files_after`].
                if manifest_file.content != ManifestContentType::Deletes
                    || manifest_file.added_snapshot_id != current.snapshot_id()
                {
                    continue;
                }
                let manifest = manifest_file.load_manifest(table.file_io()).await?;
                for entry in manifest.entries() {
                    if entry.status() == ManifestStatus::Added {
                        collected.push((entry.data_file().clone(), entry.sequence_number()));
                    }
                }
            }
        }

        // Walk to the parent; stop at the root or a dangling parent id (mirrors [`files_after`]).
        match current.parent_snapshot_id() {
            Some(parent_id) => match metadata.snapshot_by_id(parent_id) {
                Some(parent) => current = parent.clone(),
                None => break,
            },
            None => break,
        }
    }

    Ok(collected)
}

/// The sequence number of the snapshot the operation started from, or `0` if there is none — the Rust port
/// of Java `MergingSnapshotProducer.startingSequenceNumber` (`core/MergingSnapshotProducer.java` L741-748).
///
/// Java: when `startingSnapshotId` is non-null AND present in the metadata, return that snapshot's sequence
/// number; otherwise return `TableMetadata.INITIAL_SEQUENCE_NUMBER` (= 0). The `0` literal here IS
/// `INITIAL_SEQUENCE_NUMBER` (`spec::table_metadata::INITIAL_SEQUENCE_NUMBER`, a `pub(crate)` constant equal
/// to 0); it is inlined to avoid widening the spec module's export surface.
fn starting_sequence_number(table: &Table, starting_snapshot_id: Option<i64>) -> i64 {
    match starting_snapshot_id {
        Some(id) => table
            .metadata()
            .snapshot_by_id(id)
            .map_or(0, |snapshot| snapshot.sequence_number()),
        None => 0,
    }
}

/// Reject the commit if any DELETE file ADDED by a concurrent commit since `starting_snapshot_id` APPLIES to
/// one of the DATA files this operation REMOVES — the serializable-isolation guard that you cannot drop a
/// data file out from under a concurrent row-level delete (Java
/// `MergingSnapshotProducer.validateNoNewDeletesForDataFiles`, `core/MergingSnapshotProducer.java`
/// L519-551). Shared by `OverwriteFiles` (the `!deletedDataFiles.isEmpty()` branch of
/// `BaseOverwriteFiles.validate`) and, in a later increment, `RowDelta`.
///
/// **V2 guard (Java L526-528):** if there is no current snapshot (`parent == null`) or the table is below
/// format version 2 (`base.formatVersion() < 2`), no delete files can exist, so this is a no-op `Ok(())`.
///
/// **Enumerate concurrently-added deletes (Java L530):** the concurrently-added DELETE files are gathered via
/// [`added_delete_files_with_seq_after`] (the DELETE-manifest walk + the V2 guard), then optionally narrowed
/// by `conflict_filter` with the existing [`InclusiveMetricsEvaluator`] — mirroring Java passing `dataFilter`
/// into `addedDeleteFiles` (a delete file whose metrics cannot match the filter cannot conflict). `None` ⇒
/// no metrics narrowing (every concurrently-added delete is a candidate — the conservative default).
///
/// **Starting sequence number (Java L533):** [`starting_sequence_number`] — the sequence number of the
/// starting snapshot, or 0 when there is none.
///
/// **Applicability — mirrors Java `DeleteFileIndex.forDataFile(startingSequenceNumber, dataFile)`
/// (`core/DeleteFileIndex.java` L151-167):** a concurrently-added delete applies to a removed data file iff
/// 1. its DATA sequence number is `>= startingSequenceNumber` (Java `PositionDeletes.filter(seq)` /
///    `EqualityDeletes.filter(seq, file)` keep entries at index `findStartIndex(seqs, seq)`, i.e.
///    `data_seq >= seq`; `seq == startingSequenceNumber` here). A concurrently-ADDED delete inherits its
///    snapshot's sequence number, so this is effectively always true — the partition test below is the
///    load-bearing one — but the comparison is kept for faithfulness; AND
/// 2. it MATCHES the data file by partition: same `partition_spec_id` AND equal partition tuple
///    (`posDeletesByPartition.get(specId, partition)` / `eqDeletesByPartition.get(specId, partition)`). A
///    partition-scoped position delete (no `referenced_data_file`) and an equality delete both match by
///    partition; a path-scoped position delete (`referenced_data_file == Some(path)`) additionally matches
///    only the data file at that exact path (Java `findPathDeletes` keyed on `dataFile.location()`).
///    Global (unpartitioned) equality deletes apply to ANY data file (Java `findGlobalDeletes`).
///
/// The applicability test is implemented DIRECTLY here rather than via [`crate::delete_file_index`]'s
/// `PopulatedDeleteFileIndex`: that index keys on the SCAN-time semantics (it compares against the DATA
/// file's OWN sequence number and requires `DeleteFileContext`/`ManifestEntry` plumbing the snapshot walk
/// does not produce), whereas this validation compares against the operation's `startingSequenceNumber`. The
/// direct test is self-contained and cites the Java `forDataFile` semantics line-for-line above.
///
/// **`ignore_equality_deletes` (Java L538-548):** when `true`, only POSITION deletes count as a conflict
/// (Java keeps the commit unless an applicable delete is a `POSITION_DELETES` — the "found new position
/// delete for replaced data file" message); when `false`, ANY applicable delete is a conflict (the "found
/// new delete for replaced data file" message). `OverwriteFiles` passes `false`.
///
/// On the FIRST conflicting data file this returns a NON-retryable [`ErrorKind::DataInvalid`] error matching
/// Java's message ("Cannot commit, found new delete for replaced data file: <path>" /
/// "...found new position delete..."), so the commit retry loop stops and the error propagates (Java's
/// non-retryable `ValidationException`).
pub(crate) async fn validate_no_new_deletes_for_data_files(
    table: &Table,
    starting_snapshot_id: Option<i64>,
    conflict_filter: Option<&Predicate>,
    data_files: &[DataFile],
    ignore_equality_deletes: bool,
) -> Result<()> {
    // Java L526-528: no current table state (`parent == null`) or a pre-V2 table ⇒ no delete files exist.
    if table.metadata().current_snapshot().is_none()
        || table.metadata().format_version() < FormatVersion::V2
    {
        return Ok(());
    }

    // Java L530: the DELETE files concurrently added since the start (with their data sequence numbers).
    let added_deletes = added_delete_files_with_seq_after(table, starting_snapshot_id).await?;
    if added_deletes.is_empty() {
        return Ok(());
    }

    // Java passes `dataFilter` into `addedDeleteFiles`: a delete whose metrics cannot match the conflict
    // filter cannot conflict. Bind the filter ONCE (None ⇒ no narrowing — every added delete is a candidate).
    let bound_filter = match conflict_filter {
        Some(filter) => Some(
            filter
                .clone()
                .bind(table.metadata().current_schema().clone(), true)?,
        ),
        None => None,
    };

    // Java L533: the sequence number of the starting snapshot (or 0 if none).
    let starting_sequence_number = starting_sequence_number(table, starting_snapshot_id);

    for data_file in data_files {
        // Java L536: `deletes.forDataFile(startingSequenceNumber, dataFile)` — the applicable concurrently
        // -added deletes. We compute applicability inline (see the doc comment) and branch on
        // `ignore_equality_deletes` per Java L538-548 on the first applicable delete.
        for (delete_file, delete_seq) in &added_deletes {
            // Metrics narrowing (Java `addedDeleteFiles(dataFilter)`): skip a delete whose metrics cannot
            // match the conflict filter.
            if let Some(bound_filter) = &bound_filter
                && !InclusiveMetricsEvaluator::eval(bound_filter, delete_file, true)?
            {
                continue;
            }

            if !delete_applies_to_data_file(
                delete_file,
                *delete_seq,
                data_file,
                starting_sequence_number,
            ) {
                continue;
            }

            let is_position_delete =
                delete_file.content_type() == crate::spec::DataContentType::PositionDeletes;

            if ignore_equality_deletes {
                // Java L538-543: only POSITION deletes are a conflict when equality deletes are ignored.
                if is_position_delete {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!(
                            "Cannot commit, found new position delete for replaced data file: {}",
                            data_file.file_path()
                        ),
                    ));
                }
            } else {
                // Java L544-548: ANY applicable delete is a conflict.
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Cannot commit, found new delete for replaced data file: {}",
                        data_file.file_path()
                    ),
                ));
            }
        }
    }

    Ok(())
}

/// Whether a single concurrently-added delete file APPLIES to `data_file`, mirroring Java
/// `DeleteFileIndex.forDataFile(starting_sequence_number, data_file)` (`core/DeleteFileIndex.java`
/// L151-200). See [`validate_no_new_deletes_for_data_files`] for the full citation; the rules are:
///
/// - **Sequence number (Java `*.filter` `findStartIndex`):** the delete's DATA sequence number must be
///   `>= starting_sequence_number`. An absent entry sequence number is treated conservatively as applicable
///   (it has not yet been narrowed out).
/// - **Global (unpartitioned) equality deletes (Java `findGlobalDeletes`):** an EQUALITY delete with an
///   empty partition applies to ANY data file (subject to the sequence test) — the spec's "equality delete
///   files stored with an unpartitioned spec are applied as global deletes".
/// - **Partition match (Java `findPosPartitionDeletes` / `findEqPartitionDeletes`):** otherwise the delete
///   matches only a data file with the SAME `partition_spec_id` AND an equal partition tuple.
/// - **Path-scoped position deletes (Java `findPathDeletes`):** a position delete carrying a
///   `referenced_data_file` additionally requires that path to equal the data file's path.
fn delete_applies_to_data_file(
    delete_file: &DataFile,
    delete_sequence_number: Option<i64>,
    data_file: &DataFile,
    starting_sequence_number: i64,
) -> bool {
    use crate::spec::DataContentType;

    // Java `*.filter`: keep only deletes whose data sequence number is `>= starting_sequence_number`. An
    // absent sequence number is treated as applicable (conservative — not yet narrowed out).
    if let Some(delete_seq) = delete_sequence_number
        && delete_seq < starting_sequence_number
    {
        return false;
    }

    let is_unpartitioned = delete_file.partition().fields().is_empty();

    match delete_file.content_type() {
        DataContentType::EqualityDeletes => {
            // Java `findGlobalDeletes`: an unpartitioned equality delete is a GLOBAL delete (any data file).
            if is_unpartitioned {
                return true;
            }
            // Java `findEqPartitionDeletes`: same spec id + equal partition tuple.
            delete_file.partition_spec_id == data_file.partition_spec_id
                && delete_file.partition() == data_file.partition()
        }
        DataContentType::PositionDeletes => {
            // Java `findPathDeletes`: a path-scoped position delete matches only the referenced data file.
            if let Some(referenced) = &delete_file.referenced_data_file {
                return referenced == data_file.file_path();
            }
            // Java `findPosPartitionDeletes`: same spec id + equal partition tuple.
            delete_file.partition_spec_id == data_file.partition_spec_id
                && delete_file.partition() == data_file.partition()
        }
        // A `Data` file is never a delete; it cannot apply as one.
        DataContentType::Data => false,
    }
}

/// Enumerate the DATA files DELETED from `table` by snapshots committed AFTER `starting_snapshot_id` — the
/// concurrent removals a `validateDataFilesExist` check must inspect to detect that a file this operation
/// also needs to delete was already removed by a concurrent commit.
///
/// This is the Rust port of Java `MergingSnapshotProducer.validateDataFilesExist` /  `deletedDataFiles`
/// (`core/MergingSnapshotProducer.java` L695-735, L773-822): the shared [`files_after`] walk over DATA
/// manifests, keeping `ManifestStatus::Deleted` tombstone entries (Java `entry.status() == DELETED` with
/// `ignoreExisting()`). See [`files_after`] for the walk semantics.
///
/// The `skip_deletes` flag selects the operation set, mirroring Java's two `validateDataFilesExist` op sets:
/// - `skip_deletes == false` ⇒ [`operation_removes_data_files`] = Java
///   `VALIDATE_DATA_FILES_EXIST_OPERATIONS = {OVERWRITE, REPLACE, DELETE}` (`{Overwrite, Delete}` in Rust).
///   `DeleteFiles` uses this (its `validate` always includes DELETE-op snapshots).
/// - `skip_deletes == true` ⇒ [`operation_removes_data_files_skip_deletes`] = Java
///   `VALIDATE_DATA_FILES_EXIST_SKIP_DELETE_OPERATIONS = {OVERWRITE, REPLACE}` (`{Overwrite}` in Rust).
///   `RowDelta` uses this by DEFAULT (Java `BaseRowDelta` passes `skipDeletes = !validateDeletes`, and
///   `validateDeletes` is `false` unless `validateDeletedFiles()` was called) so that a concurrent
///   merge-on-read DELETE-op snapshot does not trip the referenced-files check.
///
/// In BOTH cases the unrepresentable Java `REPLACE` operation is absent (Rust never records a `REPLACE`
/// snapshot) — faithful, not a gap.
///
/// A concurrent delete/overwrite writes the file it removes as a `Deleted` tombstone in a manifest IT wrote
/// (`rewrite_manifest_with_deletes` stamps the committing snapshot id as the manifest's `added_snapshot_id`),
/// so the `added_snapshot_id == snapshot_id` manifest filter finds those tombstones — exactly the way Java's
/// `manifest.snapshotId() == currentSnapshot.snapshotId()` filter does.
///
/// The caller intersects these deleted-file paths with the set it requires (the files it is deleting, or the
/// files its added delete files reference) to decide whether to reject the commit (Java
/// `requiredDataFiles.contains(entry.file().location())`).
pub(crate) async fn deleted_data_files_after(
    table: &Table,
    starting_snapshot_id: Option<i64>,
    skip_deletes: bool,
) -> Result<Vec<DataFile>> {
    let operation_filter = if skip_deletes {
        operation_removes_data_files_skip_deletes
    } else {
        operation_removes_data_files
    };

    files_after(
        table,
        starting_snapshot_id,
        ManifestContentType::Data,
        operation_filter,
        ManifestStatus::Deleted,
    )
    .await
}

/// Reject the commit if any DATA file ADDED by a concurrent commit since `effective_start` COULD contain
/// records matching `conflict_filter` — the filter-based serializable-isolation conflict check shared by
/// the write actions that mirror Java `MergingSnapshotProducer.validateAddedDataFiles`
/// (`OverwriteFiles.validateNoConflictingData`, `RowDelta.validateNoConflictingDataFiles`).
///
/// This is the Rust port of Java `MergingSnapshotProducer.validateAddedDataFiles`
/// (`core/MergingSnapshotProducer.java` L391-412): it enumerates the concurrently-added DATA files via
/// the shared [`added_data_files_after`] walk and throws a non-retryable `ValidationException` ("Found
/// conflicting files that can contain records matching %s: %s") on the FIRST file whose metrics permit a
/// match. The per-file "could this added file match the filter?" test is the existing
/// [`InclusiveMetricsEvaluator`] (Java `ManifestGroup.filterData` = inclusive-metrics evaluation over the
/// file's bounds / null / nan stats).
///
/// Arguments:
/// - `current` — the REFRESHED base (Java `parent` metadata); the walk inspects the snapshots it gained.
/// - `effective_start` — the starting snapshot id (exclusive). `None` ⇒ inspect from the root (validate
///   every version, Java's null `startingSnapshotId`).
/// - `conflict_filter` — the conflict-detection predicate. `None` ⇒ bind `AlwaysTrue` (ANY concurrently
///   added DATA file conflicts — the most conservative serializable check, Java
///   `dataConflictDetectionFilter()` returning `alwaysTrue()` when no filter is set).
/// - `case_sensitive` — column-resolution case sensitivity for binding the filter (Java
///   `isCaseSensitive()`; the actions default this to `true`, the Iceberg/Java default).
///
/// Returns `Ok(())` when nothing concurrently-added can match (including when the concurrent-added set is
/// empty). On the first conflict it returns a NON-retryable [`ErrorKind::DataInvalid`] error naming the
/// filter + the conflicting file path, so the commit retry loop stops and the error propagates (Java's
/// non-retryable `ValidationException`).
///
/// Sharing this in one place keeps `OverwriteFiles` and `RowDelta` (and any future filter-based check)
/// from drifting on the load-bearing walk + bind + per-file evaluation + error contract.
pub(crate) async fn validate_no_conflicting_added_data_files(
    current: &Table,
    effective_start: Option<i64>,
    conflict_filter: Option<&Predicate>,
    case_sensitive: bool,
) -> Result<()> {
    let added = added_data_files_after(current, effective_start).await?;
    if let Some(file) = first_conflicting_file(&added, current, conflict_filter, case_sensitive)? {
        return Err(Error::new(
            ErrorKind::DataInvalid,
            format!(
                "Found conflicting files that can contain records matching {}: {}",
                conflict_filter.map_or_else(|| "true".to_string(), |filter| format!("{filter}")),
                file.file_path()
            ),
        ));
    }

    Ok(())
}

/// Reject the commit if any DELETE file ADDED by a concurrent commit since `effective_start` COULD apply to
/// records matching `conflict_filter` — the filter-based serializable-isolation conflict check for the
/// merge-on-read delete path, mirroring Java `MergingSnapshotProducer.validateNoNewDeleteFiles`.
///
/// This is the Rust port of Java `MergingSnapshotProducer.validateNoNewDeleteFiles`
/// (`core/MergingSnapshotProducer.java` L562-570): it enumerates the concurrently-added DELETE files via the
/// shared [`added_delete_files_after`] walk (which applies the V2 guard) and throws a non-retryable
/// `ValidationException` ("Found new conflicting delete files that can apply to records matching %s: %s") on
/// the FIRST file whose metrics permit a match. The per-file "could this added delete file apply to records
/// matching the filter?" test is the SAME [`first_conflicting_file`] (the existing
/// [`InclusiveMetricsEvaluator`]) the data-file check uses.
///
/// Arguments mirror [`validate_no_conflicting_added_data_files`]. The only differences from the data-file
/// check are (1) the DELETE-manifest walk + V2 guard (in [`added_delete_files_after`]) and (2) the
/// DELETE-specific error message — the per-file conflict test is shared.
///
/// **Over-scan vs Java (documented):** see [`added_delete_files_after`] — this port omits Java's
/// `DeleteFileIndex` `startingSequenceNumber` refinement, a conservative over-scan (can only over-reject).
pub(crate) async fn validate_no_conflicting_added_delete_files(
    current: &Table,
    effective_start: Option<i64>,
    conflict_filter: Option<&Predicate>,
    case_sensitive: bool,
) -> Result<()> {
    let added = added_delete_files_after(current, effective_start).await?;
    if let Some(file) = first_conflicting_file(&added, current, conflict_filter, case_sensitive)? {
        return Err(Error::new(
            ErrorKind::DataInvalid,
            format!(
                "Found new conflicting delete files that can apply to records matching {}: {}",
                conflict_filter.map_or_else(|| "true".to_string(), |filter| format!("{filter}")),
                file.file_path()
            ),
        ));
    }

    Ok(())
}

/// Reject the commit if any DATA file DELETED by a concurrent commit since `effective_start` COULD contain
/// records matching `conflict_filter` — the filter-based serializable-isolation check that a concurrent
/// commit did not remove data this operation's row filter also targets, mirroring Java
/// `MergingSnapshotProducer.validateDeletedDataFiles` (the `Expression` variant).
///
/// This is the Rust port of Java `MergingSnapshotProducer.validateDeletedDataFiles`
/// (`core/MergingSnapshotProducer.java` L636-654, the `dataFilter` overload): it enumerates the
/// concurrently-DELETED DATA files via the shared [`deleted_data_files_after`] walk (with
/// `skip_deletes = false` ⇒ the op set `{Overwrite, Delete}`, Java
/// `VALIDATE_DATA_FILES_EXIST_OPERATIONS = {OVERWRITE, REPLACE, DELETE}` minus the unrepresentable Java
/// `REPLACE` operation — Rust never records a `REPLACE` snapshot, so its absence is faithful, not a gap) and
/// throws a non-retryable `ValidationException` ("Found conflicting deleted files that can contain records
/// matching %s: %s") on the FIRST removed file whose metrics permit a match. The per-file "could this deleted
/// file have contained records matching the filter?" test is the SAME [`first_conflicting_file`] (the
/// existing [`InclusiveMetricsEvaluator`]) the added-file / added-delete checks use, so the three cannot
/// drift on the load-bearing bind + per-file evaluation contract.
///
/// Arguments mirror [`validate_no_conflicting_added_delete_files`]. The only differences from the
/// added-delete check are (1) the DELETED-data-file walk (concurrent removals, not concurrent additions) and
/// (2) the deleted-files error message — the per-file conflict test is shared.
///
/// Arguments:
/// - `current` — the REFRESHED base (Java `parent` metadata); the walk inspects the snapshots it gained.
/// - `effective_start` — the starting snapshot id (exclusive). `None` ⇒ inspect from the root (Java's null
///   `startingSnapshotId`).
/// - `conflict_filter` — the row/conflict-detection predicate. `None` ⇒ bind `AlwaysTrue` and render the
///   filter as `true` (any concurrently-deleted DATA file conflicts — the most conservative serializable
///   check), mirroring the sibling [`validate_no_conflicting_added_delete_files`].
/// - `case_sensitive` — column-resolution case sensitivity for binding the filter (Java `isCaseSensitive()`;
///   the actions default this to `true`, the Iceberg/Java default).
///
/// Returns `Ok(())` when nothing concurrently-deleted can match (including an empty concurrent-removed set).
/// On the first conflict it returns a NON-retryable [`ErrorKind::DataInvalid`] error naming the filter + the
/// conflicting file path, so the commit retry loop stops and the error propagates (Java's non-retryable
/// `ValidationException`).
///
/// **Conservative posture (documented):** the per-file [`InclusiveMetricsEvaluator`] over-approximates —
/// it can only over-REJECT (treat a non-matching deletion as a conflict), never under-reject, so it is safe
/// under serializable isolation. The unrepresentable Java `REPLACE` operation is omitted from the walk's op
/// set (Rust records no `REPLACE` snapshot), which can only under-scan relative to Java — faithful because
/// the Rust write path never produces a `REPLACE`, so there is nothing to scan.
pub(crate) async fn validate_deleted_data_files(
    current: &Table,
    effective_start: Option<i64>,
    conflict_filter: Option<&Predicate>,
    case_sensitive: bool,
) -> Result<()> {
    let deleted = deleted_data_files_after(current, effective_start, false).await?;
    if let Some(file) = first_conflicting_file(&deleted, current, conflict_filter, case_sensitive)?
    {
        return Err(Error::new(
            ErrorKind::DataInvalid,
            format!(
                "Found conflicting deleted files that can contain records matching {}: {}",
                conflict_filter.map_or_else(|| "true".to_string(), |filter| format!("{filter}")),
                file.file_path()
            ),
        ));
    }

    Ok(())
}

/// Return the first file in `files` that COULD contain records matching `conflict_filter` — the shared
/// per-file conflict test behind both [`validate_no_conflicting_added_data_files`] and
/// [`validate_no_conflicting_added_delete_files`].
///
/// Binds `conflict_filter` to `current`'s current schema ONCE (the caller's filter when `Some`, else
/// `AlwaysTrue` = any file conflicts — the most conservative serializable check, Java
/// `dataConflictDetectionFilter()` returning `alwaysTrue()` when no filter is set), then tests each file
/// with the existing [`InclusiveMetricsEvaluator`] (Java `ManifestGroup.filterData` = inclusive-metrics
/// evaluation over the file's bounds / null / nan stats). Returns the FIRST matching file (Java throws on
/// the first conflict entry), or `None` when nothing can match (including an empty `files`).
///
/// `include_empty_files = true` keeps a zero-record file's evaluation conservative (it never excludes on
/// emptiness alone). The bind happens once for the whole set, not per file.
fn first_conflicting_file(
    files: &[DataFile],
    current: &Table,
    conflict_filter: Option<&Predicate>,
    case_sensitive: bool,
) -> Result<Option<DataFile>> {
    if files.is_empty() {
        // No concurrently-added file of the relevant content — nothing can conflict.
        return Ok(None);
    }

    let schema = current.metadata().current_schema().clone();
    let bound_filter: BoundPredicate = conflict_filter
        .cloned()
        .unwrap_or(Predicate::AlwaysTrue)
        .bind(schema, case_sensitive)?;

    for file in files {
        if InclusiveMetricsEvaluator::eval(&bound_filter, file, true)? {
            return Ok(Some(file.clone()));
        }
    }

    Ok(None)
}

#[cfg(test)]
mod multispec_tests {
    //! Multi-spec producer tests (Java `MergingSnapshotProducer` / `FastAppend` per-spec manifest
    //! groups). Driven end-to-end through the `fast_append` / `row_delta` actions — the realistic path
    //! that exercises `SnapshotProducer::write_added_manifests` / `write_added_delete_manifests` and the
    //! lifted `validate_added_data_files` / `validate_added_delete_files`.
    //!
    //! Fixtures: a V2 minimal table partitioned by `identity(x)` (spec 0), evolved by `add_field("y")`
    //! to spec 1 = `identity(x) + identity(y)`. A file under spec 0 carries a 1-field partition `(x)`; a
    //! file under spec 1 carries a 2-field partition `(x, y)`. Both specs stay resolvable after the
    //! evolution, so a single commit can add files under both.

    use std::collections::HashMap;

    use crate::memory::tests::new_memory_catalog;
    use crate::spec::{
        DataContentType, DataFile, DataFileBuilder, DataFileFormat, Literal, ManifestContentType,
        ManifestStatus, Struct,
    };
    use crate::table::Table;
    use crate::transaction::tests::make_v2_minimal_table_in_catalog;
    use crate::transaction::{ApplyTransactionAction, Transaction};
    use crate::{Catalog, ErrorKind};

    // ============================================================================================
    // Fixtures.
    // ============================================================================================

    /// A data file under spec 0 (`identity(x)`), partition `(x = part_value)`.
    fn data_file_spec0(path: &str, part_value: i64) -> DataFile {
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

    /// A data file with a 1-field partition `(value)` claiming an arbitrary `spec_id` — for a
    /// same-arity rename fixture where spec 1 is also 1-field (`identity(y)`). Shape-identical to
    /// [`data_file_spec0`] but lets the caller stamp the new spec id.
    fn data_file_spec0_under(path: &str, value: i64, spec_id: i32) -> DataFile {
        DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(path.to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(spec_id)
            .partition(Struct::from_iter([Some(Literal::long(value))]))
            .build()
            .unwrap()
    }

    /// A data file under spec 1 (`identity(x) + identity(y)`), partition `(x, y)`.
    fn data_file_spec1(path: &str, x: i64, y: i64, spec_id: i32) -> DataFile {
        DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(path.to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(spec_id)
            .partition(Struct::from_iter([
                Some(Literal::long(x)),
                Some(Literal::long(y)),
            ]))
            .build()
            .unwrap()
    }

    /// A parquet position-delete file under spec 0, partition `(x = part_value)`.
    fn delete_file_spec0(path: &str, part_value: i64) -> DataFile {
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

    /// A parquet position-delete file under spec 1, partition `(x, y)`.
    fn delete_file_spec1(path: &str, x: i64, y: i64, spec_id: i32) -> DataFile {
        DataFileBuilder::default()
            .content(DataContentType::PositionDeletes)
            .file_path(path.to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(spec_id)
            .partition(Struct::from_iter([
                Some(Literal::long(x)),
                Some(Literal::long(y)),
            ]))
            .build()
            .unwrap()
    }

    /// Evolve the table's partition spec by adding `identity(y)` and return `(table, new_spec_id)`.
    async fn evolve_spec(catalog: &impl Catalog, table: &Table) -> (Table, i32) {
        let tx = Transaction::new(table);
        let action = tx.update_partition_spec().add_field("y");
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(catalog).await.unwrap();
        let new_spec_id = table.metadata().default_partition_spec_id();
        assert_ne!(
            new_spec_id, 0,
            "fixture sanity: the spec evolved away from 0"
        );
        (table, new_spec_id)
    }

    /// Evolve the table's partition spec to a SAME-ARITY but DIFFERENT-NAME 1-field spec: drop
    /// `identity(x)` and add `identity(y)` (on V2 the removed field is OMITTED, not void-replaced —
    /// `update_partition_spec` "removing the only base field on V2 omits it entirely"), so the new
    /// spec is `(identity(y))`, a 1-field spec like spec 0 but rendering the partition path under field
    /// `y` instead of `x`. Returns `(table, new_spec_id)`. This is the panic-FREE multi-spec shape: a
    /// spec-0 file rendered under the (default) new spec would NOT trip the arity `zip_eq` guard — it
    /// would silently render the WRONG field name, which is exactly the summary-path corruption the
    /// per-file-spec fix prevents.
    async fn evolve_spec_same_arity_rename(catalog: &impl Catalog, table: &Table) -> (Table, i32) {
        let tx = Transaction::new(table);
        let action = tx.update_partition_spec().remove_field("x").add_field("y");
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(catalog).await.unwrap();
        let new_spec_id = table.metadata().default_partition_spec_id();
        let new_spec = table.metadata().default_partition_spec();
        assert_eq!(
            new_spec.fields().len(),
            1,
            "fixture sanity: the renamed spec must stay 1-field (no void placeholder on V2): {new_spec:?}"
        );
        assert_eq!(
            new_spec.fields()[0].name,
            "y",
            "fixture sanity: the renamed spec's only field is identity(y): {new_spec:?}"
        );
        assert_ne!(
            new_spec_id, 0,
            "fixture sanity: the spec evolved away from 0"
        );
        (table, new_spec_id)
    }

    /// Set a table property in its own commit (e.g. `write.summary.partition-limit`).
    async fn set_property(catalog: &impl Catalog, table: &Table, key: &str, value: &str) -> Table {
        let tx = Transaction::new(table);
        let action = tx
            .update_table_properties()
            .set(key.to_string(), value.to_string());
        let tx = action.apply(tx).unwrap();
        tx.commit(catalog).await.unwrap()
    }

    /// A fresh V1 table (schema x/y/z longs, spec 0 = `identity(x)`) in `catalog` — for the V1
    /// multi-spec append probe. V1 spec evolution VOID-replaces removed fields (field-id stability),
    /// so an evolved V1 spec stays multi-spec-resolvable just like V2.
    async fn make_v1_minimal_table_in_catalog(catalog: &impl Catalog) -> Table {
        use crate::spec::{
            FormatVersion, NestedField, PartitionSpec, PrimitiveType, Schema, Transform, Type,
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
            .format_version(FormatVersion::V1)
            .build();
        catalog
            .create_table(table_ident.namespace(), table_creation)
            .await
            .unwrap()
    }

    /// The `(spec_id, content)` of every NEW manifest the current snapshot wrote, paired with the set of
    /// live (Added) file paths it carries. Filters to manifests this snapshot itself wrote.
    async fn new_manifests_by_spec(table: &Table) -> Vec<(i32, ManifestContentType, Vec<String>)> {
        let snapshot = table.metadata().current_snapshot().unwrap();
        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();
        let mut result = Vec::new();
        for manifest_file in manifest_list.entries() {
            if manifest_file.added_snapshot_id != snapshot.snapshot_id() {
                continue;
            }
            let manifest = manifest_file.load_manifest(table.file_io()).await.unwrap();
            let added: Vec<String> = manifest
                .entries()
                .iter()
                .filter(|entry| entry.status() == ManifestStatus::Added)
                .map(|entry| entry.file_path().to_string())
                .collect();
            result.push((
                manifest_file.partition_spec_id,
                manifest_file.content,
                added,
            ));
        }
        result
    }

    /// The path → manifest-spec-id map of every live (Added/Existing) entry in the current snapshot —
    /// what a scan would read, with each file's containing-manifest spec id.
    async fn live_paths_with_manifest_spec(table: &Table) -> HashMap<String, i32> {
        let snapshot = table.metadata().current_snapshot().unwrap();
        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();
        let mut live = HashMap::new();
        for manifest_file in manifest_list.entries() {
            let manifest = manifest_file.load_manifest(table.file_io()).await.unwrap();
            for entry in manifest.entries() {
                if entry.is_alive() {
                    live.insert(
                        entry.file_path().to_string(),
                        manifest_file.partition_spec_id,
                    );
                }
            }
        }
        live
    }

    // ============================================================================================
    // Added DATA files under two specs ⇒ one DATA manifest per spec.
    // ============================================================================================

    /// MULTI-SPEC FAST APPEND. A single fast-append commit adding one file under spec 0 and one under
    /// spec 1 produces TWO data manifests — one per spec — each stamped with its own spec id, with the
    /// partition tuples intact, and both files live for a scan. Risk pinned: the default-spec-only
    /// producer would write BOTH files into ONE default-spec manifest, wrongly stamping the spec-0 file's
    /// partition under the 2-field default spec (partition-tuple corruption / wrong manifest spec id).
    /// Java `FastAppend.appendFile` groups into `newDataFilesBySpec` and writes per-spec manifests.
    #[tokio::test]
    async fn test_fast_append_two_specs_produces_per_spec_data_manifests() {
        let catalog = new_memory_catalog().await;
        let table = make_v2_minimal_table_in_catalog(&catalog).await;
        let (table, new_spec_id) = evolve_spec(&catalog, &table).await;

        // One commit adding a spec-0 file AND a spec-1 file.
        let tx = Transaction::new(&table);
        let action = tx.fast_append().add_data_files(vec![
            data_file_spec0("test/old.parquet", 5),
            data_file_spec1("test/new.parquet", 7, 9, new_spec_id),
        ]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        // Exactly two NEW data manifests, one per spec id.
        let manifests = new_manifests_by_spec(&table).await;
        let data_manifests: Vec<_> = manifests
            .iter()
            .filter(|(_, content, _)| *content == ManifestContentType::Data)
            .collect();
        assert_eq!(
            data_manifests.len(),
            2,
            "a two-spec append must write ONE data manifest PER spec, got: {manifests:?}"
        );

        // Each manifest carries exactly its own spec's file.
        let spec0_manifest = data_manifests
            .iter()
            .find(|(spec_id, _, _)| *spec_id == 0)
            .expect("a spec-0 data manifest must exist");
        let spec1_manifest = data_manifests
            .iter()
            .find(|(spec_id, _, _)| *spec_id == new_spec_id)
            .expect("a spec-1 data manifest must exist");
        assert_eq!(spec0_manifest.2, vec!["test/old.parquet".to_string()]);
        assert_eq!(spec1_manifest.2, vec!["test/new.parquet".to_string()]);

        // Both files live for a scan, each under its own manifest spec id (partition tuples intact: the
        // spec-0 file is NOT re-stamped under the 2-field default spec).
        let live = live_paths_with_manifest_spec(&table).await;
        assert_eq!(live.get("test/old.parquet"), Some(&0));
        assert_eq!(live.get("test/new.parquet"), Some(&new_spec_id));
    }

    /// MULTI-SPEC CUMULATIVE TOTALS. After appending a spec-0 file, then a commit adding one file under
    /// each spec, the snapshot summary `total-data-files` reflects ALL three files (previous + added).
    /// Risk pinned: a per-spec grouping bug that dropped one group's files would under-count the totals;
    /// per-commit `added-data-files` assertions cannot catch a previous-total seed bug.
    #[tokio::test]
    async fn test_fast_append_multispec_cumulative_totals() {
        let catalog = new_memory_catalog().await;
        let table = make_v2_minimal_table_in_catalog(&catalog).await;
        let (table, new_spec_id) = evolve_spec(&catalog, &table).await;

        // First commit: one spec-0 file.
        let tx = Transaction::new(&table);
        let action = tx
            .fast_append()
            .add_data_files(vec![data_file_spec0("test/a.parquet", 1)]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        // Second commit: one spec-0 + one spec-1 file in ONE multi-spec commit.
        let tx = Transaction::new(&table);
        let action = tx.fast_append().add_data_files(vec![
            data_file_spec0("test/b.parquet", 2),
            data_file_spec1("test/c.parquet", 3, 4, new_spec_id),
        ]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        let summary = &table
            .metadata()
            .current_snapshot()
            .unwrap()
            .summary()
            .additional_properties;
        assert_eq!(
            summary.get("added-data-files"),
            Some(&"2".to_string()),
            "this commit added two data files across two specs"
        );
        assert_eq!(
            summary.get("total-data-files"),
            Some(&"3".to_string()),
            "cumulative total = previous (1) + added (2); a dropped spec group would under-count"
        );
    }

    // ============================================================================================
    // THE SUMMARY-COLLECTOR PIN: per-partition summary keys render each file under ITS OWN spec.
    // ============================================================================================

    /// MULTI-SPEC PARTITION SUMMARY KEYS (the summary-collector per-file-spec fix).
    ///
    /// A multi-spec commit's `partitions.{path}` summary keys MUST render each file's partition under
    /// THAT file's own spec's field names — Java `SnapshotSummary.Builder.addedFile(spec(file.specId()),
    /// file)` → `updatePartitions(spec, file)` → `spec.partitionToPath(file.partition())`. The producer's
    /// `summary()` previously rendered EVERY file under the table default spec; on a multi-spec commit a
    /// spec-0 file's path would be computed under the (default) new spec, producing the WRONG key and
    /// miscounting `changed-partition-count`.
    ///
    /// The fixture deliberately uses a SAME-ARITY rename (spec 0 = `identity(x)`, spec 1 = `identity(y)`)
    /// and the SAME partition VALUE 5 on both files so the bug does NOT trip the arity `zip_eq` panic the
    /// other multi-spec tests catch incidentally — it silently collapses both files onto the SAME `y=5`
    /// path. Risk pinned: (a) the spec-0 file's key is `partitions.x=5` (rendered under spec 0's field
    /// `x`), NOT `partitions.y=5`; (b) `changed-partition-count` is 2 (two distinct per-spec tuples), not
    /// 1 (the collapse the default-spec bug produces — both render `y=5`). Under the revert-to-default
    /// mutation this test fails on BOTH the missing `partitions.x=5` key AND `changed-partition-count`.
    #[tokio::test]
    async fn test_fast_append_multispec_partition_summary_keys_use_file_spec() {
        let catalog = new_memory_catalog().await;
        let table = make_v2_minimal_table_in_catalog(&catalog).await;
        // High partition-summary limit so the per-partition `partitions.{path}` keys are emitted.
        let table = set_property(
            &catalog,
            &table,
            crate::spec::TableProperties::PROPERTY_WRITE_PARTITION_SUMMARY_LIMIT,
            "100",
        )
        .await;
        // Same-arity rename: spec 0 = identity(x), spec 1 = identity(y). Default spec is now spec 1 (y).
        let (table, new_spec_id) = evolve_spec_same_arity_rename(&catalog, &table).await;

        // One commit adding a spec-0 file (partition x=5) AND a spec-1 file (partition y=5) — the SAME
        // numeric value 5, distinguished only by which spec's field names the path renders.
        let tx = Transaction::new(&table);
        let action = tx.fast_append().add_data_files(vec![
            data_file_spec0("test/old.parquet", 5),
            data_file_spec0_under("test/new.parquet", 5, new_spec_id),
        ]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        let summary = &table
            .metadata()
            .current_snapshot()
            .unwrap()
            .summary()
            .additional_properties;

        // (a) The spec-0 file renders under spec 0's field `x` (`partitions.x=5`), NOT the default
        // spec's field `y`. The default-spec bug renders it as `partitions.y=5` (wrong) and the
        // spec-0 key vanishes.
        assert!(
            summary.contains_key("partitions.x=5"),
            "the spec-0 file's partition path must render under spec 0's field `x` \
             (`partitions.x=5`); summary keys: {:?}",
            summary.keys().collect::<Vec<_>>()
        );
        // The spec-1 file renders under spec 1's field `y`.
        assert!(
            summary.contains_key("partitions.y=5"),
            "the spec-1 file's partition path must render under spec 1's field `y` \
             (`partitions.y=5`); summary keys: {:?}",
            summary.keys().collect::<Vec<_>>()
        );
        // (b) Two DISTINCT per-spec partition tuples ⇒ count 2. The default-spec bug collapses both
        // onto `y=5` ⇒ count 1.
        assert_eq!(
            summary.get("changed-partition-count"),
            Some(&"2".to_string()),
            "two distinct per-spec partition tuples (x=5 under spec 0, y=5 under spec 1) ⇒ \
             changed-partition-count 2; the default-spec bug collapses both onto y=5 ⇒ 1. summary: {summary:?}"
        );
        assert_eq!(
            summary.get("partition-summaries-included"),
            Some(&"true".to_string()),
            "per-partition summaries must be included under a generous partition-limit"
        );
    }

    // ============================================================================================
    // V1 multi-spec DATA append (spec evolution exists on V1 via void replacement).
    // ============================================================================================

    /// V1 MULTI-SPEC DATA APPEND. Spec evolution is legal on V1 (removed fields are VOID-replaced to
    /// keep field ids stable), so a V1 table can carry multiple resolvable specs. This probes that a
    /// single V1 fast-append adding one spec-0 file and one spec-1 file WORKS (Java parity — the V1
    /// arm of `new_cluster_manifest_writer` builds a V1 manifest per spec group, content-agnostic) and
    /// produces ONE V1 data manifest PER spec, each stamped with its own spec id. Documents the V1
    /// behavior the brief asked to probe: it WORKS, not fail-loud.
    #[tokio::test]
    async fn test_v1_fast_append_two_specs_produces_per_spec_data_manifests() {
        let catalog = new_memory_catalog().await;
        let table = make_v1_minimal_table_in_catalog(&catalog).await;
        assert_eq!(
            table.metadata().format_version(),
            crate::spec::FormatVersion::V1,
            "fixture sanity: V1 table"
        );
        // Evolve: spec 1 = identity(x) + identity(y) (2-field), spec 0 stays identity(x) (1-field).
        let (table, new_spec_id) = evolve_spec(&catalog, &table).await;

        let tx = Transaction::new(&table);
        let action = tx.fast_append().add_data_files(vec![
            data_file_spec0("test/v1-old.parquet", 5),
            data_file_spec1("test/v1-new.parquet", 7, 9, new_spec_id),
        ]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        let manifests = new_manifests_by_spec(&table).await;
        let data_manifests: Vec<_> = manifests
            .iter()
            .filter(|(_, content, _)| *content == ManifestContentType::Data)
            .collect();
        assert_eq!(
            data_manifests.len(),
            2,
            "a V1 two-spec append must write ONE data manifest PER spec, got: {manifests:?}"
        );
        let spec0_manifest = data_manifests
            .iter()
            .find(|(spec_id, _, _)| *spec_id == 0)
            .expect("a V1 spec-0 data manifest must exist");
        let spec1_manifest = data_manifests
            .iter()
            .find(|(spec_id, _, _)| *spec_id == new_spec_id)
            .expect("a V1 spec-1 data manifest must exist");
        assert_eq!(spec0_manifest.2, vec!["test/v1-old.parquet".to_string()]);
        assert_eq!(spec1_manifest.2, vec!["test/v1-new.parquet".to_string()]);

        let live = live_paths_with_manifest_spec(&table).await;
        assert_eq!(live.get("test/v1-old.parquet"), Some(&0));
        assert_eq!(live.get("test/v1-new.parquet"), Some(&new_spec_id));
    }

    // ============================================================================================
    // Added DELETE files under two specs ⇒ one DELETE manifest per spec (V2 position deletes).
    // ============================================================================================

    /// MULTI-SPEC ROW DELTA. A single `row_delta` commit adding one position-delete file under spec 0 and
    /// one under spec 1 produces TWO delete manifests — one per spec — each stamped with its own spec id,
    /// with the partition tuples intact. Risk pinned: the default-spec-only producer would write BOTH
    /// deletes into ONE default-spec delete manifest, wrongly stamping the spec-0 delete's partition (a
    /// delete then stops matching its target rows = silent resurrection). Java `MergingSnapshotProducer`
    /// groups into `newDeleteFilesBySpec` and writes per-spec delete manifests.
    #[tokio::test]
    async fn test_row_delta_two_specs_produces_per_spec_delete_manifests() {
        let catalog = new_memory_catalog().await;
        let table = make_v2_minimal_table_in_catalog(&catalog).await;
        // Seed live data so the deletes have targets (and the table is non-empty).
        let tx = Transaction::new(&table);
        let action = tx
            .fast_append()
            .add_data_files(vec![data_file_spec0("test/seed.parquet", 0)]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();
        let (table, new_spec_id) = evolve_spec(&catalog, &table).await;

        // One row_delta commit adding a spec-0 delete AND a spec-1 delete.
        let tx = Transaction::new(&table);
        let action = tx.row_delta().add_deletes(vec![
            delete_file_spec0("test/old-del.parquet", 5),
            delete_file_spec1("test/new-del.parquet", 7, 9, new_spec_id),
        ]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        let manifests = new_manifests_by_spec(&table).await;
        let delete_manifests: Vec<_> = manifests
            .iter()
            .filter(|(_, content, _)| *content == ManifestContentType::Deletes)
            .collect();
        assert_eq!(
            delete_manifests.len(),
            2,
            "a two-spec row_delta must write ONE delete manifest PER spec, got: {manifests:?}"
        );

        let spec0_manifest = delete_manifests
            .iter()
            .find(|(spec_id, _, _)| *spec_id == 0)
            .expect("a spec-0 delete manifest must exist");
        let spec1_manifest = delete_manifests
            .iter()
            .find(|(spec_id, _, _)| *spec_id == new_spec_id)
            .expect("a spec-1 delete manifest must exist");
        assert_eq!(spec0_manifest.2, vec!["test/old-del.parquet".to_string()]);
        assert_eq!(spec1_manifest.2, vec!["test/new-del.parquet".to_string()]);
    }

    // ============================================================================================
    // Unknown-spec rejection (the lifted validation's exact Java message), data + delete.
    // ============================================================================================

    /// UNKNOWN-SPEC DATA REJECTION. Adding a data file whose `partition_spec_id` matches no table spec
    /// fails with Java's EXACT message ("Cannot find partition spec %s for data file: %s"). Risk pinned:
    /// the lift from "spec == default" to "spec EXISTS" must still reject a genuinely-unknown spec id
    /// (the validation guard must fire on the bad case, not just accept the good ones).
    #[tokio::test]
    async fn test_fast_append_unknown_spec_id_data_rejected() {
        let catalog = new_memory_catalog().await;
        let table = make_v2_minimal_table_in_catalog(&catalog).await;

        // Spec id 99 does not exist on the table.
        let bogus = data_file_spec1("test/bogus.parquet", 1, 2, 99);
        let tx = Transaction::new(&table);
        let action = tx.fast_append().add_data_files(vec![bogus]);
        let tx = action.apply(tx).unwrap();
        let err = tx
            .commit(&catalog)
            .await
            .expect_err("an unknown spec id must be rejected");
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(
            err.message()
                .contains("Cannot find partition spec 99 for data file: test/bogus.parquet"),
            "unexpected message: {}",
            err.message()
        );
    }

    /// UNKNOWN-SPEC DELETE REJECTION. Adding a delete file whose `partition_spec_id` matches no table
    /// spec fails with Java's EXACT message ("Cannot find partition spec %s for delete file: %s") — the
    /// delete-file noun, distinct from the data-file message. Risk pinned: the lifted delete validation
    /// must reject an unknown spec id AND use the delete-file noun (Java has two distinct messages).
    #[tokio::test]
    async fn test_row_delta_unknown_spec_id_delete_rejected() {
        let catalog = new_memory_catalog().await;
        let table = make_v2_minimal_table_in_catalog(&catalog).await;
        let tx = Transaction::new(&table);
        let action = tx
            .fast_append()
            .add_data_files(vec![data_file_spec0("test/seed.parquet", 0)]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        // Spec id 99 does not exist on the table.
        let bogus = delete_file_spec1("test/bogus-del.parquet", 1, 2, 99);
        let tx = Transaction::new(&table);
        let action = tx.row_delta().add_deletes(vec![bogus]);
        let tx = action.apply(tx).unwrap();
        let err = tx
            .commit(&catalog)
            .await
            .expect_err("an unknown spec id must be rejected");
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(
            err.message()
                .contains("Cannot find partition spec 99 for delete file: test/bogus-del.parquet"),
            "unexpected message: {}",
            err.message()
        );
    }

    // ============================================================================================
    // Partition-value-vs-wrong-spec-type rejection.
    // ============================================================================================

    /// PARTITION-VALUE-VS-WRONG-SPEC-TYPE REJECTION. A file claiming spec 0 (a 1-field `identity(x)`
    /// partition) but carrying a 2-field partition tuple is rejected — the partition value must be
    /// compatible with THAT spec's partition type (not the default's). Risk pinned: the per-spec lift
    /// must validate the partition value against the FILE's claimed spec, so a tuple that matches the
    /// default spec's arity but not the claimed spec's is still caught.
    #[tokio::test]
    async fn test_fast_append_partition_value_against_wrong_spec_type_rejected() {
        let catalog = new_memory_catalog().await;
        let table = make_v2_minimal_table_in_catalog(&catalog).await;
        let (table, _new_spec_id) = evolve_spec(&catalog, &table).await;

        // Claims spec 0 (1-field partition type) but carries a 2-field tuple — incompatible with spec 0.
        let mismatched = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("test/mismatch.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(0)
            .partition(Struct::from_iter([
                Some(Literal::long(1)),
                Some(Literal::long(2)),
            ]))
            .build()
            .unwrap();
        let tx = Transaction::new(&table);
        let action = tx.fast_append().add_data_files(vec![mismatched]);
        let tx = action.apply(tx).unwrap();
        let err = tx
            .commit(&catalog)
            .await
            .expect_err("a partition tuple incompatible with the claimed spec must be rejected");
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(
            err.message()
                .contains("Partition value is not compatible with partition type"),
            "unexpected message: {}",
            err.message()
        );
    }
}
