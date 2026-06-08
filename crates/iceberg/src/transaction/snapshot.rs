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
use crate::spec::{
    DataFile, DataFileFormat, FormatVersion, MAIN_BRANCH, Manifest, ManifestContentType,
    ManifestEntry, ManifestFile, ManifestListWriter, ManifestWriter, ManifestWriterBuilder,
    Operation, Snapshot, SnapshotReference, SnapshotRetention, SnapshotSummaryCollector, Struct,
    StructType, Summary, TableProperties, update_snapshot_summaries,
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
    fn process_manifests(
        &self,
        _snapshot_produce: &SnapshotProducer<'_>,
        manifests: Vec<ManifestFile>,
    ) -> Vec<ManifestFile> {
        manifests
    }
}

pub(crate) trait ManifestProcess: Send + Sync {
    fn process_manifests(
        &self,
        snapshot_produce: &SnapshotProducer<'_>,
        manifests: Vec<ManifestFile>,
    ) -> Vec<ManifestFile>;
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
    // Data files removed by this snapshot, resolved against the current snapshot at commit time. Held
    // so the snapshot summary can reflect the deleted file/record counts (Java overwrite/delete summary).
    // Empty for add-only operations such as fast append.
    removed_data_files: Vec<DataFile>,
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
            removed_data_files: vec![],
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

    /// Validate the added DELETE files (Java `RowDelta.addDeletes` / `MergingSnapshotProducer.add`):
    /// each must be a `PositionDeletes` or `EqualityDeletes` content file (a `Data` file is rejected —
    /// it must be added as a row, not a delete), and its partition spec must match the table default.
    pub(crate) fn validate_added_delete_files(&self) -> Result<()> {
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
            if self.table.metadata().default_partition_spec_id() != delete_file.partition_spec_id {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    "Delete file partition spec id does not match table default partition spec id",
                ));
            }
            Self::validate_partition_value(
                delete_file.partition(),
                self.table.metadata().default_partition_type(),
            )?;
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
            // Check if the data file partition spec id matches the table default partition spec id.
            if self.table.metadata().default_partition_spec_id() != data_file.partition_spec_id {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    "Data file partition spec id does not match table default partition spec id",
                ));
            }
            Self::validate_partition_value(
                data_file.partition(),
                self.table.metadata().default_partition_type(),
            )?;
        }

        Ok(())
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

    /// Return every current data manifest (the candidates the producer's `process_deletes` filters).
    ///
    /// Shared by the delete-bearing operations (`DeleteFiles`, `OverwriteFiles`): each exposes every
    /// current data manifest so `process_deletes` can decide per manifest whether to rewrite, carry
    /// forward, or drop it. Returns an empty list when the table has no current snapshot.
    pub(crate) async fn current_data_manifests(&self) -> Result<Vec<ManifestFile>> {
        let Some(snapshot) = self.table.metadata().current_snapshot() else {
            return Ok(vec![]);
        };

        let manifest_list = snapshot
            .load_manifest_list(self.table.file_io(), &self.table.metadata_ref())
            .await?;

        Ok(manifest_list
            .entries()
            .iter()
            .filter(|entry| entry.content == ManifestContentType::Data)
            .cloned()
            .collect())
    }

    /// Resolve `delete_paths` against the current snapshot's live data entries, returning the matching
    /// [`DataFile`]s, and fail if any requested path matched no live entry.
    ///
    /// Shared by `DeleteFiles` and `OverwriteFiles` (Rule of Three: two identical non-trivial uses).
    /// The requested path set is only known to the calling operation (the producer downstream sees just
    /// the resolved `DataFile`s), so the missing-path check (Java `failMissingDeletePaths`) must happen
    /// here during resolution: a present-and-absent mix errors rather than silently dropping the present
    /// file. Returns an empty vector when `delete_paths` is empty.
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

    fn new_manifest_writer(&mut self, content: ManifestContentType) -> Result<ManifestWriter> {
        let new_manifest_path = format!(
            "{}/{}/{}-m{}.{}",
            self.table.metadata().location(),
            META_ROOT_PATH,
            self.commit_uuid,
            self.manifest_counter.next().unwrap(),
            DataFileFormat::Avro
        );
        let output_file = self.table.file_io().new_output(new_manifest_path)?;
        let builder = ManifestWriterBuilder::new(
            output_file,
            Some(self.snapshot_id),
            self.key_metadata.clone(),
            self.table.metadata().current_schema().clone(),
            self.table
                .metadata()
                .default_partition_spec()
                .as_ref()
                .clone(),
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

    // Write manifest file for added data files and return the ManifestFile for ManifestList.
    async fn write_added_manifest(&mut self) -> Result<ManifestFile> {
        let added_data_files = std::mem::take(&mut self.added_data_files);
        if added_data_files.is_empty() {
            return Err(Error::new(
                ErrorKind::PreconditionFailed,
                "No added data files found when write an added manifest file",
            ));
        }

        let snapshot_id = self.snapshot_id;
        let format_version = self.table.metadata().format_version();
        let manifest_entries = added_data_files.into_iter().map(|data_file| {
            let builder = ManifestEntry::builder()
                .status(crate::spec::ManifestStatus::Added)
                .data_file(data_file);
            if format_version == FormatVersion::V1 {
                builder.snapshot_id(snapshot_id).build()
            } else {
                // For format version > 1, we set the snapshot id at the inherited time to avoid rewrite the manifest file when
                // commit failed.
                builder.build()
            }
        });
        let mut writer = self.new_manifest_writer(ManifestContentType::Data)?;
        for entry in manifest_entries {
            writer.add_entry(entry)?;
        }
        writer.write_manifest_file().await
    }

    /// Write a DELETE manifest for the added delete files and return its [`ManifestFile`] for the
    /// manifest list. Mirrors [`write_added_manifest`](Self::write_added_manifest) but uses the
    /// `Deletes` manifest writer (Java `MergingSnapshotProducer` writes added delete files into a
    /// delete manifest). The entries are `Added` with no sequence number for V2/V3 so they inherit the
    /// new snapshot's sequence number at read time — exactly the mechanism added data files use — which
    /// makes the delete apply to earlier data (`data_seq <= delete_seq`).
    async fn write_added_delete_manifest(&mut self) -> Result<ManifestFile> {
        let added_delete_files = std::mem::take(&mut self.added_delete_files);
        if added_delete_files.is_empty() {
            return Err(Error::new(
                ErrorKind::PreconditionFailed,
                "No added delete files found when writing an added delete manifest file",
            ));
        }

        let snapshot_id = self.snapshot_id;
        let format_version = self.table.metadata().format_version();
        let manifest_entries = added_delete_files.into_iter().map(|delete_file| {
            let builder = ManifestEntry::builder()
                .status(crate::spec::ManifestStatus::Added)
                .data_file(delete_file);
            if format_version == FormatVersion::V1 {
                // Position/equality deletes are V2+ concepts; a V1 table has no delete manifests.
                builder.snapshot_id(snapshot_id).build()
            } else {
                // For format version > 1, set the snapshot id + sequence number at inherited time so the
                // manifest does not need rewriting on a commit retry (same as added data files).
                builder.build()
            }
        });
        let mut writer = self.new_manifest_writer(ManifestContentType::Deletes)?;
        for entry in manifest_entries {
            writer.add_entry(entry)?;
        }
        writer.write_manifest_file().await
    }

    async fn manifest_file<OP: SnapshotProduceOperation, MP: ManifestProcess>(
        &mut self,
        snapshot_produce_operation: &OP,
        manifest_process: &MP,
    ) -> Result<Vec<ManifestFile>> {
        // The data files to remove were resolved in `commit()` (before `summary()`, so the summary can
        // reflect the deletes) and stored in `self.removed_data_files`. Take them here to drive the
        // manifest rewrite without re-resolving.
        let delete_files = std::mem::take(&mut self.removed_data_files);

        // Assert the new snapshot contributes content: added data files, added DELETE files, removed
        // (deleted) data files, or added snapshot properties. An add-deletes-only commit (delete files,
        // no data files) is allowed (the merge-on-read `RowDelta` path); a delete-only data commit
        // (rewrite data manifests, no adds) is allowed; a truly-empty commit is not.
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

        // Process added data entries (DATA manifest).
        if !self.added_data_files.is_empty() {
            let added_manifest = self.write_added_manifest().await?;
            manifest_files.push(added_manifest);
        }

        // Process added DELETE entries (DELETE manifest) — merge-on-read deletes added in this snapshot.
        if !self.added_delete_files.is_empty() {
            let added_delete_manifest = self.write_added_delete_manifest().await?;
            manifest_files.push(added_delete_manifest);
        }

        let manifest_files = manifest_process.process_manifests(self, manifest_files);
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
            FormatVersion::V2 => Ok(builder.build_v2_data()),
            FormatVersion::V3 => Ok(builder.build_v3_data()),
        }
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

        for data_file in &self.added_data_files {
            summary_collector.add_file(
                data_file,
                table_metadata.current_schema().clone(),
                table_metadata.default_partition_spec().clone(),
            );
        }

        // Reflect added DELETE files (position / equality) in the summary. `add_file` branches on the
        // file's content type and increments the added-delete-file + added-position/equality-delete
        // counters (Java `MergingSnapshotProducer.add(DeleteFile)` → the delete-file summary). Empty for
        // operations that add no delete files.
        for delete_file in &self.added_delete_files {
            summary_collector.add_file(
                delete_file,
                table_metadata.current_schema().clone(),
                table_metadata.default_partition_spec().clone(),
            );
        }

        // Reflect deleted files/records in the summary (Java overwrite/delete summary). `removed_data_files`
        // is populated in `commit()` (the resolved delete set) before `summary()` is called; it is empty
        // for add-only operations such as fast append, so this loop is a no-op there.
        for data_file in &self.removed_data_files {
            summary_collector.remove_file(
                data_file,
                table_metadata.current_schema().clone(),
                table_metadata.default_partition_spec().clone(),
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

/// Operations whose snapshots can ADD data files — the only ones a "no conflicting data" validation needs
/// to inspect (Java `MergingSnapshotProducer.VALIDATE_ADDED_FILES_OPERATIONS = {APPEND, OVERWRITE}`). A
/// `Delete` / `Replace` snapshot never introduces brand-new conflicting rows.
fn operation_adds_data_files(operation: &Operation) -> bool {
    matches!(operation, Operation::Append | Operation::Overwrite)
}

/// Enumerate the DATA files ADDED to `table` by snapshots committed AFTER `starting_snapshot_id` — the
/// concurrent commits a serializable-isolation conflict check must inspect.
///
/// This is the Rust port of Java `MergingSnapshotProducer.addedDataFiles` + `validationHistory`
/// (`core/MergingSnapshotProducer.java`): it walks the parent chain of `table`'s current snapshot (the
/// refreshed base / Java `parent`) back via `parent_snapshot_id`, INCLUSIVE of the current snapshot and
/// EXCLUSIVE of `starting_snapshot_id` (Java `SnapshotUtil.ancestorsBetween(parent.snapshotId(),
/// startingSnapshotId)`). For each visited snapshot whose operation can add data
/// ([`operation_adds_data_files`] = Java `VALIDATE_ADDED_FILES_OPERATIONS`), it loads that snapshot's
/// manifest list, keeps the DATA manifests it ADDED (`manifest.added_snapshot_id == snapshot.snapshot_id()`,
/// Java `manifest.snapshotId() == currentSnapshot.snapshotId()`), and collects every `Added`-status entry's
/// [`DataFile`] (Java `ignoreDeleted().ignoreExisting()` + `entry.snapshotId() ∈ newSnapshots`).
///
/// `starting_snapshot_id == None` means "validate from the beginning of history" — every ancestor of the
/// current snapshot is inspected (Java passes a null starting id to `ancestorsBetween`, which walks to the
/// root). When the current snapshot already IS `starting_snapshot_id` (no concurrent commit landed), the walk
/// yields nothing. A table with no current snapshot likewise yields nothing.
///
/// This is the shared foundation the per-action conflict validations (`ReplacePartitions`
/// `validateNoConflictingData`, and the future `OverwriteFiles` / `RowDelta` / `DeleteFiles` checks) build on.
pub(crate) async fn added_data_files_after(
    table: &Table,
    starting_snapshot_id: Option<i64>,
) -> Result<Vec<DataFile>> {
    let metadata = table.metadata();

    // The "parent" of the operation in Java terms: the current head of the refreshed base. If there is no
    // current snapshot, nothing has been added.
    let Some(mut current) = metadata.current_snapshot().cloned() else {
        return Ok(vec![]);
    };

    let mut added = Vec::new();

    loop {
        // Java `ancestorsBetween` is EXCLUSIVE of the starting snapshot: stop before re-visiting it (and
        // never inspect the snapshot the operation started from — its files are part of the base, not a
        // concurrent commit).
        if Some(current.snapshot_id()) == starting_snapshot_id {
            break;
        }

        if operation_adds_data_files(&current.summary().operation) {
            let manifest_list = current
                .load_manifest_list(table.file_io(), metadata)
                .await?;
            for manifest_file in manifest_list.entries() {
                // Only DATA manifests that THIS snapshot added (Java `manifest.snapshotId() ==
                // currentSnapshot.snapshotId()`) — carried-forward manifests belong to older snapshots and
                // their files are not "added since the starting snapshot".
                if manifest_file.content != ManifestContentType::Data
                    || manifest_file.added_snapshot_id != current.snapshot_id()
                {
                    continue;
                }
                let manifest = manifest_file.load_manifest(table.file_io()).await?;
                for entry in manifest.entries() {
                    // Only ADDED entries (Java `ignoreDeleted().ignoreExisting()`) — an `Existing` entry was
                    // added by an earlier snapshot and copied forward, a `Deleted` tombstone is a removal.
                    if entry.status() == crate::spec::ManifestStatus::Added {
                        added.push(entry.data_file().clone());
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

    Ok(added)
}

/// Enumerate the DATA files a SINGLE snapshot `source_snapshot_id` ADDED — the files a cherry-pick of that
/// snapshot must re-add onto the current branch.
///
/// This is the per-single-snapshot analogue of [`added_data_files_after`] (which walks a chain). It mirrors
/// Java `CherryPickOperation`'s `SnapshotChanges.addedDataFiles()` (`manifestsCreatedBy(snap)` +
/// `addedDataFiles(manifest)`): load the source snapshot's manifest list, keep the DATA manifests THAT
/// SNAPSHOT created (`manifest.added_snapshot_id == source_snapshot_id`, Java `manifest.snapshotId() ==
/// snapshot.snapshotId()`), and collect every `Added`-status entry's [`DataFile`] (Java
/// `e.status() == ADDED`). Carried-forward manifests (added by an older snapshot) and `Existing`/`Deleted`
/// entries are excluded — they are not files THIS snapshot added.
///
/// Errors if the snapshot id is not in `table.metadata()` (the caller validates this first, but the helper is
/// defensive). The source is read from the current table metadata — cherry-pick replays a snapshot the table
/// already knows about (one off the current branch, e.g. a staged/forked snapshot).
pub(crate) async fn added_data_files_by_snapshot(
    table: &Table,
    source_snapshot_id: i64,
) -> Result<Vec<DataFile>> {
    snapshot_changed_data_files(
        table,
        source_snapshot_id,
        crate::spec::ManifestStatus::Added,
    )
    .await
}

/// Enumerate the DATA files a SINGLE snapshot `source_snapshot_id` REMOVED (marked `Deleted`) — the files a
/// dynamic-overwrite cherry-pick of that snapshot must re-delete from the current branch.
///
/// The removal sibling of [`added_data_files_by_snapshot`], mirroring Java `CherryPickOperation`'s
/// `SnapshotChanges.removedDataFiles()` for the dynamic-overwrite (`replace-partitions`) replay path
/// (`for (DataFile deletedFile : changes.removedDataFiles()) delete(deletedFile)`). A snapshot's removed data
/// files are the `Deleted`-status entries in the DATA manifests THAT SNAPSHOT created (an overwrite/delete
/// snapshot rewrites a manifest with its `added_snapshot_id`, marking the removed entries `Deleted`).
pub(crate) async fn removed_data_files_by_snapshot(
    table: &Table,
    source_snapshot_id: i64,
) -> Result<Vec<DataFile>> {
    snapshot_changed_data_files(
        table,
        source_snapshot_id,
        crate::spec::ManifestStatus::Deleted,
    )
    .await
}

/// Shared body for [`added_data_files_by_snapshot`] / [`removed_data_files_by_snapshot`]: collect the
/// DATA-file entries with `status == wanted_status` from the manifests `source_snapshot_id` created.
async fn snapshot_changed_data_files(
    table: &Table,
    source_snapshot_id: i64,
    wanted_status: crate::spec::ManifestStatus,
) -> Result<Vec<DataFile>> {
    let metadata = table.metadata();
    let Some(source) = metadata.snapshot_by_id(source_snapshot_id) else {
        return Err(Error::new(
            ErrorKind::DataInvalid,
            format!("Cannot read changes of unknown snapshot ID: {source_snapshot_id}"),
        ));
    };

    let manifest_list = source.load_manifest_list(table.file_io(), metadata).await?;

    let mut files = Vec::new();
    for manifest_file in manifest_list.entries() {
        // Only DATA manifests that THIS snapshot created (Java `manifestsCreatedBy`:
        // `manifest.snapshotId() == snapshot.snapshotId()`). A carried-forward manifest belongs to an older
        // snapshot, so its entries are not changes introduced by this snapshot.
        if manifest_file.content != ManifestContentType::Data
            || manifest_file.added_snapshot_id != source_snapshot_id
        {
            continue;
        }
        let manifest = manifest_file.load_manifest(table.file_io()).await?;
        for entry in manifest.entries() {
            if entry.status() == wanted_status {
                files.push(entry.data_file().clone());
            }
        }
    }

    Ok(files)
}
