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
use crate::spec::snapshot_summary::{MANIFESTS_CREATED, MANIFESTS_KEPT, MANIFESTS_REPLACED};
use crate::spec::{
    DataContentType, DataFile, DataFileFormat, FormatVersion, MAIN_BRANCH, ManifestContentType,
    ManifestEntry, ManifestFile, ManifestListWriter, ManifestWriter, ManifestWriterBuilder,
    Operation, Snapshot, SnapshotReference, SnapshotRetention, SnapshotSummaryCollector, Struct,
    StructType, Summary, TableProperties, update_snapshot_summaries,
};
use crate::table::Table;
use crate::transaction::ActionCommit;
use crate::{Error, ErrorKind, TableRequirement, TableUpdate};

pub(crate) const META_ROOT_PATH: &str = "metadata";

pub(crate) struct CommitResult {
    pub commit: ActionCommit,
    /// Manifest paths written into the new snapshot. Pass to `clean_uncommitted`
    /// to delete orphaned prior-attempt residuals. See `RewriteFilesAction` for usage.
    pub committed_manifest_paths: HashSet<String>,
}

/// Set the per-commit manifest activity counters on `summary` from the final
/// manifest list and the action's replaced count.
///
/// Java analog: `org.apache.iceberg.SnapshotProducer::commitSummary`.
fn inject_manifest_summary_keys(
    summary: &mut Summary,
    manifests: &[ManifestFile],
    snapshot_id: i64,
    replaced_count: u64,
) {
    let mut created = 0u64;
    let mut kept = 0u64;
    for m in manifests {
        if m.added_snapshot_id == snapshot_id {
            created += 1;
        } else {
            kept += 1;
        }
    }
    // Do not overwrite values that may have been explicitly set by an action
    // like RewriteManifestsAction. Only insert counters if they are absent.
    summary
        .additional_properties
        .entry(MANIFESTS_CREATED.to_string())
        .or_insert_with(|| created.to_string());
    summary
        .additional_properties
        .entry(MANIFESTS_KEPT.to_string())
        .or_insert_with(|| kept.to_string());
    summary
        .additional_properties
        .entry(MANIFESTS_REPLACED.to_string())
        .or_insert_with(|| replaced_count.to_string());
}

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

    /// Returns delete files (equality/position deletes) to be written into a new
    /// `ManifestContent::Deletes` manifest for this snapshot. Default: empty.
    fn added_delete_entries(&self) -> &[DataFile] {
        &[]
    }

    /// Returns `true` when the operation has content to commit even if
    /// `added_data_files` is empty (e.g. `DeleteFilesAction` rewrites existing
    /// manifests without adding new data). Default: `false`.
    fn has_pending_content(&self) -> bool {
        false
    }
}

pub(crate) struct DefaultManifestProcess;

impl ManifestProcess for DefaultManifestProcess {
    async fn process_manifests(
        &self,
        _snapshot_produce: &SnapshotProducer<'_>,
        manifests: Vec<ManifestFile>,
        _new_manifest_paths: &HashSet<String>,
    ) -> Result<Vec<ManifestFile>> {
        Ok(manifests)
    }
}

/// Hook that a snapshot-producing action can use to transform the manifest list
/// after it's been assembled (carry-forward + new added + new delete) but
/// before it's written into the snapshot's manifest list. The merging snapshot
/// producer uses this hook to bin-pack siblings via `MergingState::merge_manifests`.
pub(crate) trait ManifestProcess: Send + Sync {
    fn process_manifests(
        &self,
        snapshot_produce: &SnapshotProducer<'_>,
        manifests: Vec<ManifestFile>,
        new_manifest_paths: &HashSet<String>,
    ) -> impl Future<Output = Result<Vec<ManifestFile>>> + Send;

    /// Number of manifests this process replaced as part of the transformation.
    /// Surfaces as the snapshot summary's `manifests-replaced` key. Default 0
    /// for processes that don't replace anything.
    fn replaced_manifests_count(&self) -> u64 {
        0
    }
}

pub(crate) struct SnapshotProducer<'a> {
    pub(crate) table: &'a Table,
    snapshot_id: i64,
    commit_uuid: Uuid,
    key_metadata: Option<Vec<u8>>,
    snapshot_properties: HashMap<String, String>,
    added_data_files: Vec<DataFile>,
    removed_data_files: Vec<DataFile>,
    /// Explicit data sequence number for added manifest entries. When `Some`, all entries written
    /// by `write_added_manifest()` carry this sequence number instead of inheriting from the new
    /// snapshot. Used by `RewriteFilesAction::data_sequence_number()` to preserve the
    /// original sequence number of compacted files when equality deletes are present.
    data_sequence_number: Option<i64>,
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
            removed_data_files: vec![],
            data_sequence_number: None,
            manifest_counter: (0..),
        }
    }

    pub(crate) fn with_removed_data_files(mut self, files: Vec<DataFile>) -> Self {
        self.removed_data_files = files;
        self
    }

    pub(crate) fn with_data_sequence_number(mut self, seq_num: i64) -> Self {
        self.data_sequence_number = Some(seq_num);
        self
    }

    pub(crate) fn snapshot_id(&self) -> i64 {
        self.snapshot_id
    }

    pub(crate) fn commit_uuid(&self) -> Uuid {
        self.commit_uuid
    }

    pub(crate) fn key_metadata(&self) -> Option<&[u8]> {
        self.key_metadata.as_deref()
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

    /// Reject the commit if any ancestor since `starting_snapshot_id` (or all
    /// ancestors when `None`) added a delete file that targets one of the
    /// replaced paths.
    ///
    /// Equality deletes are ignored when the producer has a
    /// `data_sequence_number` set — the rewrite preserves the original sequence
    /// number, so equality deletes at higher sequence numbers continue to apply
    /// correctly and don't conflict.
    ///
    /// Java analog: `org.apache.iceberg.MergingSnapshotProducer::validateNoNewDeletesForDataFiles`.
    pub(crate) async fn validate_no_new_deletes_for_data_files(
        &self,
        starting_snapshot_id: Option<i64>,
        replaced_data_files: &HashSet<String>,
    ) -> Result<()> {
        let metadata = self.table.metadata();
        if metadata.format_version() == FormatVersion::V1 {
            return Ok(());
        }
        let Some(parent) = metadata.current_snapshot() else {
            return Ok(());
        };
        if replaced_data_files.is_empty() {
            return Ok(());
        }

        let ignore_equality_deletes = self.data_sequence_number.is_some();

        let (starting_seq_num, final_starting_snapshot_id) = match starting_snapshot_id {
            Some(id) => match metadata.snapshot_by_id(id) {
                Some(s) => (s.sequence_number(), Some(id)),
                None => {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!(
                            "Cannot determine history between starting snapshot {} and the last known ancestor",
                            id
                        ),
                    ));
                }
            },
            None => (0, None),
        };

        let metadata_ref = self.table.metadata_ref();
        let mut current = Some(parent.clone());
        while let Some(snap) = current {
            if Some(snap.snapshot_id()) == final_starting_snapshot_id {
                break;
            }

            let manifest_list = snap
                .load_manifest_list(self.table.file_io(), &metadata_ref)
                .await?;

            for ml_entry in manifest_list.entries() {
                if ml_entry.content != ManifestContentType::Deletes {
                    continue;
                }
                // Only delete manifests this snapshot itself added — older
                // ones were already in scope at `starting_snapshot_id`.
                if ml_entry.added_snapshot_id != snap.snapshot_id() {
                    continue;
                }

                let manifest = self.table.object_cache().get_manifest(ml_entry).await?;
                for entry in manifest.entries() {
                    if !entry.is_alive() {
                        continue;
                    }
                    let f = &entry.data_file;
                    match f.content_type() {
                        DataContentType::PositionDeletes => match f.referenced_data_file() {
                            Some(ref_path) => {
                                if replaced_data_files.contains(&ref_path) {
                                    return Err(Error::new(
                                        ErrorKind::DataInvalid,
                                        format!(
                                            "Cannot commit, found new position delete \
                                                 for replaced data file: {ref_path}"
                                        ),
                                    ));
                                }
                            }
                            None => {
                                return Err(Error::new(
                                    ErrorKind::DataInvalid,
                                    format!(
                                        "Cannot commit, found new position delete file {} \
                                             missing referenced_data_file hint",
                                        f.file_path
                                    ),
                                ));
                            }
                        },
                        DataContentType::EqualityDeletes => {
                            if !ignore_equality_deletes
                                && entry.sequence_number().unwrap_or(0) > starting_seq_num
                            {
                                return Err(Error::new(
                                    ErrorKind::DataInvalid,
                                    format!(
                                        "Cannot commit, found new equality delete file \
                                         {} added at sequence {} since starting sequence \
                                         {starting_seq_num}",
                                        f.file_path,
                                        entry.sequence_number().unwrap_or(0)
                                    ),
                                ));
                            }
                        }
                        DataContentType::Data => {}
                    }
                }
            }

            current = snap
                .parent_snapshot_id()
                .and_then(|pid| metadata.snapshot_by_id(pid))
                .cloned();
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
                let manifest = self
                    .table
                    .object_cache()
                    .get_manifest(manifest_list_entry)
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

    /// Validates that every data file in `files` is still alive in the current
    /// snapshot. This ensures that a concurrent rewrite or delete hasn't already
    /// removed the files being replaced.
    ///
    /// Java analog: `MergingSnapshotProducer.validateDataFilesExist`
    pub(crate) async fn validate_data_files_exist(&self, files: &HashSet<String>) -> Result<()> {
        if files.is_empty() {
            return Ok(());
        }

        let Some(current_snapshot) = self.table.metadata().current_snapshot() else {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Cannot validate file existence on a table with no snapshots",
            ));
        };

        let manifest_list = current_snapshot
            .load_manifest_list(self.table.file_io(), &self.table.metadata_ref())
            .await?;

        let mut found_files = HashSet::new();
        for manifest_list_entry in manifest_list.entries() {
            let manifest = self
                .table
                .object_cache()
                .get_manifest(manifest_list_entry)
                .await?;
            for entry in manifest.entries() {
                if entry.is_alive() && files.contains(entry.file_path()) {
                    found_files.insert(entry.file_path().to_string());
                }
            }
        }

        let missing: Vec<&str> = files
            .iter()
            .map(|s| s.as_str())
            .filter(|p| !found_files.contains(*p))
            .collect();

        if !missing.is_empty() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Cannot commit, files being replaced are no longer alive in the current snapshot: {}",
                    missing.join(", ")
                ),
            ));
        }

        Ok(())
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

    // Write a manifest containing deleted entries and return the ManifestFile for the ManifestList.
    //
    // ManifestContentType::Data is correct here: we are writing a data manifest that records
    // which data files were deleted (Deleted-status entries). This is distinct from
    // ManifestContentType::Deletes, which is for Iceberg v2 delete files (position/equality
    // deletes). Do not change this to ManifestContentType::Deletes.
    async fn write_delete_manifest(
        &mut self,
        deleted_entries: Vec<ManifestEntry>,
    ) -> Result<ManifestFile> {
        let mut writer = self.new_manifest_writer(ManifestContentType::Data)?;
        for entry in deleted_entries {
            writer.add_delete_entry(entry)?;
        }
        writer.write_manifest_file().await
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
        self.write_entries_as_manifest(
            &added_data_files,
            ManifestContentType::Data,
            self.data_sequence_number,
        )
        .await
    }

    async fn write_added_delete_manifest(
        &mut self,
        delete_files: &[DataFile],
    ) -> Result<ManifestFile> {
        self.write_entries_as_manifest(delete_files, ManifestContentType::Deletes, None)
            .await
    }

    async fn write_entries_as_manifest(
        &mut self,
        files: &[DataFile],
        content: ManifestContentType,
        data_seq_num: Option<i64>,
    ) -> Result<ManifestFile> {
        let snapshot_id = self.snapshot_id;
        let format_version = self.table.metadata().format_version();
        let entries = files.iter().map(|data_file| {
            let builder = ManifestEntry::builder()
                .status(crate::spec::ManifestStatus::Added)
                .data_file(data_file.clone());
            if format_version == FormatVersion::V1 {
                builder.snapshot_id(snapshot_id).build()
            } else if let Some(seq_num) = data_seq_num {
                // Explicitly set sequence number instead of inheriting from the new snapshot.
                // Required when compacting files that predate an equality delete: the compacted
                // files must retain the original sequence number so equality deletes with a higher
                // sequence number continue to apply.
                builder.sequence_number(seq_num).build()
            } else {
                builder.build()
            }
        });
        let mut writer = self.new_manifest_writer(content)?;
        for entry in entries {
            writer.add_entry(entry)?;
        }
        writer.write_manifest_file().await
    }

    async fn manifest_file<OP: SnapshotProduceOperation, MP: ManifestProcess>(
        &mut self,
        snapshot_produce_operation: &OP,
        manifest_process: &MP,
    ) -> Result<Vec<ManifestFile>> {
        // Assert current snapshot producer contains new content to add to new snapshot.
        //
        // TODO: Allowing snapshot property setup with no added data files is a workaround.
        // We should clean it up after all necessary actions are supported.
        // For details, please refer to https://github.com/apache/iceberg-rust/issues/1548
        if self.added_data_files.is_empty()
            && self.snapshot_properties.is_empty()
            && !snapshot_produce_operation.has_pending_content()
        {
            return Err(Error::new(
                ErrorKind::PreconditionFailed,
                "No added data files or added snapshot properties found when write a manifest file",
            ));
        }

        let existing_manifests = snapshot_produce_operation.existing_manifest(self).await?;
        let mut manifest_files = existing_manifests;
        let mut new_manifest_paths = HashSet::new();

        // Process added entries.
        if !self.added_data_files.is_empty() {
            let added_manifest = self.write_added_manifest().await?;
            new_manifest_paths.insert(added_manifest.manifest_path.clone());
            manifest_files.push(added_manifest);
        }

        // Write a ManifestContent::Deletes manifest for any added delete files.
        let added_delete_files = snapshot_produce_operation.added_delete_entries();
        if !added_delete_files.is_empty() {
            let delete_manifest = self.write_added_delete_manifest(added_delete_files).await?;
            new_manifest_paths.insert(delete_manifest.manifest_path.clone());
            manifest_files.push(delete_manifest);
        }

        // # TODO
        // Support process delete entries.
        let deleted_entries = snapshot_produce_operation.delete_entries(self).await?;
        if !deleted_entries.is_empty() {
            let delete_manifest = self.write_delete_manifest(deleted_entries).await?;
            new_manifest_paths.insert(delete_manifest.manifest_path.clone());
            manifest_files.push(delete_manifest);
        }

        let manifest_files = manifest_process
            .process_manifests(self, manifest_files, &new_manifest_paths)
            .await?;
        Ok(manifest_files)
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

        for data_file in snapshot_produce_operation.added_delete_entries() {
            summary_collector.add_file(
                data_file,
                table_metadata.current_schema().clone(),
                table_metadata.default_partition_spec().clone(),
            );
        }

        // NOTE: summary statistics for removed files (deleted-records, removed-files-size, etc.)
        // come from the caller-supplied DataFile structs, not from the actual snapshot entries.
        // delete_entries() validates that paths exist but does not verify metadata matches.
        // Callers must ensure the DataFile statistics they pass are accurate.
        for data_file in &self.removed_data_files {
            summary_collector.remove_file(
                data_file,
                table_metadata.current_schema().clone(),
                table_metadata.default_partition_spec().clone(),
            );
        }

        // The current snapshot becomes the parent of the new one being created.
        let previous_snapshot = table_metadata.current_snapshot();

        let mut additional_properties = summary_collector.build();
        additional_properties.extend(self.snapshot_properties.clone());

        let summary = Summary {
            operation: snapshot_produce_operation.operation(),
            additional_properties,
        };

        update_snapshot_summaries(
            summary,
            previous_snapshot.map(|s| s.summary()),
            snapshot_produce_operation.operation() == Operation::Overwrite,
        )
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

    /// Finished building the action and return the [`ActionCommit`] to the transaction
    /// along with the set of committed manifest paths for `clean_uncommitted`.
    pub(crate) async fn commit<OP: SnapshotProduceOperation, MP: ManifestProcess>(
        mut self,
        snapshot_produce_operation: OP,
        process: MP,
    ) -> Result<CommitResult> {
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

        // Build the file-counter summary BEFORE manifest_file empties
        // self.added_data_files (via mem::take in write_added_manifest). The
        // manifest counters are added post-hoc once the final list is known.
        let mut summary = self.summary(&snapshot_produce_operation).map_err(|err| {
            Error::new(ErrorKind::Unexpected, "Failed to create snapshot summary.").with_source(err)
        })?;

        let new_manifests = self
            .manifest_file(&snapshot_produce_operation, &process)
            .await?;

        let committed_manifest_paths: HashSet<String> = new_manifests
            .iter()
            .map(|m| m.manifest_path.clone())
            .collect();

        inject_manifest_summary_keys(
            &mut summary,
            &new_manifests,
            self.snapshot_id,
            process.replaced_manifests_count(),
        );

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

        Ok(CommitResult {
            commit: ActionCommit::new(updates, requirements),
            committed_manifest_paths,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::spec::{DataContentType, DataFileBuilder, DataFileFormat, Literal, Struct};
    use crate::transaction::tests::{apply_updates_to_table, make_v1_table, make_v2_minimal_table};
    use crate::transaction::{Transaction, TransactionAction};

    fn data_file(table: &Table, path: &str) -> DataFile {
        DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(path.to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .build()
            .unwrap()
    }

    #[tokio::test]
    async fn validate_no_new_deletes_short_circuits_for_v1() {
        let table = make_v1_table();
        let producer = SnapshotProducer::new(&table, Uuid::now_v7(), None, HashMap::new(), vec![
            data_file(&table, "data/x.parquet"),
        ]);
        let mut replaced = HashSet::new();
        replaced.insert("data/replaced.parquet".to_string());

        producer
            .validate_no_new_deletes_for_data_files(None, &replaced)
            .await
            .expect("V1 must short-circuit");
    }

    #[tokio::test]
    async fn validate_no_new_deletes_short_circuits_for_empty_table() {
        let table = make_v2_minimal_table();
        let producer = SnapshotProducer::new(&table, Uuid::now_v7(), None, HashMap::new(), vec![
            data_file(&table, "data/x.parquet"),
        ]);
        let mut replaced = HashSet::new();
        replaced.insert("data/replaced.parquet".to_string());

        producer
            .validate_no_new_deletes_for_data_files(None, &replaced)
            .await
            .expect("empty-snapshot table must short-circuit");
    }

    #[tokio::test]
    async fn validate_no_new_deletes_short_circuits_for_empty_replaced_set() {
        let table = make_v2_minimal_table();
        let f = data_file(&table, "data/x.parquet");
        let tx = Transaction::new(&table);
        let append = tx.fast_append().add_data_files(vec![f.clone()]);
        let updates = Arc::new(append)
            .commit(&table)
            .await
            .unwrap()
            .take_updates();
        let table = apply_updates_to_table(&table, &updates);

        let producer = SnapshotProducer::new(&table, Uuid::now_v7(), None, HashMap::new(), vec![
            data_file(&table, "data/y.parquet"),
        ]);
        let replaced = HashSet::new();

        producer
            .validate_no_new_deletes_for_data_files(None, &replaced)
            .await
            .expect("empty replaced set must short-circuit");
    }

    #[tokio::test]
    async fn validate_no_new_deletes_passes_when_no_delete_manifests() {
        let table = make_v2_minimal_table();
        let f1 = data_file(&table, "data/f1.parquet");
        let f2 = data_file(&table, "data/f2.parquet");
        let tx = Transaction::new(&table);
        let append = tx
            .fast_append()
            .add_data_files(vec![f1.clone(), f2.clone()]);
        let updates = Arc::new(append)
            .commit(&table)
            .await
            .unwrap()
            .take_updates();
        let table = apply_updates_to_table(&table, &updates);

        let producer = SnapshotProducer::new(&table, Uuid::now_v7(), None, HashMap::new(), vec![
            data_file(&table, "data/compacted.parquet"),
        ])
        .with_removed_data_files(vec![f1.clone()]);

        let mut replaced = HashSet::new();
        replaced.insert(f1.file_path.clone());

        producer
            .validate_no_new_deletes_for_data_files(None, &replaced)
            .await
            .expect("table without delete manifests must pass");

        // A path that never existed also passes — no false positives.
        let mut other = HashSet::new();
        other.insert("data/never-existed.parquet".to_string());
        producer
            .validate_no_new_deletes_for_data_files(None, &other)
            .await
            .expect("path not present in any ancestor still passes");
    }

    #[tokio::test]
    async fn test_validate_rejects_when_planning_snapshot_expired() {
        let table = make_v2_minimal_table();
        let f1 = data_file(&table, "data/f1.parquet");

        // 1. Add data file
        let tx = Transaction::new(&table);
        let append = tx.fast_append().add_data_files(vec![f1.clone()]);
        let updates = Arc::new(append)
            .commit(&table)
            .await
            .unwrap()
            .take_updates();
        let table = apply_updates_to_table(&table, &updates);
        let first_snapshot_id = table.metadata().current_snapshot_id().unwrap();

        // 2. Try to rewrite f1, but pass a NON-EXISTENT starting_snapshot_id
        // (simulating it was expired)
        let expired_id = first_snapshot_id - 100;
        let producer = SnapshotProducer::new(&table, Uuid::now_v7(), None, HashMap::new(), vec![
            data_file(&table, "data/compacted.parquet"),
        ])
        .with_removed_data_files(vec![f1.clone()]);

        let mut replaced = HashSet::new();
        replaced.insert(f1.file_path.clone());

        let result = producer
            .validate_no_new_deletes_for_data_files(Some(expired_id), &replaced)
            .await;

        assert!(
            result.is_err(),
            "Expired planning snapshot must be rejected"
        );
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Cannot determine history between starting snapshot"),
            "Error message should mention history determination failure"
        );
    }

    #[tokio::test]
    async fn test_validate_real_equality_delete_conflict_still_rejected() {
        let table = make_v2_minimal_table();
        let f1 = data_file(&table, "data/f1.parquet");

        // 1. Add data file (Snapshot A)
        let tx = Transaction::new(&table);
        let append = tx.fast_append().add_data_files(vec![f1.clone()]);
        let updates = Arc::new(append)
            .commit(&table)
            .await
            .unwrap()
            .take_updates();
        let table = apply_updates_to_table(&table, &updates);
        let snap_a_id = table.metadata().current_snapshot_id().unwrap();

        // 2. Add an equality delete (Snapshot B)
        let eq_delete = DataFileBuilder::default()
            .content(DataContentType::EqualityDeletes)
            .file_path("data/delete1.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .build()
            .unwrap();

        let tx = Transaction::new(&table);
        let delete = tx.row_delta().add_delete_files(vec![eq_delete]);
        let updates = Arc::new(delete)
            .commit(&table)
            .await
            .unwrap()
            .take_updates();
        let table = apply_updates_to_table(&table, &updates);

        // 3. Try to rewrite f1, starting from Snapshot A.
        // It SHOULD reject because Snapshot B added a delete since A.
        let producer = SnapshotProducer::new(&table, Uuid::now_v7(), None, HashMap::new(), vec![
            data_file(&table, "data/compacted.parquet"),
        ])
        .with_removed_data_files(vec![f1.clone()]);

        let mut replaced = HashSet::new();
        replaced.insert(f1.file_path.clone());

        let result = producer
            .validate_no_new_deletes_for_data_files(Some(snap_a_id), &replaced)
            .await;

        assert!(result.is_err(), "Real conflict must still be rejected");
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("found new equality delete file"),
            "Error message should mention new equality delete"
        );
    }

    #[tokio::test]
    async fn test_validate_rejects_when_position_delete_targets_replaced_file() {
        let table = make_v2_minimal_table();
        let f1 = data_file(&table, "data/f1.parquet");

        // 1. Add data file (Snapshot A)
        let tx = Transaction::new(&table);
        let append = tx.fast_append().add_data_files(vec![f1.clone()]);
        let updates = Arc::new(append)
            .commit(&table)
            .await
            .unwrap()
            .take_updates();
        let table = apply_updates_to_table(&table, &updates);
        let snap_a_id = table.metadata().current_snapshot_id().unwrap();

        // 2. Add a position delete (Snapshot B)
        let pos_delete = DataFileBuilder::default()
            .content(DataContentType::PositionDeletes)
            .file_path("data/delete1.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .referenced_data_file(Some(f1.file_path.clone()))
            .build()
            .unwrap();

        let tx = Transaction::new(&table);
        let delete = tx.row_delta().add_delete_files(vec![pos_delete]);
        let updates = Arc::new(delete)
            .commit(&table)
            .await
            .unwrap()
            .take_updates();
        let table = apply_updates_to_table(&table, &updates);

        // 3. Try to rewrite f1, starting from Snapshot A.
        let producer = SnapshotProducer::new(&table, Uuid::now_v7(), None, HashMap::new(), vec![
            data_file(&table, "data/compacted.parquet"),
        ])
        .with_removed_data_files(vec![f1.clone()]);

        let mut replaced = HashSet::new();
        replaced.insert(f1.file_path.clone());

        let result = producer
            .validate_no_new_deletes_for_data_files(Some(snap_a_id), &replaced)
            .await;

        assert!(result.is_err(), "New position delete must be rejected");
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("found new position delete for replaced data file"),
            "Error message should mention new position delete"
        );
    }

    #[tokio::test]
    async fn test_validate_equality_delete_boundary_sequence_number() {
        let table = make_v2_minimal_table();
        let f1 = data_file(&table, "data/f1.parquet");

        // 1. Add data file (Snapshot A)
        let tx = Transaction::new(&table);
        let append = tx.fast_append().add_data_files(vec![f1.clone()]);
        let updates = Arc::new(append)
            .commit(&table)
            .await
            .unwrap()
            .take_updates();
        let table = apply_updates_to_table(&table, &updates);

        // 2. Add an equality delete (Snapshot B)
        let eq_delete = DataFileBuilder::default()
            .content(DataContentType::EqualityDeletes)
            .file_path("data/delete1.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .build()
            .unwrap();

        let tx = Transaction::new(&table);
        let add_delete = tx.row_delta().add_delete_files(vec![eq_delete]);
        let updates = Arc::new(add_delete)
            .commit(&table)
            .await
            .unwrap()
            .take_updates();
        let table = apply_updates_to_table(&table, &updates);

        // snap_b is the current snapshot — planning STARTS from snap_b so the
        // validator's loop short-circuits immediately (current == starting).
        // This tests that a delete present at planning time does not trigger rejection.
        let snap_b_id = table.metadata().current_snapshot_id().unwrap();

        let producer = SnapshotProducer::new(&table, Uuid::now_v7(), None, HashMap::new(), vec![
            data_file(&table, "data/compacted.parquet"),
        ])
        .with_removed_data_files(vec![f1.clone()]);

        let mut replaced = HashSet::new();
        replaced.insert(f1.file_path.clone());

        producer
            .validate_no_new_deletes_for_data_files(Some(snap_b_id), &replaced)
            .await
            .expect("equality delete already present at planning time must pass");
    }

    #[tokio::test]
    async fn test_validate_ignore_equality_deletes_branch() {
        let table = make_v2_minimal_table();
        let f1 = data_file(&table, "data/f1.parquet");

        // 1. Add data file (Snapshot A)
        let tx = Transaction::new(&table);
        let append = tx.fast_append().add_data_files(vec![f1.clone()]);
        let updates = Arc::new(append)
            .commit(&table)
            .await
            .unwrap()
            .take_updates();
        let table = apply_updates_to_table(&table, &updates);
        let snap_a_id = table.metadata().current_snapshot_id().unwrap();

        // 2. Add an equality delete (Snapshot B)
        let eq_delete = DataFileBuilder::default()
            .content(DataContentType::EqualityDeletes)
            .file_path("data/delete1.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .build()
            .unwrap();

        let tx = Transaction::new(&table);
        let delete = tx.row_delta().add_delete_files(vec![eq_delete]);
        let updates = Arc::new(delete)
            .commit(&table)
            .await
            .unwrap()
            .take_updates();
        let table = apply_updates_to_table(&table, &updates);

        // 3. Try to rewrite f1, starting from Snapshot A, with ignore_equality_deletes = true.
        // This is safe when the rewrite preserves the original sequence number.
        let producer = SnapshotProducer::new(&table, Uuid::now_v7(), None, HashMap::new(), vec![
            data_file(&table, "data/compacted.parquet"),
        ])
        .with_removed_data_files(vec![f1.clone()])
        .with_data_sequence_number(1); // Setting this triggers ignore_equality_deletes = true

        let mut replaced = HashSet::new();
        replaced.insert(f1.file_path.clone());

        producer
            .validate_no_new_deletes_for_data_files(Some(snap_a_id), &replaced)
            .await
            .expect("should pass when ignore_equality_deletes is true and Some(seq) provided");

        // 4. Test the case where ignore_equality_deletes = true BUT no data_sequence_number is provided
        // (This shouldn't happen via RewriteFilesAction, but we test the validator's internal logic)
        // Currently SnapshotProducer doesn't expose a way to set ignore_equality_deletes directly,
        // but it's derived from data_sequence_number.is_some().
    }
}
