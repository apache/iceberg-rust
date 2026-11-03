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

//! Shared engine for snapshot-producing operations that both add and delete files.
//!
//! [`MergingSnapshotProducer`] is the Rust equivalent of Java's
//! `MergingSnapshotProducer`. It handles manifest filtering, new manifest
//! creation, and writes the final snapshot through [`write_snapshot_commit`].

use std::collections::{HashMap, HashSet};

use uuid::Uuid;

use crate::error::{Result, invalid_data};
use crate::spec::{
    DataContentType, DataFile, FormatVersion, ManifestContentType, ManifestEntry, ManifestFile,
    ManifestStatus, Operation,
};
use crate::table::Table;
use crate::transaction::ActionCommit;
use crate::transaction::snapshot::{
    SnapshotCommitInput, generate_snapshot_id, manifest_list_path, manifest_path,
    new_manifest_writer, validate_files_not_live, validate_partition_value, write_snapshot_commit,
};

/// Filters existing manifests by removing entries for deleted data files.
///
/// This is equivalent to Java's `ManifestFilterManager`. When a rewrite or
/// overwrite operation deletes files, the filter manager rewrites affected
/// manifests so that deleted entries are dropped and surviving entries are
/// re-emitted with [`ManifestStatus::Existing`].
pub(crate) struct ManifestFilterManager {
    deleted_file_paths: HashSet<String>,
    fail_missing_delete_paths: bool,
}

impl ManifestFilterManager {
    pub(crate) fn new(fail_missing_delete_paths: bool) -> Self {
        Self {
            deleted_file_paths: HashSet::new(),
            fail_missing_delete_paths,
        }
    }

    pub(crate) fn add_delete(&mut self, path: String) {
        self.deleted_file_paths.insert(path);
    }

    /// Filter `manifests` by removing entries whose file path is in the delete
    /// set. Returns the surviving manifests plus the removed data files.
    pub(crate) async fn filter_manifests(
        &self,
        table: &Table,
        manifests: Vec<ManifestFile>,
        snapshot_id: i64,
        commit_uuid: Uuid,
        manifest_counter: &mut u64,
    ) -> Result<(Vec<ManifestFile>, Vec<DataFile>)> {
        if self.deleted_file_paths.is_empty() {
            return Ok((manifests, Vec::new()));
        }

        let mut result: Vec<ManifestFile> = Vec::with_capacity(manifests.len());
        let mut removed_files: Vec<DataFile> = Vec::new();
        let mut found_paths: HashSet<String> = HashSet::new();

        for manifest_file in &manifests {
            // Only filter data manifests; pass delete manifests through unchanged.
            if manifest_file.content != ManifestContentType::Data {
                result.push(manifest_file.clone());
                continue;
            }

            let manifest = table.manifest_reader().read(manifest_file).await?;

            // Check whether this manifest contains any files we want to delete.
            let has_deletes = manifest
                .entries()
                .iter()
                .any(|e| e.is_alive() && self.deleted_file_paths.contains(e.file_path()));

            if !has_deletes {
                // Manifest is unaffected — pass through verbatim.
                result.push(manifest_file.clone());
                continue;
            }

            // Resolve the partition spec for this manifest. After partition
            // evolution, old manifests may carry a non-default spec — we must
            // use the manifest's own spec so the rewritten manifest stays valid.
            let manifest_spec = table
                .metadata()
                .partition_spec_by_id(manifest_file.partition_spec_id)
                .ok_or_else(|| {
                    invalid_data!(
                        "Manifest references unknown partition spec {}",
                        manifest_file.partition_spec_id,
                    )
                })?
                .clone();
            let schema = table.metadata().current_schema().clone();

            // Rewrite: keep surviving entries as EXISTING, and record the
            // removed ones as DELETED so that the file they name can be
            // cleaned up when this snapshot expires.
            let mut surviving_entries: Vec<ManifestEntry> = Vec::new();
            let mut removed_entries: Vec<ManifestEntry> = Vec::new();
            for entry in manifest.entries() {
                if entry.is_alive() && self.deleted_file_paths.contains(entry.file_path()) {
                    found_paths.insert(entry.file_path().to_string());
                    removed_files.push(entry.data_file().clone());
                    removed_entries.push(entry.as_ref().clone());
                } else if entry.is_alive() {
                    // Surviving entry — re-emit as EXISTING with original ids preserved.
                    let seq = entry.sequence_number().ok_or_else(|| {
                        invalid_data!(
                            "Manifest entry for {} is missing sequence_number",
                            entry.file_path(),
                        )
                    })?;
                    let file_seq = entry.file_sequence_number.ok_or_else(|| {
                        invalid_data!(
                            "Manifest entry for {} is missing file_sequence_number",
                            entry.file_path(),
                        )
                    })?;
                    let existing = ManifestEntry::builder()
                        .status(ManifestStatus::Existing)
                        .snapshot_id(
                            entry
                                .snapshot_id()
                                .unwrap_or(manifest_file.added_snapshot_id),
                        )
                        .sequence_number(seq)
                        .file_sequence_number(file_seq)
                        .data_file(entry.data_file().clone())
                        .build();
                    surviving_entries.push(existing);
                }
                // Already-deleted entries (status == Deleted) are dropped.
            }

            if surviving_entries.is_empty() && removed_entries.is_empty() {
                // Nothing left to say about this manifest — omit entirely.
                continue;
            }

            // Write the filtered manifest using the manifest's own partition spec.
            let new_manifest_path = manifest_path(table, commit_uuid, *manifest_counter)?;
            *manifest_counter += 1;
            // Use the current snapshot_id so that the manifest list writer
            // can assign sequence numbers to this rewritten manifest.
            let mut writer = new_manifest_writer(
                table,
                new_manifest_path,
                snapshot_id,
                ManifestContentType::Data,
                schema,
                manifest_spec,
            )?;
            for entry in surviving_entries {
                writer.add_existing_entry(entry)?;
            }
            for entry in removed_entries {
                // `add_delete_entry` stamps the removing snapshot on the entry
                // and keeps the file's own sequence numbers, which is what an
                // expiring reader needs to attribute the removal.
                writer.add_delete_entry(entry)?;
            }
            let new_manifest = writer.write_manifest_file().await?;
            result.push(new_manifest);
        }

        // Validate that every delete target was found.
        if self.fail_missing_delete_paths {
            let missing: Vec<&str> = self
                .deleted_file_paths
                .iter()
                .filter(|p| !found_paths.contains(p.as_str()))
                .map(String::as_str)
                .collect();
            if !missing.is_empty() {
                return Err(invalid_data!(
                    "Failed to find the following files to delete in the current snapshot: {}",
                    missing.join(", "),
                ));
            }
        }

        Ok((result, removed_files))
    }
}

/// Shared engine for operations that both add and remove data files.
///
/// This struct is the Rust equivalent of Java's `MergingSnapshotProducer`.
/// It manages:
/// - Tracking files to add and delete
/// - Filtering existing manifests to remove deleted files
/// - Writing new manifests for added files
/// - Writing the snapshot through [`write_snapshot_commit`], which computes a
///   summary that accounts for both additions and removals
///
/// Concrete transaction actions like [`RewriteFilesAction`] create one as the
/// retry-persistent state of a logical execution, configured with the
/// appropriate [`Operation`] type.
pub(crate) struct MergingSnapshotProducer {
    operation: Operation,
    added_data_files: Vec<DataFile>,
    filter_manager: ManifestFilterManager,
    commit_uuid: Uuid,
    snapshot_id: Option<i64>,
    data_sequence_number: Option<i64>,
    /// Names the manifests this producer writes. It is never reset, so a
    /// commit retry cannot overwrite a manifest an earlier attempt wrote
    /// under the same commit uuid.
    manifest_counter: u64,
    attempt: u64,
}

impl MergingSnapshotProducer {
    pub(crate) fn new(
        operation: Operation,
        added_data_files: Vec<DataFile>,
        deleted_data_files: &[DataFile],
        data_sequence_number: Option<i64>,
    ) -> Self {
        let mut filter_manager = ManifestFilterManager::new(true);
        for file in deleted_data_files {
            filter_manager.add_delete(file.file_path.clone());
        }
        Self {
            operation,
            added_data_files,
            filter_manager,
            commit_uuid: Uuid::now_v7(),
            snapshot_id: None,
            data_sequence_number,
            manifest_counter: 0,
            attempt: 0,
        }
    }

    /// Reject a data sequence number above the one the new snapshot will carry.
    fn validate_data_sequence_number(&self, table: &Table) -> Result<()> {
        let Some(sequence_number) = self.data_sequence_number else {
            return Ok(());
        };
        let snapshot_sequence_number = table.metadata().next_sequence_number();
        if sequence_number > snapshot_sequence_number {
            return Err(invalid_data!(
                "Data sequence number {sequence_number} is greater than the snapshot's {snapshot_sequence_number}."
            ));
        }
        Ok(())
    }

    fn validate_added_data_files(&self, table: &Table) -> Result<()> {
        let metadata = table.metadata();
        for data_file in &self.added_data_files {
            if data_file.content_type() != DataContentType::Data {
                return Err(invalid_data!(
                    "Only data content type is allowed in added data files, but {} is {:?}",
                    data_file.file_path,
                    data_file.content_type()
                ));
            }
            if metadata.default_partition_spec_id() != data_file.partition_spec_id {
                return Err(invalid_data!(
                    "Data file partition spec id does not match table default partition spec id"
                ));
            }
            validate_partition_value(data_file.partition(), metadata.default_partition_type())?;
        }
        Ok(())
    }

    /// Produce manifests and commit a new snapshot.
    pub(crate) async fn commit_snapshot(&mut self, table: &Table) -> Result<ActionCommit> {
        self.validate_data_sequence_number(table)?;
        self.validate_added_data_files(table)?;
        validate_files_not_live(table, &self.added_data_files).await?;
        let snapshot_id = *self
            .snapshot_id
            .get_or_insert_with(|| generate_snapshot_id(table));

        // 1. Load existing manifests from the current snapshot.
        let existing_manifests = match table.metadata().current_snapshot() {
            Some(snapshot) => {
                let manifest_list = table.manifest_list_reader(snapshot).load().await?;
                manifest_list
                    .entries()
                    .iter()
                    // Java's third arm is the new snapshot's id, which no existing manifest has.
                    .filter(|e| e.has_added_files() || e.has_existing_files())
                    .cloned()
                    .collect()
            }
            None => Vec::new(),
        };

        // 2. Filter existing manifests — remove deleted file entries.
        let (mut manifests, removed_files) = self
            .filter_manager
            .filter_manifests(
                table,
                existing_manifests,
                snapshot_id,
                self.commit_uuid,
                &mut self.manifest_counter,
            )
            .await?;

        // 3. Write a new manifest for added files.
        if !self.added_data_files.is_empty() {
            manifests.push(self.write_added_manifest(table, snapshot_id).await?);
        }

        // 4. Write the manifest list and snapshot.
        let manifest_list_path =
            manifest_list_path(table, snapshot_id, self.attempt, self.commit_uuid)?;
        self.attempt += 1;
        write_snapshot_commit(table, SnapshotCommitInput {
            snapshot_id,
            manifest_list_path,
            operation: self.operation.clone(),
            properties: HashMap::new(),
            added_files: &self.added_data_files,
            removed_files: &removed_files,
            manifests,
            truncate_full_table: false,
        })
        .await
    }

    async fn write_added_manifest(
        &mut self,
        table: &Table,
        snapshot_id: i64,
    ) -> Result<ManifestFile> {
        let new_manifest_path = manifest_path(table, self.commit_uuid, self.manifest_counter)?;
        self.manifest_counter += 1;

        let mut writer = new_manifest_writer(
            table,
            new_manifest_path,
            snapshot_id,
            ManifestContentType::Data,
            table.metadata().current_schema().clone(),
            table.metadata().default_partition_spec().clone(),
        )?;

        let format_version = table.metadata().format_version();
        for data_file in &self.added_data_files {
            if let Some(sequence_number) = self.data_sequence_number {
                writer.add_file(data_file.clone(), sequence_number)?;
                continue;
            }
            let entry_builder = ManifestEntry::builder()
                .status(ManifestStatus::Added)
                .data_file(data_file.clone());
            let entry = if format_version == FormatVersion::V1 {
                // V1 requires snapshot_id on each entry.
                entry_builder.snapshot_id(snapshot_id).build()
            } else {
                entry_builder.build()
            };
            writer.add_entry(entry)?;
        }

        writer.write_manifest_file().await
    }
}
