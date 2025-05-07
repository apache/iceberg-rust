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
use std::sync::Arc;

use uuid::Uuid;

use super::snapshot::{
    DefaultManifestProcess, MergeManifestProcess, SnapshotProduceAction, SnapshotProduceOperation,
};
use super::{
    Transaction, MANIFEST_MERGE_ENABLED, MANIFEST_MERGE_ENABLED_DEFAULT, MANIFEST_MIN_MERGE_COUNT,
    MANIFEST_MIN_MERGE_COUNT_DEFAULT, MANIFEST_TARGET_SIZE_BYTES,
    MANIFEST_TARGET_SIZE_BYTES_DEFAULT,
};
use crate::error::Result;
use crate::spec::{
    DataContentType, DataFile, ManifestContentType, ManifestEntry, ManifestFile, ManifestStatus,
    Operation,
};

/// Transaction action for rewriting files.
pub struct RewriteFilesAction<'a> {
    snapshot_produce_action: SnapshotProduceAction<'a>,
    target_size_bytes: u32,
    min_count_to_merge: u32,
    merge_enabled: bool,
}

struct RewriteFilesOperation;

impl<'a> RewriteFilesAction<'a> {
    pub fn new(
        tx: Transaction<'a>,
        snapshot_id: i64,
        commit_uuid: Uuid,
        key_metadata: Vec<u8>,
        snapshot_properties: HashMap<String, String>,
    ) -> Result<Self> {
        let target_size_bytes: u32 = tx
            .current_table
            .metadata()
            .properties()
            .get(MANIFEST_TARGET_SIZE_BYTES)
            .and_then(|s| s.parse().ok())
            .unwrap_or(MANIFEST_TARGET_SIZE_BYTES_DEFAULT);
        let min_count_to_merge: u32 = tx
            .current_table
            .metadata()
            .properties()
            .get(MANIFEST_MIN_MERGE_COUNT)
            .and_then(|s| s.parse().ok())
            .unwrap_or(MANIFEST_MIN_MERGE_COUNT_DEFAULT);
        let merge_enabled = tx
            .current_table
            .metadata()
            .properties()
            .get(MANIFEST_MERGE_ENABLED)
            .and_then(|s| s.parse().ok())
            .unwrap_or(MANIFEST_MERGE_ENABLED_DEFAULT);

        Ok(Self {
            snapshot_produce_action: SnapshotProduceAction::new(
                tx,
                snapshot_id,
                key_metadata,
                commit_uuid,
                snapshot_properties,
            )
            .unwrap(),
            target_size_bytes,
            min_count_to_merge,
            merge_enabled,
        })
    }

    /// Add data files to the snapshot.

    pub fn add_data_files(
        &mut self,
        data_files: impl IntoIterator<Item = DataFile>,
    ) -> Result<&mut Self> {
        self.snapshot_produce_action.add_data_files(data_files)?;
        Ok(self)
    }

    /// Add remove files to the snapshot.
    pub fn delete_files(
        &mut self,
        remove_data_files: impl IntoIterator<Item = DataFile>,
    ) -> Result<&mut Self> {
        self.snapshot_produce_action
            .delete_files(remove_data_files)?;
        Ok(self)
    }

    /// Finished building the action and apply it to the transaction.
    pub async fn apply(self) -> Result<Transaction<'a>> {
        if self.merge_enabled {
            let process =
                MergeManifestProcess::new(self.target_size_bytes, self.min_count_to_merge);
            self.snapshot_produce_action
                .apply(RewriteFilesOperation, process)
                .await
        } else {
            self.snapshot_produce_action
                .apply(RewriteFilesOperation, DefaultManifestProcess)
                .await
        }
    }
}

impl SnapshotProduceOperation for RewriteFilesOperation {
    fn operation(&self) -> Operation {
        Operation::Replace
    }

    async fn delete_entries(
        &self,
        snapshot_produce: &SnapshotProduceAction<'_>,
    ) -> Result<Vec<ManifestEntry>> {
        // generate delete manifest entries from removed files
        let snapshot = snapshot_produce
            .tx
            .current_table
            .metadata()
            .current_snapshot();

        if let Some(snapshot) = snapshot {
            let gen_manifest_entry = |old_entry: &Arc<ManifestEntry>| {
                let builder = ManifestEntry::builder()
                    .status(ManifestStatus::Deleted)
                    .snapshot_id(old_entry.snapshot_id().unwrap())
                    .sequence_number(old_entry.sequence_number().unwrap())
                    .file_sequence_number(old_entry.file_sequence_number().unwrap())
                    .data_file(old_entry.data_file().clone());

                builder.build()
            };

            let manifest_list = snapshot
                .load_manifest_list(
                    snapshot_produce.tx.current_table.file_io(),
                    snapshot_produce.tx.current_table.metadata(),
                )
                .await?;

            let mut deleted_entries = Vec::new();

            for manifest_file in manifest_list.entries() {
                let manifest = manifest_file
                    .load_manifest(snapshot_produce.tx.current_table.file_io())
                    .await?;

                for entry in manifest.entries() {
                    if entry.content_type() == DataContentType::Data
                        && snapshot_produce
                            .removed_data_file_paths
                            .contains(entry.data_file().file_path())
                    {
                        deleted_entries.push(gen_manifest_entry(entry));
                    }

                    if entry.content_type() == DataContentType::PositionDeletes
                        || entry.content_type() == DataContentType::EqualityDeletes
                            && snapshot_produce
                                .removed_delete_file_paths
                                .contains(entry.data_file().file_path())
                    {
                        deleted_entries.push(gen_manifest_entry(entry));
                    }
                }
            }

            Ok(deleted_entries)
        } else {
            Ok(vec![])
        }
    }

    async fn existing_manifest(
        &self,
        snapshot_produce: &mut SnapshotProduceAction<'_>,
    ) -> Result<Vec<ManifestFile>> {
        let Some(snapshot) = snapshot_produce
            .tx
            .current_table
            .metadata()
            .current_snapshot()
        else {
            return Ok(vec![]);
        };

        let manifest_list = snapshot
            .load_manifest_list(
                snapshot_produce.tx.current_table.file_io(),
                snapshot_produce.tx.current_table.metadata(),
            )
            .await?;

        let mut existing_files = Vec::new();

        for manifest_file in manifest_list.entries() {
            let manifest = manifest_file
                .load_manifest(snapshot_produce.tx.current_table.file_io())
                .await?;

            let found_deleted_files: HashSet<_> = manifest
                .entries()
                .iter()
                .filter_map(|entry| {
                    if snapshot_produce
                        .removed_data_file_paths
                        .contains(entry.data_file().file_path())
                        || snapshot_produce
                            .removed_delete_file_paths
                            .contains(entry.data_file().file_path())
                    {
                        Some(entry.data_file().file_path().to_string())
                    } else {
                        None
                    }
                })
                .collect();

            if found_deleted_files.is_empty() {
                existing_files.push(manifest_file.clone());
            } else {
                // Rewrite the manifest file without the deleted data files
                if manifest
                    .entries()
                    .iter()
                    .any(|entry| !found_deleted_files.contains(entry.data_file().file_path()))
                {
                    let mut manifest_writer = snapshot_produce.new_manifest_writer(
                        &ManifestContentType::Data,
                        snapshot_produce
                            .tx
                            .current_table
                            .metadata()
                            .default_partition_spec_id(),
                    )?;

                    for entry in manifest.entries() {
                        if !found_deleted_files.contains(entry.data_file().file_path()) {
                            manifest_writer.add_entry((**entry).clone())?;
                        }
                    }

                    existing_files.push(manifest_writer.write_manifest_file().await?);
                }
            }
        }

        Ok(existing_files)
    }
}
