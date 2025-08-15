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

use async_trait::async_trait;
use uuid::Uuid;

use super::snapshot::{DefaultManifestProcess, SnapshotProduceOperation, SnapshotProducer};
use super::{ActionCommit, TransactionAction};
use crate::error::{Error, ErrorKind, Result};
use crate::spec::{DataFile, ManifestContentType, ManifestEntry, ManifestEntryRef, ManifestFile, ManifestStatus, Operation};
use crate::table::Table;

/// Transaction action for rewriting files.
pub struct RewriteFilesAction {
    commit_uuid: Option<Uuid>,
    key_metadata: Option<Vec<u8>>,
    snapshot_properties: HashMap<String, String>,
    data_files_to_add: Vec<DataFile>,
    // Data files and delete files to delete
    data_files_to_delete: Vec<DataFile>,
}

pub struct RewriteFilesOperation;

impl RewriteFilesAction {
    pub fn new() -> Self {
        Self {
            commit_uuid: None,
            key_metadata: None,
            snapshot_properties: Default::default(),
            data_files_to_add: vec![],
            data_files_to_delete: vec![],
        }
    }

    /// Add data files to the snapshot.
    pub fn add_data_files(
        mut self,
        data_files: impl IntoIterator<Item = DataFile>,
    ) -> Result<Self> {
        self.data_files_to_add.extend(data_files);
        Ok(self)
    }

    /// Add data files to delete to the snapshot.
    pub fn delete_data_files(
        mut self,
        data_files: impl IntoIterator<Item = DataFile>,
    ) -> Result<Self> {
        self.data_files_to_delete.extend(data_files);
        Ok(self)
    }

    /// Set commit UUID for the snapshot.
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
}

#[async_trait]
impl TransactionAction for RewriteFilesAction {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        let snapshot_producer = SnapshotProducer::new(
            table,
            self.commit_uuid.unwrap_or_else(Uuid::now_v7),
            self.key_metadata.clone(),
            self.snapshot_properties.clone(),
            self.data_files_to_add.clone(),
            self.data_files_to_delete.clone(),
        );

        // todo should be able to configure merge manifest process
        snapshot_producer
            .commit(RewriteFilesOperation, DefaultManifestProcess)
            .await
    }
}

fn set_deleted_status(entry: &ManifestEntryRef) -> Result<ManifestEntry> {
    let builder = ManifestEntry::builder()
        .status(ManifestStatus::Deleted)
        .snapshot_id(entry.snapshot_id().ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("Missing snapshot_id for entry with file path: {}", entry.file_path()),
            )
        })?)
        .sequence_number(entry.sequence_number().ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("Missing sequence_number for entry with file path: {}", entry.file_path()),
            )
        })?)
        .file_sequence_number(entry.file_sequence_number().ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("Missing file_sequence_number for entry with file path: {}", entry.file_path()),
            )
        })?)
        .data_file(entry.data_file().clone());

    Ok(builder.build())
}

impl SnapshotProduceOperation for RewriteFilesOperation {
    fn operation(&self) -> Operation {
        Operation::Replace
    }

    async fn delete_entries(
        &self,
        snapshot_producer: &SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestEntry>> {
        // Find entries that are associated with files to delete
        let snapshot = snapshot_producer.table.metadata().current_snapshot();

        if let Some(snapshot) = snapshot {
            let manifest_list = snapshot
                .load_manifest_list(
                    snapshot_producer.table.file_io(),
                    snapshot_producer.table.metadata(),
                )
                .await?;

            let mut deleted_entries = Vec::new();

            for manifest_file in manifest_list.entries() {
                let manifest = manifest_file
                    .load_manifest(snapshot_producer.table.file_io())
                    .await?;

                for entry in manifest.entries() {
                    if snapshot_producer
                        .data_files_to_delete
                        .iter()
                        .any(|f| f.file_path == entry.data_file().file_path)
                    {
                        deleted_entries.push(set_deleted_status(entry)?);
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
        snapshot_producer: &mut SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestFile>> {
        let Some(snapshot) = snapshot_producer.table.metadata().current_snapshot() else {
            return Ok(vec![]);
        };

        let manifest_list = snapshot
            .load_manifest_list(
                snapshot_producer.table.file_io(),
                snapshot_producer.table.metadata(),
            )
            .await?;

        let mut existing_files = Vec::new();

        for manifest_file in manifest_list.entries() {
            let manifest = manifest_file
                .load_manifest(snapshot_producer.table.file_io())
                .await?;

            // Find files to delete from the current manifest entries
            let found_files_to_delete: HashSet<_> = manifest
                .entries()
                .iter()
                .filter_map(|entry| {
                    if snapshot_producer
                        .data_files_to_delete
                        .iter()
                        .any(|f| f.file_path == entry.data_file().file_path)
                    {
                        Some(entry.data_file().file_path().to_string())
                    } else {
                        None
                    }
                })
                .collect();

            if found_files_to_delete.is_empty()
                && (manifest_file.has_added_files() || manifest_file.has_existing_files())
            {
                // All files from the existing manifest entries are still valid
                existing_files.push(manifest_file.clone());
            } else {
                // Some files are about to be deleted
                // Rewrite the manifest file and exclude the data files to delete
                let mut manifest_writer = snapshot_producer.new_manifest_writer(
                    ManifestContentType::Data,
                    manifest_file.partition_spec_id,
                )?;

                manifest
                    .entries()
                    .iter()
                    .filter(|entry| {
                        entry.status() != ManifestStatus::Deleted
                            && !found_files_to_delete.contains(entry.data_file().file_path())
                    })
                    .try_for_each(|entry| manifest_writer.add_entry((**entry).clone()))?;

                existing_files.push(manifest_writer.write_manifest_file().await?);
            }
        }

        Ok(existing_files)
    }
}
