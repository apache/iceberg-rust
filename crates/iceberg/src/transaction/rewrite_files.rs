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
use crate::spec::{
    DataContentType, DataFile, ManifestContentType, ManifestEntry, ManifestFile, ManifestStatus,
    Operation,
};
use crate::table::Table;
use crate::transaction::validate::SnapshotValidator;

/// Transaction action for rewriting files.
pub struct RewriteFilesAction {
    commit_uuid: Option<Uuid>,
    key_metadata: Option<Vec<u8>>,
    snapshot_properties: HashMap<String, String>,
    added_data_files: Vec<DataFile>,
    added_delete_files: Vec<DataFile>,
    deleted_data_files: Vec<DataFile>,
    deleted_delete_files: Vec<DataFile>,
    data_sequence_number: Option<i64>,
    starting_snapshot_id: Option<i64>,
}

pub struct RewriteFilesOperation {
    added_data_files: Vec<DataFile>,
    added_delete_files: Vec<DataFile>,
    deleted_data_files: Vec<DataFile>,
    deleted_delete_files: Vec<DataFile>,
    starting_snapshot_id: Option<i64>,
    data_sequence_number: Option<i64>,
}

impl RewriteFilesAction {
    pub fn new() -> Self {
        Self {
            commit_uuid: None,
            key_metadata: None,
            snapshot_properties: Default::default(),
            added_data_files: vec![],
            added_delete_files: vec![],
            deleted_data_files: vec![],
            deleted_delete_files: vec![],
            data_sequence_number: None,
            starting_snapshot_id: None,
        }
    }

    /// Add added data files to the snapshot.
    pub fn add_data_files(
        mut self,
        data_files: impl IntoIterator<Item = DataFile>,
    ) -> Result<Self> {
        for data_file in data_files {
            match data_file.content {
                DataContentType::Data => self.added_data_files.push(data_file),
                DataContentType::PositionDeletes | DataContentType::EqualityDeletes => {
                    self.added_delete_files.push(data_file)
                }
            }
        }
        Ok(self)
    }

    /// Add deleted data files to the snapshot.
    pub fn delete_data_files(
        mut self,
        data_files: impl IntoIterator<Item = DataFile>,
    ) -> Result<Self> {
        for data_file in data_files {
            match data_file.content {
                DataContentType::Data => self.deleted_data_files.push(data_file),
                DataContentType::PositionDeletes | DataContentType::EqualityDeletes => {
                    self.deleted_delete_files.push(data_file)
                }
            }
        }

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

    /// Set the data sequence number for this rewrite operation.
    /// The number will be used for all new data files that are added in this rewrite.
    pub fn set_data_sequence_number(mut self, sequence_number: i64) -> Self {
        self.data_sequence_number = Some(sequence_number);
        self
    }

    /// Set the snapshot ID used in any reads for this operation.
    pub fn set_starting_snapshot_id(mut self, snapshot_id: i64) -> Self {
        self.starting_snapshot_id = Some(snapshot_id);
        self
    }
}

impl Default for RewriteFilesAction {
    fn default() -> Self {
        Self::new()
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
            self.added_data_files.clone(),
            self.added_delete_files.clone(),
            self.deleted_data_files.clone(),
            self.deleted_delete_files.clone(),
        );

        let rewrite_operation = RewriteFilesOperation {
            added_data_files: self.added_data_files.clone(),
            added_delete_files: self.added_delete_files.clone(),
            deleted_data_files: self.deleted_data_files.clone(),
            deleted_delete_files: self.deleted_delete_files.clone(),
            starting_snapshot_id: self.starting_snapshot_id,
            data_sequence_number: self.data_sequence_number,
        };

        // todo should be able to configure to use the merge manifest process
        snapshot_producer
            .commit(rewrite_operation, DefaultManifestProcess)
            .await
    }
}

fn copy_with_deleted_status(entry: &ManifestEntry) -> Result<ManifestEntry> {
    let builder = ManifestEntry::builder()
        .status(ManifestStatus::Deleted)
        .snapshot_id_opt(entry.snapshot_id())
        .sequence_number_opt(entry.sequence_number())
        .data_file(entry.data_file().clone());

    // todo copy file seq no as well

    Ok(builder.build())
}

impl SnapshotValidator for RewriteFilesOperation {
    async fn validate(&self, base: &Table, parent_snapshot_id: Option<i64>) -> Result<()> {
        // Validate replaced and added files
        if self.deleted_data_files.is_empty() && self.deleted_delete_files.is_empty() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Files to delete cannot be empty",
            ));
        }
        if self.deleted_data_files.is_empty() && !self.added_data_files.is_empty() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Data files to add must be empty because there's no data file to be rewritten",
            ));
        }
        if self.deleted_delete_files.is_empty() && !self.added_delete_files.is_empty() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Delete files to add must be empty because there's no delete file to be rewritten",
            ));
        }

        // todo add use_starting_seq_number to determine if we want to use data_sequence_number
        // If there are replaced data files, there cannot be any new row-level deletes for those data files
        if !self.deleted_data_files.is_empty() {
            self.validate_no_new_deletes_for_data_files(
                base,
                self.starting_snapshot_id,
                parent_snapshot_id,
                &self.deleted_data_files,
                self.data_sequence_number.is_some(),
            )
            .await?;
        }

        Ok(())
    }
}

impl SnapshotProduceOperation for RewriteFilesOperation {
    fn operation(&self) -> Operation {
        Operation::Replace
    }

    async fn delete_entries(
        &self,
        snapshot_producer: &SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestEntry>> {
        // Find entries that are associated with deleted files
        let snapshot = snapshot_producer.table.metadata().current_snapshot();

        if let Some(snapshot) = snapshot {
            let manifest_list = snapshot
                .load_manifest_list(
                    snapshot_producer.table.file_io(),
                    snapshot_producer.table.metadata(),
                )
                .await?;

            let mut delete_entries = Vec::new();

            for manifest_file in manifest_list.entries() {
                let manifest = manifest_file
                    .load_manifest(snapshot_producer.table.file_io())
                    .await?;

                for entry in manifest.entries() {
                    match entry.content_type() {
                        DataContentType::Data => {
                            if snapshot_producer
                                .deleted_data_files
                                .iter()
                                .any(|f| f.file_path == entry.data_file().file_path)
                            {
                                delete_entries.push(copy_with_deleted_status(entry.as_ref())?)
                            }
                        }
                        DataContentType::PositionDeletes | DataContentType::EqualityDeletes => {
                            if snapshot_producer
                                .deleted_delete_files
                                .iter()
                                .any(|f| f.file_path == entry.data_file().file_path)
                            {
                                delete_entries.push(copy_with_deleted_status(entry.as_ref())?)
                            }
                        }
                    }
                }
            }

            Ok(delete_entries)
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
                    match entry.content_type() {
                        DataContentType::Data => {
                            if snapshot_producer
                                .deleted_data_files
                                .iter()
                                .any(|f| f.file_path == entry.data_file().file_path)
                            {
                                return Some(entry.data_file().file_path().to_string());
                            }
                        }
                        DataContentType::EqualityDeletes | DataContentType::PositionDeletes => {
                            if snapshot_producer
                                .deleted_delete_files
                                .iter()
                                .any(|f| f.file_path == entry.data_file().file_path)
                            {
                                return Some(entry.data_file().file_path().to_string());
                            }
                        }
                    }
                    None
                })
                .collect();

            if found_files_to_delete.is_empty()
                && (manifest_file.has_added_files() || manifest_file.has_existing_files())
            {
                // All files from the existing manifest entries are still valid
                existing_files.push(manifest_file.clone());
            } else {
                // Some files are deleted already
                // Rewrite the manifest file and exclude the deleted data files
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
