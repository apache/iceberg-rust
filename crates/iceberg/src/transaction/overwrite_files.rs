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

use std::collections::HashMap;

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
use crate::spec::{DataFile, ManifestEntry, ManifestFile, Operation};
use crate::transaction::rewrite_files::RewriteFilesOperation;

pub const USE_STARTING_SEQUENCE_NUMBER: &str = "use-starting-sequence-number";
pub const USE_STARTING_SEQUENCE_NUMBER_DEFAULT: bool = true;

/// Transaction action for rewriting files.
pub struct OverwriteFilesAction<'a> {
    snapshot_produce_action: SnapshotProduceAction<'a>,
    target_size_bytes: u32,
    min_count_to_merge: u32,
    merge_enabled: bool,
}

struct OverwriteFilesOperation {
    inner: RewriteFilesOperation,
}

impl<'a> OverwriteFilesAction<'a> {
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

        let snapshot_produce_action = SnapshotProduceAction::new(
            tx,
            snapshot_id,
            key_metadata,
            commit_uuid,
            snapshot_properties,
        )
        .unwrap();

        Ok(Self {
            snapshot_produce_action,
            target_size_bytes,
            min_count_to_merge,
            merge_enabled,
        })
    }

    /// Add data files to the snapshot.

    pub fn add_data_files(
        mut self,
        data_files: impl IntoIterator<Item = DataFile>,
    ) -> Result<Self> {
        self.snapshot_produce_action.add_data_files(data_files)?;
        Ok(self)
    }

    /// Add remove files to the snapshot.
    pub fn delete_files(
        mut self,
        remove_data_files: impl IntoIterator<Item = DataFile>,
    ) -> Result<Self> {
        self.snapshot_produce_action
            .delete_files(remove_data_files)?;
        Ok(self)
    }

    /// Finished building the action and apply it to the transaction.
    pub async fn apply(self) -> Result<Transaction<'a>> {
        let inner = RewriteFilesOperation;

        if self.merge_enabled {
            let process =
                MergeManifestProcess::new(self.target_size_bytes, self.min_count_to_merge);
            self.snapshot_produce_action
                .apply(OverwriteFilesOperation { inner }, process)
                .await
        } else {
            self.snapshot_produce_action
                .apply(OverwriteFilesOperation { inner }, DefaultManifestProcess)
                .await
        }
    }

    pub fn with_starting_sequence_number(mut self, seq: i64) -> Result<Self> {
        // If the compaction should use the sequence number of the snapshot at compaction start time for
        // new data files, instead of using the sequence number of the newly produced snapshot.
        // This avoids commit conflicts with updates that add newer equality deletes at a higher sequence number.
        let use_starting_sequence_number = self
            .snapshot_produce_action
            .tx
            .current_table
            .metadata()
            .properties()
            .get(USE_STARTING_SEQUENCE_NUMBER)
            .and_then(|s| s.parse().ok())
            .unwrap_or(USE_STARTING_SEQUENCE_NUMBER_DEFAULT);

        if !use_starting_sequence_number {
            return Err(crate::error::Error::new(
                crate::ErrorKind::Unexpected,
                "Cannot set data file sequence number when use-starting-sequence-number is false"
                    .to_string(),
            ));
        }

        self.snapshot_produce_action
            .set_new_data_file_sequence_number(seq);

        Ok(self)
    }

    pub fn with_starting_sequence_number_from_branch(mut self, branch: &str) -> Result<Self> {
        // If the compaction should use the sequence number of the snapshot at compaction start time for
        // new data files, instead of using the sequence number of the newly produced snapshot.
        // This avoids commit conflicts with updates that add newer equality deletes at a higher sequence number.
        let use_starting_sequence_number = self
            .snapshot_produce_action
            .tx
            .current_table
            .metadata()
            .properties()
            .get(USE_STARTING_SEQUENCE_NUMBER)
            .and_then(|s| s.parse().ok())
            .unwrap_or(USE_STARTING_SEQUENCE_NUMBER_DEFAULT);

        if !use_starting_sequence_number {
            return Err(crate::error::Error::new(
                crate::ErrorKind::Unexpected,
                "Cannot set data file sequence number when use-starting-sequence-number is false"
                    .to_string(),
            ));
        }

        if let Some(snapshot) = self
            .snapshot_produce_action
            .tx
            .current_table
            .metadata()
            .snapshot_for_ref(branch)
        {
            self.snapshot_produce_action
                .set_new_data_file_sequence_number(snapshot.sequence_number());
        }

        Ok(self)
    }

    pub fn with_to_branch(mut self, to_branch: String) -> Self {
        self.snapshot_produce_action.set_target_branch(to_branch);
        self
    }
}

impl SnapshotProduceOperation for OverwriteFilesOperation {
    fn operation(&self) -> Operation {
        Operation::Overwrite
    }

    async fn delete_entries(
        &self,
        snapshot_produce: &SnapshotProduceAction<'_>,
    ) -> Result<Vec<ManifestEntry>> {
        self.inner.delete_entries(snapshot_produce).await
    }

    async fn existing_manifest(
        &self,
        snapshot_produce: &mut SnapshotProduceAction<'_>,
    ) -> Result<Vec<ManifestFile>> {
        self.inner.existing_manifest(snapshot_produce).await
    }
}
