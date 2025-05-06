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

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::hash::Hash;
use std::pin::Pin;

use itertools::Itertools;
use uuid::Uuid;

use super::snapshot::{DefaultManifestProcess, SnapshotProduceAction, SnapshotProduceOperation};
use super::Transaction;
use crate::spec::{
    DataFile, ManifestContentType, ManifestEntry, ManifestFile, ManifestList, ManifestWriter,
    Operation, UNASSIGNED_SEQUENCE_NUMBER, UNASSIGNED_SNAPSHOT_ID,
};
use crate::{Error, ErrorKind, Result};

/// New created manifest count during rewrite manifest action.
pub const CREATED_MANIFESTS_COUNT: &str = "manifests-created";
/// Kept manifest count during rewrite manifest action.
pub const KEPT_MANIFESTS_COUNT: &str = "manifests-kept";
/// Count of manifest been rewrite and delete during rewrite manifest action.
pub const REPLACED_MANIFESTS_COUNT: &str = "manifests-replaced";
/// Count of manifest entry been process during rewrite manifest action.
pub const PROCESSED_ENTRY_COUNT: &str = "entries-processed";

pub type ClusterFunc<T> = Box<dyn Fn(&DataFile) -> T>;
pub type PredicateFunc = Box<dyn Fn(&ManifestFile) -> Pin<Box<dyn Future<Output = bool> + Send>>>;

/// Action used for rewriting manifests for a table.
pub struct RewriteManifestAction<'a, T> {
    cluster_by_func: Option<ClusterFunc<T>>,
    manifest_predicate: Option<PredicateFunc>,
    manifest_writers: HashMap<(T, i32), ManifestWriter>,

    // Manifest file that user added to the snapshot
    added_manifests: Vec<ManifestFile>,
    // Manifest file that user deleted from the snapshot
    deleted_manifests: Vec<ManifestFile>,
    // New manifest files that generated after rewriting
    new_manifests: Vec<ManifestFile>,
    // Original manifest file that don't need to rewrite
    keep_manifests: Vec<ManifestFile>,

    // Used to record the manifests that need to be rewritten
    rewrite_manifests: HashSet<ManifestFile>,

    snapshot_produce_action: SnapshotProduceAction<'a>,

    /// Statistics for count of process manifest entries
    process_entry_count: usize,
}

impl<'a, T: Hash + Eq> RewriteManifestAction<'a, T> {
    pub(crate) fn new(
        tx: Transaction<'a>,
        cluster_by_func: Option<ClusterFunc<T>>,
        manifest_predicate: Option<PredicateFunc>,
        snapshot_id: i64,
        commit_uuid: Uuid,
        key_metadata: Vec<u8>,
        snapshot_properties: HashMap<String, String>,
    ) -> Result<Self> {
        Ok(Self {
            cluster_by_func,
            manifest_predicate,
            manifest_writers: HashMap::new(),
            added_manifests: vec![],
            deleted_manifests: vec![],
            new_manifests: vec![],
            keep_manifests: vec![],
            snapshot_produce_action: SnapshotProduceAction::new(
                tx,
                snapshot_id,
                key_metadata,
                commit_uuid,
                snapshot_properties,
            )?,
            rewrite_manifests: HashSet::new(),
            process_entry_count: 0,
        })
    }

    /// Add the manifest file for new snapshot
    pub fn add_manifest(&mut self, manifest: ManifestFile) -> Result<()> {
        if manifest.has_added_files() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Cannot add manifest file with added files to the snapshot in RewriteManifest action",
            ));
        }
        if manifest.has_deleted_files() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Cannot add manifest file with deleted files to the snapshot in RewriteManifest action",
            ));
        }
        if manifest.added_snapshot_id != UNASSIGNED_SNAPSHOT_ID {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Cannot add manifest file with non-empty snapshot id to the snapshot in RewriteManifest action. Snapshot id will be assigned during commit",
            ));
        }
        if manifest.sequence_number != UNASSIGNED_SEQUENCE_NUMBER {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Cannot add manifest file with non-empty sequence number to the snapshot in RewriteManifest action. Sequence number will be assigned during commit",
            ));
        }

        if self.snapshot_produce_action.enable_inherit_snapshot_id {
            self.added_manifests.push(manifest);
        } else {
            // # TODO
            // For table can't inherit snapshot id, we should rewrite the whole manifest file and add it at rewritten_added_manifests.
            // See: https://github.com/apache/iceberg/blob/4dbcdfc85a64dc1d97d7434e353c7f9e4c18e1b3/core/src/main/java/org/apache/iceberg/BaseRewriteManifests.java#L48
            return Err(Error::new(
                ErrorKind::FeatureUnsupported,
                "Rewrite manifest file is supported: Cannot add manifest file to the snapshot in RewriteManifest action when snapshot id inheritance is disabled",
            ));
        }
        Ok(())
    }

    /// Delete the manifest file from snapshot
    pub fn delete_manifest(&mut self, manifest: ManifestFile) -> Result<()> {
        self.deleted_manifests.push(manifest);
        Ok(())
    }

    fn require_rewrite(&self) -> bool {
        self.cluster_by_func.is_some()
    }

    #[inline]
    async fn if_rewrite(&self, manifest: &ManifestFile) -> bool {
        // Always rewrite if not predicate is provided
        if let Some(predicate) = self.manifest_predicate.as_ref() {
            predicate(manifest).await
        } else {
            true
        }
    }

    async fn perform_rewrite(&mut self, manifest_list: ManifestList) -> Result<()> {
        let remain_manifest = manifest_list
            .consume_entries()
            .into_iter()
            .filter(|manifest| !self.deleted_manifests.contains(manifest));
        for manifest in remain_manifest {
            if manifest.content == ManifestContentType::Deletes || !self.if_rewrite(&manifest).await
            {
                self.keep_manifests.push(manifest.clone());
            } else {
                self.rewrite_manifests.insert(manifest.clone());
                let (manifest_entries, _) = manifest
                    .load_manifest(self.snapshot_produce_action.tx.current_table.file_io())
                    .await?
                    .into_parts();
                for entry in manifest_entries.into_iter().filter(|e| e.is_alive()) {
                    let key = (
                        self.cluster_by_func
                            .as_ref()
                            .expect("Never enter this function if cluster_by_func is None")(
                            entry.data_file(),
                        ),
                        manifest.partition_spec_id,
                    );
                    match self.manifest_writers.entry(key) {
                        Entry::Occupied(mut e) => {
                            // # TODO
                            // Close when file reach target size and reset the writer
                            e.get_mut().add_existing_entry(entry.as_ref().clone())?;
                        }
                        Entry::Vacant(e) => {
                            let mut writer = self.snapshot_produce_action.new_manifest_writer(
                                &manifest.content,
                                manifest.partition_spec_id,
                            )?;
                            writer.add_existing_entry(entry.as_ref().clone())?;
                            e.insert(writer);
                        }
                    }
                    self.process_entry_count += 1;
                }
            }
        }
        // write all manifest files
        for (_, writer) in self.manifest_writers.drain() {
            let manifest_file = writer.write_manifest_file().await?;
            self.new_manifests.push(manifest_file);
        }
        Ok(())
    }

    fn keep_active_manifests(&mut self, manifest_list: ManifestList) -> Result<()> {
        self.keep_manifests.clear();
        self.keep_manifests
            .extend(
                manifest_list
                    .consume_entries()
                    .into_iter()
                    .filter(|manifest| {
                        // # TODO
                        // Which case will reach here?
                        !self.rewrite_manifests.contains(manifest)
                            && !self.deleted_manifests.contains(manifest)
                    }),
            );
        Ok(())
    }

    #[inline]
    fn active_file_count<'t>(manifest_iter: impl Iterator<Item = &'t ManifestFile>) -> Result<u32> {
        let mut count = 0;
        for manifest in manifest_iter {
            count += manifest.added_files_count.ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    "Manifest file should have added files count",
                )
            })?;
            count += manifest.existing_files_count.ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    "Manifest file should have existing files count",
                )
            })?;
        }
        Ok(count)
    }

    async fn validate_files_counts(&self) -> Result<()> {
        let create_manifest = self.new_manifests.iter().chain(self.added_manifests.iter());
        let create_manifest_file_count = Self::active_file_count(create_manifest)?;

        let replaced_manifest = self
            .rewrite_manifests
            .iter()
            .chain(self.deleted_manifests.iter());
        let replaced_manifest_file_count = Self::active_file_count(replaced_manifest)?;

        if replaced_manifest_file_count != create_manifest_file_count {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "The number of files in the new manifest files should be equal to the number of files in the replaced manifest files",
            ));
        }

        Ok(())
    }

    fn summary(&self) -> Result<HashMap<String, String>> {
        let mut summary = HashMap::new();
        summary.insert(
            CREATED_MANIFESTS_COUNT.to_string(),
            (self.new_manifests.len() + self.added_manifests.len()).to_string(),
        );
        summary.insert(
            KEPT_MANIFESTS_COUNT.to_string(),
            self.keep_manifests.len().to_string(),
        );
        summary.insert(
            REPLACED_MANIFESTS_COUNT.to_string(),
            (self.rewrite_manifests.len() + self.deleted_manifests.len()).to_string(),
        );
        summary.insert(
            PROCESSED_ENTRY_COUNT.to_string(),
            self.process_entry_count.to_string(),
        );
        // # TODO
        // Sets the maximum number of changed partitions before partition summaries will be excluded.
        Ok(summary)
    }

    /// Apply the change to table
    pub async fn apply(mut self) -> Result<Transaction<'a>> {
        // read all manifest files of current snapshot
        let current_manifests_list = if let Some(snapshot) = self
            .snapshot_produce_action
            .tx
            .current_table
            .metadata()
            .current_snapshot()
        {
            snapshot
                .load_manifest_list(
                    self.snapshot_produce_action.tx.current_table.file_io(),
                    &self.snapshot_produce_action.tx.current_table.metadata_ref(),
                )
                .await?
        } else {
            // Do nothing for empty snapshot
            return Ok(self.snapshot_produce_action.tx);
        };

        // validate delete manifest file, make sure they are in the current manifest
        if self
            .deleted_manifests
            .iter()
            .any(|m| !current_manifests_list.entries().contains(m))
        {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Cannot delete manifest file that is not in the current snapshot",
            ));
        }

        if self.require_rewrite() {
            self.perform_rewrite(current_manifests_list).await?;
        } else {
            self.keep_active_manifests(current_manifests_list)?;
        }

        self.validate_files_counts().await?;

        let summary = self.summary()?;

        // Rewrite the snapshot id of all added manifest
        let existing_manifests = self
            .new_manifests
            .into_iter()
            .chain(self.added_manifests.into_iter())
            .map(|mut manifest_file| {
                manifest_file.added_snapshot_id = self.snapshot_produce_action.snapshot_id;
                manifest_file
            })
            .chain(self.keep_manifests.into_iter())
            .collect_vec();

        self.snapshot_produce_action
            .apply(
                RewriteManifestActionOperation {
                    existing_manifests,
                    summary,
                },
                DefaultManifestProcess,
            )
            .await
    }
}

struct RewriteManifestActionOperation {
    existing_manifests: Vec<ManifestFile>,
    summary: HashMap<String, String>,
}

impl SnapshotProduceOperation for RewriteManifestActionOperation {
    fn operation(&self) -> Operation {
        Operation::Replace
    }

    async fn delete_entries(
        &self,
        _snapshot_produce: &SnapshotProduceAction<'_>,
    ) -> Result<Vec<ManifestEntry>> {
        Ok(vec![])
    }

    async fn existing_manifest(
        &self,
        _snapshot_produce: &SnapshotProduceAction<'_>,
    ) -> Result<Vec<ManifestFile>> {
        Ok(self.existing_manifests.clone())
    }

    fn summary(&self) -> HashMap<String, String> {
        self.summary.clone()
    }
}
