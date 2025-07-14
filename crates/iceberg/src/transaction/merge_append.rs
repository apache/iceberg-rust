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

use std::collections::{BTreeMap, HashMap};
use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use uuid::Uuid;

use crate::Result;
use crate::io::FileIO;
use crate::spec::{DataFile, ManifestContentType, ManifestFile, ManifestStatus, ManifestWriter};
use crate::table::Table;
use crate::transaction::append::FastAppendOperation;
use crate::transaction::snapshot::{DefaultManifestProcess, ManifestProcess, SnapshotProducer};
use crate::transaction::{ActionCommit, TransactionAction};
use crate::utils::bin::ListPacker;

/// MergeAppendAction is a transaction action similar to fast append except that it will merge manifests
/// based on the target size.
pub struct MergeAppendAction {
    target_size_bytes: u32,
    min_count_to_merge: u32,
    check_duplicate: bool,
    merge_enabled: bool,
    // below are properties used to create SnapshotProducer when commit
    commit_uuid: Option<Uuid>,
    key_metadata: Option<Vec<u8>>,
    snapshot_properties: HashMap<String, String>,
    added_data_files: Vec<DataFile>,
}

/// Target size of manifest file when merging manifests.
pub const MANIFEST_TARGET_SIZE_BYTES: &str = "commit.manifest.target-size-bytes";
const MANIFEST_TARGET_SIZE_BYTES_DEFAULT: u32 = 8 * 1024 * 1024; // 8 MB
/// Minimum number of manifests to merge.
pub const MANIFEST_MIN_MERGE_COUNT: &str = "commit.manifest.min-count-to-merge";
const MANIFEST_MIN_MERGE_COUNT_DEFAULT: u32 = 100;
/// Whether allow to merge manifests.
pub const MANIFEST_MERGE_ENABLED: &str = "commit.manifest-merge.enabled";
const MANIFEST_MERGE_ENABLED_DEFAULT: bool = false;

impl MergeAppendAction {
    pub(crate) fn new(table: &Table) -> Result<Self> {
        let target_size_bytes: u32 = table
            .metadata()
            .properties()
            .get(MANIFEST_TARGET_SIZE_BYTES)
            .and_then(|s| s.parse().ok())
            .unwrap_or(MANIFEST_TARGET_SIZE_BYTES_DEFAULT);
        let min_count_to_merge: u32 = table
            .metadata()
            .properties()
            .get(MANIFEST_MIN_MERGE_COUNT)
            .and_then(|s| s.parse().ok())
            .unwrap_or(MANIFEST_MIN_MERGE_COUNT_DEFAULT);
        let merge_enabled = table
            .metadata()
            .properties()
            .get(MANIFEST_MERGE_ENABLED)
            .and_then(|s| s.parse().ok())
            .unwrap_or(MANIFEST_MERGE_ENABLED_DEFAULT);
        Ok(Self {
            check_duplicate: true,
            target_size_bytes,
            min_count_to_merge,
            merge_enabled,
            commit_uuid: None,
            key_metadata: None,
            snapshot_properties: HashMap::default(),
            added_data_files: vec![],
        })
    }

    pub fn with_check_duplicate(mut self, v: bool) -> Self {
        self.check_duplicate = v;
        self
    }

    pub fn set_commit_uuid(mut self, commit_uuid: Uuid) -> Self {
        self.commit_uuid = Some(commit_uuid);
        self
    }

    pub fn set_key_metadata(mut self, key_metadata: Vec<u8>) -> Self {
        self.key_metadata = Some(key_metadata);
        self
    }

    pub fn set_snapshot_properties(mut self, snapshot_properties: HashMap<String, String>) -> Self {
        self.snapshot_properties = snapshot_properties;
        self
    }

    /// Add data files to the snapshot.
    pub fn add_data_files(
        &mut self,
        data_files: impl IntoIterator<Item = DataFile>,
    ) -> Result<&mut Self> {
        self.added_data_files.extend(data_files);
        Ok(self)
    }
}

#[async_trait]
impl TransactionAction for MergeAppendAction {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        let snapshot_producer = SnapshotProducer::new(
            table,
            self.commit_uuid.unwrap_or_else(Uuid::now_v7),
            self.key_metadata.clone(),
            self.snapshot_properties.clone(),
            self.added_data_files.clone(),
        );

        // validate added files
        snapshot_producer.validate_added_data_files(&self.added_data_files)?;

        // Checks duplicate files
        if self.check_duplicate {
            snapshot_producer
                .validate_duplicate_files(&self.added_data_files)
                .await?;
        }

        if self.merge_enabled {
            snapshot_producer
                .commit(FastAppendOperation, MergeManifsetProcess {
                    target_size_bytes: self.target_size_bytes,
                    min_count_to_merge: self.min_count_to_merge,
                })
                .await
        } else {
            snapshot_producer
                .commit(FastAppendOperation, DefaultManifestProcess)
                .await
        }
    }
}

struct MergeManifestManager {
    target_size_bytes: u32,
    min_count_to_merge: u32,
    content: ManifestContentType,
}

impl MergeManifestManager {
    pub fn new(
        target_size_bytes: u32,
        min_count_to_merge: u32,
        content: ManifestContentType,
    ) -> Self {
        Self {
            target_size_bytes,
            min_count_to_merge,
            content,
        }
    }

    fn group_by_spec(&self, manifests: Vec<ManifestFile>) -> BTreeMap<i32, Vec<ManifestFile>> {
        let mut grouped_manifests = BTreeMap::new();
        for manifest in manifests {
            grouped_manifests
                .entry(manifest.partition_spec_id)
                .or_insert_with(Vec::new)
                .push(manifest);
        }
        grouped_manifests
    }

    async fn merge_bin(
        &self,
        snapshot_id: i64,
        file_io: FileIO,
        manifest_bin: Vec<ManifestFile>,
        mut writer: ManifestWriter,
    ) -> Result<ManifestFile> {
        for manifest_file in manifest_bin {
            let manifest_file = manifest_file.load_manifest(&file_io).await?;
            for manifest_entry in manifest_file.entries() {
                if manifest_entry.status() == ManifestStatus::Deleted
                    && manifest_entry
                        .snapshot_id()
                        .is_some_and(|id| id == snapshot_id)
                {
                    //only files deleted by this snapshot should be added to the new manifest
                    writer.add_delete_entry(manifest_entry.as_ref().clone())?;
                } else if manifest_entry.status() == ManifestStatus::Added
                    && manifest_entry
                        .snapshot_id()
                        .is_some_and(|id| id == snapshot_id)
                {
                    //added entries from this snapshot are still added, otherwise they should be existing
                    writer.add_entry(manifest_entry.as_ref().clone())?;
                } else if manifest_entry.status() != ManifestStatus::Deleted {
                    // add all non-deleted files from the old manifest as existing files
                    writer.add_existing_entry(manifest_entry.as_ref().clone())?;
                }
            }
        }

        writer.write_manifest_file().await
    }

    async fn merge_group(
        &self,
        snapshot_produce: &mut SnapshotProducer<'_>,
        first_manifest: &ManifestFile,
        group_manifests: Vec<ManifestFile>,
    ) -> Result<Vec<ManifestFile>> {
        let packer: ListPacker<ManifestFile> = ListPacker::new(self.target_size_bytes);
        let manifest_bins =
            packer.pack(group_manifests, |manifest| manifest.manifest_length as u32);

        let manifest_merge_futures = manifest_bins
            .into_iter()
            .map(|manifest_bin| {
                if manifest_bin.len() == 1 {
                    Ok(Box::pin(async { Ok(manifest_bin) })
                        as Pin<
                            Box<dyn Future<Output = Result<Vec<ManifestFile>>> + Send>,
                        >)
                }
                //  if the bin has the first manifest (the new data files or an appended manifest file) then only
                //  merge it if the number of manifests is above the minimum count. this is applied only to bins
                //  with an in-memory manifest so that large manifests don't prevent merging older groups.
                else if manifest_bin
                    .iter()
                    .any(|manifest| manifest == first_manifest)
                    && manifest_bin.len() < self.min_count_to_merge as usize
                {
                    Ok(Box::pin(async { Ok(manifest_bin) })
                        as Pin<
                            Box<dyn Future<Output = Result<Vec<ManifestFile>>> + Send>,
                        >)
                } else {
                    let writer = snapshot_produce.new_manifest_writer(self.content)?;
                    let snapshot_id = snapshot_produce.snapshot_id;
                    let file_io = snapshot_produce.table.file_io().clone();
                    Ok((Box::pin(async move {
                        Ok(vec![
                            self.merge_bin(
                                snapshot_id,
                                file_io,
                                manifest_bin,
                                writer,
                            )
                            .await?,
                        ])
                    }))
                        as Pin<Box<dyn Future<Output = Result<Vec<ManifestFile>>> + Send>>)
                }
            })
            .collect::<Result<Vec<Pin<Box<dyn Future<Output = Result<Vec<ManifestFile>>> + Send>>>>>()?;

        let merged_bins: Vec<Vec<ManifestFile>> =
            futures::future::join_all(manifest_merge_futures.into_iter())
                .await
                .into_iter()
                .collect::<Result<Vec<_>>>()?;

        Ok(merged_bins.into_iter().flatten().collect())
    }

    async fn merge_manifest(
        &self,
        snapshot_produce: &mut SnapshotProducer<'_>,
        manifests: Vec<ManifestFile>,
    ) -> Result<Vec<ManifestFile>> {
        if manifests.is_empty() {
            return Ok(manifests);
        }

        let first_manifest = manifests[0].clone();

        let group_manifests = self.group_by_spec(manifests);

        let mut merge_manifests = vec![];
        for (_spec_id, manifests) in group_manifests.into_iter().rev() {
            merge_manifests.extend(
                self.merge_group(snapshot_produce, &first_manifest, manifests)
                    .await?,
            );
        }

        Ok(merge_manifests)
    }
}

struct MergeManifsetProcess {
    target_size_bytes: u32,
    min_count_to_merge: u32,
}

impl ManifestProcess for MergeManifsetProcess {
    async fn process_manifests(
        &self,
        snapshot_produce: &mut SnapshotProducer<'_>,
        manifests: Vec<ManifestFile>,
    ) -> Result<Vec<ManifestFile>> {
        let (unmerg_data_manifests, unmerge_delete_manifest) = manifests
            .into_iter()
            .partition(|m| m.content == ManifestContentType::Data);
        let mut data_manifests = {
            let merge_manifest_manager = MergeManifestManager::new(
                self.target_size_bytes,
                self.min_count_to_merge,
                ManifestContentType::Data,
            );
            merge_manifest_manager
                .merge_manifest(snapshot_produce, unmerg_data_manifests)
                .await?
        };
        data_manifests.extend(unmerge_delete_manifest);
        Ok(data_manifests)
    }
}
