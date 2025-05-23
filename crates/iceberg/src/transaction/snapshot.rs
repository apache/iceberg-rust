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

use std::collections::{BTreeMap, HashMap, HashSet};
use std::future::Future;
use std::ops::RangeFrom;
use std::pin::Pin;

use uuid::Uuid;

use crate::error::Result;
use crate::io::FileIO;
use crate::spec::{
    DataContentType, DataFile, DataFileFormat, FormatVersion, ManifestContentType, ManifestEntry,
    ManifestFile, ManifestListWriter, ManifestStatus, ManifestWriter, ManifestWriterBuilder,
    Operation, Snapshot, SnapshotReference, SnapshotRetention, Struct, StructType, Summary,
    MAIN_BRANCH,
};
use crate::transaction::Transaction;
use crate::utils::bin::ListPacker;
use crate::{Error, ErrorKind, TableRequirement, TableUpdate};

const META_ROOT_PATH: &str = "metadata";

pub(crate) trait SnapshotProduceOperation: Send + Sync {
    fn operation(&self) -> Operation;
    #[allow(unused)]
    fn delete_entries(
        &self,
        snapshot_produce: &SnapshotProduceAction,
    ) -> impl Future<Output = Result<Vec<ManifestEntry>>> + Send;
    fn existing_manifest(
        &self,
        snapshot_produce: &mut SnapshotProduceAction,
    ) -> impl Future<Output = Result<Vec<ManifestFile>>> + Send;
}

pub(crate) struct DefaultManifestProcess;

impl ManifestProcess for DefaultManifestProcess {
    async fn process_manifest<'a>(
        &self,
        _snapshot_producer: &mut SnapshotProduceAction<'a>,
        manifests: Vec<ManifestFile>,
    ) -> Result<Vec<ManifestFile>> {
        Ok(manifests)
    }
}

pub(crate) trait ManifestProcess: Send + Sync {
    fn process_manifest<'a>(
        &self,
        snapshot_produce: &mut SnapshotProduceAction<'a>,
        manifests: Vec<ManifestFile>,
    ) -> impl Future<Output = Result<Vec<ManifestFile>>> + Send;
}

pub(crate) struct SnapshotProduceAction<'a> {
    pub tx: Transaction<'a>,
    snapshot_id: i64,
    key_metadata: Vec<u8>,
    commit_uuid: Uuid,
    snapshot_properties: HashMap<String, String>,
    pub added_data_files: Vec<DataFile>,
    pub added_delete_files: Vec<DataFile>,

    removed_data_files: Vec<DataFile>,
    removed_delete_files: Vec<DataFile>,

    // for filtering out files that are removed by action
    pub removed_data_file_paths: HashSet<String>,
    pub removed_delete_file_paths: HashSet<String>,

    // A counter used to generate unique manifest file names.
    // It starts from 0 and increments for each new manifest file.
    // Note: This counter is limited to the range of (0..u64::MAX).
    manifest_counter: RangeFrom<u64>,

    new_data_file_sequence_number: Option<i64>,
}

impl<'a> SnapshotProduceAction<'a> {
    pub(crate) fn new(
        tx: Transaction<'a>,
        snapshot_id: i64,
        key_metadata: Vec<u8>,
        commit_uuid: Uuid,
        snapshot_properties: HashMap<String, String>,
    ) -> Result<Self> {
        Ok(Self {
            tx,
            snapshot_id,
            commit_uuid,
            snapshot_properties,
            added_data_files: vec![],
            added_delete_files: vec![],
            removed_data_files: vec![],
            removed_delete_files: vec![],
            removed_data_file_paths: HashSet::new(),
            removed_delete_file_paths: HashSet::new(),
            manifest_counter: (0..),
            key_metadata,
            new_data_file_sequence_number: None,
        })
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
            if !field
                .field_type
                .as_primitive_type()
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::Unexpected,
                        "Partition field should only be primitive type.",
                    )
                })?
                .compatible(&value.as_primitive_literal().unwrap())
            {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    "Partition value is not compatible partition type",
                ));
            }
        }
        Ok(())
    }

    /// Add data files to the snapshot.
    pub fn add_data_files(
        &mut self,
        data_files: impl IntoIterator<Item = DataFile>,
    ) -> Result<&mut Self> {
        let data_files: Vec<DataFile> = data_files.into_iter().collect();
        for data_file in data_files {
            // Check if the data file partition spec id matches the table default partition spec id.
            if self.tx.current_table.metadata().default_partition_spec_id()
                != data_file.partition_spec_id
            {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    "Data file partition spec id does not match table default partition spec id",
                ));
            }
            Self::validate_partition_value(
                data_file.partition(),
                self.tx.current_table.metadata().default_partition_type(),
            )?;
            if data_file.content_type() == DataContentType::Data {
                self.added_data_files.push(data_file);
            } else {
                self.added_delete_files.push(data_file);
            }
        }
        Ok(self)
    }

    pub fn delete_files(
        &mut self,
        remove_data_files: impl IntoIterator<Item = DataFile>,
    ) -> Result<&mut Self> {
        for data_file in remove_data_files.into_iter() {
            Self::validate_partition_value(
                data_file.partition(),
                self.tx.current_table.metadata().default_partition_type(),
            )?;
            if data_file.content_type() == DataContentType::Data {
                self.removed_data_file_paths
                    .insert(data_file.file_path.clone());
                self.removed_data_files.push(data_file);
            } else {
                self.removed_delete_file_paths
                    .insert(data_file.file_path.clone());
                self.removed_delete_files.push(data_file);
            }
        }
        Ok(self)
    }

    pub fn new_manifest_writer(
        &mut self,
        content_type: &ManifestContentType,
        partition_spec_id: i32,
    ) -> Result<ManifestWriter> {
        let new_manifest_path = format!(
            "{}/{}/{}-m{}.{}",
            self.tx.current_table.metadata().location(),
            META_ROOT_PATH,
            self.commit_uuid,
            self.manifest_counter.next().unwrap(),
            DataFileFormat::Avro
        );
        let output = self
            .tx
            .current_table
            .file_io()
            .new_output(new_manifest_path)?;
        let partition_spec = self
            .tx
            .current_table
            .metadata()
            .partition_spec_by_id(partition_spec_id)
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    "Invalid partition spec id for new manifest writer",
                )
                .with_context("partition spec id", partition_spec_id.to_string())
            })?
            .as_ref()
            .clone();
        let builder = ManifestWriterBuilder::new(
            output,
            Some(self.snapshot_id),
            self.key_metadata.clone(),
            self.tx.current_table.metadata().current_schema().clone(),
            partition_spec,
        );
        if self.tx.current_table.metadata().format_version() == FormatVersion::V1 {
            Ok(builder.build_v1())
        } else {
            match content_type {
                ManifestContentType::Data => Ok(builder.build_v2_data()),
                ManifestContentType::Deletes => Ok(builder.build_v2_deletes()),
            }
        }
    }

    // Write manifest file for added data files and return the ManifestFile for ManifestList.
    async fn write_added_manifest(
        &mut self,
        added_data_files: Vec<DataFile>,
        data_seq: Option<i64>,
    ) -> Result<ManifestFile> {
        let snapshot_id = self.snapshot_id;
        let format_version = self.tx.current_table.metadata().format_version();
        let content_type = {
            let mut data_num = 0;
            let mut delete_num = 0;
            for f in &added_data_files {
                match f.content_type() {
                    DataContentType::Data => data_num += 1,
                    DataContentType::PositionDeletes => delete_num += 1,
                    DataContentType::EqualityDeletes => delete_num += 1,
                }
            }
            if data_num == added_data_files.len() {
                ManifestContentType::Data
            } else if delete_num == added_data_files.len() {
                ManifestContentType::Deletes
            } else {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    "added DataFile for a ManifestFile should be same type (Data or Delete)",
                ));
            }
        };
        let manifest_entries = added_data_files.into_iter().map(|data_file| {
            let builder = ManifestEntry::builder()
                .status(crate::spec::ManifestStatus::Added)
                .data_file(data_file)
                .sequence_number_opt(data_seq);

            if format_version == FormatVersion::V1 {
                builder.snapshot_id(snapshot_id).build()
            } else {
                // For format version > 1, we set the snapshot id at the inherited time to avoid rewrite the manifest file when
                // commit failed.
                builder.build()
            }
        });
        let mut writer = self.new_manifest_writer(
            &content_type,
            self.tx.current_table.metadata().default_partition_spec_id(),
        )?;
        for entry in manifest_entries {
            writer.add_entry(entry)?;
        }
        writer.write_manifest_file().await
    }

    async fn write_delete_manifest(
        &mut self,
        deleted_entries: Vec<ManifestEntry>,
    ) -> Result<Vec<ManifestFile>> {
        if deleted_entries.is_empty() {
            return Ok(vec![]);
        }

        // Group deleted entries by spec_id
        let mut partition_groups = HashMap::new();
        for entry in deleted_entries {
            partition_groups
                .entry(entry.data_file().partition_spec_id)
                .or_insert_with(Vec::new)
                .push(entry);
        }

        // Write a delete manifest per spec_id group
        let mut deleted_manifests = Vec::new();
        for (spec_id, entries) in partition_groups {
            let mut data_file_writer: Option<ManifestWriter> = None;
            let mut delete_file_writer: Option<ManifestWriter> = None;
            for entry in entries {
                match entry.content_type() {
                    DataContentType::Data => {
                        if data_file_writer.is_none() {
                            data_file_writer = Some(
                                self.new_manifest_writer(&ManifestContentType::Data, spec_id)?,
                            );
                        }
                        data_file_writer.as_mut().unwrap().add_delete_entry(entry)?;
                    }
                    DataContentType::EqualityDeletes | DataContentType::PositionDeletes => {
                        if delete_file_writer.is_none() {
                            delete_file_writer = Some(
                                self.new_manifest_writer(&ManifestContentType::Deletes, spec_id)?,
                            );
                        }
                        delete_file_writer
                            .as_mut()
                            .unwrap()
                            .add_delete_entry(entry)?;
                    }
                }
            }
            if let Some(writer) = data_file_writer {
                deleted_manifests.push(writer.write_manifest_file().await?);
            }
            if let Some(writer) = delete_file_writer {
                deleted_manifests.push(writer.write_manifest_file().await?);
            }
        }

        Ok(deleted_manifests)
    }

    async fn manifest_file<OP: SnapshotProduceOperation, MP: ManifestProcess>(
        &mut self,
        snapshot_produce_operation: &OP,
        manifest_process: &MP,
    ) -> Result<Vec<ManifestFile>> {
        let mut manifest_files = vec![];
        let data_files = std::mem::take(&mut self.added_data_files);
        let added_delete_files = std::mem::take(&mut self.added_delete_files);

        if !data_files.is_empty() {
            let added_manifest = self
                .write_added_manifest(data_files, self.new_data_file_sequence_number)
                .await?;
            manifest_files.push(added_manifest);
        }

        if !added_delete_files.is_empty() {
            let added_delete_manifest = self
                .write_added_manifest(added_delete_files, self.new_data_file_sequence_number)
                .await?;
            manifest_files.push(added_delete_manifest);
        }

        let delete_manifests = self
            .write_delete_manifest(snapshot_produce_operation.delete_entries(self).await?)
            .await?;
        manifest_files.extend(delete_manifests);

        let existing_manifests = snapshot_produce_operation.existing_manifest(self).await?;

        manifest_files.extend(existing_manifests);
        manifest_process
            .process_manifest(self, manifest_files)
            .await
    }

    // # TODO
    // Fulfill this function
    fn summary<OP: SnapshotProduceOperation>(&self, snapshot_produce_operation: &OP) -> Summary {
        Summary {
            operation: snapshot_produce_operation.operation(),
            additional_properties: self.snapshot_properties.clone(),
        }
    }

    fn generate_manifest_list_file_path(&self, attempt: i64) -> String {
        format!(
            "{}/{}/snap-{}-{}-{}.{}",
            self.tx.current_table.metadata().location(),
            META_ROOT_PATH,
            self.snapshot_id,
            attempt,
            self.commit_uuid,
            DataFileFormat::Avro
        )
    }

    /// Finished building the action and apply it to the transaction.
    pub async fn apply<OP: SnapshotProduceOperation, MP: ManifestProcess>(
        mut self,
        snapshot_produce_operation: OP,
        process: MP,
    ) -> Result<Transaction<'a>> {
        let new_manifests = self
            .manifest_file(&snapshot_produce_operation, &process)
            .await?;
        let next_seq_num = self.tx.current_table.metadata().next_sequence_number();

        let summary = self.summary(&snapshot_produce_operation);

        let manifest_list_path = self.generate_manifest_list_file_path(0);

        let mut manifest_list_writer = match self.tx.current_table.metadata().format_version() {
            FormatVersion::V1 => ManifestListWriter::v1(
                self.tx
                    .current_table
                    .file_io()
                    .new_output(manifest_list_path.clone())?,
                self.snapshot_id,
                self.tx.current_table.metadata().current_snapshot_id(),
            ),
            FormatVersion::V2 => ManifestListWriter::v2(
                self.tx
                    .current_table
                    .file_io()
                    .new_output(manifest_list_path.clone())?,
                self.snapshot_id,
                self.tx.current_table.metadata().current_snapshot_id(),
                next_seq_num,
            ),
        };
        manifest_list_writer.add_manifests(new_manifests.into_iter())?;
        manifest_list_writer.close().await?;

        let commit_ts = chrono::Utc::now().timestamp_millis();
        let new_snapshot = Snapshot::builder()
            .with_manifest_list(manifest_list_path)
            .with_snapshot_id(self.snapshot_id)
            .with_parent_snapshot_id(self.tx.current_table.metadata().current_snapshot_id())
            .with_sequence_number(next_seq_num)
            .with_summary(summary)
            .with_schema_id(self.tx.current_table.metadata().current_schema_id())
            .with_timestamp_ms(commit_ts)
            .build();

        self.tx.apply(
            vec![
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
            ],
            vec![
                TableRequirement::UuidMatch {
                    uuid: self.tx.current_table.metadata().uuid(),
                },
                TableRequirement::RefSnapshotIdMatch {
                    r#ref: MAIN_BRANCH.to_string(),
                    snapshot_id: self.tx.current_table.metadata().current_snapshot_id(),
                },
            ],
        )?;
        Ok(self.tx)
    }

    pub fn set_new_data_file_sequence_number(&mut self, new_data_file_sequence_number: i64) {
        self.new_data_file_sequence_number = Some(new_data_file_sequence_number);
    }
}

pub(crate) struct MergeManifestProcess {
    target_size_bytes: u32,
    min_count_to_merge: u32,
}

impl MergeManifestProcess {
    pub fn new(target_size_bytes: u32, min_count_to_merge: u32) -> Self {
        Self {
            target_size_bytes,
            min_count_to_merge,
        }
    }
}

impl ManifestProcess for MergeManifestProcess {
    async fn process_manifest<'a>(
        &self,
        snapshot_produce: &mut SnapshotProduceAction<'a>,
        manifests: Vec<ManifestFile>,
    ) -> Result<Vec<ManifestFile>> {
        let (unmerge_data_manifest, unmerge_delete_manifest): (Vec<_>, Vec<_>) = manifests
            .into_iter()
            .partition(|manifest| matches!(manifest.content, ManifestContentType::Data));
        let mut data_manifest = {
            let manifest_merge_manager = MergeManifestManager::new(
                self.target_size_bytes,
                self.min_count_to_merge,
                ManifestContentType::Data,
            );
            manifest_merge_manager
                .merge_manifest(snapshot_produce, unmerge_data_manifest)
                .await?
        };
        data_manifest.extend(unmerge_delete_manifest);
        Ok(data_manifest)
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

    async fn merge_group<'a>(
        &self,
        snapshot_produce: &mut SnapshotProduceAction<'a>,
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
                    let writer = snapshot_produce.new_manifest_writer(&self.content,snapshot_produce.tx.current_table.metadata().default_partition_spec_id())?;
                    let snapshot_id = snapshot_produce.snapshot_id;
                    let file_io = snapshot_produce.tx.current_table.file_io().clone();
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

    pub(crate) async fn merge_manifest<'a>(
        &self,
        snapshot_produce: &mut SnapshotProduceAction<'a>,
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
