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
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use uuid::Uuid;

use super::snapshot::SnapshotProducer;
use super::MANIFEST_TARGET_SIZE_BYTES_DEFAULT;
use crate::error::Result;
use crate::spec::{
    update_snapshot_summaries, DataFile, DataFileFormat, FormatVersion, ManifestContentType,
    ManifestFile, ManifestListWriter, ManifestWriter, ManifestWriterBuilder, Operation, Snapshot,
    SnapshotReference, SnapshotRetention, Summary, MAIN_BRANCH,
};
use crate::table::Table;
use crate::transaction::{ActionCommit, TransactionAction};
use crate::{Error, ErrorKind, TableRequirement, TableUpdate};

const META_ROOT_PATH: &str = "metadata";

const KEPT_MANIFESTS_COUNT: &str = "manifests-kept";
const CREATED_MANIFESTS_COUNT: &str = "manifests-created";
const REPLACED_MANIFESTS_COUNT: &str = "manifests-replaced";
const PROCESSED_ENTRY_COUNT: &str = "entries-processed";

/// Function that maps a DataFile to a cluster key for grouping entries into manifests.
type ClusterByFunc = Box<dyn Fn(&DataFile) -> String + Send + Sync>;

/// Predicate function to select which manifests to rewrite.
type ManifestPredicate = Box<dyn Fn(&ManifestFile) -> bool + Send + Sync>;

/// Transaction action for rewriting manifest files.
///
/// This action reorganizes manifest files without changing the underlying data files.
/// It can consolidate small manifests, split large ones, or re-cluster entries by
/// partition values or custom keys.
///
/// Manifests with delete content type are never rewritten.
pub struct RewriteManifestsAction {
    commit_uuid: Option<Uuid>,
    key_metadata: Option<Vec<u8>>,
    snapshot_properties: HashMap<String, String>,
    snapshot_id: Option<i64>,
    target_branch: Option<String>,
    target_size_bytes: u32,

    cluster_by_func: Option<ClusterByFunc>,
    manifest_predicate: Option<ManifestPredicate>,
    added_manifests: Vec<ManifestFile>,
    deleted_manifests: Vec<ManifestFile>,
}

impl RewriteManifestsAction {
    pub fn new() -> Self {
        Self {
            commit_uuid: None,
            key_metadata: None,
            snapshot_properties: HashMap::new(),
            snapshot_id: None,
            target_branch: None,
            target_size_bytes: MANIFEST_TARGET_SIZE_BYTES_DEFAULT,

            cluster_by_func: None,
            manifest_predicate: None,
            added_manifests: Vec::new(),
            deleted_manifests: Vec::new(),
        }
    }

    /// Set a clustering function that determines how data file entries are grouped
    /// into new manifests. Files with the same cluster key will be written to the
    /// same manifest.
    pub fn cluster_by(mut self, func: ClusterByFunc) -> Self {
        self.cluster_by_func = Some(func);
        self
    }

    /// Set a predicate to filter which manifests should be rewritten.
    /// Manifests that don't match the predicate will be kept as-is.
    pub fn rewrite_if(mut self, predicate: ManifestPredicate) -> Self {
        self.manifest_predicate = Some(predicate);
        self
    }

    /// Manually add a manifest to the snapshot. The manifest must not contain
    /// any added or deleted file entries.
    pub fn add_manifest(mut self, manifest: ManifestFile) -> Self {
        self.added_manifests.push(manifest);
        self
    }

    /// Manually remove a manifest from the snapshot. The manifest must exist
    /// in the current snapshot.
    pub fn delete_manifest(mut self, manifest: ManifestFile) -> Self {
        self.deleted_manifests.push(manifest);
        self
    }

    /// Set snapshot properties.
    pub fn set_snapshot_properties(mut self, properties: HashMap<String, String>) -> Self {
        self.snapshot_properties = properties;
        self
    }

    /// Set the target branch for this action.
    pub fn set_target_branch(mut self, target_branch: String) -> Self {
        self.target_branch = Some(target_branch);
        self
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

    /// Set snapshot id.
    pub fn set_snapshot_id(mut self, snapshot_id: i64) -> Self {
        self.snapshot_id = Some(snapshot_id);
        self
    }

    /// Set the target manifest size in bytes.
    pub fn set_target_size_bytes(mut self, target_size_bytes: u32) -> Self {
        self.target_size_bytes = target_size_bytes;
        self
    }
}

impl Default for RewriteManifestsAction {
    fn default() -> Self {
        Self::new()
    }
}

/// Helper to create new manifest writers with unique file paths.
struct ManifestWriterFactory<'a> {
    table: &'a Table,
    commit_uuid: Uuid,
    snapshot_id: i64,
    key_metadata: Option<Vec<u8>>,
    manifest_counter: Arc<AtomicU64>,
}

impl<'a> ManifestWriterFactory<'a> {
    fn new(
        table: &'a Table,
        commit_uuid: Uuid,
        snapshot_id: i64,
        key_metadata: Option<Vec<u8>>,
    ) -> Self {
        Self {
            table,
            commit_uuid,
            snapshot_id,
            key_metadata,
            manifest_counter: Arc::new(AtomicU64::new(0)),
        }
    }

    fn new_manifest_writer(
        &self,
        content: ManifestContentType,
        partition_spec_id: i32,
    ) -> Result<ManifestWriter> {
        let new_manifest_path = format!(
            "{}/{}/{}-m{}.{}",
            self.table.metadata().location(),
            META_ROOT_PATH,
            self.commit_uuid,
            self.manifest_counter.fetch_add(1, Ordering::SeqCst),
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
                .partition_spec_by_id(partition_spec_id)
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        "Invalid partition spec id for new manifest writer",
                    )
                    .with_context("partition spec id", partition_spec_id.to_string())
                })?
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
}

/// Count of active (added + existing) files in a list of manifests.
fn active_files_count(manifests: &[ManifestFile]) -> u32 {
    manifests
        .iter()
        .map(|m| m.added_files_count.unwrap_or(0) + m.existing_files_count.unwrap_or(0))
        .sum()
}

#[async_trait::async_trait]
impl TransactionAction for RewriteManifestsAction {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        let target_branch = self.target_branch.as_deref().unwrap_or(MAIN_BRANCH);

        let commit_uuid = self.commit_uuid.unwrap_or_else(Uuid::now_v7);
        let snapshot_id = self
            .snapshot_id
            .unwrap_or_else(|| SnapshotProducer::generate_unique_snapshot_id(table));

        let metadata_ref = table.metadata_ref();

        let parent_snapshot = metadata_ref.snapshot_for_ref(target_branch);
        let parent_snapshot_id = parent_snapshot.map(|s| s.snapshot_id());

        // Load current manifests
        let current_manifests = if let Some(snapshot) = parent_snapshot {
            let manifest_list = snapshot
                .load_manifest_list(table.file_io(), metadata_ref.as_ref())
                .await?;
            manifest_list
                .consume_entries()
                .into_iter()
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };

        let current_manifest_set: HashSet<ManifestFile> =
            current_manifests.iter().cloned().collect();

        let deleted_set: HashSet<&ManifestFile> = self.deleted_manifests.iter().collect();

        // Validate deleted manifests exist in current snapshot
        for manifest in &self.deleted_manifests {
            if !current_manifest_set.contains(manifest) {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Deleted manifest does not exist in the current snapshot: {}",
                        manifest.manifest_path
                    ),
                ));
            }
        }

        // Validate added manifests don't have added/deleted files
        for manifest in &self.added_manifests {
            if manifest.has_added_files() && manifest.added_files_count != Some(0) {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Cannot add manifest with added files: {}",
                        manifest.manifest_path
                    ),
                ));
            }
            if manifest.has_deleted_files() && manifest.deleted_files_count != Some(0) {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Cannot add manifest with deleted files: {}",
                        manifest.manifest_path
                    ),
                ));
            }
        }

        let writer_factory =
            ManifestWriterFactory::new(table, commit_uuid, snapshot_id, self.key_metadata.clone());

        let mut new_manifests: Vec<ManifestFile> = Vec::new();
        let mut kept_manifests: Vec<ManifestFile> = Vec::new();
        let mut rewritten_manifests: Vec<ManifestFile> = Vec::new();
        let mut entry_count: usize = 0;

        if self.cluster_by_func.is_some() {
            // Perform rewrite with clustering
            let cluster_func = self.cluster_by_func.as_ref().unwrap();

            // Writers keyed by (cluster_key, partition_spec_id)
            let mut writers: HashMap<(String, i32), ManifestWriter> = HashMap::new();

            // Filter out deleted manifests, then process remaining
            let remaining_manifests: Vec<ManifestFile> = current_manifests
                .into_iter()
                .filter(|m| !deleted_set.contains(m))
                .collect();

            for manifest_file in &remaining_manifests {
                // Never rewrite delete manifests
                if manifest_file.content == ManifestContentType::Deletes {
                    kept_manifests.push(manifest_file.clone());
                    continue;
                }

                // Check predicate
                if let Some(ref predicate) = self.manifest_predicate {
                    if !predicate(manifest_file) {
                        kept_manifests.push(manifest_file.clone());
                        continue;
                    }
                }

                // Rewrite this manifest
                rewritten_manifests.push(manifest_file.clone());

                let manifest = manifest_file.load_manifest(table.file_io()).await?;

                for entry in manifest.entries() {
                    if !entry.is_alive() {
                        continue;
                    }

                    let key = cluster_func(entry.data_file());
                    let spec_id = manifest_file.partition_spec_id;
                    let writer_key = (key, spec_id);

                    if !writers.contains_key(&writer_key) {
                        let writer = writer_factory
                            .new_manifest_writer(ManifestContentType::Data, spec_id)?;
                        writers.insert(writer_key.clone(), writer);
                    }

                    let writer = writers.get_mut(&writer_key).unwrap();
                    writer.add_existing_entry(entry.as_ref().clone())?;
                    entry_count += 1;
                }
            }

            // Close all writers and collect new manifests
            for (_key, writer) in writers {
                let manifest_file = writer.write_manifest_file().await?;
                new_manifests.push(manifest_file);
            }
        } else {
            // No clustering - just keep non-deleted manifests
            for manifest_file in current_manifests {
                if !deleted_set.contains(&manifest_file) {
                    kept_manifests.push(manifest_file);
                }
            }
        }

        // Validate file counts
        let created_count =
            active_files_count(&new_manifests) + active_files_count(&self.added_manifests);
        let replaced_count =
            active_files_count(&rewritten_manifests) + active_files_count(&self.deleted_manifests);

        if created_count != replaced_count {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Rewrite manifests file count mismatch: created {} files but replaced {} files",
                    created_count, replaced_count
                ),
            ));
        }

        // Build summary
        let mut summary_properties = HashMap::new();
        summary_properties.insert(
            CREATED_MANIFESTS_COUNT.to_string(),
            new_manifests.len().to_string(),
        );
        summary_properties.insert(
            KEPT_MANIFESTS_COUNT.to_string(),
            kept_manifests.len().to_string(),
        );
        summary_properties.insert(
            REPLACED_MANIFESTS_COUNT.to_string(),
            rewritten_manifests.len().to_string(),
        );
        summary_properties.insert(PROCESSED_ENTRY_COUNT.to_string(), entry_count.to_string());
        summary_properties.extend(self.snapshot_properties.clone());

        let summary = Summary {
            operation: Operation::Replace,
            additional_properties: summary_properties,
        };

        let previous_snapshot = parent_snapshot;
        let summary =
            update_snapshot_summaries(summary, previous_snapshot.map(|s| s.summary()), false)?;

        // Assemble final manifest list: new manifests + added manifests + kept manifests
        let mut all_manifests: Vec<ManifestFile> = Vec::new();
        all_manifests.extend(new_manifests);
        all_manifests.extend(self.added_manifests.clone());
        all_manifests.extend(kept_manifests);

        // Write manifest list
        let next_seq_num = metadata_ref.next_sequence_number();
        let first_row_id = metadata_ref.next_row_id();

        let manifest_list_path = format!(
            "{}/{}/snap-{}-{}-{}.{}",
            metadata_ref.location(),
            META_ROOT_PATH,
            snapshot_id,
            0,
            commit_uuid,
            DataFileFormat::Avro
        );

        let mut manifest_list_writer = match metadata_ref.format_version() {
            FormatVersion::V1 => ManifestListWriter::v1(
                table.file_io().new_output(manifest_list_path.clone())?,
                snapshot_id,
                parent_snapshot_id,
            ),
            FormatVersion::V2 => ManifestListWriter::v2(
                table.file_io().new_output(manifest_list_path.clone())?,
                snapshot_id,
                parent_snapshot_id,
                next_seq_num,
            ),
            FormatVersion::V3 => ManifestListWriter::v3(
                table.file_io().new_output(manifest_list_path.clone())?,
                snapshot_id,
                parent_snapshot_id,
                next_seq_num,
                Some(first_row_id),
            ),
        };

        manifest_list_writer.add_manifests(all_manifests.into_iter())?;
        let writer_next_row_id = manifest_list_writer.next_row_id();
        manifest_list_writer.close().await?;

        // Build snapshot
        let commit_ts = chrono::Utc::now().timestamp_millis();
        let new_snapshot = Snapshot::builder()
            .with_manifest_list(manifest_list_path)
            .with_snapshot_id(snapshot_id)
            .with_parent_snapshot_id(parent_snapshot_id)
            .with_sequence_number(next_seq_num)
            .with_summary(summary)
            .with_schema_id(metadata_ref.current_schema_id())
            .with_timestamp_ms(commit_ts);

        let new_snapshot = if let Some(writer_next_row_id) = writer_next_row_id {
            let assigned_rows = writer_next_row_id - metadata_ref.next_row_id();
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
                ref_name: target_branch.to_string(),
                reference: SnapshotReference::new(
                    snapshot_id,
                    SnapshotRetention::branch(None, None, None),
                ),
            },
        ];

        let requirements = vec![
            TableRequirement::UuidMatch {
                uuid: metadata_ref.uuid(),
            },
            TableRequirement::RefSnapshotIdMatch {
                r#ref: target_branch.to_string(),
                snapshot_id: parent_snapshot_id,
            },
        ];

        Ok(ActionCommit::new(updates, requirements))
    }
}
