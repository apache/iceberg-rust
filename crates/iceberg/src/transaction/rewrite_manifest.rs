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

use std::cmp::Ordering;
use std::collections::HashMap;
use std::ops::RangeFrom;
use std::sync::Arc;

use async_trait::async_trait;
use uuid::Uuid;

use crate::spec::{
    DataFile, DataFileFormat, FormatVersion, Literal, MAIN_BRANCH, ManifestContentType,
    ManifestEntryRef, ManifestFile, ManifestListWriter, ManifestWriter, ManifestWriterBuilder,
    Operation, Snapshot, SnapshotReference, SnapshotRetention, Summary,
    update_snapshot_summaries,
};
use crate::table::Table;
use crate::transaction::snapshot::generate_unique_snapshot_id;
use crate::transaction::{ActionCommit, TransactionAction};
use crate::{Error, ErrorKind, TableRequirement, TableUpdate};

const META_ROOT_PATH: &str = "metadata";

/// Default target size for compacted manifest files (8 MB).
const DEFAULT_TARGET_MANIFEST_SIZE_BYTES: u64 = 8 * 1024 * 1024;

/// Default minimum manifest size — manifests smaller than this are candidates for compaction (4 MB).
const DEFAULT_MIN_MANIFEST_SIZE_BYTES: u64 = 4 * 1024 * 1024;

#[derive(Hash, Eq, PartialEq)]
struct ManifestGroupKey {
    partition_spec_id: i32,
    content: ManifestContentType,
}

/// Action to compact manifest files without modifying data files.
///
/// Manifest compaction merges many small manifest files into fewer, larger ones.
/// This improves scan planning performance by reducing the number of manifest files
/// that need to be read.
///
/// Manifests whose size is at or above `min_manifest_size_bytes` are kept as-is.
/// Smaller manifests are merged and split into new manifests targeting
/// `target_manifest_size_bytes` each.
pub struct RewriteManifestsAction {
    commit_uuid: Option<Uuid>,
    key_metadata: Option<Vec<u8>>,
    snapshot_properties: HashMap<String, String>,
    /// Manifests smaller than this are candidates for compaction.
    min_manifest_size_bytes: u64,
    /// Target size for each compacted output manifest.
    target_manifest_size_bytes: u64,
}

impl RewriteManifestsAction {
    pub(crate) fn new() -> Self {
        Self {
            commit_uuid: None,
            key_metadata: None,
            snapshot_properties: HashMap::new(),
            min_manifest_size_bytes: DEFAULT_MIN_MANIFEST_SIZE_BYTES,
            target_manifest_size_bytes: DEFAULT_TARGET_MANIFEST_SIZE_BYTES,
        }
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
    pub fn set_snapshot_properties(
        mut self,
        snapshot_properties: HashMap<String, String>,
    ) -> Self {
        self.snapshot_properties = snapshot_properties;
        self
    }

    /// Set the minimum manifest file size in bytes.
    ///
    /// Manifests whose `manifest_length` is at or above this threshold are
    /// kept unchanged and not included in compaction. Only manifests smaller
    /// than this value are merged together.
    pub fn set_min_manifest_size_bytes(mut self, size: u64) -> Self {
        self.min_manifest_size_bytes = size;
        self
    }

    /// Set the target size for each compacted output manifest in bytes.
    ///
    /// When compacting, the rolling writer will start a new manifest once
    /// the estimated size of the current manifest reaches this threshold.
    pub fn set_target_manifest_size_bytes(mut self, size: u64) -> Self {
        self.target_manifest_size_bytes = size;
        self
    }
}

#[async_trait]
impl TransactionAction for RewriteManifestsAction {
    async fn commit(self: Arc<Self>, table: &Table) -> crate::Result<ActionCommit> {
        let snapshot = table.metadata().current_snapshot().ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                "Cannot compact manifests: table has no current snapshot",
            )
        })?;

        let commit_uuid = self.commit_uuid.unwrap_or_else(Uuid::now_v7);
        let snapshot_id = generate_unique_snapshot_id(table);

        // Load manifest list from current snapshot
        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), &table.metadata_ref())
            .await?;

        // Partition manifests: large ones are kept as-is, small ones are compacted
        let mut kept_manifests: Vec<ManifestFile> = Vec::new();
        let mut to_compact: HashMap<ManifestGroupKey, Vec<&ManifestFile>> = HashMap::new();

        for manifest_file in manifest_list.entries() {
            if (manifest_file.manifest_length as u64) >= self.min_manifest_size_bytes {
                kept_manifests.push(manifest_file.clone());
            } else {
                to_compact
                    .entry(ManifestGroupKey {
                        partition_spec_id: manifest_file.partition_spec_id,
                        content: manifest_file.content,
                    })
                    .or_default()
                    .push(manifest_file);
            }
        }

        // Compact each group of small manifests
        let mut manifest_counter: RangeFrom<u64> = 0..;
        let mut compacted_manifests = kept_manifests;
        for (group_key, group_files) in &to_compact {
            let mut group_result = compact_group(
                table,
                group_key,
                group_files,
                snapshot_id,
                commit_uuid,
                &mut manifest_counter,
                self.key_metadata.clone(),
                self.target_manifest_size_bytes,
            )
            .await?;
            compacted_manifests.append(&mut group_result);
        }

        // Build manifest list
        let next_seq_num = table.metadata().next_sequence_number();
        let first_row_id = table.metadata().next_row_id();
        let manifest_list_path = format!(
            "{}/{}/snap-{}-0-{}.{}",
            table.metadata().location(),
            META_ROOT_PATH,
            snapshot_id,
            commit_uuid,
            DataFileFormat::Avro
        );

        let mut manifest_list_writer = match table.metadata().format_version() {
            FormatVersion::V1 => ManifestListWriter::v1(
                table.file_io().new_output(manifest_list_path.clone())?,
                snapshot_id,
                table.metadata().current_snapshot_id(),
            ),
            FormatVersion::V2 => ManifestListWriter::v2(
                table.file_io().new_output(manifest_list_path.clone())?,
                snapshot_id,
                table.metadata().current_snapshot_id(),
                next_seq_num,
            ),
            FormatVersion::V3 => ManifestListWriter::v3(
                table.file_io().new_output(manifest_list_path.clone())?,
                snapshot_id,
                table.metadata().current_snapshot_id(),
                next_seq_num,
                Some(first_row_id),
            ),
        };

        // Build summary
        let additional_properties = self.snapshot_properties.clone();
        // For manifest compaction, there are no added/deleted data files,
        // so we just carry forward the operation type.
        let summary = Summary {
            operation: Operation::Replace,
            additional_properties,
        };
        let previous_summary = snapshot.summary();
        let summary = update_snapshot_summaries(summary, Some(previous_summary), false)
            .map_err(|err| {
                Error::new(ErrorKind::Unexpected, "Failed to create snapshot summary.")
                    .with_source(err)
            })?;

        manifest_list_writer.add_manifests(compacted_manifests.into_iter())?;
        let writer_next_row_id = manifest_list_writer.next_row_id();
        manifest_list_writer.close().await?;

        // Build snapshot
        let commit_ts = chrono::Utc::now().timestamp_millis();
        let new_snapshot = Snapshot::builder()
            .with_manifest_list(manifest_list_path)
            .with_snapshot_id(snapshot_id)
            .with_parent_snapshot_id(table.metadata().current_snapshot_id())
            .with_sequence_number(next_seq_num)
            .with_summary(summary)
            .with_schema_id(table.metadata().current_schema_id())
            .with_timestamp_ms(commit_ts);

        let new_snapshot = if let Some(writer_next_row_id) = writer_next_row_id {
            let assigned_rows = writer_next_row_id - table.metadata().next_row_id();
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
                    snapshot_id,
                    SnapshotRetention::branch(None, None, None),
                ),
            },
        ];

        let requirements = vec![
            TableRequirement::UuidMatch {
                uuid: table.metadata().uuid(),
            },
            TableRequirement::RefSnapshotIdMatch {
                r#ref: MAIN_BRANCH.to_string(),
                snapshot_id: table.metadata().current_snapshot_id(),
            },
        ];

        Ok(ActionCommit::new(updates, requirements))
    }
}

/// Compact a group of manifests sharing the same partition spec and content type
/// into one or more new manifest files, using a rolling writer that splits
/// output at the target size.
#[allow(clippy::too_many_arguments)]
async fn compact_group(
    table: &Table,
    group_key: &ManifestGroupKey,
    group_files: &[&ManifestFile],
    snapshot_id: i64,
    commit_uuid: Uuid,
    manifest_counter: &mut RangeFrom<u64>,
    key_metadata: Option<Vec<u8>>,
    target_manifest_size_bytes: u64,
) -> crate::Result<Vec<ManifestFile>> {
    // Load all manifests and collect alive entries
    let mut alive_entries: Vec<ManifestEntryRef> = Vec::new();
    for manifest_file in group_files {
        let manifest = manifest_file.load_manifest(table.file_io()).await?;
        for entry in manifest.entries() {
            if entry.is_alive() {
                alive_entries.push(Arc::clone(entry));
            }
        }
    }

    // Sort entries by partition values for better scan planning
    alive_entries.sort_by(|a, b| compare_partition(a.data_file(), b.data_file()));

    // Estimate average bytes per entry from the source manifests.
    // This gives us a rough estimate of how large each entry will be in the
    // output manifest, allowing us to decide when to roll to a new file.
    let total_source_bytes: u64 = group_files.iter().map(|m| m.manifest_length as u64).sum();
    let avg_entry_bytes = if alive_entries.is_empty() {
        0u64
    } else {
        total_source_bytes / alive_entries.len() as u64
    };

    // Use a rolling writer to split output at the target size
    let mut rolling_writer = RollingManifestWriter::new(
        table,
        group_key,
        snapshot_id,
        commit_uuid,
        manifest_counter,
        key_metadata,
        target_manifest_size_bytes,
        avg_entry_bytes,
    );

    for entry in &alive_entries {
        let entry_snapshot_id = entry.snapshot_id().ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Manifest entry missing snapshot_id for file: {}",
                    entry.file_path()
                ),
            )
        })?;
        let sequence_number = entry.sequence_number().ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Manifest entry missing sequence_number for file: {}",
                    entry.file_path()
                ),
            )
        })?;

        rolling_writer
            .add_entry(
                entry.data_file().clone(),
                entry_snapshot_id,
                sequence_number,
                entry.file_sequence_number,
            )
            .await?;
    }

    rolling_writer.finish().await
}

/// A rolling manifest writer that creates a new manifest file once the
/// estimated size of the current one exceeds `target_size_bytes`.
struct RollingManifestWriter<'a> {
    table: &'a Table,
    group_key: &'a ManifestGroupKey,
    snapshot_id: i64,
    commit_uuid: Uuid,
    manifest_counter: &'a mut RangeFrom<u64>,
    key_metadata: Option<Vec<u8>>,
    target_size_bytes: u64,
    avg_entry_bytes: u64,
    current_writer: Option<ManifestWriter>,
    current_entry_count: u64,
    completed_manifests: Vec<ManifestFile>,
}

impl<'a> RollingManifestWriter<'a> {
    #[allow(clippy::too_many_arguments)]
    fn new(
        table: &'a Table,
        group_key: &'a ManifestGroupKey,
        snapshot_id: i64,
        commit_uuid: Uuid,
        manifest_counter: &'a mut RangeFrom<u64>,
        key_metadata: Option<Vec<u8>>,
        target_size_bytes: u64,
        avg_entry_bytes: u64,
    ) -> Self {
        Self {
            table,
            group_key,
            snapshot_id,
            commit_uuid,
            manifest_counter,
            key_metadata,
            target_size_bytes,
            avg_entry_bytes,
            current_writer: None,
            current_entry_count: 0,
            completed_manifests: Vec::new(),
        }
    }

    fn new_writer(&mut self) -> crate::Result<ManifestWriter> {
        let partition_spec = self
            .table
            .metadata()
            .partition_spec_by_id(self.group_key.partition_spec_id)
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Partition spec not found for id: {}",
                        self.group_key.partition_spec_id
                    ),
                )
            })?;

        let manifest_path = format!(
            "{}/{}/{}-m{}.{}",
            self.table.metadata().location(),
            META_ROOT_PATH,
            self.commit_uuid,
            self.manifest_counter.next().unwrap(),
            DataFileFormat::Avro,
        );
        let output_file = self.table.file_io().new_output(manifest_path)?;
        let builder = ManifestWriterBuilder::new(
            output_file,
            Some(self.snapshot_id),
            self.key_metadata.clone(),
            self.table.metadata().current_schema().clone(),
            partition_spec.as_ref().clone(),
        );

        let writer = match self.table.metadata().format_version() {
            FormatVersion::V1 => builder.build_v1(),
            FormatVersion::V2 => match self.group_key.content {
                ManifestContentType::Data => builder.build_v2_data(),
                ManifestContentType::Deletes => builder.build_v2_deletes(),
            },
            FormatVersion::V3 => match self.group_key.content {
                ManifestContentType::Data => builder.build_v3_data(),
                ManifestContentType::Deletes => builder.build_v3_deletes(),
            },
        };
        Ok(writer)
    }

    /// Add an entry to the rolling writer. If the estimated size of the current
    /// manifest exceeds the target, the current writer is flushed and a new one
    /// is started.
    async fn add_entry(
        &mut self,
        data_file: DataFile,
        entry_snapshot_id: i64,
        sequence_number: i64,
        file_sequence_number: Option<i64>,
    ) -> crate::Result<()> {
        // Roll to a new writer if the current one has reached the target size
        if self.should_roll() {
            self.flush().await?;
        }

        if self.current_writer.is_none() {
            self.current_writer = Some(self.new_writer()?);
            self.current_entry_count = 0;
        }

        self.current_writer
            .as_mut()
            .unwrap()
            .add_existing_file(data_file, entry_snapshot_id, sequence_number, file_sequence_number)?;
        self.current_entry_count += 1;
        Ok(())
    }

    fn should_roll(&self) -> bool {
        if self.current_writer.is_none() || self.current_entry_count == 0 {
            return false;
        }
        let estimated_size = self.current_entry_count * self.avg_entry_bytes;
        estimated_size >= self.target_size_bytes
    }

    /// Flush the current writer and store the resulting manifest file.
    async fn flush(&mut self) -> crate::Result<()> {
        if let Some(writer) = self.current_writer.take() {
            let manifest_file = writer.write_manifest_file().await?;
            self.completed_manifests.push(manifest_file);
            self.current_entry_count = 0;
        }
        Ok(())
    }

    /// Flush any remaining entries and return all completed manifest files.
    async fn finish(mut self) -> crate::Result<Vec<ManifestFile>> {
        self.flush().await?;
        Ok(self.completed_manifests)
    }
}

/// Compare two data files by their partition values for sorting.
/// None values sort before Some values.
fn compare_partition(a: &DataFile, b: &DataFile) -> Ordering {
    let fields_a = a.partition().fields();
    let fields_b = b.partition().fields();

    for (fa, fb) in fields_a.iter().zip(fields_b.iter()) {
        let ord = match (fa, fb) {
            (None, None) => Ordering::Equal,
            (None, Some(_)) => Ordering::Less,
            (Some(_), None) => Ordering::Greater,
            (Some(la), Some(lb)) => compare_literal(la, lb),
        };
        if ord != Ordering::Equal {
            return ord;
        }
    }

    Ordering::Equal
}

/// Compare two literals using their primitive representation.
fn compare_literal(a: &Literal, b: &Literal) -> Ordering {
    match (a.as_primitive_literal(), b.as_primitive_literal()) {
        (Some(pa), Some(pb)) => pa.partial_cmp(&pb).unwrap_or(Ordering::Equal),
        (None, None) => Ordering::Equal,
        (None, Some(_)) => Ordering::Less,
        (Some(_), None) => Ordering::Greater,
    }
}
