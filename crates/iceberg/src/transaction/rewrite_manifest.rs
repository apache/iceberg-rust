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
use itertools::Itertools;
use uuid::Uuid;

use crate::spec::{
    DataFile, DataFileFormat, FormatVersion, Literal, MAIN_BRANCH, ManifestContentType,
    ManifestFile, ManifestListWriter, ManifestWriterBuilder, Operation, Snapshot,
    SnapshotReference, SnapshotRetention, Summary, update_snapshot_summaries,
};
use crate::table::Table;
use crate::transaction::snapshot::generate_unique_snapshot_id;
use crate::transaction::{ActionCommit, TransactionAction};
use crate::{Error, ErrorKind, TableRequirement, TableUpdate};

const META_ROOT_PATH: &str = "metadata";

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
pub struct RewriteManifestsAction {
    commit_uuid: Option<Uuid>,
    key_metadata: Option<Vec<u8>>,
    snapshot_properties: HashMap<String, String>,
}

impl RewriteManifestsAction {
    pub(crate) fn new() -> Self {
        Self {
            commit_uuid: None,
            key_metadata: None,
            snapshot_properties: HashMap::new(),
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

        // Group manifests by (partition_spec_id, content_type)
        let groups: HashMap<ManifestGroupKey, Vec<&ManifestFile>> = manifest_list
            .entries()
            .iter()
            .into_group_map_by(|m| ManifestGroupKey {
                partition_spec_id: m.partition_spec_id,
                content: m.content,
            });

        // Compact each group
        let mut manifest_counter: RangeFrom<u64> = 0..;
        let mut compacted_manifests = Vec::new();
        for (group_key, group_files) in &groups {
            let mut group_result = compact_group(
                table,
                group_key,
                group_files,
                snapshot_id,
                commit_uuid,
                &mut manifest_counter,
                self.key_metadata.clone(),
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
/// into one or more new manifest files.
async fn compact_group(
    table: &Table,
    group_key: &ManifestGroupKey,
    group_files: &[&ManifestFile],
    snapshot_id: i64,
    commit_uuid: Uuid,
    manifest_counter: &mut RangeFrom<u64>,
    key_metadata: Option<Vec<u8>>,
) -> crate::Result<Vec<ManifestFile>> {
    // Load all manifests and collect alive entries
    let mut alive_entries = Vec::new();
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

    // Look up the correct partition spec for this group
    let partition_spec = table
        .metadata()
        .partition_spec_by_id(group_key.partition_spec_id)
        .ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Partition spec not found for id: {}",
                    group_key.partition_spec_id
                ),
            )
        })?;

    // Create manifest writer
    // TODO: Implement rolling writer to split output into multiple manifests
    // based on a target manifest size, instead of writing all entries into one.
    let manifest_path = format!(
        "{}/{}/{}-m{}.{}",
        table.metadata().location(),
        META_ROOT_PATH,
        commit_uuid,
        manifest_counter.next().unwrap(),
        DataFileFormat::Avro,
    );
    let output_file = table.file_io().new_output(manifest_path)?;
    let builder = ManifestWriterBuilder::new(
        output_file,
        Some(snapshot_id),
        key_metadata,
        table.metadata().current_schema().clone(),
        partition_spec.as_ref().clone(),
    );

    let mut writer = match table.metadata().format_version() {
        FormatVersion::V1 => builder.build_v1(),
        FormatVersion::V2 => match group_key.content {
            ManifestContentType::Data => builder.build_v2_data(),
            ManifestContentType::Deletes => builder.build_v2_deletes(),
        },
        FormatVersion::V3 => match group_key.content {
            ManifestContentType::Data => builder.build_v3_data(),
            ManifestContentType::Deletes => builder.build_v3_deletes(),
        },
    };

    // Write all alive entries preserving their original metadata
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

        writer.add_existing_file(
            entry.data_file().clone(),
            entry_snapshot_id,
            sequence_number,
            entry.file_sequence_number,
        )?;
    }

    let manifest_file = writer.write_manifest_file().await?;
    Ok(vec![manifest_file])
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
