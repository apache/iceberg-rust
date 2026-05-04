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
use std::sync::Arc;

use async_trait::async_trait;
use futures::future::try_join_all;
use uuid::Uuid;

use crate::TableRequirement;
use crate::error::Result;
use crate::spec::snapshot_summary::{
    ENTRIES_PROCESSED, MANIFESTS_CREATED, MANIFESTS_KEPT, MANIFESTS_REPLACED,
};
use crate::spec::{
    DataFileFormat, FormatVersion, MAIN_BRANCH, ManifestContentType, ManifestEntry, ManifestFile,
    ManifestWriterBuilder, Operation, Struct,
};
use crate::table::Table;
use crate::transaction::snapshot::{
    DefaultManifestProcess, META_ROOT_PATH, SnapshotProduceOperation, SnapshotProducer,
};
use crate::transaction::{ActionCommit, TransactionAction};

/// Action that rewrites cross-partition DATA manifests into per-partition manifests.
///
/// DELETE manifests and already-single-partition DATA manifests are carried forward unchanged.
/// Returns an empty `ActionCommit` when there are no cross-partition manifests to rewrite.
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
            snapshot_properties: HashMap::default(),
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

    /// Set additional snapshot summary properties.
    pub fn set_snapshot_properties(mut self, snapshot_properties: HashMap<String, String>) -> Self {
        self.snapshot_properties = snapshot_properties;
        self
    }
}

#[async_trait]
impl TransactionAction for RewriteManifestsAction {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        let commit_uuid = self.commit_uuid.unwrap_or_else(Uuid::now_v7);

        // Nothing to rewrite without an existing snapshot. Emit requirements so concurrent
        // writers cannot silently invalidate this no-op decision before it reaches the catalog.
        let Some(snapshot) = table.metadata().current_snapshot() else {
            let requirements = vec![
                TableRequirement::UuidMatch {
                    uuid: table.metadata().uuid(),
                },
                TableRequirement::RefSnapshotIdMatch {
                    r#ref: MAIN_BRANCH.to_string(),
                    snapshot_id: table.metadata().current_snapshot_id(),
                },
            ];
            return Ok(ActionCommit::new(vec![], requirements));
        };

        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), &table.metadata_ref())
            .await?;

        // Classify manifests: keep DELETE manifests and single-partition DATA manifests;
        // group alive entries from cross-partition DATA manifests for rewriting.
        let mut kept_manifests: Vec<ManifestFile> = Vec::new();
        // partition → alive entries (with inherited sequence/snapshot fields).
        // HashMap gives O(1) inserts; before enumerating we sort by min file path so
        // the counter → manifest path mapping is deterministic across runs.
        let mut partition_entries: HashMap<Struct, Vec<ManifestEntry>> = HashMap::new();
        let mut manifests_replaced: usize = 0;
        let mut manifests_kept: usize = 0;
        let mut entries_processed: usize = 0;

        // Carry DELETE manifests forward unchanged; collect DATA manifests for parallel loading.
        let (delete_mfs, data_mfs): (Vec<_>, Vec<_>) = manifest_list
            .entries()
            .iter()
            .partition(|mf| mf.content == ManifestContentType::Deletes);
        kept_manifests.extend(delete_mfs.into_iter().cloned());

        // Load all DATA manifests in parallel.
        let loaded_manifests =
            try_join_all(data_mfs.iter().map(|mf| mf.load_manifest(table.file_io()))).await?;

        for (manifest_file, manifest) in data_mfs.iter().zip(loaded_manifests.iter()) {
            // Collect alive entries with inherited fields resolved.
            let alive_entries: Vec<ManifestEntry> = manifest
                .entries()
                .iter()
                .filter(|e| e.is_alive())
                .map(|e| {
                    let mut entry = (**e).clone();
                    entry.inherit_data(manifest_file);
                    entry
                })
                .collect();

            // Count unique partition values to decide whether this manifest is cross-partition.
            let unique_partition_count = alive_entries
                .iter()
                .map(|e| e.data_file.partition())
                .collect::<std::collections::HashSet<_>>()
                .len();

            if unique_partition_count <= 1 {
                // Single partition (or empty): keep unchanged.
                kept_manifests.push((*manifest_file).clone());
                manifests_kept += 1;
            } else {
                // Cross-partition: pool entries for per-partition rewriting.
                manifests_replaced += 1;
                entries_processed += alive_entries.len();
                for entry in alive_entries {
                    let partition = entry.data_file.partition().clone();
                    partition_entries.entry(partition).or_default().push(entry);
                }
            }
        }

        // If all manifests are already single-partition, this is a no-op. Emit requirements
        // so concurrent writers cannot silently invalidate this no-op decision.
        if partition_entries.is_empty() {
            let requirements = vec![
                TableRequirement::UuidMatch {
                    uuid: table.metadata().uuid(),
                },
                TableRequirement::RefSnapshotIdMatch {
                    r#ref: MAIN_BRANCH.to_string(),
                    snapshot_id: table.metadata().current_snapshot_id(),
                },
            ];
            return Ok(ActionCommit::new(vec![], requirements));
        }

        // Build snapshot properties including the bypass key for the SnapshotProducer gate
        // (which requires non-empty snapshot_properties when added_data_files is empty).
        // TODO: Remove this workaround once https://github.com/apache/iceberg-rust/issues/1548
        // is resolved and FastAppendAction validates its own preconditions.
        let mut snapshot_properties = self.snapshot_properties.clone();
        snapshot_properties.insert("rewrite-manifests".to_string(), "true".to_string());
        snapshot_properties.insert(
            MANIFESTS_CREATED.to_string(),
            partition_entries.len().to_string(),
        );
        snapshot_properties.insert(
            MANIFESTS_REPLACED.to_string(),
            manifests_replaced.to_string(),
        );
        snapshot_properties.insert(MANIFESTS_KEPT.to_string(), manifests_kept.to_string());
        snapshot_properties.insert(ENTRIES_PROCESSED.to_string(), entries_processed.to_string());

        // Construct SnapshotProducer first so we can read the generated snapshot_id.
        // The snapshot_id is needed as added_snapshot_id in the new manifest files so
        // that ManifestListWriter::assign_sequence_numbers passes for V2/V3.
        let snapshot_producer = SnapshotProducer::new(
            table,
            commit_uuid,
            self.key_metadata.clone(),
            snapshot_properties,
            vec![], // no new data files — metadata-only operation
        );
        let new_snapshot_id = snapshot_producer.snapshot_id();

        // Write one new manifest per partition group, all in parallel.
        let file_io = table.file_io().clone();
        let schema = table.metadata().current_schema().clone();
        let partition_spec = table.metadata().default_partition_spec().as_ref().clone();
        let format_version = table.metadata().format_version();
        let location = table.metadata().location().to_string();

        // Sort partition groups by the lexicographically smallest file path they contain.
        // This makes the counter → manifest path mapping deterministic across repeated runs
        // on the same snapshot without requiring Ord on Struct.
        let mut sorted_partitions: Vec<(Struct, Vec<ManifestEntry>)> =
            partition_entries.into_iter().collect();
        sorted_partitions.sort_by(|(_, a), (_, b)| {
            let a_min = a
                .iter()
                .map(|e| e.data_file.file_path())
                .min()
                .unwrap_or("");
            let b_min = b
                .iter()
                .map(|e| e.data_file.file_path())
                .min()
                .unwrap_or("");
            a_min.cmp(b_min)
        });

        let new_manifests: Vec<ManifestFile> = try_join_all(
            sorted_partitions
                .into_iter()
                .enumerate()
                .map(|(i, (_, entries))| {
                    let path = format!(
                        "{}/{}/{}-rewrite-m{}.{}",
                        location,
                        META_ROOT_PATH,
                        commit_uuid,
                        i,
                        DataFileFormat::Avro,
                    );
                    let file_io = file_io.clone();
                    let schema = schema.clone();
                    let partition_spec = partition_spec.clone();
                    let key_metadata = self.key_metadata.clone();
                    async move {
                        let output_file = file_io.new_output(path)?;
                        let builder = ManifestWriterBuilder::new(
                            output_file,
                            Some(new_snapshot_id),
                            key_metadata,
                            schema,
                            partition_spec,
                        );
                        let mut writer = match format_version {
                            FormatVersion::V1 => builder.build_v1(),
                            FormatVersion::V2 => builder.build_v2_data(),
                            FormatVersion::V3 => builder.build_v3_data(),
                        };
                        for entry in entries {
                            writer.add_existing_file(
                                entry.data_file,
                                entry.snapshot_id.unwrap_or(0),
                                entry.sequence_number.unwrap_or(0),
                                entry.file_sequence_number,
                            )?;
                        }
                        writer.write_manifest_file().await
                    }
                }),
        )
        .await?;

        // Assemble the final manifest list: kept manifests then new per-partition manifests.
        let mut all_manifests = kept_manifests;
        all_manifests.extend(new_manifests);

        let result = snapshot_producer
            .commit(
                RewriteManifestsOperation {
                    manifests: all_manifests,
                },
                DefaultManifestProcess,
            )
            .await?;
        Ok(result.commit)
    }
}

struct RewriteManifestsOperation {
    manifests: Vec<ManifestFile>,
}

impl SnapshotProduceOperation for RewriteManifestsOperation {
    fn operation(&self) -> Operation {
        Operation::Replace
    }

    async fn delete_entries(
        &self,
        _snapshot_produce: &SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestEntry>> {
        Ok(vec![])
    }

    async fn existing_manifest(
        &self,
        _snapshot_produce: &SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestFile>> {
        Ok(self.manifests.clone())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use crate::TableUpdate;
    use crate::spec::{
        DataContentType, DataFileBuilder, DataFileFormat, Literal, MAIN_BRANCH,
        ManifestContentType, ManifestEntry, ManifestFile, ManifestListWriter, ManifestStatus,
        ManifestWriterBuilder, Operation, PartitionSpec, Schema, Snapshot, SnapshotReference,
        SnapshotRetention, Struct, Summary,
    };
    use crate::transaction::tests::make_v2_minimal_table;
    use crate::transaction::{Transaction, TransactionAction};

    // ---------------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------------

    fn make_data_file(path: &str, partition_x: i64) -> crate::spec::DataFile {
        DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(path.to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(0)
            .partition(Struct::from_iter([Some(Literal::long(partition_x))]))
            .build()
            .unwrap()
    }

    /// Apply an `ActionCommit` to a table and return the updated table.
    fn apply_action_commit(
        table: crate::table::Table,
        updates: Vec<TableUpdate>,
    ) -> crate::table::Table {
        let metadata_builder = table.metadata().clone().into_builder(None);
        let metadata_builder = updates
            .into_iter()
            .fold(metadata_builder, |b, u| u.apply(b).unwrap());
        let result = metadata_builder.build().unwrap();
        table.with_metadata(Arc::new(result.metadata))
    }

    /// Fast-append `files` to `table` and return the updated table.
    async fn fast_append(
        table: crate::table::Table,
        files: Vec<crate::spec::DataFile>,
    ) -> crate::table::Table {
        let tx = Transaction::new(&table);
        let action = tx.fast_append().add_data_files(files);
        let mut commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = commit.take_updates();
        apply_action_commit(table, updates)
    }

    // ---------------------------------------------------------------------------
    // Tests
    // ---------------------------------------------------------------------------

    /// Single-partition table: all manifests have only one unique partition value.
    /// rewrite_manifests should return an empty ActionCommit (no new snapshot).
    #[tokio::test]
    async fn test_single_partition_no_rewrite() {
        let table = make_v2_minimal_table();

        // Append two files, both with partition x=100.
        let table = fast_append(table, vec![
            make_data_file("test/a.parquet", 100),
            make_data_file("test/b.parquet", 100),
        ])
        .await;

        let tx = Transaction::new(&table);
        let action = tx.rewrite_manifests();
        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();

        // No cross-partition manifests → no new snapshot.
        assert!(
            action_commit.take_updates().is_empty(),
            "single-partition table should produce no updates"
        );
    }

    /// Cross-partition table: a manifest contains files from two different partitions.
    /// rewrite_manifests should split it and preserve alive file count.
    #[tokio::test]
    async fn test_cross_partition_rewrite() {
        let table = make_v2_minimal_table();

        // Append two files with different partition values in ONE call →
        // single manifest with 2 partitions.
        let table = fast_append(table, vec![
            make_data_file("test/p100.parquet", 100),
            make_data_file("test/p200.parquet", 200),
        ])
        .await;

        let tx = Transaction::new(&table);
        let action = tx.rewrite_manifests();
        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();

        let updates = action_commit.take_updates();
        assert!(
            !updates.is_empty(),
            "cross-partition table should produce updates"
        );

        let new_snapshot = if let TableUpdate::AddSnapshot { snapshot } = &updates[0] {
            snapshot
        } else {
            panic!("expected AddSnapshot");
        };

        let manifest_list = new_snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();

        // The one cross-partition manifest should have been split into two.
        assert_eq!(
            manifest_list.entries().len(),
            2,
            "cross-partition manifest should be split into per-partition manifests"
        );

        // Count alive entries in the new manifests and compare with original.
        let mut total_alive = 0usize;
        for mf in manifest_list.entries() {
            let manifest = mf.load_manifest(table.file_io()).await.unwrap();
            for entry in manifest.entries() {
                if entry.is_alive() {
                    total_alive += 1;
                }
            }
        }
        assert_eq!(
            total_alive, 2,
            "alive file count must be preserved across rewrite"
        );
    }

    /// All entries in rewritten manifests must have ManifestStatus::Existing.
    #[tokio::test]
    async fn test_rewritten_entries_have_existing_status() {
        let table = make_v2_minimal_table();

        let table = fast_append(table, vec![
            make_data_file("test/s100.parquet", 100),
            make_data_file("test/s200.parquet", 200),
        ])
        .await;

        let tx = Transaction::new(&table);
        let action = tx.rewrite_manifests();
        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();

        let new_snapshot = if let TableUpdate::AddSnapshot { snapshot } = &updates[0] {
            snapshot
        } else {
            panic!("expected AddSnapshot");
        };

        let manifest_list = new_snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();

        for mf in manifest_list.entries() {
            let manifest = mf.load_manifest(table.file_io()).await.unwrap();
            for entry in manifest.entries() {
                assert_eq!(
                    entry.status,
                    ManifestStatus::Existing,
                    "every entry in a rewritten manifest must have status Existing"
                );
            }
        }
    }

    /// snapshot_id, sequence_number, and file_sequence_number must be preserved in
    /// rewritten entries.
    #[tokio::test]
    async fn test_sequence_number_preservation() {
        let table = make_v2_minimal_table();

        let table = fast_append(table, vec![
            make_data_file("test/seq100.parquet", 100),
            make_data_file("test/seq200.parquet", 200),
        ])
        .await;

        // Collect original snapshot_id and sequence_number from the current snapshot's manifests.
        let orig_snapshot = table.metadata().current_snapshot().unwrap();
        let orig_manifest_list = orig_snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();

        let mut orig_entries: Vec<ManifestEntry> = Vec::new();
        for mf in orig_manifest_list.entries() {
            let manifest = mf.load_manifest(table.file_io()).await.unwrap();
            for entry in manifest.entries() {
                let mut e = (**entry).clone();
                e.inherit_data(mf);
                orig_entries.push(e);
            }
        }

        // Rewrite and collect new entries.
        let tx = Transaction::new(&table);
        let action = tx.rewrite_manifests();
        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();

        let new_snapshot = if let TableUpdate::AddSnapshot { snapshot } = &updates[0] {
            snapshot
        } else {
            panic!("expected AddSnapshot");
        };

        let new_manifest_list = new_snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();

        let mut new_entries: Vec<ManifestEntry> = Vec::new();
        for mf in new_manifest_list.entries() {
            let manifest = mf.load_manifest(table.file_io()).await.unwrap();
            for entry in manifest.entries() {
                new_entries.push((**entry).clone());
            }
        }

        // Sort both by file_path for a stable comparison.
        orig_entries.sort_by(|a, b| a.data_file.file_path().cmp(b.data_file.file_path()));
        new_entries.sort_by(|a, b| a.data_file.file_path().cmp(b.data_file.file_path()));

        assert_eq!(orig_entries.len(), new_entries.len());

        for (orig, new) in orig_entries.iter().zip(new_entries.iter()) {
            assert_eq!(
                orig.snapshot_id,
                new.snapshot_id,
                "snapshot_id must be preserved for {}",
                orig.data_file.file_path()
            );
            assert_eq!(
                orig.sequence_number,
                new.sequence_number,
                "sequence_number must be preserved for {}",
                orig.data_file.file_path()
            );
            assert_eq!(
                orig.file_sequence_number,
                new.file_sequence_number,
                "file_sequence_number must be preserved for {}",
                orig.data_file.file_path()
            );
        }
    }

    /// Build a table whose current snapshot contains a single DATA manifest with
    /// two alive entries (different partitions) and one DELETED (tombstone) entry.
    async fn make_table_with_tombstone(base: crate::table::Table) -> crate::table::Table {
        let file_io = base.file_io().clone();
        let schema = base.metadata().current_schema().clone();
        let partition_spec = base.metadata().default_partition_spec().as_ref().clone();
        let location = base.metadata().location();

        let snap_id: i64 = 999_000_001;
        let seq_num: i64 = 1;

        let manifest_path = format!("{location}/metadata/test-tombstone-manifest.avro");
        let output = file_io.new_output(&manifest_path).unwrap();
        let mut writer =
            ManifestWriterBuilder::new(output, Some(snap_id), None, schema, partition_spec)
                .build_v2_data();

        // Two alive entries with different partitions.
        writer
            .add_existing_file(
                make_data_file("test/alive-100.parquet", 100),
                snap_id,
                seq_num,
                Some(seq_num),
            )
            .unwrap();
        writer
            .add_existing_file(
                make_data_file("test/alive-200.parquet", 200),
                snap_id,
                seq_num,
                Some(seq_num),
            )
            .unwrap();

        // One DELETED (tombstone) entry — must not appear after rewrite.
        writer
            .add_delete_file(
                make_data_file("test/deleted-100.parquet", 100),
                seq_num,
                Some(seq_num),
            )
            .unwrap();

        let manifest_file = writer.write_manifest_file().await.unwrap();

        let ml_path = format!("{location}/metadata/snap-tombstone-{snap_id}.avro");
        let ml_output = file_io.new_output(&ml_path).unwrap();
        let mut ml_writer = ManifestListWriter::v2(ml_output, snap_id, None, seq_num);
        ml_writer
            .add_manifests(std::iter::once(manifest_file))
            .unwrap();
        ml_writer.close().await.unwrap();

        let snapshot = Snapshot::builder()
            .with_snapshot_id(snap_id)
            .with_sequence_number(seq_num)
            .with_timestamp_ms(1_700_000_000_000)
            .with_manifest_list(ml_path)
            .with_summary(Summary {
                operation: Operation::Overwrite,
                additional_properties: HashMap::new(),
            })
            .with_schema_id(base.metadata().current_schema_id())
            .build();

        apply_action_commit(base, vec![
            TableUpdate::AddSnapshot { snapshot },
            TableUpdate::SetSnapshotRef {
                ref_name: MAIN_BRANCH.to_string(),
                reference: SnapshotReference::new(
                    snap_id,
                    SnapshotRetention::branch(None, None, None),
                ),
            },
        ])
    }

    /// Tombstone (DELETED) entries must NOT appear in the rewritten manifests.
    #[tokio::test]
    async fn test_tombstone_entries_excluded() {
        let base = make_v2_minimal_table();
        let table = make_table_with_tombstone(base).await;

        let tx = Transaction::new(&table);
        let action = tx.rewrite_manifests();
        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();

        let new_snapshot = if let TableUpdate::AddSnapshot { snapshot } = &updates[0] {
            snapshot
        } else {
            panic!("expected AddSnapshot");
        };

        let manifest_list = new_snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();

        for mf in manifest_list.entries() {
            let manifest = mf.load_manifest(table.file_io()).await.unwrap();
            for entry in manifest.entries() {
                assert_ne!(
                    entry.status,
                    ManifestStatus::Deleted,
                    "tombstone entry {} must not appear in rewritten manifests",
                    entry.data_file.file_path()
                );
            }
        }
    }

    /// Build a table whose current snapshot has one cross-partition DATA manifest
    /// and one DELETE manifest.
    async fn make_table_with_delete_manifest(base: crate::table::Table) -> crate::table::Table {
        let file_io = base.file_io().clone();
        let schema = base.metadata().current_schema().clone();
        let partition_spec = base.metadata().default_partition_spec().as_ref().clone();
        let location = base.metadata().location();

        let snap_id: i64 = 999_000_002;
        let seq_num: i64 = 1;

        // DATA manifest with two different partitions (cross-partition).
        let data_manifest_path = format!("{location}/metadata/test-data-manifest.avro");
        let data_output = file_io.new_output(&data_manifest_path).unwrap();
        let mut data_writer = ManifestWriterBuilder::new(
            data_output,
            Some(snap_id),
            None,
            schema.clone(),
            partition_spec.clone(),
        )
        .build_v2_data();
        data_writer
            .add_existing_file(
                make_data_file("test/d-100.parquet", 100),
                snap_id,
                seq_num,
                Some(seq_num),
            )
            .unwrap();
        data_writer
            .add_existing_file(
                make_data_file("test/d-200.parquet", 200),
                snap_id,
                seq_num,
                Some(seq_num),
            )
            .unwrap();
        let data_manifest_file = data_writer.write_manifest_file().await.unwrap();

        // DELETE manifest (position deletes).
        let del_manifest_path = format!("{location}/metadata/test-delete-manifest.avro");
        let del_output = file_io.new_output(&del_manifest_path).unwrap();
        let del_schema = Arc::new(
            Schema::builder()
                .with_fields(vec![Arc::new(crate::spec::NestedField::optional(
                    1,
                    "id",
                    crate::spec::Type::Primitive(crate::spec::PrimitiveType::Long),
                ))])
                .build()
                .unwrap(),
        );
        let del_partition_spec = PartitionSpec::builder(del_schema.clone())
            .with_spec_id(0)
            .build()
            .unwrap();
        let mut del_writer = ManifestWriterBuilder::new(
            del_output,
            Some(snap_id),
            None,
            del_schema,
            del_partition_spec,
        )
        .build_v2_deletes();
        // Write a position delete file entry.
        del_writer
            .add_existing_file(
                {
                    DataFileBuilder::default()
                        .content(DataContentType::PositionDeletes)
                        .file_path("test/pos-delete.parquet".to_string())
                        .file_format(DataFileFormat::Parquet)
                        .file_size_in_bytes(50)
                        .record_count(1)
                        .partition_spec_id(0)
                        .partition(Struct::empty())
                        .build()
                        .unwrap()
                },
                snap_id,
                seq_num,
                Some(seq_num),
            )
            .unwrap();
        let del_manifest_file = del_writer.write_manifest_file().await.unwrap();

        let ml_path = format!("{location}/metadata/snap-delete-{snap_id}.avro");
        let ml_output = file_io.new_output(&ml_path).unwrap();
        let mut ml_writer = ManifestListWriter::v2(ml_output, snap_id, None, seq_num);
        ml_writer
            .add_manifests([data_manifest_file, del_manifest_file].into_iter())
            .unwrap();
        ml_writer.close().await.unwrap();

        let snapshot = Snapshot::builder()
            .with_snapshot_id(snap_id)
            .with_sequence_number(seq_num)
            .with_timestamp_ms(1_700_000_000_000)
            .with_manifest_list(ml_path)
            .with_summary(Summary {
                operation: Operation::Overwrite,
                additional_properties: HashMap::new(),
            })
            .with_schema_id(base.metadata().current_schema_id())
            .build();

        apply_action_commit(base, vec![
            TableUpdate::AddSnapshot { snapshot },
            TableUpdate::SetSnapshotRef {
                ref_name: MAIN_BRANCH.to_string(),
                reference: SnapshotReference::new(
                    snap_id,
                    SnapshotRetention::branch(None, None, None),
                ),
            },
        ])
    }

    /// DELETE manifests must be carried forward unchanged into the resulting manifest list.
    #[tokio::test]
    async fn test_delete_manifest_passthrough() {
        let base = make_v2_minimal_table();
        let table = make_table_with_delete_manifest(base).await;

        let tx = Transaction::new(&table);
        let action = tx.rewrite_manifests();
        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();

        let new_snapshot = if let TableUpdate::AddSnapshot { snapshot } = &updates[0] {
            snapshot
        } else {
            panic!("expected AddSnapshot");
        };

        let manifest_list = new_snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();

        let delete_manifests: Vec<&ManifestFile> = manifest_list
            .entries()
            .iter()
            .filter(|mf| mf.content == ManifestContentType::Deletes)
            .collect();

        assert_eq!(
            delete_manifests.len(),
            1,
            "the DELETE manifest must be carried forward into the new snapshot"
        );
    }

    /// Calling rewrite_manifests twice on the same table state must produce identical
    /// manifest file paths (same counter → partition assignment both times).
    #[tokio::test]
    async fn test_rewrite_manifests_is_deterministic() {
        let table = make_v2_minimal_table();

        // Append four files across three partitions in a single call so they share
        // one cross-partition manifest.
        let table = fast_append(table, vec![
            make_data_file("test/p100-a.parquet", 100),
            make_data_file("test/p200-b.parquet", 200),
            make_data_file("test/p300-c.parquet", 300),
            make_data_file("test/p100-d.parquet", 100),
        ])
        .await;

        // Helper: run rewrite_manifests and collect the resulting manifest paths in order.
        let collect_paths = |table: &crate::table::Table| {
            let table = table.clone();
            async move {
                let tx = Transaction::new(&table);
                let action = tx.rewrite_manifests();
                let mut commit = Arc::new(action).commit(&table).await.unwrap();
                let updates = commit.take_updates();
                let snapshot = if let TableUpdate::AddSnapshot { snapshot } = &updates[0] {
                    snapshot.clone()
                } else {
                    panic!("expected AddSnapshot");
                };
                let ml = snapshot
                    .load_manifest_list(table.file_io(), table.metadata())
                    .await
                    .unwrap();
                ml.entries()
                    .iter()
                    .map(|mf| mf.manifest_path.clone())
                    .collect::<Vec<_>>()
            }
        };

        let paths_first = collect_paths(&table).await;
        let paths_second = collect_paths(&table).await;

        // The UUIDs will differ (new commit each time) but the counter suffix must match
        // the same partition order in both runs. Strip everything up to "-rewrite-" and
        // compare the stable suffix ("-rewrite-m<N>.avro").
        let rewrite_suffix = |path: &str| -> String {
            // Path looks like: .../metadata/<uuid>-rewrite-m<N>.avro
            path.find("-rewrite-")
                .map(|i| path[i..].to_string())
                .unwrap_or_else(|| path.to_string())
        };

        let suffixes_first: Vec<_> = paths_first.iter().map(|p| rewrite_suffix(p)).collect();
        let suffixes_second: Vec<_> = paths_second.iter().map(|p| rewrite_suffix(p)).collect();

        assert_eq!(
            suffixes_first, suffixes_second,
            "rewrite_manifests must assign the same counter to the same partition on each run; \
             first={paths_first:?} second={paths_second:?}"
        );
    }
}
