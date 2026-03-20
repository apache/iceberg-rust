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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use crate::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
    use crate::spec::{
        DataContentType, DataFileBuilder, DataFileFormat, Literal, ManifestStatus, Operation,
        Struct,
    };
    use crate::transaction::tests::make_v3_minimal_table_in_catalog;
    use crate::transaction::{ApplyTransactionAction, Transaction, TransactionAction};
    use crate::{Catalog, CatalogBuilder};

    /// Create a memory catalog that works on all platforms (including Windows)
    /// by using a `memory://` warehouse path instead of a local filesystem path.
    async fn new_test_catalog() -> impl Catalog {
        MemoryCatalogBuilder::default()
            .load(
                "test",
                HashMap::from([(
                    MEMORY_CATALOG_WAREHOUSE.to_string(),
                    "memory://test-warehouse".to_string(),
                )]),
            )
            .await
            .unwrap()
    }

    fn test_data_file(path: &str, partition_spec_id: i32, partition_val: i64) -> crate::spec::DataFile {
        DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(path.to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(partition_spec_id)
            .partition(Struct::from_iter([Some(Literal::long(partition_val))]))
            .build()
            .unwrap()
    }

    /// Helper: do N fast appends with one file each, returning the updated table.
    async fn append_n_files(
        catalog: &impl Catalog,
        table: crate::table::Table,
        n: usize,
    ) -> crate::table::Table {
        let spec_id = table.metadata().default_partition_spec_id();
        let mut table = table;
        for i in 0..n {
            let file = test_data_file(
                &format!("test/file_{i}.parquet"),
                spec_id,
                (i as i64) * 100,
            );
            let tx = Transaction::new(&table);
            let action = tx.fast_append().add_data_files(vec![file]);
            let tx = action.apply(tx).unwrap();
            table = tx.commit(catalog).await.unwrap();
        }
        table
    }

    /// Helper: count manifests in the current snapshot.
    async fn count_manifests(table: &crate::table::Table) -> usize {
        let snapshot = table.metadata().current_snapshot().unwrap();
        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();
        manifest_list.entries().len()
    }

    /// Helper: collect all alive (file_path, status, snapshot_id, sequence_number) from current snapshot.
    async fn collect_entries(
        table: &crate::table::Table,
    ) -> Vec<(String, ManifestStatus, Option<i64>, Option<i64>)> {
        let snapshot = table.metadata().current_snapshot().unwrap();
        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();
        let mut entries = Vec::new();
        for mf in manifest_list.entries() {
            let manifest = mf.load_manifest(table.file_io()).await.unwrap();
            for entry in manifest.entries() {
                entries.push((
                    entry.file_path().to_string(),
                    entry.status(),
                    entry.snapshot_id(),
                    entry.sequence_number(),
                ));
            }
        }
        entries
    }

    #[tokio::test]
    async fn test_rewrite_manifests_no_snapshot() {
        // Compacting a table with no snapshot should error.
        let catalog = new_test_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        let tx = Transaction::new(&table);
        let action = tx.rewrite_manifests();
        let result = Arc::new(action).commit(&table).await;
        assert!(result.is_err());
        let err = result.err().unwrap();
        assert!(
            err.to_string().contains("no current snapshot"),
            "Expected 'no current snapshot' error, got: {err}"
        );
    }

    #[tokio::test]
    async fn test_rewrite_manifests_basic_compaction() {
        // Multiple appends create multiple manifests; compaction merges them.
        let catalog = new_test_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        // 3 appends → 3 manifests
        let table = append_n_files(&catalog, table, 3).await;
        assert_eq!(count_manifests(&table).await, 3);

        // Compact with a large min_size so all manifests are considered "small"
        let tx = Transaction::new(&table);
        let action = tx
            .rewrite_manifests()
            .set_min_manifest_size_bytes(u64::MAX);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        // Should now have fewer manifests (1 group → 1 output manifest)
        assert_eq!(count_manifests(&table).await, 1);

        // All 3 files should still be alive
        let entries = collect_entries(&table).await;
        assert_eq!(entries.len(), 3);
        for (path, status, _, _) in &entries {
            assert_eq!(*status, ManifestStatus::Existing, "entry {path}");
        }

        // Operation should be Replace
        let snapshot = table.metadata().current_snapshot().unwrap();
        assert_eq!(snapshot.summary().operation, Operation::Replace);
    }

    #[tokio::test]
    async fn test_rewrite_manifests_preserves_entry_metadata() {
        // Verify that snapshot_id, sequence_number are preserved through compaction.
        let catalog = new_test_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        // Append files in separate commits to get distinct snapshot/sequence numbers
        let table = append_n_files(&catalog, table, 2).await;
        let entries_before = collect_entries(&table).await;

        // Compact — large min_size forces all manifests into compaction
        let tx = Transaction::new(&table);
        let action = tx
            .rewrite_manifests()
            .set_min_manifest_size_bytes(u64::MAX);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        let entries_after = collect_entries(&table).await;

        // Same number of entries
        assert_eq!(entries_before.len(), entries_after.len());

        // For each file, the snapshot_id and sequence_number should be preserved
        for (path, _, snap_before, seq_before) in &entries_before {
            let after = entries_after
                .iter()
                .find(|(p, _, _, _)| p == path)
                .unwrap_or_else(|| panic!("File {path} missing after compaction"));
            assert_eq!(
                snap_before, &after.2,
                "snapshot_id mismatch for {path}"
            );
            assert_eq!(
                seq_before, &after.3,
                "sequence_number mismatch for {path}"
            );
        }
    }

    #[tokio::test]
    async fn test_rewrite_manifests_skips_large_manifests() {
        // Manifests at or above min_manifest_size_bytes should be kept as-is.
        let catalog = new_test_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        // 3 appends → 3 manifests
        let table = append_n_files(&catalog, table, 3).await;
        assert_eq!(count_manifests(&table).await, 3);

        // Set min_size=0 so all manifests have length >= 0, hence "large" and skipped
        let tx = Transaction::new(&table);
        let action = tx
            .rewrite_manifests()
            .set_min_manifest_size_bytes(0);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        // All manifests kept → still 3
        assert_eq!(count_manifests(&table).await, 3);
    }

    #[tokio::test]
    async fn test_rewrite_manifests_deleted_entries_filtered() {
        // After a rewrite that marks files as deleted, compaction should drop them.
        let catalog = new_test_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let spec_id = table.metadata().default_partition_spec_id();

        // Append a file
        let original = test_data_file("test/original.parquet", spec_id, 100);
        let tx = Transaction::new(&table);
        let action = tx.fast_append().add_data_files(vec![original.clone()]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        // Rewrite: delete original, add replacement
        let replacement = test_data_file("test/replacement.parquet", spec_id, 200);
        let tx = Transaction::new(&table);
        let action = tx
            .rewrite()
            .add_data_files(vec![replacement.clone()])
            .delete_data_files(vec![original.clone()]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        // Should have entries including a deleted one
        let entries_before = collect_entries(&table).await;
        assert!(
            entries_before
                .iter()
                .any(|(p, s, _, _)| p == "test/original.parquet" && *s == ManifestStatus::Deleted)
        );

        // Compact — large min_size forces all manifests into compaction
        let tx = Transaction::new(&table);
        let action = tx
            .rewrite_manifests()
            .set_min_manifest_size_bytes(u64::MAX);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        // After compaction, deleted entries should be gone
        let entries_after = collect_entries(&table).await;
        assert!(
            !entries_after
                .iter()
                .any(|(p, _, _, _)| p == "test/original.parquet"),
            "Deleted file should not survive compaction, entries: {entries_after:?}"
        );

        // Replacement should still be alive
        assert!(
            entries_after
                .iter()
                .any(|(p, s, _, _)| p == "test/replacement.parquet"
                    && *s == ManifestStatus::Existing),
            "Replacement file should be Existing after compaction, entries: {entries_after:?}"
        );
    }

    #[tokio::test]
    async fn test_rewrite_manifests_rolling_writer() {
        // With a very small target size, the rolling writer should produce
        // multiple output manifests.
        let catalog = new_test_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        // 5 appends → 5 manifests
        let table = append_n_files(&catalog, table, 5).await;
        assert_eq!(count_manifests(&table).await, 5);

        // Compact with large min_size to force compaction, target_size=1 to roll per entry
        let tx = Transaction::new(&table);
        let action = tx
            .rewrite_manifests()
            .set_min_manifest_size_bytes(u64::MAX)
            .set_target_manifest_size_bytes(1);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        // Should have multiple output manifests (one per entry, so 5)
        let manifest_count = count_manifests(&table).await;
        assert!(
            manifest_count >= 5,
            "Expected at least 5 manifests with target_size=1, got {manifest_count}"
        );

        // All files should still be present and alive
        let entries = collect_entries(&table).await;
        assert_eq!(entries.len(), 5);
    }

    #[tokio::test]
    async fn test_rewrite_manifests_entries_sorted_by_partition() {
        // After compaction, entries should be sorted by partition value.
        let catalog = new_test_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let spec_id = table.metadata().default_partition_spec_id();

        // Append files with decreasing partition values
        let files = vec![
            test_data_file("test/c.parquet", spec_id, 300),
            test_data_file("test/a.parquet", spec_id, 100),
            test_data_file("test/b.parquet", spec_id, 200),
        ];
        let mut table = table;
        for file in files {
            let tx = Transaction::new(&table);
            let action = tx.fast_append().add_data_files(vec![file]);
            let tx = action.apply(tx).unwrap();
            table = tx.commit(&catalog).await.unwrap();
        }

        // Compact — large min_size forces all manifests into compaction
        let tx = Transaction::new(&table);
        let action = tx
            .rewrite_manifests()
            .set_min_manifest_size_bytes(u64::MAX);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        assert_eq!(count_manifests(&table).await, 1);

        // Entries should be sorted by partition value
        let entries = collect_entries(&table).await;
        assert_eq!(entries.len(), 3);
        // The paths should be in partition-value order: a (100), b (200), c (300)
        assert_eq!(entries[0].0, "test/a.parquet");
        assert_eq!(entries[1].0, "test/b.parquet");
        assert_eq!(entries[2].0, "test/c.parquet");
    }

    #[tokio::test]
    async fn test_rewrite_manifests_idempotent() {
        // Compacting twice should produce the same result.
        let catalog = new_test_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        let table = append_n_files(&catalog, table, 3).await;

        // First compaction — large min_size forces all manifests into compaction
        let tx = Transaction::new(&table);
        let action = tx
            .rewrite_manifests()
            .set_min_manifest_size_bytes(u64::MAX);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();
        let entries_after_first = collect_entries(&table).await;

        // Second compaction
        let tx = Transaction::new(&table);
        let action = tx
            .rewrite_manifests()
            .set_min_manifest_size_bytes(u64::MAX);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();
        let entries_after_second = collect_entries(&table).await;

        // Same files, same metadata
        assert_eq!(entries_after_first.len(), entries_after_second.len());
        for (first, second) in entries_after_first.iter().zip(entries_after_second.iter()) {
            assert_eq!(first.0, second.0, "file path mismatch");
            assert_eq!(first.2, second.2, "snapshot_id mismatch");
            assert_eq!(first.3, second.3, "sequence_number mismatch");
        }
    }
}
