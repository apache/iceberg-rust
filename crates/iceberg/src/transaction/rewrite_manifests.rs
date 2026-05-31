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
use uuid::Uuid;

use crate::error::Result;
use crate::spec::{
    DataFileFormat, FormatVersion, MAIN_BRANCH, ManifestContentType, ManifestFile,
    ManifestListWriter, ManifestWriter, ManifestWriterBuilder, Operation, Snapshot,
    SnapshotReference, SnapshotRetention, Struct, Summary,
};
use crate::table::Table;
use crate::transaction::{ActionCommit, TransactionAction};
use crate::{Error, ErrorKind, TableRequirement, TableUpdate};

const META_ROOT_PATH: &str = "metadata";
const DEFAULT_TARGET_MANIFEST_SIZE_BYTES: u64 = 8 * 1024 * 1024;

/// Rewrite the data manifests of a table's current snapshot.
///
/// Mirrors Java's `RewriteManifests`: groups live entries from existing data
/// manifests bound to the current default partition spec by partition tuple
/// and writes them into a smaller number of larger manifests, while
/// preserving every entry's data file, snapshot id, and data/file sequence
/// numbers (and, on v3, row lineage). Delete manifests and data manifests
/// bound to non-default partition specs are kept unchanged. Produces a
/// `Replace` snapshot.
pub struct RewriteManifestsAction {
    target_size_bytes: Option<u64>,
    snapshot_properties: HashMap<String, String>,
    commit_uuid: Option<Uuid>,
    key_metadata: Option<Vec<u8>>,
}

impl RewriteManifestsAction {
    pub(crate) fn new() -> Self {
        Self {
            target_size_bytes: None,
            snapshot_properties: HashMap::new(),
            commit_uuid: None,
            key_metadata: None,
        }
    }

    /// Override the target manifest file size in bytes used to roll a new
    /// manifest. Defaults to 8 MiB to match the Java
    /// `commit.manifest.target-size-bytes` default.
    pub fn with_target_size_bytes(mut self, target_size_bytes: u64) -> Self {
        self.target_size_bytes = Some(target_size_bytes);
        self
    }

    /// Set the snapshot summary properties.
    pub fn set_snapshot_properties(mut self, snapshot_properties: HashMap<String, String>) -> Self {
        self.snapshot_properties = snapshot_properties;
        self
    }

    /// Set commit UUID for the snapshot.
    pub fn set_commit_uuid(mut self, commit_uuid: Uuid) -> Self {
        self.commit_uuid = Some(commit_uuid);
        self
    }

    /// Set encryption key metadata for newly written manifest files.
    pub fn set_key_metadata(mut self, key_metadata: Vec<u8>) -> Self {
        self.key_metadata = Some(key_metadata);
        self
    }
}

#[async_trait]
impl TransactionAction for RewriteManifestsAction {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        let metadata = table.metadata();
        let Some(current_snapshot) = metadata.current_snapshot() else {
            return Err(Error::new(
                ErrorKind::PreconditionFailed,
                "RewriteManifests requires the table to have a current snapshot",
            ));
        };

        let target_size_bytes = self
            .target_size_bytes
            .unwrap_or(DEFAULT_TARGET_MANIFEST_SIZE_BYTES);
        let default_spec_id = metadata.default_partition_spec_id();
        let format_version = metadata.format_version();

        let manifest_list = current_snapshot
            .load_manifest_list(table.file_io(), &table.metadata_ref())
            .await?;

        let mut kept: Vec<ManifestFile> = Vec::new();
        let mut to_rewrite: Vec<ManifestFile> = Vec::new();
        for manifest in manifest_list.entries() {
            let is_data = manifest.content == ManifestContentType::Data;
            let on_default_spec = manifest.partition_spec_id == default_spec_id;
            let has_live = manifest.has_added_files() || manifest.has_existing_files();
            if is_data && on_default_spec && has_live {
                to_rewrite.push(manifest.clone());
            } else {
                kept.push(manifest.clone());
            }
        }

        let single_below_target =
            to_rewrite.len() == 1 && (to_rewrite[0].manifest_length as u64) <= target_size_bytes;
        if to_rewrite.is_empty() || single_below_target {
            return Ok(ActionCommit::new(vec![], vec![]));
        }

        let mut replaced_active_files: u64 = 0;
        for m in &to_rewrite {
            replaced_active_files += m.added_files_count.unwrap_or(0) as u64;
            replaced_active_files += m.existing_files_count.unwrap_or(0) as u64;
        }

        let commit_uuid = self.commit_uuid.unwrap_or_else(Uuid::now_v7);
        let snapshot_id = generate_unique_snapshot_id(table);

        let mut grouped: Vec<Vec<crate::spec::ManifestEntry>> = Vec::new();
        let mut group_index: HashMap<Struct, usize> = HashMap::new();
        let mut entries_processed: u64 = 0;

        for manifest_file in &to_rewrite {
            let manifest = manifest_file.load_manifest(table.file_io()).await?;
            for entry in manifest.entries() {
                if !entry.is_alive() {
                    continue;
                }
                let key = entry.data_file().partition.clone();
                let idx = match group_index.get(&key) {
                    Some(&i) => i,
                    None => {
                        let i = grouped.len();
                        group_index.insert(key, i);
                        grouped.push(Vec::new());
                        i
                    }
                };
                grouped[idx].push((**entry).clone());
                entries_processed += 1;
            }
        }

        if entries_processed != replaced_active_files {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Manifest list active file count ({replaced_active_files}) disagrees with manifest entry count ({entries_processed})"
                ),
            ));
        }

        for group in &mut grouped {
            group.sort_by(|a, b| a.data_file().file_path.cmp(&b.data_file().file_path));
        }

        let mut counter: u64 = 0;
        let mut new_manifests: Vec<ManifestFile> = Vec::new();

        for group in grouped {
            let mut writer = new_manifest_writer(
                table,
                &commit_uuid,
                &mut counter,
                self.key_metadata.clone(),
                snapshot_id,
            )?;
            let mut accumulated: u64 = 0;
            let mut min_first_row_id: Option<u64> = None;

            for entry in group {
                let entry_proxy_size = entry.data_file().file_size_in_bytes;
                if accumulated > 0
                    && accumulated.saturating_add(entry_proxy_size) > target_size_bytes
                {
                    let mut written = writer.write_manifest_file().await?;
                    if format_version == FormatVersion::V3 {
                        written.first_row_id = min_first_row_id;
                    }
                    new_manifests.push(written);
                    writer = new_manifest_writer(
                        table,
                        &commit_uuid,
                        &mut counter,
                        self.key_metadata.clone(),
                        snapshot_id,
                    )?;
                    accumulated = 0;
                    min_first_row_id = None;
                }
                if let Some(frid) = entry.data_file().first_row_id
                    && frid >= 0
                {
                    let frid_u = frid as u64;
                    min_first_row_id = Some(min_first_row_id.map_or(frid_u, |m| m.min(frid_u)));
                }
                let snap_id = entry.snapshot_id().ok_or_else(|| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        "Live manifest entry is missing snapshot_id",
                    )
                })?;
                let seq = entry.sequence_number().ok_or_else(|| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        "Live manifest entry is missing sequence_number",
                    )
                })?;
                let file_seq = entry.file_sequence_number;
                let data_file = entry.data_file().clone();
                writer.add_existing_file(data_file, snap_id, seq, file_seq)?;
                accumulated = accumulated.saturating_add(entry_proxy_size);
            }
            let mut written = writer.write_manifest_file().await?;
            if format_version == FormatVersion::V3 {
                written.first_row_id = min_first_row_id;
            }
            new_manifests.push(written);
        }

        let new_active_files: u64 = new_manifests
            .iter()
            .map(|m| {
                m.added_files_count.unwrap_or(0) as u64 + m.existing_files_count.unwrap_or(0) as u64
            })
            .sum();
        if new_active_files != replaced_active_files {
            return Err(Error::new(
                ErrorKind::Unexpected,
                format!(
                    "Rewrite produced {new_active_files} active files, expected {replaced_active_files}"
                ),
            ));
        }

        let manifest_list_path = generate_manifest_list_file_path(table, snapshot_id, commit_uuid);
        let next_seq_num = metadata.next_sequence_number();
        let next_row_id = metadata.next_row_id();
        let mut list_writer = match format_version {
            FormatVersion::V1 => ManifestListWriter::v1(
                table.file_io().new_output(manifest_list_path.clone())?,
                snapshot_id,
                metadata.current_snapshot_id(),
            ),
            FormatVersion::V2 => ManifestListWriter::v2(
                table.file_io().new_output(manifest_list_path.clone())?,
                snapshot_id,
                metadata.current_snapshot_id(),
                next_seq_num,
            ),
            FormatVersion::V3 => ManifestListWriter::v3(
                table.file_io().new_output(manifest_list_path.clone())?,
                snapshot_id,
                metadata.current_snapshot_id(),
                next_seq_num,
                Some(next_row_id),
            ),
        };
        let manifests_created = new_manifests.len();
        let manifests_replaced = to_rewrite.len();
        let manifests_kept = kept.len();
        list_writer.add_manifests(new_manifests.into_iter().chain(kept.into_iter()))?;
        list_writer.close().await?;

        let mut additional_properties = HashMap::new();
        for k in [
            "total-data-files",
            "total-delete-files",
            "total-records",
            "total-files-size",
            "total-position-deletes",
            "total-equality-deletes",
        ] {
            if let Some(v) = current_snapshot.summary().additional_properties.get(k) {
                additional_properties.insert(k.to_string(), v.clone());
            }
        }
        additional_properties.insert(
            "manifests-created".to_string(),
            manifests_created.to_string(),
        );
        additional_properties.insert(
            "manifests-replaced".to_string(),
            manifests_replaced.to_string(),
        );
        additional_properties.insert("manifests-kept".to_string(), manifests_kept.to_string());
        additional_properties.insert(
            "entries-processed".to_string(),
            entries_processed.to_string(),
        );
        for (k, v) in &self.snapshot_properties {
            additional_properties.insert(k.clone(), v.clone());
        }
        let summary = Summary {
            operation: Operation::Replace,
            additional_properties,
        };

        let commit_ts = chrono::Utc::now().timestamp_millis();
        let snapshot_builder = Snapshot::builder()
            .with_manifest_list(manifest_list_path)
            .with_snapshot_id(snapshot_id)
            .with_parent_snapshot_id(metadata.current_snapshot_id())
            .with_sequence_number(next_seq_num)
            .with_summary(summary)
            .with_schema_id(metadata.current_schema_id())
            .with_timestamp_ms(commit_ts);
        let new_snapshot = match format_version {
            FormatVersion::V3 => snapshot_builder.with_row_range(next_row_id, 0).build(),
            _ => snapshot_builder.build(),
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
                uuid: metadata.uuid(),
            },
            TableRequirement::RefSnapshotIdMatch {
                r#ref: MAIN_BRANCH.to_string(),
                snapshot_id: metadata.current_snapshot_id(),
            },
        ];
        Ok(ActionCommit::new(updates, requirements))
    }
}

fn new_manifest_writer(
    table: &Table,
    commit_uuid: &Uuid,
    counter: &mut u64,
    key_metadata: Option<Vec<u8>>,
    snapshot_id: i64,
) -> Result<ManifestWriter> {
    let n = *counter;
    *counter += 1;
    let path = format!(
        "{}/{}/{}-m{}.{}",
        table.metadata().location(),
        META_ROOT_PATH,
        commit_uuid,
        n,
        DataFileFormat::Avro
    );
    let output = table.file_io().new_output(path)?;
    let builder = ManifestWriterBuilder::new(
        output,
        Some(snapshot_id),
        key_metadata,
        table.metadata().current_schema().clone(),
        table.metadata().default_partition_spec().as_ref().clone(),
    );
    Ok(match table.metadata().format_version() {
        FormatVersion::V1 => builder.build_v1(),
        FormatVersion::V2 => builder.build_v2_data(),
        FormatVersion::V3 => builder.build_v3_data(),
    })
}

fn generate_manifest_list_file_path(table: &Table, snapshot_id: i64, commit_uuid: Uuid) -> String {
    format!(
        "{}/{}/snap-{}-{}-{}.{}",
        table.metadata().location(),
        META_ROOT_PATH,
        snapshot_id,
        0,
        commit_uuid,
        DataFileFormat::Avro
    )
}

fn generate_unique_snapshot_id(table: &Table) -> i64 {
    let gen_id = || -> i64 {
        let (lhs, rhs) = Uuid::new_v4().as_u64_pair();
        let id = (lhs ^ rhs) as i64;
        if id < 0 { -id } else { id }
    };
    let mut id = gen_id();
    while table.metadata().snapshots().any(|s| s.snapshot_id() == id) {
        id = gen_id();
    }
    id
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use crate::memory::tests::new_memory_catalog;
    use crate::spec::{
        DataContentType, DataFile, DataFileBuilder, DataFileFormat, Literal, Operation, Struct,
    };
    use crate::table::Table;
    use crate::transaction::tests::{make_v2_minimal_table, make_v3_minimal_table_in_catalog};
    use crate::transaction::{ApplyTransactionAction, Transaction, TransactionAction};
    use crate::{Catalog, TableUpdate};

    fn data_file(name: &str, partition: i64, size: u64, records: u64) -> DataFile {
        DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(format!("test/{name}.parquet"))
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(size)
            .record_count(records)
            .partition_spec_id(0)
            .partition(Struct::from_iter([Some(Literal::long(partition))]))
            .build()
            .unwrap()
    }

    async fn append_one(catalog: &impl Catalog, table: Table, file: DataFile) -> Table {
        let tx = Transaction::new(&table);
        tx.fast_append()
            .add_data_files(vec![file])
            .apply(tx)
            .unwrap()
            .commit(catalog)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn test_no_current_snapshot_errors() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);
        let action = tx.rewrite_manifests();
        let res = Arc::new(action).commit(&table).await;
        assert!(res.is_err(), "expected PreconditionFailed without snapshot");
    }

    #[tokio::test]
    async fn test_single_small_manifest_is_noop() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let table = append_one(&catalog, table, data_file("a", 1, 100, 1)).await;
        let original_snapshot_id = table.metadata().current_snapshot_id();

        let tx = Transaction::new(&table);
        let action = tx.rewrite_manifests();
        let mut commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = commit.take_updates();
        assert!(updates.is_empty(), "single small manifest should be no-op");

        let table = tx
            .rewrite_manifests()
            .apply(Transaction::new(&table))
            .unwrap()
            .commit(&catalog)
            .await
            .unwrap();
        assert_eq!(
            table.metadata().current_snapshot_id(),
            original_snapshot_id,
            "no-op should not change the current snapshot"
        );
    }

    #[tokio::test]
    async fn test_multi_manifest_merge_v2_preserves_sequence_numbers() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        let table = append_one(&catalog, table, data_file("a", 1, 1_000, 10)).await;
        let seq_a = table
            .metadata()
            .current_snapshot()
            .unwrap()
            .sequence_number();
        let table = append_one(&catalog, table, data_file("b", 1, 2_000, 20)).await;
        let seq_b = table
            .metadata()
            .current_snapshot()
            .unwrap()
            .sequence_number();
        let table = append_one(&catalog, table, data_file("c", 2, 3_000, 30)).await;
        let seq_c = table
            .metadata()
            .current_snapshot()
            .unwrap()
            .sequence_number();
        assert!(seq_a < seq_b && seq_b < seq_c);

        let pre_manifest_count = table
            .metadata()
            .current_snapshot()
            .unwrap()
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap()
            .entries()
            .len();
        assert_eq!(pre_manifest_count, 3);

        let tx = Transaction::new(&table);
        let table = tx
            .rewrite_manifests()
            .apply(tx)
            .unwrap()
            .commit(&catalog)
            .await
            .unwrap();

        let snapshot = table.metadata().current_snapshot().unwrap();
        assert_eq!(snapshot.summary().operation, Operation::Replace);

        let post_list = snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();
        let total_entries: usize = {
            let mut n = 0;
            for m in post_list.entries() {
                let manifest = m.load_manifest(table.file_io()).await.unwrap();
                n += manifest.entries().len();
            }
            n
        };
        assert_eq!(total_entries, 3, "all entries preserved across rewrite");

        let mut seen_seqs: Vec<i64> = Vec::new();
        for m in post_list.entries() {
            let manifest = m.load_manifest(table.file_io()).await.unwrap();
            for entry in manifest.entries() {
                seen_seqs.push(entry.sequence_number().unwrap());
            }
        }
        seen_seqs.sort();
        assert_eq!(seen_seqs, vec![seq_a, seq_b, seq_c]);

        assert!(post_list.entries().len() < pre_manifest_count);

        let summary = &snapshot.summary().additional_properties;
        assert_eq!(summary.get("total-records").unwrap(), "60");
        assert_eq!(summary.get("total-data-files").unwrap(), "3");
        assert_eq!(summary.get("entries-processed").unwrap(), "3");
        assert_eq!(summary.get("manifests-replaced").unwrap(), "3");
    }

    #[tokio::test]
    async fn test_target_size_rolls_multiple_manifests() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let mut t = table;
        for i in 0..6 {
            t = append_one(&catalog, t, data_file(&format!("f{i}"), 1, 10_000, 1)).await;
        }

        let tx = Transaction::new(&t);
        let t = tx
            .rewrite_manifests()
            .with_target_size_bytes(15_000)
            .apply(tx)
            .unwrap()
            .commit(&catalog)
            .await
            .unwrap();

        let post_list = t
            .metadata()
            .current_snapshot()
            .unwrap()
            .load_manifest_list(t.file_io(), t.metadata())
            .await
            .unwrap();
        assert!(
            post_list.entries().len() > 1,
            "rolling should produce multiple manifests when target is small"
        );

        let mut total = 0;
        for m in post_list.entries() {
            let manifest = m.load_manifest(t.file_io()).await.unwrap();
            total += manifest.entries().len();
        }
        assert_eq!(total, 6);
    }

    #[tokio::test]
    async fn test_v3_row_lineage_preserved() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let table = append_one(&catalog, table, data_file("a", 1, 100, 30)).await;
        let table = append_one(&catalog, table, data_file("b", 1, 100, 17)).await;
        let table = append_one(&catalog, table, data_file("c", 1, 100, 11)).await;

        let pre_first_row_ids: Vec<Option<i64>> = {
            let list = table
                .metadata()
                .current_snapshot()
                .unwrap()
                .load_manifest_list(table.file_io(), table.metadata())
                .await
                .unwrap();
            let mut v = Vec::new();
            for m in list.entries() {
                let manifest = m.load_manifest(table.file_io()).await.unwrap();
                for entry in manifest.entries() {
                    v.push(entry.data_file().first_row_id);
                }
            }
            v.sort();
            v
        };

        let next_row_id_before = table.metadata().next_row_id();

        let tx = Transaction::new(&table);
        let table = tx
            .rewrite_manifests()
            .apply(tx)
            .unwrap()
            .commit(&catalog)
            .await
            .unwrap();

        assert_eq!(
            table.metadata().next_row_id(),
            next_row_id_before,
            "rewrite must not consume new row ids"
        );
        let snap = table.metadata().current_snapshot().unwrap();
        assert_eq!(snap.row_range(), Some((next_row_id_before, 0)));

        let post_first_row_ids: Vec<Option<i64>> = {
            let list = snap
                .load_manifest_list(table.file_io(), table.metadata())
                .await
                .unwrap();
            let mut v = Vec::new();
            for m in list.entries() {
                let manifest = m.load_manifest(table.file_io()).await.unwrap();
                for entry in manifest.entries() {
                    v.push(entry.data_file().first_row_id);
                }
            }
            v.sort();
            v
        };
        assert_eq!(pre_first_row_ids, post_first_row_ids);
    }

    #[tokio::test]
    async fn test_summary_and_replace_operation() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let table = append_one(&catalog, table, data_file("a", 1, 100, 10)).await;
        let table = append_one(&catalog, table, data_file("b", 2, 200, 20)).await;

        let tx = Transaction::new(&table);
        let mut commit = Arc::new(
            tx.rewrite_manifests()
                .set_snapshot_properties(HashMap::from([(
                    "trigger".to_string(),
                    "manual".to_string(),
                )])),
        )
        .commit(&table)
        .await
        .unwrap();
        let updates = commit.take_updates();
        let snap = match &updates[0] {
            TableUpdate::AddSnapshot { snapshot } => snapshot,
            _ => unreachable!(),
        };
        let s = &snap.summary().additional_properties;
        assert_eq!(snap.summary().operation, Operation::Replace);
        assert_eq!(s.get("trigger").unwrap(), "manual");
        assert_eq!(s.get("entries-processed").unwrap(), "2");
        assert_eq!(s.get("manifests-replaced").unwrap(), "2");
        assert_eq!(s.get("manifests-kept").unwrap(), "0");
        // Two distinct partitions → grouped into two new manifests.
        assert_eq!(s.get("manifests-created").unwrap(), "2");
        assert_eq!(s.get("total-records").unwrap(), "30");
    }
}
