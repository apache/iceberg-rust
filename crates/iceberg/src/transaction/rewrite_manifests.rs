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
use futures::TryStreamExt;
use futures::stream::{self, StreamExt};
use uuid::Uuid;

use crate::error::Result;
use crate::spec::{
    DataFile, DataFileFormat, FormatVersion, MAIN_BRANCH, ManifestContentType, ManifestFile,
    ManifestListWriter, ManifestWriter, ManifestWriterBuilder, Operation, Snapshot,
    SnapshotReference, SnapshotRetention, Struct, Summary, TableProperties,
};
use crate::table::Table;
use crate::transaction::snapshot::generate_unique_snapshot_id;
use crate::transaction::{ActionCommit, TransactionAction};
use crate::{Error, ErrorKind, TableRequirement, TableUpdate};

const FALLBACK_BYTES_PER_ENTRY: u64 = 256;

pub struct RewriteManifestsAction {
    target_size_bytes: Option<u64>,
    snapshot_properties: HashMap<String, String>,
}

impl RewriteManifestsAction {
    pub(crate) fn new() -> Self {
        Self {
            target_size_bytes: None,
            snapshot_properties: HashMap::new(),
        }
    }

    pub fn set_target_size_bytes(mut self, target_size_bytes: u64) -> Self {
        self.target_size_bytes = Some(target_size_bytes);
        self
    }

    pub fn set_snapshot_properties(mut self, snapshot_properties: HashMap<String, String>) -> Self {
        self.snapshot_properties = snapshot_properties;
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

        let target_size_bytes = self.target_size_bytes.unwrap_or_else(|| {
            metadata
                .properties()
                .get(TableProperties::PROPERTY_COMMIT_MANIFEST_TARGET_SIZE_BYTES)
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(TableProperties::PROPERTY_COMMIT_MANIFEST_TARGET_SIZE_BYTES_DEFAULT)
        });
        let default_spec_id = metadata.default_partition_spec_id();
        let format_version = metadata.format_version();

        let manifest_list = table.manifest_list_reader(current_snapshot).load().await?;

        let (to_rewrite, kept): (Vec<ManifestFile>, Vec<ManifestFile>) =
            manifest_list.entries().iter().cloned().partition(|m| {
                m.content == ManifestContentType::Data
                    && m.partition_spec_id == default_spec_id
                    && (m.has_added_files() || m.has_existing_files())
            });

        if to_rewrite.is_empty() {
            return Ok(ActionCommit::new(vec![], vec![]));
        }

        let total_size: u64 = to_rewrite
            .iter()
            .map(|m| u64::try_from(m.manifest_length).unwrap_or(0))
            .sum();
        if to_rewrite.len() == 1 && total_size <= target_size_bytes {
            return Ok(ActionCommit::new(vec![], vec![]));
        }
        let total_input_entries: u64 = to_rewrite
            .iter()
            .map(|m| {
                u64::from(m.added_files_count.unwrap_or(0))
                    + u64::from(m.existing_files_count.unwrap_or(0))
            })
            .sum();
        let bytes_per_entry = if total_input_entries > 0 {
            (total_size / total_input_entries).max(1)
        } else {
            FALLBACK_BYTES_PER_ENTRY
        };
        let manifests_replaced = to_rewrite.len();

        let commit_uuid = Uuid::now_v7();
        let snapshot_id = generate_unique_snapshot_id(table);

        let loaded: Vec<_> = stream::iter(to_rewrite)
            .map(|m| {
                let file_io = table.file_io().clone();
                async move { m.load_manifest(&file_io).await }
            })
            .buffer_unordered(16)
            .try_collect()
            .await?;

        let mut grouped: Vec<Vec<(DataFile, i64, i64, Option<i64>)>> = Vec::new();
        let mut group_index: HashMap<Struct, usize> = HashMap::new();
        let mut entries_processed: u64 = 0;

        for manifest in loaded {
            for entry in manifest.entries() {
                if !entry.is_alive() {
                    continue;
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
                let data_file = entry.data_file().clone();
                let idx = match group_index.get(&data_file.partition) {
                    Some(&i) => i,
                    None => {
                        let i = grouped.len();
                        group_index.insert(data_file.partition.clone(), i);
                        grouped.push(Vec::new());
                        i
                    }
                };
                grouped[idx].push((data_file, snap_id, seq, entry.file_sequence_number));
                entries_processed += 1;
            }
        }

        let mut counter: u64 = 0;
        let mut new_manifests: Vec<ManifestFile> = Vec::with_capacity(grouped.len());

        for group in grouped {
            let mut writer = new_manifest_writer(table, &commit_uuid, counter, snapshot_id)?;
            counter += 1;
            let mut accumulated: u64 = 0;
            let mut min_first_row_id: Option<u64> = None;

            for (data_file, snap_id, seq, file_seq) in group {
                if accumulated > 0
                    && accumulated.saturating_add(bytes_per_entry) > target_size_bytes
                {
                    let mut written = writer.write_manifest_file().await?;
                    if format_version == FormatVersion::V3 {
                        written.first_row_id = min_first_row_id;
                    }
                    new_manifests.push(written);
                    writer = new_manifest_writer(table, &commit_uuid, counter, snapshot_id)?;
                    counter += 1;
                    accumulated = 0;
                    min_first_row_id = None;
                }
                if let Some(frid_u) = data_file.first_row_id.and_then(|f| u64::try_from(f).ok()) {
                    min_first_row_id = Some(min_first_row_id.map_or(frid_u, |m| m.min(frid_u)));
                }
                writer.add_existing_file(data_file, snap_id, seq, file_seq)?;
                accumulated = accumulated.saturating_add(bytes_per_entry);
            }
            let mut written = writer.write_manifest_file().await?;
            if format_version == FormatVersion::V3 {
                written.first_row_id = min_first_row_id;
            }
            new_manifests.push(written);
        }

        let manifest_list_path = format!(
            "{}/metadata/snap-{}-0-{}.{}",
            metadata.location(),
            snapshot_id,
            commit_uuid,
            DataFileFormat::Avro,
        );
        let next_seq_num = metadata.next_sequence_number();
        let next_row_id = metadata.next_row_id();
        let output = table.file_io().new_output(manifest_list_path.clone())?;
        let mut list_writer = match format_version {
            FormatVersion::V1 => {
                ManifestListWriter::v1(output, snapshot_id, metadata.current_snapshot_id())
            }
            FormatVersion::V2 => ManifestListWriter::v2(
                output,
                snapshot_id,
                metadata.current_snapshot_id(),
                next_seq_num,
            ),
            FormatVersion::V3 => ManifestListWriter::v3(
                output,
                snapshot_id,
                metadata.current_snapshot_id(),
                next_seq_num,
                Some(next_row_id),
            ),
        };
        let manifests_created = new_manifests.len();
        let manifests_kept = kept.len();
        list_writer.add_manifests(new_manifests.into_iter().chain(kept))?;
        list_writer.close().await?;

        let mut additional_properties: HashMap<String, String> = self.snapshot_properties.clone();
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
    n: u64,
    snapshot_id: i64,
) -> Result<ManifestWriter> {
    let metadata = table.metadata();
    let path = format!(
        "{}/metadata/{}-m{}.{}",
        metadata.location(),
        commit_uuid,
        n,
        DataFileFormat::Avro,
    );
    let output = table.file_io().new_output(path)?;
    let builder = ManifestWriterBuilder::new(
        output,
        Some(snapshot_id),
        None,
        metadata.current_schema().clone(),
        metadata.default_partition_spec().as_ref().clone(),
    );
    Ok(match metadata.format_version() {
        FormatVersion::V1 => builder.build_v1(),
        FormatVersion::V2 => builder.build_v2_data(),
        FormatVersion::V3 => builder.build_v3_data(),
    })
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
        match Arc::new(action).commit(&table).await {
            Ok(_) => panic!("expected error"),
            Err(e) => assert_eq!(e.kind(), crate::ErrorKind::PreconditionFailed),
        }
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
        assert!(updates.is_empty());

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
    async fn test_multi_manifest_merge_preserves_sequence_numbers() {
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
            .manifest_list_reader(table.metadata().current_snapshot().unwrap())
            .load()
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

        let post_list = table.manifest_list_reader(snapshot).load().await.unwrap();
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
    async fn test_target_size_from_table_property() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let mut t = table;
        for i in 0..6 {
            t = append_one(&catalog, t, data_file(&format!("f{i}"), 1, 10_000, 1)).await;
        }

        let tx = Transaction::new(&t);
        let t = tx
            .update_table_properties()
            .set(
                "commit.manifest.target-size-bytes".to_string(),
                "400".to_string(),
            )
            .apply(tx)
            .unwrap()
            .commit(&catalog)
            .await
            .unwrap();

        let tx = Transaction::new(&t);
        let t = tx
            .rewrite_manifests()
            .apply(tx)
            .unwrap()
            .commit(&catalog)
            .await
            .unwrap();

        let post_list = t
            .manifest_list_reader(t.metadata().current_snapshot().unwrap())
            .load()
            .await
            .unwrap();
        assert!(post_list.entries().len() > 1);
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
            .set_target_size_bytes(400)
            .apply(tx)
            .unwrap()
            .commit(&catalog)
            .await
            .unwrap();

        let post_list = t
            .manifest_list_reader(t.metadata().current_snapshot().unwrap())
            .load()
            .await
            .unwrap();
        assert!(post_list.entries().len() > 1);

        let mut total = 0;
        for m in post_list.entries() {
            let manifest = m.load_manifest(t.file_io()).await.unwrap();
            let n = manifest.entries().len();
            assert!(
                n <= 2,
                "each rolled manifest should hold at most ~2 entries when target is just above bytes_per_entry"
            );
            total += n;
        }
        assert_eq!(total, 6);
    }

    #[tokio::test]
    async fn test_oversized_single_manifest_is_split() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let files: Vec<_> = (0..6)
            .map(|i| data_file(&format!("f{i}"), 1, 10_000, 1))
            .collect();

        let tx = Transaction::new(&table);
        let table = tx
            .fast_append()
            .add_data_files(files)
            .apply(tx)
            .unwrap()
            .commit(&catalog)
            .await
            .unwrap();

        let pre_list = table
            .manifest_list_reader(table.metadata().current_snapshot().unwrap())
            .load()
            .await
            .unwrap();
        assert_eq!(pre_list.entries().len(), 1);

        let tx = Transaction::new(&table);
        let table = tx
            .rewrite_manifests()
            .set_target_size_bytes(400)
            .apply(tx)
            .unwrap()
            .commit(&catalog)
            .await
            .unwrap();

        let snap = table.metadata().current_snapshot().unwrap();
        let post_list = table.manifest_list_reader(snap).load().await.unwrap();
        assert!(post_list.entries().len() > 1);
        assert_eq!(
            snap.summary()
                .additional_properties
                .get("manifests-replaced")
                .unwrap(),
            "1"
        );

        let mut total = 0;
        for m in post_list.entries() {
            total += m
                .load_manifest(table.file_io())
                .await
                .unwrap()
                .entries()
                .len();
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

        async fn collect(t: &Table) -> Vec<(String, Option<i64>, Option<i64>, Option<i64>)> {
            let list = t
                .manifest_list_reader(t.metadata().current_snapshot().unwrap())
                .load()
                .await
                .unwrap();
            let mut v = Vec::new();
            for m in list.entries() {
                let manifest = m.load_manifest(t.file_io()).await.unwrap();
                for entry in manifest.entries() {
                    v.push((
                        entry.data_file().file_path().to_string(),
                        entry.data_file().first_row_id,
                        entry.sequence_number(),
                        entry.file_sequence_number,
                    ));
                }
            }
            v.sort();
            v
        }

        let pre = collect(&table).await;
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

        let post = collect(&table).await;
        assert_eq!(
            pre, post,
            "first_row_id, sequence_number, and file_sequence_number must be preserved per-entry"
        );
    }

    #[tokio::test]
    async fn test_summary_and_replace_operation() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let table = append_one(&catalog, table, data_file("a", 1, 100, 10)).await;
        let table = append_one(&catalog, table, data_file("b", 2, 200, 20)).await;

        let pre_total_files_size = table
            .metadata()
            .current_snapshot()
            .unwrap()
            .summary()
            .additional_properties
            .get("total-files-size")
            .cloned();

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
        let TableUpdate::AddSnapshot { snapshot: snap } = &updates[0] else {
            unreachable!()
        };
        let s = &snap.summary().additional_properties;
        assert_eq!(snap.summary().operation, Operation::Replace);
        assert_eq!(s.get("trigger").unwrap(), "manual");
        assert_eq!(s.get("entries-processed").unwrap(), "2");
        assert_eq!(s.get("manifests-replaced").unwrap(), "2");
        assert_eq!(s.get("manifests-kept").unwrap(), "0");
        assert_eq!(s.get("manifests-created").unwrap(), "2");
        assert_eq!(s.get("total-records").unwrap(), "30");
        assert_eq!(s.get("total-files-size").cloned(), pre_total_files_size);
    }

    #[tokio::test]
    async fn test_idempotent_after_consolidation() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let mut t = table;
        for i in 0..3 {
            t = append_one(&catalog, t, data_file(&format!("f{i}"), 1, 100, 1)).await;
        }

        let tx = Transaction::new(&t);
        let t = tx
            .rewrite_manifests()
            .apply(tx)
            .unwrap()
            .commit(&catalog)
            .await
            .unwrap();
        let post_count = t
            .manifest_list_reader(t.metadata().current_snapshot().unwrap())
            .load()
            .await
            .unwrap()
            .entries()
            .len();
        assert_eq!(post_count, 1, "single partition consolidates to 1 manifest");

        let mut commit2 = Arc::new(Transaction::new(&t).rewrite_manifests())
            .commit(&t)
            .await
            .unwrap();
        assert!(
            commit2.take_updates().is_empty(),
            "second rewrite must no-op when input already fits in one manifest"
        );
    }

    #[tokio::test]
    async fn test_partition_grouping_and_catalog_commit() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let table = append_one(&catalog, table, data_file("a", 1, 100, 1)).await;
        let table = append_one(&catalog, table, data_file("b", 1, 100, 1)).await;
        let table = append_one(&catalog, table, data_file("c", 2, 100, 1)).await;
        let table = append_one(&catalog, table, data_file("d", 2, 100, 1)).await;

        let pre_id = table.metadata().current_snapshot_id().unwrap();
        let pre_seq = table
            .metadata()
            .current_snapshot()
            .unwrap()
            .sequence_number();

        let tx = Transaction::new(&table);
        let table = tx
            .rewrite_manifests()
            .apply(tx)
            .unwrap()
            .commit(&catalog)
            .await
            .unwrap();

        let snap = table.metadata().current_snapshot().unwrap();
        assert_eq!(snap.parent_snapshot_id(), Some(pre_id));
        assert_eq!(snap.sequence_number(), pre_seq + 1);
        assert_ne!(snap.snapshot_id(), pre_id);

        let post_list = table.manifest_list_reader(snap).load().await.unwrap();
        assert_eq!(post_list.entries().len(), 2);
        for m in post_list.entries() {
            let manifest = m.load_manifest(table.file_io()).await.unwrap();
            let partitions: std::collections::HashSet<Struct> = manifest
                .entries()
                .iter()
                .map(|e| e.data_file().partition.clone())
                .collect();
            assert_eq!(
                partitions.len(),
                1,
                "all entries within a manifest share one partition tuple"
            );
        }
    }
}
