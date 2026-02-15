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

use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;

use crate::error::Result;
use crate::table::Table;
use crate::transaction::{ActionCommit, TransactionAction};
use crate::{TableRequirement, TableUpdate};

/// Action that removes old snapshots from table metadata.
///
/// This does NOT delete physical files — it only removes snapshot references
/// from metadata. Use [`find_unreferenced_files`] after committing to identify
/// files that can be safely deleted.
pub struct ExpireSnapshotsAction {
    snapshot_ids: HashSet<i64>,
    older_than_ms: Option<i64>,
    min_snapshots_to_keep: usize,
}

impl ExpireSnapshotsAction {
    pub(crate) fn new() -> Self {
        Self {
            snapshot_ids: HashSet::new(),
            older_than_ms: None,
            min_snapshots_to_keep: 1,
        }
    }

    /// Add explicit snapshot IDs to expire.
    pub fn expire_snapshot_ids(mut self, ids: impl IntoIterator<Item = i64>) -> Self {
        self.snapshot_ids.extend(ids);
        self
    }

    /// Expire snapshots older than this timestamp (milliseconds since epoch).
    pub fn expire_older_than_ms(mut self, timestamp_ms: i64) -> Self {
        self.older_than_ms = Some(timestamp_ms);
        self
    }

    /// Minimum number of snapshots to keep (floor at 1 — current always kept).
    pub fn retain_last(mut self, n: usize) -> Self {
        self.min_snapshots_to_keep = n.max(1);
        self
    }
}

#[async_trait]
impl TransactionAction for ExpireSnapshotsAction {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        let metadata = table.metadata();
        let current_snapshot_id = metadata.current_snapshot_id();

        // Collect all snapshot IDs and timestamps
        let mut all_snapshots: Vec<(i64, i64)> = metadata
            .snapshots()
            .map(|s| (s.snapshot_id(), s.timestamp_ms()))
            .collect();

        // Build candidate set: explicit IDs + age-based
        let mut candidates: HashSet<i64> = self.snapshot_ids.clone();

        if let Some(cutoff_ms) = self.older_than_ms {
            for (id, ts) in &all_snapshots {
                if *ts < cutoff_ms {
                    candidates.insert(*id);
                }
            }
        }

        // Never expire the current snapshot
        if let Some(current_id) = current_snapshot_id {
            candidates.remove(&current_id);
        }

        // Respect min_snapshots_to_keep: protect the N most recent
        all_snapshots.sort_by(|a, b| b.1.cmp(&a.1)); // sort by timestamp desc
        let protected: HashSet<i64> = all_snapshots
            .iter()
            .take(self.min_snapshots_to_keep)
            .map(|(id, _)| *id)
            .collect();
        for id in &protected {
            candidates.remove(id);
        }

        // Filter to IDs that actually exist in metadata
        let existing_ids: HashSet<i64> = metadata.snapshots().map(|s| s.snapshot_id()).collect();
        candidates.retain(|id| existing_ids.contains(id));

        if candidates.is_empty() {
            return Ok(ActionCommit::new(vec![], vec![]));
        }

        let snapshot_ids: Vec<i64> = candidates.into_iter().collect();

        Ok(ActionCommit::new(
            vec![TableUpdate::RemoveSnapshots { snapshot_ids }],
            vec![TableRequirement::UuidMatch {
                uuid: metadata.uuid(),
            }],
        ))
    }
}

/// Files referenced only by expired snapshots that can be safely deleted.
pub struct UnreferencedFiles {
    /// Data file paths (Parquet files) no longer referenced by any live snapshot.
    pub data_files: Vec<String>,
    /// Manifest file paths no longer referenced by any live snapshot.
    pub manifest_files: Vec<String>,
    /// Manifest list file paths no longer referenced by any live snapshot.
    pub manifest_lists: Vec<String>,
}

impl UnreferencedFiles {
    /// Total number of unreferenced files across all categories.
    pub fn total_files(&self) -> usize {
        self.data_files.len() + self.manifest_files.len() + self.manifest_lists.len()
    }

    /// Iterator over all unreferenced file paths.
    pub fn all_paths(&self) -> impl Iterator<Item = &String> {
        self.data_files
            .iter()
            .chain(self.manifest_files.iter())
            .chain(self.manifest_lists.iter())
    }
}

/// Identify files only referenced by expired snapshots (safe to delete).
///
/// Call this BEFORE committing the expire transaction, while the table still
/// has the snapshots that are about to be expired. The returned file sets
/// are the difference: files referenced by expired snapshots minus files
/// referenced by surviving snapshots.
///
/// Data files use alive/deleted deduplication: a file that appears as `Added`
/// in a carried-forward manifest but also as `Deleted` in a newer manifest
/// within the same snapshot is NOT considered alive for that snapshot.
pub async fn find_unreferenced_files(
    table: &Table,
    expired_snapshot_ids: &[i64],
) -> Result<UnreferencedFiles> {
    let metadata = table.metadata();
    let expired_set: HashSet<i64> = expired_snapshot_ids.iter().copied().collect();

    let mut live_data_files = HashSet::new();
    let mut live_manifest_files = HashSet::new();
    let mut live_manifest_lists = HashSet::new();

    let mut expired_data_files = HashSet::new();
    let mut expired_manifest_files = HashSet::new();
    let mut expired_manifest_lists = HashSet::new();

    for snapshot in metadata.snapshots() {
        let is_expired = expired_set.contains(&snapshot.snapshot_id());

        let manifest_list_path = snapshot.manifest_list().to_string();
        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), metadata)
            .await?;

        if is_expired {
            expired_manifest_lists.insert(manifest_list_path);
        } else {
            live_manifest_lists.insert(manifest_list_path);
        }

        // For data files, track alive and deleted separately per snapshot,
        // then compute truly alive = alive - deleted (handles carried-forward
        // manifests where a file is Added in one manifest and Deleted in another).
        let mut snap_alive = HashSet::new();
        let mut snap_deleted = HashSet::new();

        for manifest_file in manifest_list.entries() {
            let mf_path = manifest_file.manifest_path.clone();

            let manifest = manifest_file.load_manifest(table.file_io()).await?;
            for entry in manifest.entries() {
                let df_path = entry.file_path().to_string();
                if entry.is_alive() {
                    snap_alive.insert(df_path);
                } else {
                    snap_deleted.insert(df_path);
                }
            }

            if is_expired {
                expired_manifest_files.insert(mf_path);
            } else {
                live_manifest_files.insert(mf_path);
            }
        }

        // Truly alive = alive entries minus deleted entries in same snapshot
        let truly_alive: HashSet<String> = snap_alive.difference(&snap_deleted).cloned().collect();

        if is_expired {
            expired_data_files.extend(truly_alive);
        } else {
            live_data_files.extend(truly_alive);
        }
    }

    Ok(UnreferencedFiles {
        data_files: expired_data_files
            .difference(&live_data_files)
            .cloned()
            .collect(),
        manifest_files: expired_manifest_files
            .difference(&live_manifest_files)
            .cloned()
            .collect(),
        manifest_lists: expired_manifest_lists
            .difference(&live_manifest_lists)
            .cloned()
            .collect(),
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::{ExpireSnapshotsAction, find_unreferenced_files};
    use crate::memory::tests::new_memory_catalog;
    use crate::spec::{
        DataContentType, DataFile, DataFileBuilder, DataFileFormat, Literal, Struct,
    };
    use crate::transaction::tests::make_v3_minimal_table_in_catalog;
    use crate::transaction::{ApplyTransactionAction, Transaction, TransactionAction};

    fn make_data_file(path: &str, record_count: u64, partition_spec_id: i32) -> DataFile {
        DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(path.to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(record_count)
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .partition_spec_id(partition_spec_id)
            .build()
            .unwrap()
    }

    #[tokio::test]
    async fn test_expire_never_expires_current_snapshot() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let spec_id = table.metadata().default_partition_spec_id();

        // Fast append 3 times to get 3 snapshots
        let mut table = table;
        let mut snapshot_ids = Vec::new();
        for i in 0..3 {
            let file = make_data_file(&format!("test/{i}.parquet"), 10, spec_id);
            let tx = Transaction::new(&table);
            let action = tx.fast_append().add_data_files(vec![file]);
            let tx = action.apply(tx).unwrap();
            table = tx.commit(&catalog).await.unwrap();
            snapshot_ids.push(table.metadata().current_snapshot_id().unwrap());
        }

        assert_eq!(table.metadata().snapshots().len(), 3);
        let current_id = table.metadata().current_snapshot_id().unwrap();

        // Try to expire ALL snapshot IDs (including current)
        let action = Arc::new(
            ExpireSnapshotsAction::new()
                .expire_snapshot_ids(snapshot_ids.clone())
                .retain_last(1),
        );
        let mut commit = action.commit(&table).await.unwrap();
        let updates = commit.take_updates();

        if !updates.is_empty() {
            // Apply to see which survived
            let tx = Transaction::new(&table);
            let expire_action = ExpireSnapshotsAction::new()
                .expire_snapshot_ids(snapshot_ids)
                .retain_last(1);
            let tx = expire_action.apply(tx).unwrap();
            let table = tx.commit(&catalog).await.unwrap();

            // Current snapshot must survive
            assert!(table.metadata().snapshot_by_id(current_id).is_some());
        }
    }

    #[tokio::test]
    async fn test_expire_by_age() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let spec_id = table.metadata().default_partition_spec_id();

        // Append first file
        let file_a = make_data_file("test/a.parquet", 10, spec_id);
        let tx = Transaction::new(&table);
        let action = tx.fast_append().add_data_files(vec![file_a]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();
        let snap1_id = table.metadata().current_snapshot_id().unwrap();
        let snap1_ts = table
            .metadata()
            .snapshot_by_id(snap1_id)
            .unwrap()
            .timestamp_ms();

        // Append second file
        let file_b = make_data_file("test/b.parquet", 10, spec_id);
        let tx = Transaction::new(&table);
        let action = tx.fast_append().add_data_files(vec![file_b]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();
        let snap2_id = table.metadata().current_snapshot_id().unwrap();

        // Expire with cutoff between the two timestamps
        let cutoff = snap1_ts + 1;
        let action = Arc::new(
            ExpireSnapshotsAction::new()
                .expire_older_than_ms(cutoff)
                .retain_last(1),
        );
        let mut commit = action.commit(&table).await.unwrap();
        let updates = commit.take_updates();

        // Should expire snap1 but not snap2 (current)
        if let Some(crate::TableUpdate::RemoveSnapshots { snapshot_ids }) = updates.first() {
            assert!(snapshot_ids.contains(&snap1_id));
            assert!(!snapshot_ids.contains(&snap2_id));
        } else {
            panic!("Expected RemoveSnapshots update");
        }
    }

    #[tokio::test]
    async fn test_expire_respects_retain_last() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let spec_id = table.metadata().default_partition_spec_id();

        // Fast append 4 times
        let mut table = table;
        for i in 0..4 {
            let file = make_data_file(&format!("test/{i}.parquet"), 10, spec_id);
            let tx = Transaction::new(&table);
            let action = tx.fast_append().add_data_files(vec![file]);
            let tx = action.apply(tx).unwrap();
            table = tx.commit(&catalog).await.unwrap();
        }
        assert_eq!(table.metadata().snapshots().len(), 4);

        // Expire with far-future cutoff but retain_last(2)
        let far_future = i64::MAX;
        let action = Arc::new(
            ExpireSnapshotsAction::new()
                .expire_older_than_ms(far_future)
                .retain_last(2),
        );
        let mut commit = action.commit(&table).await.unwrap();
        let updates = commit.take_updates();

        if let Some(crate::TableUpdate::RemoveSnapshots { snapshot_ids }) = updates.first() {
            // Should expire 2 of the 4 snapshots (keeping the 2 most recent)
            assert_eq!(snapshot_ids.len(), 2);
        } else {
            panic!("Expected RemoveSnapshots update");
        }
    }

    #[tokio::test]
    async fn test_expire_no_op_when_nothing_to_expire() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let spec_id = table.metadata().default_partition_spec_id();

        // Single snapshot
        let file = make_data_file("test/a.parquet", 10, spec_id);
        let tx = Transaction::new(&table);
        let action = tx.fast_append().add_data_files(vec![file]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        assert_eq!(table.metadata().snapshots().len(), 1);

        // Try to expire — should be no-op
        let action = Arc::new(
            ExpireSnapshotsAction::new()
                .expire_older_than_ms(i64::MAX)
                .retain_last(1),
        );
        let mut commit = action.commit(&table).await.unwrap();
        let updates = commit.take_updates();
        assert!(updates.is_empty(), "No-op: nothing to expire");
    }

    #[tokio::test]
    async fn test_find_unreferenced_files_all_carried_forward() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let spec_id = table.metadata().default_partition_spec_id();

        // Append file_a
        let file_a = make_data_file("test/a.parquet", 10, spec_id);
        let tx = Transaction::new(&table);
        let action = tx.fast_append().add_data_files(vec![file_a.clone()]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();
        let snap1_id = table.metadata().current_snapshot_id().unwrap();

        // Append file_b (file_a is carried forward)
        let file_b = make_data_file("test/b.parquet", 10, spec_id);
        let tx = Transaction::new(&table);
        let action = tx.fast_append().add_data_files(vec![file_b.clone()]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        // Expire snap1 — file_a is still alive in current snapshot (carried forward),
        // so it should NOT be unreferenced
        let unreferenced = find_unreferenced_files(&table, &[snap1_id]).await.unwrap();

        assert!(
            unreferenced.data_files.is_empty(),
            "All data files are carried forward, none should be unreferenced: {:?}",
            unreferenced.data_files
        );
        // The manifest list from snap1 should be unreferenced (each snapshot has its own)
        assert!(
            !unreferenced.manifest_lists.is_empty(),
            "Expired snapshot's manifest list should be unreferenced"
        );
    }
}
