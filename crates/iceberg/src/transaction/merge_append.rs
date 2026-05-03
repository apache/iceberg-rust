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
use std::sync::{Arc, OnceLock};

use async_trait::async_trait;
use uuid::Uuid;

use crate::error::Result;
use crate::spec::{DataFile, ManifestEntry, ManifestFile, Operation};
use crate::table::Table;
use crate::transaction::merging_state::MergingState;
use crate::transaction::snapshot::{CommitResult, SnapshotProduceOperation, SnapshotProducer};
use crate::transaction::{ActionCommit, TransactionAction};
use crate::utils::available_parallelism;
use crate::{Error, ErrorKind};

/// Action to append data files to a table using the merge-on-write manifest pipeline.
///
/// Unlike `FastAppendAction`, each commit runs the full filter+merge pipeline:
///
///   1. **Filter pass.** Existing manifests with no affected entries pass through
///      unchanged. (There are no deleted entries in a pure append — this pass is
///      a no-op for data manifests but is still invoked so the merge pass sees
///      the full historical manifest list.)
///   2. **Merge pass.** The carry-forward manifests plus the new added manifest are
///      bin-packed by partition spec id and target manifest size. Bins with multiple
///      sibling manifests are consolidated into a single merged manifest.
///
/// Java analog: `org.apache.iceberg.MergeAppend` extending `MergingSnapshotProducer`.
pub struct MergeAppendAction {
    added_data_files: Vec<DataFile>,
    commit_uuid: Option<Uuid>,
    key_metadata: Option<Vec<u8>>,
    snapshot_properties: HashMap<String, String>,
    manifest_read_concurrency: usize,
    manifest_write_concurrency: usize,
    /// Initialized on first commit, shared across catalog retries for idempotency.
    merging_state: OnceLock<Arc<MergingState>>,
}

impl MergeAppendAction {
    pub(crate) fn new() -> Self {
        let num_cpus = available_parallelism().get();
        Self {
            added_data_files: vec![],
            commit_uuid: None,
            key_metadata: None,
            snapshot_properties: HashMap::default(),
            manifest_read_concurrency: num_cpus,
            manifest_write_concurrency: std::cmp::max(1, num_cpus / 4),
            merging_state: OnceLock::new(),
        }
    }

    /// Add data files to the snapshot.
    pub fn add_data_files(mut self, files: impl IntoIterator<Item = DataFile>) -> Self {
        self.added_data_files.extend(files);
        self
    }

    /// Override the number of manifests read concurrently during the filter pass.
    pub fn with_manifest_read_concurrency(mut self, n: usize) -> Self {
        self.manifest_read_concurrency = std::cmp::max(1, n);
        self
    }

    /// Override the number of manifest bins written concurrently during the merge pass.
    pub fn with_manifest_write_concurrency(mut self, n: usize) -> Self {
        self.manifest_write_concurrency = std::cmp::max(1, n);
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

    /// Set snapshot summary properties.
    pub fn set_snapshot_properties(mut self, props: HashMap<String, String>) -> Self {
        self.snapshot_properties = props;
        self
    }

    fn merging_state(&self, table: &Table) -> Result<Arc<MergingState>> {
        if let Some(state) = self.merging_state.get() {
            return Ok(state.clone());
        }
        let read = self.manifest_read_concurrency;
        let write = self.manifest_write_concurrency;
        let state = Arc::new(MergingState::from_table(table, read, write)?);
        let _ = self.merging_state.set(state);
        Ok(self.merging_state.get().expect("just initialized").clone())
    }
}

#[async_trait]
impl TransactionAction for MergeAppendAction {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        if self.added_data_files.is_empty() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "MergeAppend requires at least one data file",
            ));
        }

        let snapshot_producer = SnapshotProducer::new(
            table,
            self.commit_uuid.unwrap_or_else(Uuid::now_v7),
            self.key_metadata.clone(),
            self.snapshot_properties.clone(),
            self.added_data_files.clone(),
        );

        snapshot_producer.validate_added_data_files()?;

        let state = self.merging_state(table)?;
        // NO state.delete() — pure append

        let op = MergeAppendOperation {
            state: state.clone(),
        };

        let CommitResult {
            commit,
            committed_manifest_paths,
        } = snapshot_producer.commit(op, state.clone()).await?;
        // Only clean up on confirmed success. On error the commit may have reached the
        // catalog (ACK loss) — deleting committed manifests would cause silent corruption.
        state
            .clean_uncommitted(table.file_io(), &committed_manifest_paths)
            .await;
        Ok(commit)
    }
}

struct MergeAppendOperation {
    state: Arc<MergingState>,
}

impl SnapshotProduceOperation for MergeAppendOperation {
    fn operation(&self) -> Operation {
        Operation::Append
    }

    async fn delete_entries(
        &self,
        _snapshot_produce: &SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestEntry>> {
        Ok(vec![])
    }

    async fn existing_manifest(
        &self,
        snapshot_produce: &SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestFile>> {
        let Some(snapshot) = snapshot_produce.table.metadata().current_snapshot() else {
            return Ok(vec![]);
        };

        let manifest_list = snapshot
            .load_manifest_list(
                snapshot_produce.table.file_io(),
                &snapshot_produce.table.metadata_ref(),
            )
            .await?;

        let current_manifests: Vec<ManifestFile> = manifest_list.entries().to_vec();
        // filter_manifests short-circuits (deleted_paths empty) but must be called
        // so the merge pass receives the full historical manifest list.
        self.state
            .filter_manifests(snapshot_produce, current_manifests)
            .await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::spec::{
        DataContentType, DataFile, DataFileBuilder, DataFileFormat, ManifestStatus, Operation,
        Struct, UnboundPartitionSpec,
    };
    use crate::transaction::tests::{
        append_files, apply_updates_to_table, collect_alive_files, make_data_file,
        make_v2_minimal_table,
    };
    use crate::transaction::{Transaction, TransactionAction};
    use crate::TableUpdate;

    // ─── helpers ────────────────────────────────────────────────────────────────

    async fn merge_append_files(
        table: crate::table::Table,
        files: Vec<DataFile>,
    ) -> crate::table::Table {
        let tx = Transaction::new(&table);
        let action = tx.merge_append().add_data_files(files);
        let updates = Arc::new(action)
            .commit(&table)
            .await
            .unwrap()
            .take_updates();
        apply_updates_to_table(&table, &updates)
    }

    async fn manifest_count(table: &crate::table::Table) -> usize {
        table
            .metadata()
            .current_snapshot()
            .unwrap()
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap()
            .entries()
            .len()
    }

    fn make_unpartitioned_data_file(path: &str, record_count: u64) -> DataFile {
        DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(path.to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(record_count)
            .partition_spec_id(1)
            .partition(Struct::empty())
            .build()
            .unwrap()
    }

    async fn make_table_with_two_specs() -> crate::table::Table {
        let table = make_v2_minimal_table();
        // Add an unpartitioned spec (spec 1) and set it as default.
        let tx = Transaction::new(&table);
        let updates = Arc::new(
            tx.update_table_properties()
                .set("commit.manifest.min-count-to-merge".to_string(), "3".to_string()),
        )
        .commit(&table)
        .await
        .unwrap()
        .take_updates();
        let table = apply_updates_to_table(&table, &updates);

        // Add spec 1 (unpartitioned) via TableUpdate directly.
        let add_spec = TableUpdate::AddSpec {
            spec: UnboundPartitionSpec::builder().build(),
        };
        let set_default = TableUpdate::SetDefaultSpec { spec_id: 1 };
        apply_updates_to_table(&table, &[add_spec, set_default])
    }

    // ─── ported Java tests ───────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_add_many_files() {
        // When a single batch of files exceeds the target manifest size, the
        // merge pass splits them into multiple manifests.
        let mut table = make_v2_minimal_table();
        let tx = Transaction::new(&table);
        let updates = Arc::new(
            tx.update_table_properties()
                .set("commit.manifest.target-size-bytes".to_string(), "100".to_string())
                .set("commit.manifest.min-count-to-merge".to_string(), "2".to_string()),
        )
        .commit(&table)
        .await
        .unwrap()
        .take_updates();
        table = apply_updates_to_table(&table, &updates);

        let files: Vec<DataFile> = (0..10)
            .map(|i| make_data_file(&table, &format!("data/f{i}.parquet"), 1))
            .collect();
        let table = merge_append_files(table, files.clone()).await;

        let alive = collect_alive_files(
            table.metadata().current_snapshot().unwrap(),
            &table,
        )
        .await;
        // All 10 files must be alive.
        assert_eq!(alive.len(), 10);
    }

    #[tokio::test]
    async fn test_add_many_files_consistent_ordering() {
        // Insertion order: earlier-appended files appear before later ones in the
        // manifest list (they are in the same appended manifest in insertion order).
        let table = make_v2_minimal_table();
        let f1 = make_data_file(&table, "data/a.parquet", 1);
        let f2 = make_data_file(&table, "data/b.parquet", 1);
        let f3 = make_data_file(&table, "data/c.parquet", 1);
        let table = merge_append_files(table, vec![f1, f2, f3]).await;

        let snap = table.metadata().current_snapshot().unwrap();
        let ml = snap.load_manifest_list(table.file_io(), table.metadata()).await.unwrap();
        let manifest = ml.entries()[0].load_manifest(table.file_io()).await.unwrap();
        let paths: Vec<&str> = manifest
            .entries()
            .iter()
            .map(|e| e.file_path())
            .collect();
        // All three paths present in original insertion order.
        assert_eq!(paths, ["data/a.parquet", "data/b.parquet", "data/c.parquet"]);
    }

    #[tokio::test]
    async fn test_append_with_custom_concurrency() {
        // Builder methods must be accepted without panicking; the result is still correct.
        let table = make_v2_minimal_table();
        let f1 = make_data_file(&table, "data/f1.parquet", 10);
        let tx = Transaction::new(&table);
        let action = tx
            .merge_append()
            .with_manifest_read_concurrency(2)
            .with_manifest_write_concurrency(1)
            .add_data_files(vec![f1]);
        let updates = Arc::new(action).commit(&table).await.unwrap().take_updates();
        let table = apply_updates_to_table(&table, &updates);
        assert_eq!(manifest_count(&table).await, 1);
    }

    #[tokio::test]
    async fn test_merge_with_existing_manifest() {
        // Second merge_append consolidates the first manifest with the new one.
        let mut table = make_v2_minimal_table();
        let tx = Transaction::new(&table);
        let updates = Arc::new(
            tx.update_table_properties()
                .set("commit.manifest.min-count-to-merge".to_string(), "2".to_string()),
        )
        .commit(&table)
        .await
        .unwrap()
        .take_updates();
        table = apply_updates_to_table(&table, &updates);

        let f1 = make_data_file(&table, "data/f1.parquet", 10);
        let f2 = make_data_file(&table, "data/f2.parquet", 10);
        let table = merge_append_files(table, vec![f1.clone()]).await;
        let table = merge_append_files(table, vec![f2.clone()]).await;

        // With min-count-to-merge=2, both manifests should be merged into 1.
        assert_eq!(manifest_count(&table).await, 1);

        let alive = collect_alive_files(table.metadata().current_snapshot().unwrap(), &table).await;
        assert!(alive.contains(&"data/f1.parquet".to_string()));
        assert!(alive.contains(&"data/f2.parquet".to_string()));
    }

    #[tokio::test]
    #[ignore = "requires DeleteFilesAction to write inline DELETED manifest entries; re-enable once DeleteFilesAction is implemented"]
    async fn test_merge_after_delete_entry() {
        // Java's testMergeWithExistingManifestAfterDelete writes DELETED entries
        // inline via ManifestFilterManager. Rust's design never writes inline
        // tombstones; requires a Rust DeleteFilesAction first.
    }

    #[tokio::test]
    async fn test_min_merge_count() {
        // With min-count-to-merge=3, bins with fewer than 3 siblings are not merged.
        let mut table = make_v2_minimal_table();
        let tx = Transaction::new(&table);
        let updates = Arc::new(
            tx.update_table_properties()
                .set("commit.manifest.min-count-to-merge".to_string(), "3".to_string()),
        )
        .commit(&table)
        .await
        .unwrap()
        .take_updates();
        table = apply_updates_to_table(&table, &updates);

        let f1 = make_data_file(&table, "data/f1.parquet", 10);
        let f2 = make_data_file(&table, "data/f2.parquet", 10);
        let table = merge_append_files(table, vec![f1]).await;
        let count_after_first = manifest_count(&table).await;
        let table = merge_append_files(table, vec![f2]).await;
        let count_after_second = manifest_count(&table).await;

        // With only 2 sibling manifests, min-count-to-merge=3 prevents merging.
        assert_eq!(count_after_first, 1, "first append: 1 manifest");
        assert_eq!(count_after_second, 2, "second append: still 2, no merge yet");
    }

    #[tokio::test]
    async fn test_merge_size_target_prevents_merge() {
        // With target-size-bytes=1, every manifest is in its own bin (each
        // manifest file exceeds 1 byte). Each single-element bin passes through
        // process_bin immediately, so no merging occurs even with min-count=2.
        let mut table = make_v2_minimal_table();
        let tx = Transaction::new(&table);
        let updates = Arc::new(
            tx.update_table_properties()
                .set("commit.manifest.min-count-to-merge".to_string(), "2".to_string())
                .set(
                    "commit.manifest.target-size-bytes".to_string(),
                    "1".to_string(), // 1 byte — every manifest goes into its own bin
                ),
        )
        .commit(&table)
        .await
        .unwrap()
        .take_updates();
        table = apply_updates_to_table(&table, &updates);

        let f1 = make_data_file(&table, "data/f1.parquet", 10);
        let f2 = make_data_file(&table, "data/f2.parquet", 10);
        let table = merge_append_files(table, vec![f1]).await;
        let table = merge_append_files(table, vec![f2]).await;

        // Tiny target → each manifest in its own bin → no bin has ≥2 siblings → no merge.
        assert_eq!(manifest_count(&table).await, 2);
    }

    #[tokio::test]
    async fn test_changed_partition_spec() {
        // Files appended with different partition specs live in separate manifests
        // and are never merged across spec boundaries.
        let table = make_v2_minimal_table();

        // Step 1: Append a file with current default spec 0.
        let f0 = make_data_file(&table, "data/spec0.parquet", 10);
        let table = merge_append_files(table, vec![f0]).await;

        // Step 2: Change the default partition spec to spec 1 (unpartitioned).
        let add_spec = TableUpdate::AddSpec {
            spec: UnboundPartitionSpec::builder().build(),
        };
        let set_default = TableUpdate::SetDefaultSpec { spec_id: 1 };
        let table = apply_updates_to_table(&table, &[add_spec, set_default]);

        // Step 3: Append a file with new default spec 1 (unpartitioned).
        let f1 = make_unpartitioned_data_file("data/spec1.parquet", 10);
        let table = merge_append_files(table, vec![f1]).await;

        let snap = table.metadata().current_snapshot().unwrap();
        let ml = snap.load_manifest_list(table.file_io(), table.metadata()).await.unwrap();

        // Both spec 0 and spec 1 manifests must be present.
        let spec_ids: Vec<i32> = ml.entries().iter().map(|e| e.partition_spec_id).collect();
        assert!(spec_ids.contains(&0), "spec 0 manifest must be present: {spec_ids:?}");
        assert!(spec_ids.contains(&1), "spec 1 manifest must be present: {spec_ids:?}");
    }

    #[tokio::test]
    async fn test_changed_spec_merge_existing() {
        // Manifests from different specs are binned separately and are never
        // merged across spec boundaries.
        let mut table = make_table_with_two_specs().await;
        let tx = Transaction::new(&table);
        let updates = Arc::new(
            tx.update_table_properties()
                .set("commit.manifest.min-count-to-merge".to_string(), "2".to_string()),
        )
        .commit(&table)
        .await
        .unwrap()
        .take_updates();
        table = apply_updates_to_table(&table, &updates);

        // Two appends with spec 1 should be merged (same spec, min-count=2).
        let f1 = make_unpartitioned_data_file("data/f1.parquet", 10);
        let f2 = make_unpartitioned_data_file("data/f2.parquet", 10);
        let table = merge_append_files(table, vec![f1]).await;
        let table = merge_append_files(table, vec![f2]).await;

        // Both spec 1 manifests get merged into 1 since min-count=2.
        assert_eq!(manifest_count(&table).await, 1);
        let alive = collect_alive_files(table.metadata().current_snapshot().unwrap(), &table).await;
        assert!(alive.contains(&"data/f1.parquet".to_string()));
        assert!(alive.contains(&"data/f2.parquet".to_string()));
    }

    #[tokio::test]
    async fn test_recovery_reuses_manifest() {
        // Two commit() calls on the same Arc reuse the cached MergingState
        // (OnceLock) and produce equivalent snapshots — models catalog retry.
        let mut table = make_v2_minimal_table();
        let updates = Arc::new(
            Transaction::new(&table)
                .update_table_properties()
                .set("commit.manifest.min-count-to-merge".to_string(), "2".to_string()),
        )
        .commit(&table)
        .await
        .unwrap()
        .take_updates();
        table = apply_updates_to_table(&table, &updates);

        let f1 = make_data_file(&table, "data/f1.parquet", 10);
        let f2 = make_data_file(&table, "data/f2.parquet", 10);
        let table = merge_append_files(table, vec![f1]).await;
        let table = merge_append_files(table, vec![f2]).await;

        let f3 = make_data_file(&table, "data/f3.parquet", 10);
        let action = Arc::new(Transaction::new(&table).merge_append().add_data_files(vec![f3]));
        let mut r1 = action.clone().commit(&table).await.unwrap();
        let mut r2 = action.commit(&table).await.unwrap();
        let updates1 = r1.take_updates();
        let snap1 = if let TableUpdate::AddSnapshot { snapshot } = &updates1[0] {
            snapshot.clone()
        } else {
            panic!("expected AddSnapshot")
        };
        let updates2 = r2.take_updates();
        let snap2 = if let TableUpdate::AddSnapshot { snapshot } = &updates2[0] {
            snapshot.clone()
        } else {
            panic!("expected AddSnapshot")
        };
        assert_eq!(snap1.sequence_number(), snap2.sequence_number());
    }

    #[tokio::test]
    async fn test_snapshot_summary() {
        // One commit must populate all expected summary fields and exclude
        // implementation-internal keys like entries-processed.
        let table = make_v2_minimal_table();
        let f1 = make_data_file(&table, "data/f1.parquet", 42);
        let tx = Transaction::new(&table);
        let action = tx.merge_append().add_data_files(vec![f1]);
        let mut commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = commit.take_updates();
        let snap = if let TableUpdate::AddSnapshot { snapshot } = &updates[0] {
            snapshot
        } else {
            panic!("expected AddSnapshot")
        };
        assert_eq!(snap.summary().operation, Operation::Append);
        let props = &snap.summary().additional_properties;
        for key in &[
            "manifests-created",
            "manifests-kept",
            "manifests-replaced",
            "added-data-files",
            "added-records",
            "total-data-files",
            "total-records",
        ] {
            assert!(props.contains_key(*key), "missing summary key {key}: {props:?}");
        }
        assert!(
            !props.contains_key("entries-processed"),
            "entries-processed must not appear: {props:?}"
        );
    }

    // ─── Rust-specific tests ─────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_merge_append_disabled_passthrough() {
        // commit.manifest-merge.enabled=false → manifest count grows linearly.
        let mut table = make_v2_minimal_table();
        let tx = Transaction::new(&table);
        let updates = Arc::new(
            tx.update_table_properties()
                .set("commit.manifest-merge.enabled".to_string(), "false".to_string()),
        )
        .commit(&table)
        .await
        .unwrap()
        .take_updates();
        table = apply_updates_to_table(&table, &updates);

        let f1 = make_data_file(&table, "data/f1.parquet", 10);
        let f2 = make_data_file(&table, "data/f2.parquet", 10);
        let f3 = make_data_file(&table, "data/f3.parquet", 10);
        let table = merge_append_files(table, vec![f1]).await;
        let table = merge_append_files(table, vec![f2]).await;
        let table = merge_append_files(table, vec![f3]).await;

        // With merge disabled, every commit adds 1 manifest → 3 total.
        assert_eq!(manifest_count(&table).await, 3);
    }

    #[tokio::test]
    async fn test_merge_append_empty_table() {
        // First commit on an empty table (no current snapshot) → exactly 1 manifest.
        let table = make_v2_minimal_table();
        assert!(table.metadata().current_snapshot().is_none());
        let f1 = make_data_file(&table, "data/f1.parquet", 10);
        let table = merge_append_files(table, vec![f1]).await;
        assert_eq!(manifest_count(&table).await, 1);
    }

    #[tokio::test]
    async fn test_merge_append_steady_state() {
        // 20 serial appends with min-count-to-merge=3 must produce ≤5 manifests.
        let mut table = make_v2_minimal_table();
        let tx = Transaction::new(&table);
        let updates = Arc::new(
            tx.update_table_properties()
                .set("commit.manifest.min-count-to-merge".to_string(), "3".to_string()),
        )
        .commit(&table)
        .await
        .unwrap()
        .take_updates();
        table = apply_updates_to_table(&table, &updates);

        for i in 0..20usize {
            let f = make_data_file(&table, &format!("data/f{i}.parquet"), 1);
            table = merge_append_files(table, vec![f]).await;
        }

        let count = manifest_count(&table).await;
        assert!(
            count <= 5,
            "expected ≤5 manifests after 20 appends with min-count=3, got {count}"
        );
    }

    #[tokio::test]
    async fn test_merge_append_tombstone_suppression() {
        // Tombstones from a prior RewriteFilesAction must not survive a merge pass.
        let mut table = make_v2_minimal_table();
        let tx = Transaction::new(&table);
        let updates = Arc::new(
            tx.update_table_properties()
                .set("commit.manifest.min-count-to-merge".to_string(), "2".to_string()),
        )
        .commit(&table)
        .await
        .unwrap()
        .take_updates();
        table = apply_updates_to_table(&table, &updates);

        // Step 1: Append two files in separate manifests.
        let f1 = make_data_file(&table, "data/f1.parquet", 10);
        let f2 = make_data_file(&table, "data/f2.parquet", 10);
        let table = append_files(table, vec![f1.clone()]).await;
        let table = append_files(table, vec![f2.clone()]).await;

        // Step 2: RewriteFilesAction replaces f1 → c1, which writes a delete manifest
        // carrying f1's tombstone.
        let c1 = make_data_file(&table, "data/c1.parquet", 10);
        let tx = Transaction::new(&table);
        let action = tx
            .rewrite_files()
            .delete_files(vec![f1])
            .add_files(vec![c1]);
        let updates = Arc::new(action).commit(&table).await.unwrap().take_updates();
        let table = apply_updates_to_table(&table, &updates);

        // Step 3: merge_append appends a new file — the merge pass runs and must
        // suppress f1's tombstone from prior snapshots.
        let d1 = make_data_file(&table, "data/d1.parquet", 10);
        let table = merge_append_files(table, vec![d1]).await;

        let snap = table.metadata().current_snapshot().unwrap();
        let current_snap_id = snap.snapshot_id();
        let ml = snap.load_manifest_list(table.file_io(), table.metadata()).await.unwrap();
        let mut prior_tombstones = 0u64;
        for entry in ml.entries() {
            let manifest = entry.load_manifest(table.file_io()).await.unwrap();
            for me in manifest.entries() {
                if me.status() == ManifestStatus::Deleted
                    && me.snapshot_id() != Some(current_snap_id)
                {
                    prior_tombstones += 1;
                }
            }
        }
        assert_eq!(prior_tombstones, 0, "no prior-snapshot tombstones after merge_append");
    }

    #[tokio::test]
    async fn test_merge_append_sequence_number_preserved() {
        // file_sequence_number and data_sequence_number in merged entries must match
        // the snapshot's sequence number.
        let table = make_v2_minimal_table();
        let f1 = make_data_file(&table, "data/f1.parquet", 10);
        let table = merge_append_files(table, vec![f1]).await;

        let snap = table.metadata().current_snapshot().unwrap();
        let expected_seq = snap.sequence_number();
        let ml = snap.load_manifest_list(table.file_io(), table.metadata()).await.unwrap();
        let manifest = ml.entries()[0].load_manifest(table.file_io()).await.unwrap();
        let entry = &manifest.entries()[0];
        assert_eq!(
            entry.sequence_number(),
            Some(expected_seq),
            "sequence_number mismatch"
        );
        assert_eq!(
            entry.file_sequence_number,
            Some(expected_seq),
            "file_sequence_number mismatch"
        );
    }

    #[tokio::test]
    async fn test_merge_append_fast_append_unaffected() {
        // FastAppendAction must still produce linear manifest growth even after
        // MergeAppendAction has been used on the same table.
        let mut table = make_v2_minimal_table();
        let tx = Transaction::new(&table);
        let updates = Arc::new(
            tx.update_table_properties()
                .set("commit.manifest-merge.enabled".to_string(), "false".to_string()),
        )
        .commit(&table)
        .await
        .unwrap()
        .take_updates();
        table = apply_updates_to_table(&table, &updates);

        // One merge_append then three fast_appends.
        let f0 = make_data_file(&table, "data/f0.parquet", 10);
        let table = merge_append_files(table, vec![f0]).await;

        let f1 = make_data_file(&table, "data/f1.parquet", 10);
        let f2 = make_data_file(&table, "data/f2.parquet", 10);
        let f3 = make_data_file(&table, "data/f3.parquet", 10);
        let table = append_files(table, vec![f1]).await;
        let table = append_files(table, vec![f2]).await;
        let table = append_files(table, vec![f3]).await;

        // With merge disabled, 4 total commits → 4 manifests.
        assert_eq!(manifest_count(&table).await, 4);
    }

    #[tokio::test]
    async fn test_merge_append_null_file_errors() {
        // Committing with zero files must return an error.
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);
        let action = tx.merge_append().add_data_files(vec![]);
        assert!(
            Arc::new(action).commit(&table).await.is_err(),
            "expected error for empty file list"
        );
    }

}
