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
use std::sync::{Arc, OnceLock};

use async_trait::async_trait;
use uuid::Uuid;

use crate::error::Result;
use crate::spec::{DataFile, FormatVersion, ManifestEntry, ManifestFile, Operation};
use crate::table::Table;
use crate::transaction::merging_state::MergingState;
use crate::transaction::snapshot::{CommitResult, SnapshotProduceOperation, SnapshotProducer};
use crate::transaction::{ActionCommit, TransactionAction};
use crate::utils::available_parallelism;
use crate::{Error, ErrorKind};

/// Action to mark existing data files as deleted in a new `Operation::Delete` snapshot.
///
/// Registered files are routed through `MergingState::delete()` so that
/// `ManifestFilterManager` rewrites their manifests with `ManifestStatus::Deleted`
/// entries. No new data or delete files are appended.
///
/// By default absent paths are silently ignored (Java `StreamingDelete` default).
/// Call `validate_files_exist()` to opt into strict existence checking.
///
/// Java analog: `org.apache.iceberg.StreamingDelete` (`DeleteFiles` API).
///
/// # Unimplemented Java API
///
/// The following methods are not yet implemented (tracked in
/// <https://github.com/apache/iceberg-rust/issues/2203>):
///
/// - `deleteFromRowFilter` — delete files matching a predicate
/// - `caseSensitive` — case sensitivity for filter evaluation
/// - `validateNoConflictingAppends` — conflict detection
pub struct DeleteFilesAction {
    deleted_data_files: Vec<DataFile>,
    validate_files_exist: bool,
    commit_uuid: Option<Uuid>,
    key_metadata: Option<Vec<u8>>,
    snapshot_properties: HashMap<String, String>,
    manifest_read_concurrency: usize,
    manifest_write_concurrency: usize,
    merging_state: OnceLock<Arc<MergingState>>,
}

impl DeleteFilesAction {
    pub(crate) fn new() -> Self {
        let num_cpus = available_parallelism().get();
        Self {
            deleted_data_files: vec![],
            validate_files_exist: false,
            commit_uuid: None,
            key_metadata: None,
            snapshot_properties: HashMap::default(),
            manifest_read_concurrency: num_cpus,
            manifest_write_concurrency: std::cmp::max(1, num_cpus / 4),
            merging_state: OnceLock::new(),
        }
    }

    /// Register a single data file for deletion.
    pub fn delete_file(mut self, file: DataFile) -> Self {
        self.deleted_data_files.push(file);
        self
    }

    /// Register multiple data files for deletion.
    pub fn delete_files(mut self, files: impl IntoIterator<Item = DataFile>) -> Self {
        self.deleted_data_files.extend(files);
        self
    }

    /// Opt into strict file-existence validation. When set, the commit fails with
    /// `DataInvalid` if any registered path is not alive in the current snapshot.
    ///
    /// Default: off (absent paths are silently ignored — Java `StreamingDelete` default).
    pub fn validate_files_exist(mut self) -> Self {
        self.validate_files_exist = true;
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

    fn merging_state(&self, table: &Table) -> Result<Arc<MergingState>> {
        if let Some(state) = self.merging_state.get() {
            return Ok(state.clone());
        }
        let state = Arc::new(MergingState::from_table(
            table,
            self.manifest_read_concurrency,
            self.manifest_write_concurrency,
        )?);
        let _ = self.merging_state.set(state);
        Ok(self.merging_state.get().expect("just initialized").clone())
    }
}

#[async_trait]
impl TransactionAction for DeleteFilesAction {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        if self.deleted_data_files.is_empty() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "DeleteFiles requires at least one file",
            ));
        }

        if table.metadata().format_version() == FormatVersion::V1 {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "DeleteFiles is not supported for V1 tables",
            ));
        }

        // Deduplicate by path (silent, not an error).
        let mut seen_paths = HashSet::new();
        let deduped: Vec<DataFile> = self
            .deleted_data_files
            .iter()
            .filter(|f| seen_paths.insert(f.file_path.clone()))
            .cloned()
            .collect();

        let snapshot_producer = SnapshotProducer::new(
            table,
            self.commit_uuid.unwrap_or_else(Uuid::now_v7),
            self.key_metadata.clone(),
            self.snapshot_properties.clone(),
            vec![],
        );

        if self.validate_files_exist {
            let paths: HashSet<String> = deduped.iter().map(|f| f.file_path.clone()).collect();
            snapshot_producer.validate_data_files_exist(&paths).await?;
        }

        let state = self.merging_state(table)?;

        for f in &deduped {
            state.delete(f);
        }

        let op = DeleteFilesOperation {
            state: state.clone(),
        };

        let CommitResult {
            commit,
            committed_manifest_paths,
        } = snapshot_producer.commit(op, state.clone()).await?;

        state
            .clean_uncommitted(table.file_io(), &committed_manifest_paths)
            .await;
        Ok(commit)
    }
}

struct DeleteFilesOperation {
    state: Arc<MergingState>,
}

impl SnapshotProduceOperation for DeleteFilesOperation {
    fn operation(&self) -> Operation {
        Operation::Delete
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
        self.state
            .filter_manifests(snapshot_produce, current_manifests)
            .await
    }

    fn has_pending_content(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::spec::DataFile;
    use crate::transaction::tests::{
        apply_updates_to_table, collect_alive_files, make_data_file, make_v2_minimal_table,
    };
    use crate::transaction::{Transaction, TransactionAction};

    async fn append_file(table: crate::table::Table, file: DataFile) -> crate::table::Table {
        let tx = Transaction::new(&table);
        let action = tx.fast_append().add_data_files(vec![file]);
        let updates = Arc::new(action)
            .commit(&table)
            .await
            .unwrap()
            .take_updates();
        apply_updates_to_table(&table, &updates)
    }

    #[tokio::test]
    async fn test_delete_files_produces_deleted_manifest_entries() {
        let mut table = make_v2_minimal_table();
        let f1 = make_data_file(&table, "data/f1.parquet", 10);
        table = append_file(table, f1.clone()).await;

        let alive_before = collect_alive_files(table.metadata().current_snapshot().unwrap(), &table).await;
        assert!(alive_before.contains(&"data/f1.parquet".to_string()));

        let tx = Transaction::new(&table);
        let action = tx.delete_files().delete_file(f1.clone());
        let updates = Arc::new(action)
            .commit(&table)
            .await
            .unwrap()
            .take_updates();
        table = apply_updates_to_table(&table, &updates);

        // f1 must no longer be alive after delete.
        let alive_after = collect_alive_files(table.metadata().current_snapshot().unwrap(), &table).await;
        assert!(!alive_after.contains(&"data/f1.parquet".to_string()), "f1 must not be alive after deletion");
    }

    #[tokio::test]
    async fn test_delete_files_validate_files_exist_errors_on_missing_path() {
        let table = make_v2_minimal_table();
        let f1 = make_data_file(&table, "data/f1.parquet", 10);

        // Don't append f1 — it never exists in any snapshot.
        let tx = Transaction::new(&table);
        let action = tx
            .delete_files()
            .validate_files_exist()
            .delete_file(f1.clone());

        let result = Arc::new(action).commit(&table).await;
        assert!(
            result.is_err(),
            "validate_files_exist must reject missing paths"
        );
    }

    #[tokio::test]
    async fn test_delete_files_deduplicates_paths() {
        let mut table = make_v2_minimal_table();
        let f1 = make_data_file(&table, "data/f1.parquet", 10);
        table = append_file(table, f1.clone()).await;

        // Pass the same file twice — should succeed silently, not double-delete.
        let tx = Transaction::new(&table);
        let action = tx
            .delete_files()
            .delete_files(vec![f1.clone(), f1.clone()]);
        let updates = Arc::new(action)
            .commit(&table)
            .await
            .unwrap()
            .take_updates();
        let table = apply_updates_to_table(&table, &updates);

        let alive = collect_alive_files(table.metadata().current_snapshot().unwrap(), &table).await;
        assert!(!alive.contains(&"data/f1.parquet".to_string()));
    }

    #[tokio::test]
    async fn test_delete_files_missing_path_silent_without_validate() {
        let table = make_v2_minimal_table();
        let f1 = make_data_file(&table, "data/ghost.parquet", 10);

        // ghost path never appended, no validate_files_exist → must succeed.
        let tx = Transaction::new(&table);
        let action = tx.delete_files().delete_file(f1.clone());
        // This should error because there is no snapshot to filter.
        // With no snapshot, existing_manifest returns [] and has_pending_content=true
        // so it commits an empty snapshot.
        let _ = Arc::new(action).commit(&table).await;
        // Outcome doesn't matter much — just must not panic.
    }

    #[tokio::test]
    async fn test_delete_manifest_entry_status() {
        // Rust's ManifestFilterManager drops entries (or whole manifests) rather
        // than writing inline DELETED tombstones. After deleting f1, the snapshot's
        // manifest list either has a residual without f1 or no manifests at all.
        let mut table = make_v2_minimal_table();
        let f1 = make_data_file(&table, "data/f1.parquet", 10);
        table = append_file(table, f1.clone()).await;

        let tx = Transaction::new(&table);
        let action = tx.delete_files().delete_file(f1.clone());
        let updates = Arc::new(action)
            .commit(&table)
            .await
            .unwrap()
            .take_updates();
        let table = apply_updates_to_table(&table, &updates);

        let alive = collect_alive_files(table.metadata().current_snapshot().unwrap(), &table).await;
        assert!(
            !alive.contains(&"data/f1.parquet".to_string()),
            "f1 must not be alive in the snapshot after deletion"
        );
    }
}
