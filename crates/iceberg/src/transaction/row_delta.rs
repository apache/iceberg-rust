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
use crate::spec::{DataContentType, DataFile, FormatVersion, ManifestEntry, ManifestFile, Operation};
use crate::table::Table;
use crate::transaction::merging_state::MergingState;
use crate::transaction::snapshot::{CommitResult, SnapshotProduceOperation, SnapshotProducer};
use crate::transaction::{ActionCommit, TransactionAction};
use crate::utils::available_parallelism;
use crate::{Error, ErrorKind};

/// Action to append equality-delete and position-delete files to a table as a new
/// `Operation::Delete` snapshot.
///
/// Each commit runs the full merging snapshot producer filter+merge pipeline on
/// existing manifests and writes the supplied delete files into a new
/// `ManifestContent::Deletes` manifest.
///
/// Java analog: `org.apache.iceberg.BaseRowDelta` (`RowDelta` API).
///
/// # Unimplemented Java API
///
/// The following methods from `RowDelta` are not yet implemented (tracked in
/// <https://github.com/apache/iceberg-rust/issues/2203>):
///
/// - `addRows` / `add_data_files` — append data files alongside deletes
/// - `removeRows` — register existing data files for rewrite
/// - `removeDeletes` — drop existing delete files
/// - `validateFromSnapshot` — conflict detection starting snapshot
/// - `validateNoConflictingDeleteFiles` — reject new overlapping deletes
/// - `validateNoConflictingDataFiles` — reject new overlapping data files
/// - `validateDataFilesExistWhenDataFileReferenced` — referenced file existence check
/// - DV (deletion vector) support
pub struct RowDeltaAction {
    added_delete_files: Vec<DataFile>,
    commit_uuid: Option<Uuid>,
    key_metadata: Option<Vec<u8>>,
    snapshot_properties: HashMap<String, String>,
    manifest_read_concurrency: usize,
    manifest_write_concurrency: usize,
    merging_state: OnceLock<Arc<MergingState>>,
}

impl RowDeltaAction {
    pub(crate) fn new() -> Self {
        let num_cpus = available_parallelism().get();
        Self {
            added_delete_files: vec![],
            commit_uuid: None,
            key_metadata: None,
            snapshot_properties: HashMap::default(),
            manifest_read_concurrency: num_cpus,
            manifest_write_concurrency: std::cmp::max(1, num_cpus / 4),
            merging_state: OnceLock::new(),
        }
    }

    /// Add equality-delete or position-delete files to this snapshot.
    pub fn add_delete_files(mut self, files: impl IntoIterator<Item = DataFile>) -> Self {
        self.added_delete_files.extend(files);
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

    fn validate_added_delete_files(&self) -> Result<()> {
        for f in &self.added_delete_files {
            if f.content_type() == DataContentType::Data {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "RowDelta does not accept data files; use add_data_files: {}",
                        f.file_path
                    ),
                ));
            }
        }
        Ok(())
    }
}

#[async_trait]
impl TransactionAction for RowDeltaAction {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        if table.metadata().format_version() == FormatVersion::V1 {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "RowDelta is not supported for V1 tables",
            ));
        }

        self.validate_added_delete_files()?;

        if self.added_delete_files.is_empty() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "RowDelta requires at least one delete file",
            ));
        }

        let state = self.merging_state(table)?;

        let op = RowDeltaOperation {
            added_delete_files: self.added_delete_files.clone(),
            state: state.clone(),
        };

        let snapshot_producer = SnapshotProducer::new(
            table,
            self.commit_uuid.unwrap_or_else(Uuid::now_v7),
            self.key_metadata.clone(),
            self.snapshot_properties.clone(),
            vec![],
        );

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

struct RowDeltaOperation {
    added_delete_files: Vec<DataFile>,
    state: Arc<MergingState>,
}

impl SnapshotProduceOperation for RowDeltaOperation {
    fn operation(&self) -> Operation {
        if !self.added_delete_files.is_empty() {
            Operation::Delete
        } else {
            Operation::Append
        }
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

    fn added_delete_entries(&self) -> Vec<DataFile> {
        self.added_delete_files.clone()
    }

    fn has_pending_content(&self) -> bool {
        !self.added_delete_files.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::spec::{
        DataContentType, DataFile, DataFileBuilder, DataFileFormat, ManifestContentType,
        ManifestStatus,
    };
    use crate::transaction::tests::{apply_updates_to_table, make_v2_minimal_table};
    use crate::transaction::{Transaction, TransactionAction};

    fn delete_file(table: &crate::table::Table, path: &str, content: DataContentType) -> DataFile {
        use crate::spec::{Literal, Struct};
        DataFileBuilder::default()
            .content(content)
            .file_path(path.to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .build()
            .unwrap()
    }

    #[tokio::test]
    async fn test_row_delta_rejects_data_files() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);
        let df = delete_file(&table, "data/x.parquet", DataContentType::Data);
        let action = tx.row_delta().add_delete_files(vec![df]);
        assert!(Arc::new(action).commit(&table).await.is_err());
    }

    #[tokio::test]
    async fn test_row_delta_adds_delete_manifest() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);
        let eq_delete = delete_file(&table, "deletes/eq1.parquet", DataContentType::EqualityDeletes);
        let action = tx.row_delta().add_delete_files(vec![eq_delete]);
        let updates = Arc::new(action)
            .commit(&table)
            .await
            .unwrap()
            .take_updates();
        let table = apply_updates_to_table(&table, &updates);

        let snapshot = table.metadata().current_snapshot().unwrap();
        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();

        let deletes_manifests: Vec<_> = manifest_list
            .entries()
            .iter()
            .filter(|e| e.content == ManifestContentType::Deletes)
            .collect();

        assert_eq!(deletes_manifests.len(), 1, "expected one deletes manifest");

        let manifest = deletes_manifests[0].load_manifest(table.file_io()).await.unwrap();
        assert_eq!(manifest.entries().len(), 1);
        assert_eq!(
            manifest.entries()[0].status(),
            ManifestStatus::Added
        );
    }

    #[tokio::test]
    async fn test_row_delta_equality_delete() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);
        let eq_delete = delete_file(&table, "deletes/eq1.parquet", DataContentType::EqualityDeletes);
        let pos_delete = delete_file(&table, "deletes/pos1.parquet", DataContentType::PositionDeletes);
        let action = tx
            .row_delta()
            .add_delete_files(vec![eq_delete, pos_delete]);
        let updates = Arc::new(action)
            .commit(&table)
            .await
            .unwrap()
            .take_updates();
        let table = apply_updates_to_table(&table, &updates);

        let snapshot = table.metadata().current_snapshot().unwrap();
        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();

        let deletes_count: usize = manifest_list
            .entries()
            .iter()
            .filter(|e| e.content == ManifestContentType::Deletes)
            .count();
        assert_eq!(deletes_count, 1);
    }
}
