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
    DataContentType, DataFile, ManifestContentType, ManifestEntry, ManifestFile, Operation,
};
use crate::table::Table;
use crate::transaction::delete_aware::DeleteAwareOperation;
use crate::transaction::manifest_filter::ManifestFilterManager;
use crate::transaction::snapshot::{
    DefaultManifestProcess, SnapshotProduceOperation, SnapshotProducer,
};
use crate::transaction::{ActionCommit, TransactionAction};
use crate::{Error, ErrorKind};

/// A transaction action that rewrites a set of existing files with a new, equivalent set
/// (i.e. compaction).
///
/// `RewriteFiles` adds new files and deletes old ones in a single `Replace` snapshot. Added
/// and deleted files are bucketed by [`DataContentType`] as they come in: data files land in
/// the data buckets, while positional/equality delete files land in the delete buckets (this
/// split mirrors `#1606`).
///
/// The action stores its inputs and rebuilds a fresh [`RewriteFilesOperation`] on each commit
/// attempt via [`build_operation`](RewriteFilesAction::build_operation), so the operation's
/// filter managers are repopulated deterministically and survive `do_commit` retries.
pub struct RewriteFilesAction {
    // below are properties used to create SnapshotProducer when commit
    commit_uuid: Option<Uuid>,
    key_metadata: Option<Vec<u8>>,
    snapshot_properties: HashMap<String, String>,
    // Added/deleted files split by content type, as in #1606.
    added_data_files: Vec<DataFile>,
    added_delete_files: Vec<DataFile>,
    deleted_data_files: Vec<DataFile>,
    deleted_delete_files: Vec<DataFile>,
    // When set, only positional deletes count as conflicts during validation.
    data_sequence_number: Option<i64>,
    // The snapshot the rewrite started from; validation scans for new deletes since this id.
    starting_snapshot_id: Option<i64>,
}

impl RewriteFilesAction {
    pub(crate) fn new() -> Self {
        Self {
            commit_uuid: None,
            key_metadata: None,
            snapshot_properties: HashMap::default(),
            added_data_files: vec![],
            added_delete_files: vec![],
            deleted_data_files: vec![],
            deleted_delete_files: vec![],
            data_sequence_number: None,
            starting_snapshot_id: None,
        }
    }

    /// Add files produced by the rewrite, bucketing each by its content type: data files go
    /// to the data bucket; positional/equality delete files go to the delete bucket.
    pub fn add_data_files(mut self, files: impl IntoIterator<Item = DataFile>) -> Result<Self> {
        for f in files {
            match f.content_type() {
                DataContentType::Data => self.added_data_files.push(f),
                DataContentType::PositionDeletes | DataContentType::EqualityDeletes => {
                    self.added_delete_files.push(f)
                }
            }
        }
        Ok(self)
    }

    /// Mark files removed by the rewrite, bucketing each by its content type: data files go
    /// to the data bucket; positional/equality delete files go to the delete bucket.
    pub fn delete_files(mut self, files: impl IntoIterator<Item = DataFile>) -> Result<Self> {
        for f in files {
            match f.content_type() {
                DataContentType::Data => self.deleted_data_files.push(f),
                DataContentType::PositionDeletes | DataContentType::EqualityDeletes => {
                    self.deleted_delete_files.push(f)
                }
            }
        }
        Ok(self)
    }

    /// Set the snapshot the rewrite started from. Conflict validation scans for new delete
    /// files added since this snapshot.
    pub fn set_starting_snapshot_id(mut self, id: i64) -> Self {
        self.starting_snapshot_id = Some(id);
        self
    }

    /// Pin the data sequence number for the rewrite. When set, only positional deletes are
    /// treated as conflicts during validation.
    pub fn set_data_sequence_number(mut self, seq: i64) -> Self {
        self.data_sequence_number = Some(seq);
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
    pub fn set_snapshot_properties(mut self, snapshot_properties: HashMap<String, String>) -> Self {
        self.snapshot_properties = snapshot_properties;
        self
    }

    /// Build a fresh [`RewriteFilesOperation`] from the action's stored inputs.
    ///
    /// A new operation is built per commit attempt; the filter managers are repopulated
    /// deterministically from the deleted-file vectors (so the operation survives `do_commit`
    /// retries). Removed files become filter-manager removals via
    /// [`ManifestFilterManager::delete_file`] — data files into the data filter, delete files
    /// into the delete filter (`DataFile` covers both kinds).
    fn build_operation(&self) -> RewriteFilesOperation {
        let mut op = RewriteFilesOperation {
            deleted_data_files: self.deleted_data_files.clone(),
            deleted_delete_files: self.deleted_delete_files.clone(),
            starting_snapshot_id: self.starting_snapshot_id,
            data_sequence_number: self.data_sequence_number,
            data_filter: ManifestFilterManager::default(),
            delete_filter: ManifestFilterManager::default(),
        };

        // Removed files become filter-manager removals (DataFile covers both kinds).
        for f in &self.deleted_data_files {
            op.data_filter.delete_file(f.clone());
        }
        for f in &self.deleted_delete_files {
            op.delete_filter.delete_file(f.clone());
        }

        op
    }
}

/// The operation driven by [`SnapshotProducer`](crate::transaction::snapshot::SnapshotProducer)
/// for a `RewriteFiles` commit.
///
/// Rebuilt per commit attempt by [`RewriteFilesAction::build_operation`]. The
/// `SnapshotProduceOperation` and `DeleteAwareOperation` implementations are wired in
/// subsequent subtasks (5.2–5.4).
pub(crate) struct RewriteFilesOperation {
    deleted_data_files: Vec<DataFile>,
    deleted_delete_files: Vec<DataFile>,
    starting_snapshot_id: Option<i64>,
    data_sequence_number: Option<i64>,
    data_filter: ManifestFilterManager,
    delete_filter: ManifestFilterManager,
}

impl SnapshotProduceOperation for RewriteFilesOperation {
    fn operation(&self) -> Operation {
        Operation::Replace
    }

    async fn delete_entries(
        &self,
        _snapshot_produce: &SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestEntry>> {
        // Removed files are dropped by rewriting the carried-forward manifests in
        // `existing_manifest` via the filter managers, so no separate delete entries are
        // emitted here.
        Ok(vec![])
    }

    async fn existing_manifest(
        &mut self,
        snapshot_produce: &mut SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestFile>> {
        // No current snapshot means there are no manifests to carry forward.
        let Some(snapshot) = snapshot_produce.table.metadata().current_snapshot() else {
            return Ok(vec![]);
        };

        let manifests = snapshot_produce
            .table
            .manifest_list_reader(snapshot)
            .load()
            .await?
            .entries()
            .to_vec();

        // Partition the carried-forward manifests by content type so each filter manager
        // rewrites only the manifests it owns: data manifests through `data_filter`,
        // positional/equality delete manifests through `delete_filter`.
        let (data, deletes): (Vec<ManifestFile>, Vec<ManifestFile>) = manifests
            .into_iter()
            .partition(|m| m.content == ManifestContentType::Data);

        let mut out = self
            .data_filter
            .filter_manifests(snapshot_produce, data)
            .await?;
        out.extend(
            self.delete_filter
                .filter_manifests(snapshot_produce, deletes)
                .await?,
        );

        Ok(out)
    }
}

impl DeleteAwareOperation for RewriteFilesOperation {
    async fn validate(&self, base: &Table, parent_snapshot_id: Option<i64>) -> Result<()> {
        // Precondition: a rewrite must remove at least one file. (Intent from #1606.)
        if self.deleted_data_files.is_empty() && self.deleted_delete_files.is_empty() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Files to delete cannot be empty",
            ));
        }

        // When data files are being rewritten, ensure no new row-level deletes have appeared
        // for them since the starting snapshot. When a data sequence number is pinned, only
        // positional deletes count as conflicts (`ignore_equality_deletes`), following #1606.
        if !self.deleted_data_files.is_empty() {
            self.validate_no_new_deletes_for_data_files(
                base,
                self.starting_snapshot_id,
                parent_snapshot_id,
                &self.deleted_data_files,
                self.data_sequence_number.is_some(),
            )
            .await?;
        }

        Ok(())
    }

    fn data_filter(&mut self) -> &mut ManifestFilterManager {
        &mut self.data_filter
    }

    fn delete_filter(&mut self) -> &mut ManifestFilterManager {
        &mut self.delete_filter
    }
}

#[async_trait]
impl TransactionAction for RewriteFilesAction {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        // Rebuild a fresh operation for this attempt; the filter managers are repopulated
        // deterministically from the action's stored inputs (survives `do_commit` retries).
        let op = self.build_operation();

        // STEP 1 — conflict validation before any write. Scans the refreshed base table for
        // concurrent changes (e.g. new row-level deletes for rewritten data files) since the
        // starting snapshot.
        op.validate(table, table.metadata().current_snapshot_id())
            .await?;

        // STEP 2 — produce the `Replace` snapshot. The producer drives the operation by `&mut`
        // so the filter managers can rewrite the carried-forward manifests inside
        // `existing_manifest`.
        let snapshot_producer = SnapshotProducer::new(
            table,
            self.commit_uuid.unwrap_or_else(Uuid::now_v7),
            self.key_metadata.clone(),
            self.snapshot_properties.clone(),
            self.added_data_files.clone(),
        );

        snapshot_producer.commit(op, DefaultManifestProcess).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::{DataFileBuilder, DataFileFormat, Literal, Struct};

    fn file(path: &str, content: DataContentType) -> DataFile {
        let mut builder = DataFileBuilder::default();
        builder
            .content(content)
            .file_path(path.to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition(Struct::from_iter([Some(Literal::long(0))]))
            .partition_spec_id(0);
        // Positional/equality deletes require a referenced data file / equality ids to be
        // valid, but for these builder-bucketing tests only content_type matters.
        if content == DataContentType::EqualityDeletes {
            builder.equality_ids(Some(vec![1]));
        }
        if content == DataContentType::PositionDeletes {
            builder.referenced_data_file(Some("data/ref.parquet".to_string()));
        }
        builder.build().unwrap()
    }

    #[test]
    fn test_add_data_files_buckets_by_content_type() {
        let action = RewriteFilesAction::new()
            .add_data_files(vec![
                file("data/a.parquet", DataContentType::Data),
                file("delete/pos.parquet", DataContentType::PositionDeletes),
                file("delete/eq.parquet", DataContentType::EqualityDeletes),
            ])
            .unwrap();

        assert_eq!(action.added_data_files.len(), 1);
        assert_eq!(action.added_data_files[0].file_path(), "data/a.parquet");
        assert_eq!(action.added_delete_files.len(), 2);
        assert!(action.deleted_data_files.is_empty());
        assert!(action.deleted_delete_files.is_empty());
    }

    #[test]
    fn test_delete_files_buckets_by_content_type() {
        let action = RewriteFilesAction::new()
            .delete_files(vec![
                file("data/a.parquet", DataContentType::Data),
                file("data/b.parquet", DataContentType::Data),
                file("delete/pos.parquet", DataContentType::PositionDeletes),
            ])
            .unwrap();

        assert_eq!(action.deleted_data_files.len(), 2);
        assert_eq!(action.deleted_delete_files.len(), 1);
        assert!(action.added_data_files.is_empty());
        assert!(action.added_delete_files.is_empty());
    }

    #[test]
    fn test_setters_populate_fields() {
        let uuid = Uuid::now_v7();
        let mut props = HashMap::new();
        props.insert("k".to_string(), "v".to_string());

        let action = RewriteFilesAction::new()
            .set_starting_snapshot_id(42)
            .set_data_sequence_number(7)
            .set_commit_uuid(uuid)
            .set_key_metadata(vec![1, 2, 3])
            .set_snapshot_properties(props.clone());

        assert_eq!(action.starting_snapshot_id, Some(42));
        assert_eq!(action.data_sequence_number, Some(7));
        assert_eq!(action.commit_uuid, Some(uuid));
        assert_eq!(action.key_metadata, Some(vec![1, 2, 3]));
        assert_eq!(action.snapshot_properties, props);
    }

    #[test]
    fn test_build_operation_populates_filters_from_deleted_files() {
        let action = RewriteFilesAction::new()
            .delete_files(vec![
                file("data/a.parquet", DataContentType::Data),
                file("data/b.parquet", DataContentType::Data),
                file("delete/pos.parquet", DataContentType::PositionDeletes),
            ])
            .unwrap()
            .set_starting_snapshot_id(99)
            .set_data_sequence_number(3);

        let op = action.build_operation();

        // Carried-over scalar fields.
        assert_eq!(op.starting_snapshot_id, Some(99));
        assert_eq!(op.data_sequence_number, Some(3));
        assert_eq!(op.deleted_data_files.len(), 2);
        assert_eq!(op.deleted_delete_files.len(), 1);

        // Deleted data files routed into the data filter; delete files into delete filter.
        assert!(op.data_filter.is_removed("data/a.parquet"));
        assert!(op.data_filter.is_removed("data/b.parquet"));
        assert!(!op.data_filter.is_removed("delete/pos.parquet"));

        assert!(op.delete_filter.is_removed("delete/pos.parquet"));
        assert!(!op.delete_filter.is_removed("data/a.parquet"));
    }

    #[test]
    fn test_build_operation_empty_when_nothing_deleted() {
        let op = RewriteFilesAction::new().build_operation();

        assert!(op.data_filter.is_empty());
        assert!(op.delete_filter.is_empty());
        assert!(op.deleted_data_files.is_empty());
        assert!(op.deleted_delete_files.is_empty());
        assert_eq!(op.starting_snapshot_id, None);
        assert_eq!(op.data_sequence_number, None);
    }

    #[tokio::test]
    async fn test_validate_errors_when_no_files_deleted() {
        let table = crate::transaction::tests::make_v2_minimal_table();
        let op = RewriteFilesAction::new().build_operation();

        let err = op
            .validate(&table, table.metadata().current_snapshot_id())
            .await
            .unwrap_err();

        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(
            err.message().contains("Files to delete cannot be empty"),
            "unexpected error message: {}",
            err.message()
        );
    }

    #[tokio::test]
    async fn test_validate_passes_when_no_data_files_deleted_on_minimal_table() {
        // Only a delete file is removed (no data files), so the data-file conflict check is
        // skipped and the precondition is satisfied.
        let table = crate::transaction::tests::make_v2_minimal_table();
        let op = RewriteFilesAction::new()
            .delete_files(vec![file(
                "delete/pos.parquet",
                DataContentType::PositionDeletes,
            )])
            .unwrap()
            .build_operation();

        op.validate(&table, table.metadata().current_snapshot_id())
            .await
            .unwrap();
    }
}
