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
use crate::spec::{DataFile, ManifestEntry, ManifestFile, Operation};
use crate::table::Table;
use crate::transaction::merging_snapshot::MergingSnapshotProducer;
use crate::transaction::snapshot::{
    DefaultManifestProcess, SnapshotProduceOperation, SnapshotProducer,
};
use crate::transaction::validation;
use crate::transaction::{ActionCommit, TransactionAction};
use crate::{Error, ErrorKind};

/// A transaction action that rewrites a set of existing files with a new, equivalent set
/// (i.e. compaction).
///
/// `RewriteFiles` adds new files and deletes old ones in a single `Replace` snapshot.
///
/// Following the **compose-and-delegate** design, the action owns a
/// [`MergingSnapshotProducer`] (`msp`) as a field. The MSP holds the staging delta
/// (added/deleted data and delete files, `data_sequence_number`) and delegates all shared
/// write mechanics to a per-attempt [`SnapshotProducer`]. Added and deleted files are routed
/// to their data vs delete buckets by [`DataContentType`](crate::spec::DataContentType) inside
/// the MSP (this split mirrors `#1606`), so the action simply forwards them.
///
/// Because the MSP is owned as a field, it survives `do_commit` retries via the action's
/// persisted `Arc`. Only the action-owned `starting_snapshot_id` (the validation lower bound)
/// stays local; it is copied into a fresh [`RewriteFilesOperation`] each attempt via
/// [`build_operation`](RewriteFilesAction::build_operation).
pub struct RewriteFilesAction {
    /// The stateful, cross-retry producer. It OWNS the staged added/deleted files and
    /// `data_sequence_number`. Constructed in [`RewriteFilesAction::new`], never inside
    /// `commit`.
    msp: MergingSnapshotProducer,

    /// Action-owned config: the validation lower bound, read by the operation's `validate`.
    /// Matches Java `BaseRewriteFiles.startingSnapshotId`. Passed to the operation at build
    /// time.
    starting_snapshot_id: Option<i64>,
}

impl RewriteFilesAction {
    pub(crate) fn new() -> Self {
        Self {
            // A stable commit UUID is generated up front (like Java's `final commitUUID`);
            // `set_commit_uuid` can still override it during the build phase.
            msp: MergingSnapshotProducer::new(Uuid::now_v7(), None, HashMap::default()),
            starting_snapshot_id: None,
        }
    }

    /// Add files produced by the rewrite. Forwarded into the MSP, which routes each file to
    /// the data vs delete bucket by its content type (as in `#1606`).
    pub fn add_data_files(mut self, files: impl IntoIterator<Item = DataFile>) -> Result<Self> {
        for f in files {
            self.msp.add_data_file(f);
        }
        Ok(self)
    }

    /// Mark files removed by the rewrite. Forwarded into the MSP, which routes each file to
    /// the data vs delete bucket by its content type (as in `#1606`).
    pub fn delete_files(mut self, files: impl IntoIterator<Item = DataFile>) -> Result<Self> {
        for f in files {
            self.msp.delete_data_file(f);
        }
        Ok(self)
    }

    /// Set the snapshot the rewrite started from. Conflict validation scans for new delete
    /// files added since this snapshot. Action-owned config: stays local.
    pub fn set_starting_snapshot_id(mut self, id: i64) -> Self {
        self.starting_snapshot_id = Some(id);
        self
    }

    /// Pin the data sequence number for the rewrite. When set, only positional deletes are
    /// treated as conflicts during validation. Threads through to the MSP.
    pub fn set_data_sequence_number(mut self, seq: i64) -> Self {
        self.msp.set_data_sequence_number(seq);
        self
    }

    /// Set commit UUID for the snapshot. Threads through to the MSP.
    pub fn set_commit_uuid(mut self, commit_uuid: Uuid) -> Self {
        self.msp.set_commit_uuid(commit_uuid);
        self
    }

    /// Set key metadata for manifest files. Threads through to the MSP.
    pub fn set_key_metadata(mut self, key_metadata: Vec<u8>) -> Self {
        self.msp.set_key_metadata(Some(key_metadata));
        self
    }

    /// Set snapshot summary properties. Threads through to the MSP.
    pub fn set_snapshot_properties(mut self, snapshot_properties: HashMap<String, String>) -> Self {
        self.msp.set_snapshot_properties(snapshot_properties);
        self
    }

    /// Build a fresh [`RewriteFilesOperation`] borrowing the durable MSP.
    ///
    /// A new operation is built per commit attempt. Only the action-owned
    /// `starting_snapshot_id` is copied in; everything else the operation needs (staged
    /// deletes, `data_sequence_number`) it reads through the borrowed `&MSP`, so nothing
    /// stale leaks between attempts.
    fn build_operation(&self) -> RewriteFilesOperation<'_> {
        RewriteFilesOperation {
            msp: &self.msp,
            starting_snapshot_id: self.starting_snapshot_id,
        }
    }
}

#[async_trait]
impl TransactionAction for RewriteFilesAction {
    /// Three-layer delegation: `TransactionAction::commit` →
    /// `MergingSnapshotProducer::commit` → `SnapshotProducer::commit`. A fresh operation is
    /// built per attempt (borrowing the durable MSP); the MSP owns the staging delta and runs
    /// validation then the write. No closure, no copy-pasted validate-then-write.
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        self.msp
            .commit(table, self.build_operation(), DefaultManifestProcess)
            .await
    }
}

/// The operation driven by [`SnapshotProducer`] for a `RewriteFiles` commit.
///
/// It borrows `&'a MergingSnapshotProducer` — the seam that pulls in the staging delta.
/// Rebuilt per commit attempt by [`RewriteFilesAction::build_operation`].
///
/// It overrides `validate` (empty-delete precondition + conflicting-delete scan) and implements
/// `existing_manifest` (errors when there is no current snapshot, otherwise runs the MSP filter
/// managers over the carried-forward manifests).
// Constructed via `build_operation`, which is wired into `commit` in task 8.3.
#[allow(dead_code)]
pub(crate) struct RewriteFilesOperation<'a> {
    msp: &'a MergingSnapshotProducer,
    starting_snapshot_id: Option<i64>,
}

impl SnapshotProduceOperation for RewriteFilesOperation<'_> {
    fn operation(&self) -> Operation {
        Operation::Replace
    }

    async fn delete_entries(
        &self,
        _snapshot_produce: &SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestEntry>> {
        // Removed files are dropped by rewriting the carried-forward manifests in
        // `existing_manifest` via the MSP filter managers, so no separate delete entries are
        // emitted here.
        Ok(vec![])
    }

    /// Override the default no-op. Composes the stateless validation helper, reading the
    /// staged deletes and `data_sequence_number` through the borrowed `&MSP`.
    async fn validate(&self, base: &Table, parent_snapshot_id: Option<i64>) -> Result<()> {
        // Precondition (intent from #1606): a rewrite must remove something.
        if self.msp.deleted_data_files().is_empty() && self.msp.deleted_delete_files().is_empty() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Files to delete cannot be empty",
            ));
        }

        // Only scan for conflicting deletes when data files are being rewritten (matching prior
        // behavior). When `data_sequence_number` is pinned on the MSP, equality deletes are not
        // treated as conflicts.
        if !self.msp.deleted_data_files().is_empty() {
            let ignore_equality_deletes = self.msp.data_sequence_number().is_some();
            validation::validate_no_new_deletes_for_data_files(
                base,
                self.starting_snapshot_id,
                parent_snapshot_id,
                self.msp.deleted_data_files(),
                ignore_equality_deletes,
            )
            .await?;
        }

        Ok(())
    }

    async fn existing_manifest(
        &self,
        snapshot_produce: &mut SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestFile>> {
        // A Replace with no current snapshot is an error, not an empty result.
        let Some(snapshot) = snapshot_produce.table.metadata().current_snapshot() else {
            return Err(Error::new(
                ErrorKind::PreconditionFailed,
                "Cannot rewrite files: table has no current snapshot",
            ));
        };

        // Load the manifest list of the current snapshot, then hand the carried-forward
        // manifests to the MSP, which partitions them by content type and runs the data /
        // delete filter managers (v1 always rewrites; a cross-retry rewrite cache is future
        // work). `base` is the refreshed table the producer is writing against.
        let base = snapshot_produce.table;
        let manifest_list = base.manifest_list_reader(snapshot).load().await?;
        let manifests = manifest_list.entries().to_vec();

        self.msp
            .filter_existing_manifests(snapshot_produce, base, manifests)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::{DataContentType, DataFileBuilder, DataFileFormat, Literal, Struct};

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
        // valid, but for these forwarding tests only content_type matters.
        if content == DataContentType::EqualityDeletes {
            builder.equality_ids(Some(vec![1]));
        }
        if content == DataContentType::PositionDeletes {
            builder.referenced_data_file(Some("data/ref.parquet".to_string()));
        }
        builder.build().unwrap()
    }

    #[test]
    fn test_delete_files_forwards_data_files_into_msp() {
        // Only the data-file bucket is observable through the MSP's public accessor;
        // delete-file bucketing is covered by the detailed forwarding tests in task 8.4.
        let action = RewriteFilesAction::new()
            .delete_files(vec![
                file("data/a.parquet", DataContentType::Data),
                file("data/b.parquet", DataContentType::Data),
                file("delete/pos.parquet", DataContentType::PositionDeletes),
            ])
            .unwrap();

        let deleted = action.msp.deleted_data_files();
        assert_eq!(deleted.len(), 2);
        assert_eq!(deleted[0].file_path(), "data/a.parquet");
        assert_eq!(deleted[1].file_path(), "data/b.parquet");
    }

    #[test]
    fn test_set_data_sequence_number_threads_through_to_msp() {
        let action = RewriteFilesAction::new().set_data_sequence_number(7);
        assert_eq!(action.msp.data_sequence_number(), Some(7));
    }

    #[test]
    fn test_set_starting_snapshot_id_stays_local() {
        let action = RewriteFilesAction::new().set_starting_snapshot_id(42);
        assert_eq!(action.starting_snapshot_id, Some(42));
    }

    #[test]
    fn test_build_operation_borrows_msp_and_copies_starting_snapshot_id() {
        let action = RewriteFilesAction::new()
            .delete_files(vec![file("data/a.parquet", DataContentType::Data)])
            .unwrap()
            .set_starting_snapshot_id(99)
            .set_data_sequence_number(3);

        let op = action.build_operation();

        assert_eq!(op.operation(), Operation::Replace);
        assert_eq!(op.starting_snapshot_id, Some(99));
        // The operation reads staging through the borrowed &MSP (nothing copied).
        assert_eq!(op.msp.deleted_data_files().len(), 1);
        assert_eq!(op.msp.data_sequence_number(), Some(3));
    }
}
