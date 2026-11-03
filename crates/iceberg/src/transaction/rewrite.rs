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

//! Transaction action for rewriting data files (compaction).
//!
//! [`RewriteFilesAction`] replaces a set of data files with a new set while
//! keeping the logical table contents unchanged. This is used for compaction —
//! merging many small files into fewer large ones.
//!
//! The resulting snapshot uses [`Operation::Replace`] to indicate that files
//! were reorganised without changing the data.

use std::collections::HashMap;

use async_trait::async_trait;

use crate::error::{Result, invalid_data};
use crate::spec::{DataContentType, DataFile, ManifestContentType, Operation};
use crate::table::Table;
use crate::transaction::action::CommitStatus;
use crate::transaction::snapshot::merging::{MergingIntent, MergingSnapshotProducer};
use crate::transaction::snapshot::validate_files_not_live;
use crate::transaction::{ActionCommit, TransactionAction};

/// A transaction action that rewrites (replaces) data files.
///
/// This is the Rust equivalent of Java's `BaseRewriteFiles`. It uses
/// `MergingSnapshotProducer` to handle manifest filtering and creation,
/// and commits a snapshot with [`Operation::Replace`].
///
/// # Example
///
/// ```ignore
/// let tx = Transaction::new(&table);
/// let action = tx.rewrite_files()
///     .delete_file(old_file_1)
///     .delete_file(old_file_2)
///     .add_file(merged_file);
/// let tx = action.apply(tx)?;
/// let table = tx.commit(&catalog).await?;
/// ```
///
/// # Concurrent deletes
///
/// The action records the table's current snapshot when it is created, and
/// refuses to commit if the table gained a delete manifest after it. Java
/// narrows that to the files being replaced in
/// `validateNoNewDeletesForDataFiles`; until that exists here the check is
/// table-wide, so a delete committed to an unrelated partition while the
/// rewrite ran also fails the commit with
/// [`ErrorKind::DataInvalid`](crate::ErrorKind::DataInvalid). So do a
/// rewrite planned against a table that had no snapshot yet, and one whose
/// starting snapshot has since been expired: neither can rule out a delete.
/// Replan the rewrite against the current table and run it again.
///
/// Deletes that were already committed when the rewrite was planned are not
/// covered by this check, so apply them while rewriting.
#[derive(Clone)]
pub struct RewriteFilesAction {
    added_data_files: Vec<DataFile>,
    deleted_data_files: Vec<DataFile>,
    /// The snapshot the rewrite was planned against, if the table had one.
    /// Deletes newer than it fail the commit.
    starting_snapshot_id: Option<i64>,
}

impl RewriteFilesAction {
    pub(crate) fn new(starting_snapshot_id: Option<i64>) -> Self {
        Self {
            added_data_files: Vec::new(),
            deleted_data_files: Vec::new(),
            starting_snapshot_id,
        }
    }

    /// Register a data file to be removed from the table.
    ///
    /// The file must exist in the current snapshot; otherwise the commit
    /// will fail with a validation error.
    pub fn delete_file(mut self, file: DataFile) -> Self {
        self.deleted_data_files.push(file);
        self
    }

    /// Register a data file to be added to the table.
    ///
    /// Typically this is the merged output of the files being deleted.
    pub fn add_file(mut self, file: DataFile) -> Self {
        self.added_data_files.push(file);
        self
    }

    fn validate(&self) -> Result<()> {
        if self.deleted_data_files.is_empty() {
            return Err(invalid_data!(
                "Rewrite files requires at least one file to delete"
            ));
        }
        if let Some(file) = self
            .added_data_files
            .iter()
            .find(|f| f.content_type() != DataContentType::Data)
        {
            return Err(invalid_data!(
                "Only data content type is allowed for rewrite files, got {:?} for {}",
                file.content_type(),
                file.file_path()
            ));
        }
        Ok(())
    }

    /// Reject the rewrite when a delete manifest was added after the snapshot
    /// it was planned against.
    ///
    /// Java re-checks those deletes file by file in
    /// `validateNoNewDeletesForDataFiles`. Until that exists here the commit
    /// fails closed, because a rewritten file carries the deletes of the files
    /// it replaces only if nothing was deleted from them in the meantime.
    async fn validate_no_new_deletes(&self, table: &Table) -> Result<()> {
        // Nothing has been committed, so there is nothing to conflict with.
        // The filter rejects the rewrite for its missing sources instead.
        let Some(current_snapshot) = table.metadata().current_snapshot() else {
            return Ok(());
        };
        // The rewrite was planned against a table without a snapshot, so every
        // snapshot it has now, deletes included, landed after that.
        let Some(starting_snapshot_id) = self.starting_snapshot_id else {
            return Err(invalid_data!(
                "Cannot rewrite files: the rewrite was planned against a table with no snapshot, and the table has been committed to since"
            ));
        };
        let Some(starting_snapshot) = table.metadata().snapshot_by_id(starting_snapshot_id) else {
            // Without the starting snapshot there is no sequence number to
            // compare against, so no delete can be ruled out.
            return Err(invalid_data!(
                "Cannot rewrite files: the starting snapshot {starting_snapshot_id} is no longer in the table, so deletes added since it cannot be ruled out"
            ));
        };
        let starting_sequence_number = starting_snapshot.sequence_number();

        let manifest_list = table.manifest_list_reader(current_snapshot).load().await?;
        let conflict = manifest_list.entries().iter().find(|entry| {
            entry.content == ManifestContentType::Deletes
                && entry.sequence_number > starting_sequence_number
        });

        match conflict {
            Some(entry) => Err(invalid_data!(
                "Cannot rewrite files: delete manifest {} was added after the starting snapshot {starting_snapshot_id}",
                entry.manifest_path,
            )),
            None => Ok(()),
        }
    }
}

#[async_trait]
impl TransactionAction for RewriteFilesAction {
    type State = MergingSnapshotProducer;

    fn new_state(&self) -> MergingSnapshotProducer {
        MergingSnapshotProducer::new(MergingIntent {
            commit_uuid: None,
            operation: Operation::Replace,
            snapshot_properties: HashMap::new(),
            added_data_files: self.added_data_files.clone(),
            added_delete_files: Vec::new(),
            removed_data_files: self.deleted_data_files.clone(),
            removed_delete_files: Vec::new(),
            truncate_full_table: false,
        })
    }

    async fn commit(
        &self,
        state: &mut MergingSnapshotProducer,
        table: &Table,
    ) -> Result<ActionCommit> {
        self.validate()?;
        self.validate_no_new_deletes(table).await?;
        validate_files_not_live(table, &self.added_data_files).await?;
        state.apply(table).await
    }

    async fn cleanup(self, _state: MergingSnapshotProducer, _table: &Table, _status: CommitStatus) {
        // TODO(#3417): remove the producer's uncommitted artifacts once terminal
        // cleanup is wired up.
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use uuid::Uuid;

    use super::*;
    use crate::memory::tests::new_memory_catalog;
    use crate::spec::{
        DataContentType, DataFileBuilder, DataFileFormat, Datum, FormatVersion, Literal,
        ManifestEntryRef, ManifestFile, ManifestListWriter, ManifestStatus, ManifestWriterBuilder,
        NestedField, Operation, PrimitiveType, Schema, SnapshotRef, Struct, Transform, Type,
        UnboundPartitionField, UnboundPartitionSpec,
    };
    use crate::table::Table;
    use crate::test_utils::commit_with_fresh_state;
    use crate::transaction::tests::{
        append_files, make_data_file, make_v1_minimal_table_in_catalog,
        make_v2_minimal_table_in_catalog, make_v3_minimal_table_in_catalog,
    };
    use crate::transaction::{ApplyTransactionAction, Transaction};
    use crate::{Catalog, ErrorKind, NamespaceIdent, TableCommit, TableCreation, TableUpdate};

    /// Read back the manifest entry for `path` from the manifests of `snapshot`.
    async fn find_entry(
        table: &Table,
        snapshot: &SnapshotRef,
        path: &str,
    ) -> Option<ManifestEntryRef> {
        let manifest_list = table.manifest_list_reader(snapshot).load().await.unwrap();
        for manifest_file in manifest_list.entries() {
            let manifest = table.manifest_reader().read(manifest_file).await.unwrap();
            if let Some(entry) = manifest.entries().iter().find(|e| e.file_path() == path) {
                return Some(entry.clone());
            }
        }
        None
    }

    /// E2E: Compact 3 small files into 1 merged file.
    /// Verify: operation=Replace, file counts, record counts.
    #[tokio::test]
    async fn test_rewrite_files_compaction() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        rewrite_files_compaction(&catalog, table).await;
    }

    #[tokio::test]
    async fn test_rewrite_files_compaction_on_v1_table() {
        let catalog = new_memory_catalog().await;
        let table = make_v1_minimal_table_in_catalog(&catalog).await;
        rewrite_files_compaction(&catalog, table).await;
    }

    #[tokio::test]
    async fn test_rewrite_files_compaction_on_v2_table() {
        let catalog = new_memory_catalog().await;
        let table = make_v2_minimal_table_in_catalog(&catalog).await;
        rewrite_files_compaction(&catalog, table).await;
    }

    async fn rewrite_files_compaction(catalog: &impl Catalog, table: Table) {
        // Append 3 small files (10 records each).
        let f1 = make_data_file(&table, "test/1.parquet", 10, 100);
        let f2 = make_data_file(&table, "test/2.parquet", 10, 100);
        let f3 = make_data_file(&table, "test/3.parquet", 10, 100);
        let table = append_files(catalog, &table, vec![f1.clone(), f2.clone(), f3.clone()]).await;

        // Verify pre-compaction state.
        let summary = &table
            .metadata()
            .current_snapshot()
            .unwrap()
            .summary()
            .additional_properties;
        assert_eq!(summary.get("total-data-files").unwrap(), "3");
        assert_eq!(summary.get("total-records").unwrap(), "30");

        // Rewrite: delete 3 files, add 1 merged file (30 records).
        let merged = make_data_file(&table, "test/merged.parquet", 30, 300);
        let tx = Transaction::new(&table);
        let action = tx
            .rewrite_files()
            .delete_file(f1)
            .delete_file(f2)
            .delete_file(f3)
            .add_file(merged);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(catalog).await.unwrap();

        // Verify post-compaction snapshot.
        let snapshot = table.metadata().current_snapshot().unwrap();
        assert_eq!(snapshot.summary().operation, Operation::Replace);

        let summary = &snapshot.summary().additional_properties;
        assert_eq!(summary.get("total-data-files").unwrap(), "1");
        assert_eq!(summary.get("total-records").unwrap(), "30");
        assert_eq!(summary.get("added-data-files").unwrap(), "1");
        assert_eq!(summary.get("deleted-data-files").unwrap(), "3");

        // Verify manifest list: merged file is the only live file.
        let manifest_list = table.manifest_list_reader(snapshot).load().await.unwrap();
        let mut live_files: Vec<String> = Vec::new();
        for manifest_entry in manifest_list.entries() {
            let manifest = table.manifest_reader().read(manifest_entry).await.unwrap();
            for entry in manifest.entries() {
                if entry.is_alive() {
                    live_files.push(entry.file_path().to_string());
                }
            }
        }
        assert_eq!(live_files, vec!["test/merged.parquet"]);
    }

    /// Rewrite with a non-existent delete target should fail.
    #[tokio::test]
    async fn test_rewrite_files_missing_delete_target() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        // Append 1 file.
        let f1 = make_data_file(&table, "test/1.parquet", 10, 100);
        let table = append_files(&catalog, &table, vec![f1]).await;

        // Try to delete a file that doesn't exist.
        let ghost = make_data_file(&table, "test/ghost.parquet", 10, 100);
        let merged = make_data_file(&table, "test/merged.parquet", 10, 100);
        let tx = Transaction::new(&table);
        let action = tx.rewrite_files().delete_file(ghost).add_file(merged);
        let tx = action.apply(tx).unwrap();
        let result = tx.commit(&catalog).await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.message()
                .contains("Cannot remove files that are not live in the table"),
            "unexpected error: {}",
            err.message()
        );
    }

    /// Rewrite with no deletes should fail validation.
    #[tokio::test]
    async fn test_rewrite_files_no_deletes() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        let merged = make_data_file(&table, "test/merged.parquet", 10, 100);
        let tx = Transaction::new(&table);
        let action = tx.rewrite_files().add_file(merged);
        let tx = action.apply(tx).unwrap();
        let result = tx.commit(&catalog).await;

        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .message()
                .contains("at least one file to delete")
        );
    }

    /// Partial rewrite: delete 2 of 3 files, keeping one.
    /// Verifies that the rewritten manifest for the surviving file
    /// has the correct snapshot_id for sequence number assignment.
    #[tokio::test]
    async fn test_rewrite_files_partial() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        rewrite_files_partial(&catalog, table).await;
    }

    #[tokio::test]
    async fn test_rewrite_files_partial_on_v1_table() {
        let catalog = new_memory_catalog().await;
        let table = make_v1_minimal_table_in_catalog(&catalog).await;
        rewrite_files_partial(&catalog, table).await;
    }

    #[tokio::test]
    async fn test_rewrite_files_partial_on_v2_table() {
        let catalog = new_memory_catalog().await;
        let table = make_v2_minimal_table_in_catalog(&catalog).await;
        rewrite_files_partial(&catalog, table).await;
    }

    async fn rewrite_files_partial(catalog: &impl Catalog, table: Table) {
        let f1 = make_data_file(&table, "test/1.parquet", 10, 100);
        let f2 = make_data_file(&table, "test/2.parquet", 10, 100);
        let f3 = make_data_file(&table, "test/3.parquet", 10, 100);
        let table = append_files(catalog, &table, vec![f1.clone(), f2.clone(), f3.clone()]).await;
        let append_snapshot = table.metadata().current_snapshot().unwrap().clone();

        // Rewrite only f1 and f2, keep f3.
        let merged = make_data_file(&table, "test/merged.parquet", 20, 200);
        let tx = Transaction::new(&table);
        let action = tx
            .rewrite_files()
            .delete_file(f1)
            .delete_file(f2)
            .add_file(merged);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(catalog).await.unwrap();

        let snapshot = table.metadata().current_snapshot().unwrap();
        assert_eq!(snapshot.summary().operation, Operation::Replace);

        let summary = &snapshot.summary().additional_properties;
        assert_eq!(summary.get("total-data-files").unwrap(), "2");
        assert_eq!(summary.get("total-records").unwrap(), "30");
        assert_eq!(summary.get("deleted-data-files").unwrap(), "2");
        assert_eq!(summary.get("added-data-files").unwrap(), "1");

        // Verify live files: f3 (surviving) + merged (new).
        let manifest_list = table.manifest_list_reader(snapshot).load().await.unwrap();
        let mut live_files: Vec<String> = Vec::new();
        for manifest_entry in manifest_list.entries() {
            let manifest = table.manifest_reader().read(manifest_entry).await.unwrap();
            for entry in manifest.entries() {
                if entry.is_alive() {
                    live_files.push(entry.file_path().to_string());
                }
            }
        }
        live_files.sort();
        assert_eq!(live_files, vec!["test/3.parquet", "test/merged.parquet"]);

        let survivor = find_entry(&table, snapshot, "test/3.parquet")
            .await
            .expect("surviving file should still be in a manifest");
        assert_eq!(survivor.status(), ManifestStatus::Existing);
        assert_eq!(survivor.snapshot_id(), Some(append_snapshot.snapshot_id()));
        assert_eq!(
            survivor.sequence_number(),
            Some(append_snapshot.sequence_number())
        );
        assert_eq!(
            survivor.file_sequence_number,
            Some(append_snapshot.sequence_number())
        );
    }

    /// Rewrite on an empty table (no snapshot) should fail because
    /// the delete target doesn't exist.
    #[tokio::test]
    async fn test_rewrite_files_empty_table() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        let f1 = make_data_file(&table, "test/1.parquet", 10, 100);
        let merged = make_data_file(&table, "test/merged.parquet", 10, 100);
        let tx = Transaction::new(&table);
        let action = tx.rewrite_files().delete_file(f1).add_file(merged);
        let tx = action.apply(tx).unwrap();
        let result = tx.commit(&catalog).await;

        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .message()
                .contains("Cannot remove files that are not live in the table"),
        );
    }

    /// Add a delete manifest, attributed to the table's current snapshot, to
    /// that snapshot's manifest list.
    async fn add_delete_manifest(table: &Table) -> String {
        let snapshot = table.metadata().current_snapshot().unwrap();
        let output = table
            .file_io()
            .new_output(format!(
                "{}/deletes-{}.avro",
                table.metadata().metadata_location().unwrap(),
                Uuid::new_v4()
            ))
            .unwrap();
        let mut writer = ManifestWriterBuilder::new(
            output,
            Some(snapshot.snapshot_id()),
            table.metadata().current_schema().clone(),
            table.metadata().default_partition_spec().as_ref().clone(),
        )
        .build_v3_deletes();
        let mut delete_file = make_data_file(table, "test/1-deletes.parquet", 1, 100);
        delete_file.content = DataContentType::PositionDeletes;
        writer
            .add_file(delete_file, snapshot.sequence_number())
            .unwrap();
        let delete_manifest = writer.write_manifest_file().await.unwrap();
        let delete_manifest_path = delete_manifest.manifest_path.clone();

        append_to_manifest_list(table, delete_manifest).await;
        delete_manifest_path
    }

    /// Rewrite the manifest list of the table's current snapshot so it also
    /// contains `manifest`.
    async fn append_to_manifest_list(table: &Table, manifest: ManifestFile) {
        let snapshot = table.metadata().current_snapshot().unwrap();
        let mut entries: Vec<ManifestFile> = table
            .manifest_list_reader(snapshot)
            .load()
            .await
            .unwrap()
            .consume_entries()
            .into_iter()
            .collect();
        entries.push(manifest);

        let mut manifest_list_writer = ManifestListWriter::v3(
            table
                .file_io()
                .new_output(snapshot.manifest_list())
                .unwrap()
                .writer()
                .await
                .unwrap(),
            snapshot.snapshot_id(),
            snapshot.parent_snapshot_id(),
            snapshot.sequence_number(),
            Some(table.metadata().next_row_id()),
        );
        manifest_list_writer
            .add_manifests(entries.into_iter())
            .unwrap();
        manifest_list_writer.close().await.unwrap();
    }

    /// A delete manifest added after the snapshot the rewrite was planned
    /// against must stop the commit.
    #[tokio::test]
    async fn test_rewrite_files_rejects_deletes_added_after_starting_snapshot() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        let f1 = make_data_file(&table, "test/1.parquet", 10, 100);
        let f2 = make_data_file(&table, "test/2.parquet", 10, 100);
        let table = append_files(&catalog, &table, vec![f1.clone()]).await;

        // Plan the rewrite against this snapshot, then let another commit land.
        let tx = Transaction::new(&table);
        let merged = make_data_file(&table, "test/merged.parquet", 10, 100);
        let action = tx.rewrite_files().delete_file(f1).add_file(merged);

        let table = append_files(&catalog, &table, vec![f2]).await;
        let delete_manifest_path = add_delete_manifest(&table).await;

        let Err(err) = commit_with_fresh_state(action, &table).await else {
            panic!("a delete manifest newer than the starting snapshot must be rejected");
        };
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(
            err.to_string().contains(&format!(
                "delete manifest {delete_manifest_path} was added after"
            )),
            "{err}"
        );
    }

    /// A rewrite planned against a table with no snapshot knows nothing about
    /// what the table holds once something is committed to it.
    #[tokio::test]
    async fn test_rewrite_files_rejects_commit_after_empty_starting_table() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        // Planned before the table had any snapshot.
        let f1 = make_data_file(&table, "test/1.parquet", 10, 100);
        let merged = make_data_file(&table, "test/merged.parquet", 10, 100);
        let tx = Transaction::new(&table);
        let action = tx.rewrite_files().delete_file(f1.clone()).add_file(merged);

        // The file it wants to replace only exists because of a later commit.
        let table = append_files(&catalog, &table, vec![f1]).await;

        let Err(err) = commit_with_fresh_state(action, &table).await else {
            panic!("a rewrite planned against an empty table must be rejected");
        };
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(
            err.to_string()
                .contains("planned against a table with no snapshot"),
            "{err}"
        );
    }

    /// A rewrite whose starting snapshot has been expired cannot rule out
    /// deletes added since, and says so rather than comparing against zero.
    #[tokio::test]
    async fn test_rewrite_files_rejects_expired_starting_snapshot() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        let f1 = make_data_file(&table, "test/1.parquet", 10, 100);
        let f2 = make_data_file(&table, "test/2.parquet", 10, 100);
        let table = append_files(&catalog, &table, vec![f1.clone()]).await;
        let starting_snapshot_id = table.metadata().current_snapshot().unwrap().snapshot_id();

        // Plan the rewrite against this snapshot, then expire it behind us.
        let tx = Transaction::new(&table);
        let merged = make_data_file(&table, "test/merged.parquet", 10, 100);
        let action = tx.rewrite_files().delete_file(f1).add_file(merged);

        let table = append_files(&catalog, &table, vec![f2]).await;
        let tx = Transaction::new(&table);
        let tx = tx
            .expire_snapshots()
            .expire_snapshot_ids([starting_snapshot_id])
            .apply(tx)
            .unwrap();
        let table = tx.commit(&catalog).await.unwrap();
        assert!(
            table
                .metadata()
                .snapshot_by_id(starting_snapshot_id)
                .is_none(),
            "the starting snapshot should have been expired"
        );

        let Err(err) = commit_with_fresh_state(action, &table).await else {
            panic!("an expired starting snapshot must be rejected");
        };
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(
            err.to_string().contains(&format!(
                "the starting snapshot {starting_snapshot_id} is no longer in the table"
            )),
            "{err}"
        );
    }

    /// An added file whose partition value does not fit the default spec is
    /// rejected before anything is written.
    #[tokio::test]
    async fn test_rewrite_files_rejects_incompatible_partition_value() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        let f1 = make_data_file(&table, "test/1.parquet", 10, 100);
        let table = append_files(&catalog, &table, vec![f1.clone()]).await;

        let mut merged = make_data_file(&table, "test/merged.parquet", 10, 100);
        merged.partition = Struct::from_iter([Some(Literal::string("not-a-long"))]);
        let tx = Transaction::new(&table);
        let action = tx.rewrite_files().delete_file(f1).add_file(merged);
        let tx = action.apply(tx).unwrap();
        let err = tx.commit(&catalog).await.unwrap_err();

        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(
            err.to_string()
                .contains("Partition value is not compatible partition type"),
            "{err}"
        );
    }

    /// A delete file handed to `add_file` is rejected: a rewrite replaces data
    /// files, and the action's validation names the offending file.
    #[tokio::test]
    async fn test_rewrite_files_rejects_non_data_content_type() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        let f1 = make_data_file(&table, "test/1.parquet", 10, 100);
        let table = append_files(&catalog, &table, vec![f1.clone()]).await;

        let mut deletes = make_data_file(&table, "test/1-deletes.parquet", 1, 100);
        deletes.content = DataContentType::PositionDeletes;
        let tx = Transaction::new(&table);
        let action = tx.rewrite_files().delete_file(f1).add_file(deletes);
        let tx = action.apply(tx).unwrap();
        let err = tx.commit(&catalog).await.unwrap_err();

        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(
            err.to_string().contains(
                "Only data content type is allowed for rewrite files, got PositionDeletes for test/1-deletes.parquet"
            ),
            "{err}"
        );
    }

    /// An added file that is already live in the current snapshot is rejected.
    #[tokio::test]
    async fn test_rewrite_files_rejects_already_referenced_file() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        let f1 = make_data_file(&table, "test/1.parquet", 10, 100);
        let f2 = make_data_file(&table, "test/2.parquet", 10, 100);
        let table = append_files(&catalog, &table, vec![f1.clone(), f2.clone()]).await;

        let tx = Transaction::new(&table);
        let action = tx.rewrite_files().delete_file(f1).add_file(f2);
        let tx = action.apply(tx).unwrap();
        let err = tx.commit(&catalog).await.unwrap_err();

        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(
            err.to_string().contains(
                "Cannot add files that are already referenced by table, files: test/2.parquet"
            ),
            "{err}"
        );
    }

    #[tokio::test]
    async fn test_rewrite_files_without_output_files() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let removed = make_data_file(&table, "test/removed.parquet", 10, 100);
        let kept = make_data_file(&table, "test/kept.parquet", 5, 50);
        let table = append_files(&catalog, &table, vec![removed.clone(), kept]).await;
        let tx = Transaction::new(&table);
        let table = tx
            .rewrite_files()
            .delete_file(removed)
            .apply(tx)
            .unwrap()
            .commit(&catalog)
            .await
            .unwrap();
        let snapshot = table.metadata().current_snapshot().unwrap();
        let summary = &snapshot.summary().additional_properties;
        assert_eq!(summary.get("deleted-data-files").unwrap(), "1");
        assert_eq!(summary.get("total-data-files").unwrap(), "1");
        assert_eq!(summary.get("total-records").unwrap(), "5");
        assert_eq!(summary.get("total-files-size").unwrap(), "50");
        assert_eq!(
            find_entry(&table, snapshot, "test/removed.parquet")
                .await
                .unwrap()
                .status(),
            ManifestStatus::Deleted
        );
        assert_eq!(
            find_entry(&table, snapshot, "test/kept.parquet")
                .await
                .unwrap()
                .status(),
            ManifestStatus::Existing
        );
    }

    // Partition-spec evolution: `ts` is identity-partitioned under spec 0, then
    // `day(ts)` becomes the default spec. Old files keep spec 0 and a
    // timestamptz partition value; new files carry spec 1 and a date.

    /// 2026-08-24T09:30:00Z and 2026-08-24T17:45:00Z, in microseconds.
    const TS_MORNING: i64 = 1_787_563_800_000_000;
    const TS_EVENING: i64 = 1_787_593_500_000_000;
    /// 2026-08-24, in days since the epoch.
    const DAY: i32 = 20_689;

    /// A table partitioned by `identity(ts)`, still on its first spec.
    async fn make_identity_ts_table(
        catalog: &impl Catalog,
        format_version: FormatVersion,
    ) -> Table {
        let namespace = NamespaceIdent::new(format!("ns1-{}", Uuid::new_v4()));
        catalog
            .create_namespace(&namespace, HashMap::new())
            .await
            .unwrap();

        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::required(2, "ts", Type::Primitive(PrimitiveType::Timestamptz)).into(),
            ])
            .build()
            .unwrap();
        let spec = UnboundPartitionSpec::builder()
            .add_partition_field(
                UnboundPartitionField::builder()
                    .source_ids(vec![2])
                    .name("ts")
                    .transform(Transform::Identity)
                    .build()
                    .unwrap(),
            )
            .unwrap()
            .build();
        let creation = TableCreation::builder()
            .name("events".to_string())
            .schema(schema)
            .partition_spec(spec)
            .format_version(format_version)
            .build();
        catalog.create_table(&namespace, creation).await.unwrap()
    }

    /// Replace `identity(ts)` with `day(ts)` as the default spec.
    async fn evolve_to_day_ts(catalog: &impl Catalog, table: &Table) -> Table {
        let spec = UnboundPartitionSpec::builder()
            .add_partition_field(
                UnboundPartitionField::builder()
                    .source_ids(vec![2])
                    .name("ts_day")
                    .transform(Transform::Day)
                    .build()
                    .unwrap(),
            )
            .unwrap()
            .build();
        let commit = TableCommit::builder()
            .ident(table.identifier().clone())
            .requirements(vec![])
            .updates(vec![
                TableUpdate::AddSpec { spec },
                TableUpdate::SetDefaultSpec { spec_id: -1 },
            ])
            .build();
        let table = catalog.update_table(commit).await.unwrap();
        assert_eq!(table.metadata().default_partition_spec_id(), 1);
        table
    }

    fn make_file_in_spec(
        path: &str,
        records: u64,
        spec_id: i32,
        partition_value: Literal,
    ) -> DataFile {
        DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(path.to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(records * 10)
            .record_count(records)
            .partition(Struct::from_iter([Some(partition_value)]))
            .partition_spec_id(spec_id)
            .build()
            .unwrap()
    }

    /// Live files of the current snapshot, keyed by path, with the spec id of
    /// the manifest that lists them.
    async fn live_files_by_manifest_spec(table: &Table) -> Vec<(String, i32)> {
        let snapshot = table.metadata().current_snapshot().unwrap();
        let manifest_list = table.manifest_list_reader(snapshot).load().await.unwrap();
        let mut live = Vec::new();
        for manifest_file in manifest_list.entries() {
            let manifest = table.manifest_reader().read(manifest_file).await.unwrap();
            for entry in manifest.entries().iter().filter(|e| e.is_alive()) {
                assert_eq!(
                    entry.data_file().partition_spec_id,
                    manifest_file.partition_spec_id,
                    "{} is listed by a manifest of another spec",
                    entry.file_path()
                );
                live.push((
                    entry.file_path().to_string(),
                    manifest_file.partition_spec_id,
                ));
            }
        }
        live.sort();
        live
    }

    /// Two old-spec files and one new-spec file; returns the table and the
    /// snapshot that appended the old-spec files.
    async fn spec_evolved_table_with_files(
        catalog: &impl Catalog,
        format_version: FormatVersion,
    ) -> (Table, SnapshotRef, [DataFile; 3]) {
        let table = make_identity_ts_table(catalog, format_version).await;
        let old_1 = make_file_in_spec(
            "test/old-1.parquet",
            10,
            0,
            Literal::timestamptz(TS_MORNING),
        );
        let old_2 = make_file_in_spec(
            "test/old-2.parquet",
            10,
            0,
            Literal::timestamptz(TS_EVENING),
        );
        let table = append_files(catalog, &table, vec![old_1.clone(), old_2.clone()]).await;
        let old_snapshot = table.metadata().current_snapshot().unwrap().clone();

        let table = evolve_to_day_ts(catalog, &table).await;
        let new_1 = make_file_in_spec("test/new-1.parquet", 10, 1, Literal::date(DAY));
        let table = append_files(catalog, &table, vec![new_1.clone()]).await;
        (table, old_snapshot, [old_1, old_2, new_1])
    }

    /// Compacting one old-spec file keeps the other in a manifest written under
    /// spec 0: its entry, partition value and the manifest's partition summary
    /// are still those of `identity(ts)`, not re-encoded under `day(ts)`.
    #[tokio::test]
    async fn test_rewrite_files_keeps_survivors_on_their_partition_spec() {
        for format_version in [FormatVersion::V2, FormatVersion::V3] {
            let catalog = new_memory_catalog().await;
            let (table, old_snapshot, [old_1, _old_2, _new_1]) =
                spec_evolved_table_with_files(&catalog, format_version).await;

            let merged = make_file_in_spec("test/merged.parquet", 10, 1, Literal::date(DAY));
            let tx = Transaction::new(&table);
            let action = tx.rewrite_files().delete_file(old_1).add_file(merged);
            let table = action.apply(tx).unwrap().commit(&catalog).await.unwrap();

            assert_eq!(
                live_files_by_manifest_spec(&table).await,
                vec![
                    ("test/merged.parquet".to_string(), 1),
                    ("test/new-1.parquet".to_string(), 1),
                    ("test/old-2.parquet".to_string(), 0),
                ],
                "{format_version}"
            );

            let snapshot = table.metadata().current_snapshot().unwrap();
            let survivor = find_entry(&table, snapshot, "test/old-2.parquet")
                .await
                .unwrap();
            assert_eq!(survivor.status(), ManifestStatus::Existing);
            assert_eq!(survivor.snapshot_id(), Some(old_snapshot.snapshot_id()));
            assert_eq!(
                survivor.sequence_number(),
                Some(old_snapshot.sequence_number())
            );
            assert_eq!(
                survivor.data_file().partition,
                Struct::from_iter([Some(Literal::timestamptz(TS_EVENING))])
            );

            let manifest_list = table.manifest_list_reader(snapshot).load().await.unwrap();
            let old_spec_manifest = manifest_list
                .entries()
                .iter()
                .find(|m| m.partition_spec_id == 0)
                .unwrap();
            // The summary spans the deleted entry as well as the survivor,
            // as it does in Java, where every entry updates it.
            let summary = &old_spec_manifest.partitions.as_ref().unwrap()[0];
            let morning = Datum::timestamptz_micros(TS_MORNING).to_bytes().unwrap();
            let evening = Datum::timestamptz_micros(TS_EVENING).to_bytes().unwrap();
            assert_eq!(summary.lower_bound.as_ref(), Some(&morning));
            assert_eq!(summary.upper_bound.as_ref(), Some(&evening));

            let totals = &snapshot.summary().additional_properties;
            assert_eq!(totals.get("total-data-files").unwrap(), "3");
            assert_eq!(totals.get("total-records").unwrap(), "30");
        }
    }

    /// One rewrite removing a file from each spec: both manifests are filtered
    /// under their own spec and the merged file lands under the default spec.
    #[tokio::test]
    async fn test_rewrite_files_across_partition_specs() {
        for format_version in [FormatVersion::V2, FormatVersion::V3] {
            let catalog = new_memory_catalog().await;
            let (table, _, [old_1, _old_2, new_1]) =
                spec_evolved_table_with_files(&catalog, format_version).await;

            let merged = make_file_in_spec("test/merged.parquet", 20, 1, Literal::date(DAY));
            let tx = Transaction::new(&table);
            let action = tx
                .rewrite_files()
                .delete_file(old_1)
                .delete_file(new_1)
                .add_file(merged);
            let table = action.apply(tx).unwrap().commit(&catalog).await.unwrap();

            assert_eq!(
                live_files_by_manifest_spec(&table).await,
                vec![
                    ("test/merged.parquet".to_string(), 1),
                    ("test/old-2.parquet".to_string(), 0),
                ],
                "{format_version}"
            );
            let totals = &table
                .metadata()
                .current_snapshot()
                .unwrap()
                .summary()
                .additional_properties;
            assert_eq!(totals.get("deleted-data-files").unwrap(), "2");
            assert_eq!(totals.get("total-data-files").unwrap(), "2");
            assert_eq!(totals.get("total-records").unwrap(), "30");
        }
    }

    /// The summary counts and names the partitions a rewrite changed, each
    /// under its file's own spec: one `identity(ts)` partition lost a file, one
    /// `day(ts)` partition gained one.
    #[tokio::test]
    async fn test_rewrite_files_summarizes_partitions_of_both_specs() {
        let catalog = new_memory_catalog().await;
        let (table, _, [old_1, _old_2, _new_1]) =
            spec_evolved_table_with_files(&catalog, FormatVersion::V2).await;
        // Fast append reports the partition it changed.
        let appended = &table
            .metadata()
            .current_snapshot()
            .unwrap()
            .summary()
            .additional_properties;
        assert_eq!(
            appended.get("changed-partition-count").map(String::as_str),
            Some("1")
        );

        let tx = Transaction::new(&table);
        let tx = tx
            .update_table_properties()
            .set(
                "write.summary.partition-limit".to_string(),
                "10".to_string(),
            )
            .apply(tx)
            .unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        let merged = make_file_in_spec("test/merged.parquet", 10, 1, Literal::date(DAY));
        let tx = Transaction::new(&table);
        let action = tx.rewrite_files().delete_file(old_1).add_file(merged);
        let table = action.apply(tx).unwrap().commit(&catalog).await.unwrap();

        let summary = &table
            .metadata()
            .current_snapshot()
            .unwrap()
            .summary()
            .additional_properties;
        assert_eq!(
            summary.get("changed-partition-count").map(String::as_str),
            Some("2"),
            "{summary:?}"
        );
        // The removed file is named under `identity(ts)`, the added one under
        // `day(ts)`.
        let mut partitions: Vec<&str> = summary
            .keys()
            .filter_map(|k| k.strip_prefix("partitions."))
            .collect();
        partitions.sort();
        assert_eq!(
            partitions,
            vec!["ts=2026-08-24+09%3A30%3A00+UTC", "ts_day=2026-08-24"],
            "{summary:?}"
        );
    }
}
