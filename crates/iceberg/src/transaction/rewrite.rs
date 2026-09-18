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

use std::sync::Arc;

use async_trait::async_trait;

use crate::error::Result;
use crate::spec::{DataFile, Operation};
use crate::table::Table;
use crate::transaction::merging::MergingSnapshotProducer;
use crate::transaction::{ActionCommit, TransactionAction};
use crate::{Error, ErrorKind};

/// A transaction action that rewrites (replaces) data files.
///
/// This is the Rust equivalent of Java's `BaseRewriteFiles`. It uses
/// [`MergingSnapshotProducer`] to handle manifest filtering and creation,
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
pub struct RewriteFilesAction {
    producer: MergingSnapshotProducer,
    /// The snapshot ID at which this rewrite started reading. Used to
    /// detect conflicting deletes added after this point.
    #[allow(dead_code)] // Will be used for conflict detection in a follow-up PR.
    starting_snapshot_id: Option<i64>,
}

impl RewriteFilesAction {
    pub(crate) fn new(starting_snapshot_id: Option<i64>) -> Self {
        Self {
            producer: MergingSnapshotProducer::new(Operation::Replace),
            starting_snapshot_id,
        }
    }

    /// Register a data file to be removed from the table.
    ///
    /// The file must exist in the current snapshot; otherwise the commit
    /// will fail with a validation error.
    pub fn delete_file(mut self, file: DataFile) -> Self {
        self.producer.delete_data_file(file);
        self
    }

    /// Register a data file to be added to the table.
    ///
    /// Typically this is the merged output of the files being deleted.
    pub fn add_file(mut self, file: DataFile) -> Self {
        self.producer.add_data_file(file);
        self
    }

    /// Set the data sequence number recorded for every added file.
    ///
    /// Without this the added files inherit the sequence number of the new
    /// snapshot. A compaction that must not shadow concurrently written
    /// deletes sets the sequence number of the files it replaces instead.
    /// V1 manifest entries carry no sequence number, so this has no effect on
    /// a V1 table.
    pub fn data_sequence_number(mut self, sequence_number: i64) -> Self {
        self.producer.set_data_sequence_number(sequence_number);
        self
    }

    fn validate(&self) -> Result<()> {
        if !self.producer.has_deleted_data_files() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Rewrite files requires at least one file to delete",
            ));
        }
        if !self.producer.has_added_data_files() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Rewrite files requires at least one file to add",
            ));
        }
        Ok(())
    }
}

#[async_trait]
impl TransactionAction for RewriteFilesAction {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        self.validate()?;
        self.producer.commit_snapshot(table).await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use uuid::Uuid;

    use super::*;
    use crate::memory::tests::new_memory_catalog;
    use crate::spec::{
        Literal, ManifestEntryRef, ManifestListWriter, ManifestStatus, ManifestWriterBuilder,
        Operation, SnapshotRef, Struct,
    };
    use crate::table::Table;
    use crate::transaction::tests::{
        append_files, make_data_file, make_v3_minimal_table_in_catalog,
    };
    use crate::transaction::{ApplyTransactionAction, Transaction};
    use crate::{ErrorKind, TableUpdate};

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

        // Append 3 small files (10 records each).
        let f1 = make_data_file(&table, "test/1.parquet", 10, 100);
        let f2 = make_data_file(&table, "test/2.parquet", 10, 100);
        let f3 = make_data_file(&table, "test/3.parquet", 10, 100);
        let table = append_files(&catalog, &table, vec![f1.clone(), f2.clone(), f3.clone()]).await;

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
        let table = tx.commit(&catalog).await.unwrap();

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
                .contains("Failed to find the following files to delete"),
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

        let f1 = make_data_file(&table, "test/1.parquet", 10, 100);
        let f2 = make_data_file(&table, "test/2.parquet", 10, 100);
        let f3 = make_data_file(&table, "test/3.parquet", 10, 100);
        let table = append_files(&catalog, &table, vec![f1.clone(), f2.clone(), f3.clone()]).await;
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
        let table = tx.commit(&catalog).await.unwrap();

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

    /// An explicit data sequence number is recorded on the added file instead
    /// of the one it would inherit from the rewrite snapshot.
    #[tokio::test]
    async fn test_rewrite_files_applies_data_sequence_number() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        let f1 = make_data_file(&table, "test/1.parquet", 10, 100);
        let table = append_files(&catalog, &table, vec![f1.clone()]).await;
        let append_sequence_number = table
            .metadata()
            .current_snapshot()
            .unwrap()
            .sequence_number();

        let merged = make_data_file(&table, "test/merged.parquet", 10, 100);
        let tx = Transaction::new(&table);
        let action = tx
            .rewrite_files()
            .delete_file(f1)
            .add_file(merged)
            .data_sequence_number(append_sequence_number);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        let snapshot = table.metadata().current_snapshot().unwrap();
        assert_ne!(snapshot.sequence_number(), append_sequence_number);

        let merged_entry = find_entry(&table, snapshot, "test/merged.parquet")
            .await
            .expect("added file should be in a manifest");
        assert_eq!(merged_entry.sequence_number(), Some(append_sequence_number));
    }

    /// A data sequence number above the one the new snapshot will carry is
    /// rejected.
    #[tokio::test]
    async fn test_rewrite_files_rejects_data_sequence_number_above_the_snapshot() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        let f1 = make_data_file(&table, "test/1.parquet", 10, 100);
        let table = append_files(&catalog, &table, vec![f1.clone()]).await;
        let snapshot_sequence_number = table.metadata().next_sequence_number();

        let merged = make_data_file(&table, "test/merged.parquet", 10, 100);
        let tx = Transaction::new(&table);
        let action = tx
            .rewrite_files()
            .delete_file(f1)
            .add_file(merged)
            .data_sequence_number(snapshot_sequence_number + 1);
        let tx = action.apply(tx).unwrap();
        let err = tx.commit(&catalog).await.unwrap_err();

        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(
            err.to_string().contains(&format!(
                "Data sequence number {} is greater than the snapshot's {snapshot_sequence_number}.",
                snapshot_sequence_number + 1
            )),
            "{err}"
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
                .contains("Failed to find the following files to delete"),
        );
    }

    /// Rewrite where all files in a manifest are deleted should omit
    /// the manifest entirely (not leave an empty one).
    #[tokio::test]
    async fn test_rewrite_files_removes_empty_manifest() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        // Append 1 file → 1 manifest.
        let f1 = make_data_file(&table, "test/1.parquet", 10, 100);
        let table = append_files(&catalog, &table, vec![f1.clone()]).await;

        // Rewrite: delete f1, add merged → f1's manifest should be omitted.
        let merged = make_data_file(&table, "test/merged.parquet", 10, 100);
        let tx = Transaction::new(&table);
        let action = tx.rewrite_files().delete_file(f1).add_file(merged);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        // Verify: only 1 manifest (the new one), not 2.
        let snapshot = table.metadata().current_snapshot().unwrap();
        let manifest_list = table.manifest_list_reader(snapshot).load().await.unwrap();
        assert_eq!(
            manifest_list.entries().len(),
            1,
            "empty manifest should be omitted, not kept"
        );
    }

    /// A manifest left with only deleted entries records nothing live, whether
    /// the current snapshot or an older one left it that way.
    #[tokio::test]
    async fn test_rewrite_files_drops_all_deleted_manifests() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        let f1 = make_data_file(&table, "test/1.parquet", 10, 100);
        let f2 = make_data_file(&table, "test/2.parquet", 10, 100);
        let table = append_files(&catalog, &table, vec![f1.clone()]).await;
        let table = append_files(&catalog, &table, vec![f2]).await;
        let current_snapshot_id = table.metadata().current_snapshot().unwrap().snapshot_id();
        let earlier_snapshot_id = table
            .metadata()
            .current_snapshot()
            .unwrap()
            .parent_snapshot_id()
            .unwrap();
        let older = add_delete_only_manifest(&table, earlier_snapshot_id).await;
        let current = add_delete_only_manifest(&table, current_snapshot_id).await;

        let merged = make_data_file(&table, "test/merged.parquet", 10, 100);
        let tx = Transaction::new(&table);
        let action = tx.rewrite_files().delete_file(f1).add_file(merged);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        let snapshot = table.metadata().current_snapshot().unwrap();
        let manifest_list = table.manifest_list_reader(snapshot).load().await.unwrap();
        for path in [older, current] {
            assert!(
                !manifest_list
                    .entries()
                    .iter()
                    .any(|m| m.manifest_path == path),
                "{path} holds no live entry and should have been dropped"
            );
        }
    }

    /// Add a delete-only manifest, attributed to `added_snapshot_id`, to the
    /// manifest list of the table's current snapshot, and return its path.
    async fn add_delete_only_manifest(table: &Table, added_snapshot_id: i64) -> String {
        let snapshot = table.metadata().current_snapshot().unwrap();
        let added_sequence_number = table
            .metadata()
            .snapshot_by_id(added_snapshot_id)
            .unwrap()
            .sequence_number();
        let output = table
            .file_io()
            .new_output(format!(
                "{}/delete-only-{}.avro",
                table.metadata().metadata_location().unwrap(),
                Uuid::new_v4()
            ))
            .unwrap();
        let mut writer = ManifestWriterBuilder::new(
            output,
            Some(added_snapshot_id),
            table.metadata().current_schema().clone(),
            table.metadata().default_partition_spec().as_ref().clone(),
        )
        .build_v3_data();
        writer
            .add_delete_file(
                make_data_file(
                    table,
                    &format!("test/removed-{added_snapshot_id}.parquet"),
                    10,
                    100,
                ),
                added_sequence_number,
                Some(added_sequence_number),
            )
            .unwrap();
        let mut delete_manifest = writer.write_manifest_file().await.unwrap();
        // The manifest list writer only assigns sequence numbers to manifests of
        // the snapshot being written; this one belongs to an earlier snapshot.
        delete_manifest.sequence_number = added_sequence_number;
        delete_manifest.min_sequence_number = added_sequence_number;
        let delete_manifest_path = delete_manifest.manifest_path.clone();
        assert!(!delete_manifest.has_added_files());
        assert!(!delete_manifest.has_existing_files());

        let mut entries = table
            .manifest_list_reader(snapshot)
            .load()
            .await
            .unwrap()
            .consume_entries()
            .into_iter()
            .collect::<Vec<_>>();
        entries.push(delete_manifest);

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

        delete_manifest_path
    }

    /// A retried commit must not reuse the manifest paths of the attempt
    /// before it: the commit uuid is fixed, so only the counter separates them.
    #[tokio::test]
    async fn test_rewrite_files_writes_distinct_manifests_per_attempt() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        let f1 = make_data_file(&table, "test/1.parquet", 10, 100);
        let f2 = make_data_file(&table, "test/2.parquet", 10, 100);
        let table = append_files(&catalog, &table, vec![f1.clone(), f2.clone()]).await;

        let merged = make_data_file(&table, "test/merged.parquet", 10, 100);
        let tx = Transaction::new(&table);
        let action = Arc::new(tx.rewrite_files().delete_file(f1).add_file(merged));

        let first = manifest_paths(&table, Arc::clone(&action)).await;
        let second = manifest_paths(&table, action).await;

        assert_eq!(first.len(), 2, "{first:?}");
        assert_eq!(first.len(), second.len());
        for path in &first {
            assert!(!second.contains(path), "{path} was written twice");
        }
    }

    /// Commit `action` against `table` and return the paths of the manifests
    /// the resulting snapshot points at.
    async fn manifest_paths(table: &Table, action: Arc<RewriteFilesAction>) -> Vec<String> {
        let mut commit = action.commit(table).await.unwrap();
        let snapshot = commit
            .take_updates()
            .into_iter()
            .find_map(|update| match update {
                TableUpdate::AddSnapshot { snapshot } => Some(snapshot),
                _ => None,
            })
            .expect("commit should add a snapshot");

        let manifest_list = table
            .manifest_list_reader(&Arc::new(snapshot))
            .load()
            .await
            .unwrap();
        manifest_list
            .entries()
            .iter()
            .map(|entry| entry.manifest_path.clone())
            .collect()
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

    /// Rewrite with no adds should fail validation.
    #[tokio::test]
    async fn test_rewrite_files_no_adds() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        let f1 = make_data_file(&table, "test/1.parquet", 10, 100);
        let tx = Transaction::new(&table);
        let action = tx.rewrite_files().delete_file(f1);
        let tx = action.apply(tx).unwrap();
        let result = tx.commit(&catalog).await;

        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .message()
                .contains("at least one file to add")
        );
    }
}
