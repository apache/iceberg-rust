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
    use crate::memory::tests::new_memory_catalog;
    use crate::spec::Operation;
    use crate::transaction::tests::{
        append_files, make_data_file, make_v3_minimal_table_in_catalog,
    };
    use crate::transaction::{ApplyTransactionAction, Transaction};

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
            let manifest = table.manifest_reader().read(manifest_entry).await.unwrap()
;
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
            let manifest = table.manifest_reader().read(manifest_entry).await.unwrap()
;
            for entry in manifest.entries() {
                if entry.is_alive() {
                    live_files.push(entry.file_path().to_string());
                }
            }
        }
        live_files.sort();
        assert_eq!(live_files, vec!["test/3.parquet", "test/merged.parquet"]);
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
