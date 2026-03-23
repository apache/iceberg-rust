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
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use uuid::Uuid;

use crate::{Error, ErrorKind};
use crate::error::Result;
use crate::spec::{
    DataFile, FormatVersion, ManifestContentType, ManifestEntry, ManifestFile,
    ManifestWriterBuilder, Operation,
};
use crate::table::Table;
use crate::transaction::snapshot::{
    DefaultManifestProcess, SnapshotProduceOperation, SnapshotProducer,
};
use crate::transaction::{ActionCommit, TransactionAction};

/// Action to rewrite data files by replacing a set of existing files with new ones.
///
/// This is used for data compaction: small data files are merged into larger ones,
/// and the old files are marked as deleted in rewritten manifests. The operation
/// produces a new snapshot with `Operation::Replace`.
pub struct RewriteAction {
    check_duplicate: bool,
    commit_uuid: Option<Uuid>,
    key_metadata: Option<Vec<u8>>,
    snapshot_properties: HashMap<String, String>,
    added_data_files: Vec<DataFile>,
    deleted_data_files: Vec<DataFile>,
}

impl RewriteAction {
    pub(crate) fn new() -> Self {
        Self {
            check_duplicate: true,
            commit_uuid: None,
            key_metadata: None,
            snapshot_properties: HashMap::default(),
            added_data_files: vec![],
            deleted_data_files: vec![],
        }
    }

    /// Set whether to check duplicate files.
    pub fn with_check_duplicate(mut self, v: bool) -> Self {
        self.check_duplicate = v;
        self
    }

    /// Add data files to the snapshot.
    pub fn add_data_files(mut self, data_files: impl IntoIterator<Item = DataFile>) -> Self {
        self.added_data_files.extend(data_files);
        self
    }

    /// Specify data files to be removed from the table in this rewrite.
    pub fn delete_data_files(mut self, data_files: impl IntoIterator<Item = DataFile>) -> Self {
        self.deleted_data_files.extend(data_files);
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
}

#[async_trait]
impl TransactionAction for RewriteAction {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        let commit_uuid = self.commit_uuid.unwrap_or_else(Uuid::now_v7);

        let snapshot_producer = SnapshotProducer::new(
            table,
            commit_uuid,
            self.key_metadata.clone(),
            self.snapshot_properties.clone(),
            self.added_data_files.clone(),
            self.deleted_data_files.clone(),
        );

        snapshot_producer.validate_added_data_files()?;

        if self.check_duplicate {
            snapshot_producer.validate_duplicate_files().await?;
        }

        let deleted_file_paths: HashSet<String> = self
            .deleted_data_files
            .iter()
            .map(|f| f.file_path.clone())
            .collect();

        let snapshot_id = snapshot_producer.snapshot_id();
        snapshot_producer
            .commit(
                RewriteOperation {
                    deleted_file_paths,
                    snapshot_id,
                    commit_uuid,
                    manifest_counter: AtomicU64::new(0),
                },
                DefaultManifestProcess,
            )
            .await
    }
}

/// Internal operation that drives `RewriteAction` snapshot production.
///
/// Carries the set of file paths to mark as deleted and rewrites the
/// affected manifests while keeping unaffected manifests unchanged.
struct RewriteOperation {
    deleted_file_paths: HashSet<String>,
    snapshot_id: i64,
    commit_uuid: Uuid,
    manifest_counter: AtomicU64,
}

impl SnapshotProduceOperation for RewriteOperation {
    fn operation(&self) -> Operation {
        Operation::Replace
    }

    // TODO: Delete logic is handled inside `existing_manifest()` by rewriting
    // affected manifests inline, rather than through this trait method. If a
    // future change relies on `delete_entries` being populated (e.g., for
    // explicit delete-count tracking in snapshot summaries), this will need to
    // be refactored. See also: SnapshotProducer::summary() only counts added
    // files today.
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

        if self.deleted_file_paths.is_empty() {
            return Ok(manifest_list
                .entries()
                .iter()
                .filter(|entry| entry.has_added_files() || entry.has_existing_files())
                .cloned()
                .collect());
        }

        let mut result = Vec::new();
        let mut found_deleted_paths: HashSet<String> = HashSet::new();

        for manifest_file in manifest_list.entries() {
            if !manifest_file.has_added_files() && !manifest_file.has_existing_files() {
                continue;
            }

            let manifest = manifest_file
                .load_manifest(snapshot_produce.table.file_io())
                .await?;

            let mut has_deletes = false;
            for entry in manifest.entries() {
                if entry.is_alive()
                    && self.deleted_file_paths.contains(entry.file_path())
                {
                    found_deleted_paths.insert(entry.file_path().to_string());
                    has_deletes = true;
                }
            }

            if has_deletes {
                let rewritten = self
                    .rewrite_manifest(snapshot_produce, manifest_file, &manifest)
                    .await?;
                result.push(rewritten);
            } else {
                result.push(manifest_file.clone());
            }
        }

        // Validate that all requested delete files were actually found
        let mut missing: Vec<&String> = self
            .deleted_file_paths
            .iter()
            .filter(|p| !found_deleted_paths.contains(p.as_str()))
            .collect();
        missing.sort();
        if !missing.is_empty() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Cannot complete rewrite: delete files not found in any manifest: {:?}",
                    missing
                ),
            ));
        }

        Ok(result)
    }
}

impl RewriteOperation {
    /// Rewrite a manifest, marking entries whose file paths are in `deleted_file_paths`
    /// as `ManifestStatus::Deleted`.
    async fn rewrite_manifest(
        &self,
        snapshot_produce: &SnapshotProducer<'_>,
        manifest_file: &ManifestFile,
        manifest: &crate::spec::Manifest,
    ) -> Result<ManifestFile> {
        let table = snapshot_produce.table;

        let partition_spec = table
            .metadata()
            .partition_spec_by_id(manifest_file.partition_spec_id)
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Partition spec not found for id: {}",
                        manifest_file.partition_spec_id
                    ),
                )
            })?;

        let counter = self.manifest_counter.fetch_add(1, Ordering::SeqCst);
        // Use "-m-rewrite{N}" suffix to avoid path collision with the
        // SnapshotProducer's added-file manifests which use "-m{N}".
        let new_manifest_path = format!(
            "{}/metadata/{}-m-rewrite{}.avro",
            table.metadata().location(),
            self.commit_uuid,
            counter,
        );
        let output_file = table.file_io().new_output(&new_manifest_path)?;
        let builder = ManifestWriterBuilder::new(
            output_file,
            Some(self.snapshot_id),
            manifest_file.key_metadata.clone(),
            table.metadata().current_schema().clone(),
            partition_spec.as_ref().clone(),
        );

        let mut writer = match table.metadata().format_version() {
            FormatVersion::V1 => builder.build_v1(),
            FormatVersion::V2 => match manifest_file.content {
                ManifestContentType::Data => builder.build_v2_data(),
                ManifestContentType::Deletes => builder.build_v2_deletes(),
            },
            FormatVersion::V3 => match manifest_file.content {
                ManifestContentType::Data => builder.build_v3_data(),
                ManifestContentType::Deletes => builder.build_v3_deletes(),
            },
        };

        for entry in manifest.entries() {
            if entry.is_alive() && self.deleted_file_paths.contains(entry.file_path()) {
                let deleted: ManifestEntry = (**entry).clone();
                let sequence_number = deleted.sequence_number().ok_or_else(|| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        format!(
                            "Manifest entry missing sequence_number for file: {}",
                            entry.file_path()
                        ),
                    )
                })?;
                writer.add_delete_file(
                    deleted.data_file.clone(),
                    sequence_number,
                    deleted.file_sequence_number
                )?;
            } else if entry.is_alive() {
                // Alive entry not in the delete set → carry forward as Existing
                let cloned: ManifestEntry = (**entry).clone();
                writer.add_existing_entry(cloned)?;
            }
            // Dead (already-deleted) entries are NOT carried forward into the
            // rewritten manifest. The old snapshot still references the original
            // manifest containing the Deleted entry, so time-travel is preserved.
        }

        writer.write_manifest_file().await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use crate::memory::tests::new_memory_catalog;
    use crate::spec::{MAIN_BRANCH, ManifestStatus, Operation};
    use crate::transaction::tests::{
        collect_entries, make_v1_minimal_table_in_catalog, make_v2_minimal_table,
        make_v2_minimal_table_in_catalog, make_v3_minimal_table_in_catalog, test_data_file,
    };
    use crate::transaction::{ApplyTransactionAction, Transaction, TransactionAction};
    use crate::{Catalog, TableRequirement, TableUpdate};

    /// Convenience wrapper: create a test data file with `record_count=1` and
    /// `partition_val=300` (the defaults used by most rewrite tests).
    fn file(path: &str, spec_id: i32) -> crate::spec::DataFile {
        test_data_file(path, spec_id, 1, 300)
    }

    /// Convenience wrapper: create a test data file with a custom record count.
    fn file_with_records(path: &str, spec_id: i32, record_count: u64) -> crate::spec::DataFile {
        test_data_file(path, spec_id, record_count, 300)
    }

    #[tokio::test]
    async fn test_empty_data_rewrite_action() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);
        let action = tx.rewrite().add_data_files(vec![]);
        assert!(Arc::new(action).commit(&table).await.is_err());
    }

    #[tokio::test]
    async fn test_rewrite_snapshot_properties() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);

        let mut snapshot_properties = HashMap::new();
        snapshot_properties.insert("key".to_string(), "val".to_string());

        let data_file = file(
            "test/1.parquet",
            table.metadata().default_partition_spec_id(),
        );

        let action = tx
            .rewrite()
            .set_snapshot_properties(snapshot_properties)
            .add_data_files(vec![data_file]);
        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();

        let new_snapshot = if let TableUpdate::AddSnapshot { snapshot } = &updates[0] {
            snapshot
        } else {
            unreachable!()
        };
        assert_eq!(
            new_snapshot
                .summary()
                .additional_properties
                .get("key")
                .unwrap(),
            "val"
        );
    }

    #[tokio::test]
    async fn test_rewrite_incompatible_partition_value() {
        use crate::spec::{DataContentType, DataFileBuilder, DataFileFormat, Literal, Struct};

        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);

        let data_file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("test/3.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::string("test"))]))
            .build()
            .unwrap();

        let action = tx.rewrite().add_data_files(vec![data_file]);
        assert!(Arc::new(action).commit(&table).await.is_err());
    }

    #[tokio::test]
    async fn test_rewrite_basic() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);

        let data_file = file(
            "test/3.parquet",
            table.metadata().default_partition_spec_id(),
        );

        let action = tx.rewrite().add_data_files(vec![data_file.clone()]);
        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();
        let requirements = action_commit.take_requirements();

        assert!(
            matches!((&updates[0],&updates[1]), (TableUpdate::AddSnapshot { snapshot },TableUpdate::SetSnapshotRef { reference,ref_name }) if snapshot.snapshot_id() == reference.snapshot_id && ref_name == MAIN_BRANCH)
        );

        assert_eq!(
            vec![
                TableRequirement::UuidMatch {
                    uuid: table.metadata().uuid()
                },
                TableRequirement::RefSnapshotIdMatch {
                    r#ref: MAIN_BRANCH.to_string(),
                    snapshot_id: table.metadata().current_snapshot_id
                }
            ],
            requirements
        );

        let new_snapshot = if let TableUpdate::AddSnapshot { snapshot } = &updates[0] {
            snapshot
        } else {
            unreachable!()
        };
        assert_eq!(new_snapshot.summary().operation, Operation::Replace);

        let manifest_list = new_snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();
        assert_eq!(1, manifest_list.entries().len());
        assert_eq!(
            manifest_list.entries()[0].sequence_number,
            new_snapshot.sequence_number()
        );

        let manifest = manifest_list.entries()[0]
            .load_manifest(table.file_io())
            .await
            .unwrap();
        assert_eq!(1, manifest.entries().len());
        assert_eq!(
            new_snapshot.sequence_number(),
            manifest.entries()[0]
                .sequence_number()
                .expect("Inherit sequence number by load manifest")
        );
        assert_eq!(
            new_snapshot.snapshot_id(),
            manifest.entries()[0].snapshot_id().unwrap()
        );
        assert_eq!(data_file, *manifest.entries()[0].data_file());
    }

    #[tokio::test]
    async fn test_rewrite_with_deleted_files() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let spec_id = table.metadata().default_partition_spec_id();

        let original_file = file("test/original.parquet", spec_id);
        let tx = Transaction::new(&table);
        let action = tx.fast_append().add_data_files(vec![original_file.clone()]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        let snapshot = table.metadata().current_snapshot().unwrap();
        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();
        assert_eq!(1, manifest_list.entries().len());

        let replacement_file = file("test/replacement.parquet", spec_id);
        let tx = Transaction::new(&table);
        let action = tx
            .rewrite()
            .add_data_files(vec![replacement_file.clone()])
            .delete_data_files(vec![original_file.clone()]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        let snapshot = table.metadata().current_snapshot().unwrap();
        assert_eq!(snapshot.summary().operation, Operation::Replace);

        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();

        assert_eq!(2, manifest_list.entries().len());

        let entries = collect_entries(&table).await;

        assert!(
            entries
                .iter()
                .any(|(p, s, _, _)| p == "test/original.parquet" && *s == ManifestStatus::Deleted),
            "Original file should be marked as Deleted, entries: {entries:?}",
        );

        assert!(
            entries
                .iter()
                .any(|(p, s, _, _)| p == "test/replacement.parquet" && *s == ManifestStatus::Added),
            "Replacement file should be marked as Added, entries: {entries:?}",
        );

        // Step 3: Fast append after rewrite.
        // The rewritten manifest containing only [original:Deleted] has
        // added_files_count=0 and existing_files_count=0, so the fast_append's
        // existing_manifest filter correctly drops it (only manifests with alive
        // entries are carried forward). The delete was recorded in the rewrite
        // snapshot and doesn't need to persist in subsequent snapshots.
        let appended_file = file("test/appended.parquet", spec_id);
        let tx = Transaction::new(&table);
        let action = tx.fast_append().add_data_files(vec![appended_file.clone()]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        let entries = collect_entries(&table).await;
        assert_eq!(2, entries.len());

        assert!(
            entries.iter().any(|(p, _, _, _)| p == "test/replacement.parquet"),
            "Replacement should be present after fast_append, entries: {entries:?}",
        );
        assert!(
            entries.iter().any(|(p, _, _, _)| p == "test/appended.parquet"),
            "Appended file should be present, entries: {entries:?}",
        );
    }

    // ========================================================================
    // New test cases for corner cases and additional coverage
    // ========================================================================

    /// Deleting a file that does not exist in any manifest should return an error.
    #[tokio::test]
    async fn test_rewrite_delete_file_not_found() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let spec_id = table.metadata().default_partition_spec_id();

        // Append a file so there is a current snapshot
        let file_a = file("test/a.parquet", spec_id);
        let tx = Transaction::new(&table);
        let action = tx.fast_append().add_data_files(vec![file_a.clone()]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        // Try to delete a file that does not exist
        let nonexistent = file("test/nonexistent.parquet", spec_id);
        let replacement = file("test/replacement.parquet", spec_id);
        let tx = Transaction::new(&table);
        let action = tx
            .rewrite()
            .add_data_files(vec![replacement])
            .delete_data_files(vec![nonexistent]);
        let tx = action.apply(tx).unwrap();
        let result = tx.commit(&catalog).await;

        assert!(result.is_err(), "Should error when delete file not found");
        let err = result.err().unwrap();
        assert!(
            err.to_string().contains("not found in any manifest"),
            "Error should mention file not found, got: {err}"
        );
    }

    /// Adding a file whose path already exists should error when duplicate checking is on.
    #[tokio::test]
    async fn test_rewrite_duplicate_file_detected() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let spec_id = table.metadata().default_partition_spec_id();

        // Append a file
        let file_a = file("test/a.parquet", spec_id);
        let tx = Transaction::new(&table);
        let action = tx.fast_append().add_data_files(vec![file_a.clone()]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        // Try to add the same file path again (duplicate)
        let tx = Transaction::new(&table);
        let action = tx.rewrite().add_data_files(vec![file_a.clone()]);
        let tx = action.apply(tx).unwrap();
        let result = tx.commit(&catalog).await;

        assert!(
            result.is_err(),
            "Should error when adding duplicate file path"
        );
        let err = result.err().unwrap();
        assert!(
            err.to_string().contains("already referenced"),
            "Error should mention duplicate, got: {err}"
        );
    }

    /// Disabling duplicate check should allow adding the same file path.
    #[tokio::test]
    async fn test_rewrite_duplicate_check_disabled() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let spec_id = table.metadata().default_partition_spec_id();

        // Append a file
        let file_a = file("test/a.parquet", spec_id);
        let tx = Transaction::new(&table);
        let action = tx.fast_append().add_data_files(vec![file_a.clone()]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        // Add same path again with duplicate check disabled
        let tx = Transaction::new(&table);
        let action = tx
            .rewrite()
            .with_check_duplicate(false)
            .add_data_files(vec![file_a.clone()]);
        let tx = action.apply(tx).unwrap();
        let result = tx.commit(&catalog).await;

        assert!(
            result.is_ok(),
            "Should succeed when duplicate check is disabled, got: {:?}",
            result.err()
        );
    }

    /// Deleting files that are spread across multiple manifests should work.
    #[tokio::test]
    async fn test_rewrite_deletes_across_multiple_manifests() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let spec_id = table.metadata().default_partition_spec_id();

        // Append files in separate commits → separate manifests
        let file_a = file("test/a.parquet", spec_id);
        let tx = Transaction::new(&table);
        let action = tx.fast_append().add_data_files(vec![file_a.clone()]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        let file_b = file("test/b.parquet", spec_id);
        let tx = Transaction::new(&table);
        let action = tx.fast_append().add_data_files(vec![file_b.clone()]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        // Delete both files from their respective manifests
        let replacement = file("test/merged.parquet", spec_id);
        let tx = Transaction::new(&table);
        let action = tx
            .rewrite()
            .add_data_files(vec![replacement.clone()])
            .delete_data_files(vec![file_a.clone(), file_b.clone()]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        let entries = collect_entries(&table).await;

        // Both originals should be Deleted
        assert!(
            entries
                .iter()
                .any(|(p, s, _, _)| p == "test/a.parquet" && *s == ManifestStatus::Deleted),
            "file_a should be Deleted, entries: {entries:?}"
        );
        assert!(
            entries
                .iter()
                .any(|(p, s, _, _)| p == "test/b.parquet" && *s == ManifestStatus::Deleted),
            "file_b should be Deleted, entries: {entries:?}"
        );

        // Replacement should be Added
        assert!(
            entries
                .iter()
                .any(|(p, s, _, _)| p == "test/merged.parquet" && *s == ManifestStatus::Added),
            "merged file should be Added, entries: {entries:?}"
        );
    }

    /// Deleting all entries in a manifest should still produce a valid manifest.
    #[tokio::test]
    async fn test_rewrite_delete_all_entries_in_manifest() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let spec_id = table.metadata().default_partition_spec_id();

        // Append two files in one manifest
        let file_a = file("test/a.parquet", spec_id);
        let file_b = file("test/b.parquet", spec_id);
        let tx = Transaction::new(&table);
        let action = tx
            .fast_append()
            .add_data_files(vec![file_a.clone(), file_b.clone()]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        // Delete both files (all entries in the manifest)
        let replacement = file("test/replacement.parquet", spec_id);
        let tx = Transaction::new(&table);
        let action = tx
            .rewrite()
            .add_data_files(vec![replacement.clone()])
            .delete_data_files(vec![file_a.clone(), file_b.clone()]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        let entries = collect_entries(&table).await;

        // Both originals should be Deleted
        let deleted_count = entries
            .iter()
            .filter(|(_, s, _, _)| *s == ManifestStatus::Deleted)
            .count();
        assert_eq!(deleted_count, 2, "Both files should be Deleted");

        // Replacement should be Added
        assert!(
            entries
                .iter()
                .any(|(p, s, _, _)| p == "test/replacement.parquet" && *s == ManifestStatus::Added),
            "Replacement should be Added, entries: {entries:?}"
        );
    }

    /// Only some entries in a manifest are deleted; others must be preserved as Existing.
    #[tokio::test]
    async fn test_rewrite_partial_deletes_in_manifest() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let spec_id = table.metadata().default_partition_spec_id();

        // Append three files in one manifest
        let file_a = file("test/a.parquet", spec_id);
        let file_b = file("test/b.parquet", spec_id);
        let file_c = file("test/c.parquet", spec_id);
        let tx = Transaction::new(&table);
        let action = tx
            .fast_append()
            .add_data_files(vec![file_a.clone(), file_b.clone(), file_c.clone()]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        // Delete only file_b, keep file_a and file_c
        let replacement = file("test/replacement.parquet", spec_id);
        let tx = Transaction::new(&table);
        let action = tx
            .rewrite()
            .add_data_files(vec![replacement.clone()])
            .delete_data_files(vec![file_b.clone()]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        let entries = collect_entries(&table).await;

        // file_b should be Deleted
        assert!(
            entries
                .iter()
                .any(|(p, s, _, _)| p == "test/b.parquet" && *s == ManifestStatus::Deleted),
            "file_b should be Deleted, entries: {entries:?}"
        );

        // file_a and file_c should be Existing (preserved in rewritten manifest)
        assert!(
            entries
                .iter()
                .any(|(p, s, _, _)| p == "test/a.parquet" && *s == ManifestStatus::Existing),
            "file_a should be Existing, entries: {entries:?}"
        );
        assert!(
            entries
                .iter()
                .any(|(p, s, _, _)| p == "test/c.parquet" && *s == ManifestStatus::Existing),
            "file_c should be Existing, entries: {entries:?}"
        );

        // replacement should be Added
        assert!(
            entries
                .iter()
                .any(|(p, s, _, _)| p == "test/replacement.parquet" && *s == ManifestStatus::Added),
            "replacement should be Added, entries: {entries:?}"
        );
    }

    /// Sequential rewrites: rewrite → rewrite should chain correctly.
    #[tokio::test]
    async fn test_rewrite_sequential_rewrites() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let spec_id = table.metadata().default_partition_spec_id();

        // Append initial files
        let file_a = file("test/a.parquet", spec_id);
        let file_b = file("test/b.parquet", spec_id);
        let tx = Transaction::new(&table);
        let action = tx
            .fast_append()
            .add_data_files(vec![file_a.clone(), file_b.clone()]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        // First rewrite: replace a → a2
        let file_a2 = file("test/a2.parquet", spec_id);
        let tx = Transaction::new(&table);
        let action = tx
            .rewrite()
            .add_data_files(vec![file_a2.clone()])
            .delete_data_files(vec![file_a.clone()]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        // Second rewrite: replace b → b2
        let file_b2 = file("test/b2.parquet", spec_id);
        let tx = Transaction::new(&table);
        let action = tx
            .rewrite()
            .add_data_files(vec![file_b2.clone()])
            .delete_data_files(vec![file_b.clone()]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        let entries = collect_entries(&table).await;

        // file_a was deleted in the first rewrite. When the second rewrite rewrites
        // the manifest containing [a:Deleted, b:Existing], the dead entry for a is
        // dropped (not carried forward), and b is marked Deleted.
        assert!(
            !entries.iter().any(|(p, _, _, _)| p == "test/a.parquet"),
            "file_a (dead entry) should be dropped from rewritten manifest, entries: {entries:?}"
        );
        assert!(
            entries
                .iter()
                .any(|(p, s, _, _)| p == "test/b.parquet" && *s == ManifestStatus::Deleted),
            "file_b should be Deleted, entries: {entries:?}"
        );

        // a2 and b2 should both be present (a2 as Existing, b2 as Added)
        let alive: Vec<_> = entries
            .iter()
            .filter(|(p, _, _, _)| p == "test/a2.parquet" || p == "test/b2.parquet")
            .collect();
        assert_eq!(alive.len(), 2, "Both a2 and b2 should be present, entries: {entries:?}");
    }

    /// Manifests without any deleted entries should be preserved unchanged (not rewritten).
    #[tokio::test]
    async fn test_rewrite_preserves_unaffected_manifests() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let spec_id = table.metadata().default_partition_spec_id();

        // Append files in separate commits → separate manifests
        let file_a = file("test/a.parquet", spec_id);
        let tx = Transaction::new(&table);
        let action = tx.fast_append().add_data_files(vec![file_a.clone()]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        let file_b = file("test/b.parquet", spec_id);
        let tx = Transaction::new(&table);
        let action = tx.fast_append().add_data_files(vec![file_b.clone()]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        // Get manifest paths before rewrite
        let snapshot = table.metadata().current_snapshot().unwrap();
        let manifest_list_before = snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();
        let paths_before: Vec<String> = manifest_list_before
            .entries()
            .iter()
            .map(|m| m.manifest_path.clone())
            .collect();

        // Rewrite: only delete file_a (in first manifest)
        let replacement = file("test/replacement.parquet", spec_id);
        let tx = Transaction::new(&table);
        let action = tx
            .rewrite()
            .add_data_files(vec![replacement.clone()])
            .delete_data_files(vec![file_a.clone()]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        // Get manifest paths after rewrite
        let snapshot = table.metadata().current_snapshot().unwrap();
        let manifest_list_after = snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();
        let paths_after: Vec<String> = manifest_list_after
            .entries()
            .iter()
            .map(|m| m.manifest_path.clone())
            .collect();

        // Find file_b's manifest by content, not by index
        let mut b_manifest_path = None;
        for mf in manifest_list_before.entries() {
            let manifest = mf.load_manifest(table.file_io()).await.unwrap();
            if manifest.entries().iter().any(|e| e.file_path() == "test/b.parquet") {
                b_manifest_path = Some(mf.manifest_path.clone());
                break;
            }
        }
        let b_manifest_path = b_manifest_path.expect("Should find manifest containing file_b");

        assert!(
            paths_after.contains(&b_manifest_path),
            "Unaffected manifest should be reused. Before: {paths_before:?}, After: {paths_after:?}"
        );
    }

    /// Summary should correctly report record counts for files with different row counts.
    #[tokio::test]
    async fn test_rewrite_summary_record_counts() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let spec_id = table.metadata().default_partition_spec_id();

        // Append files with known record counts
        let small_a = file_with_records("test/small_a.parquet", spec_id, 10);
        let small_b = file_with_records("test/small_b.parquet", spec_id, 20);
        let tx = Transaction::new(&table);
        let action = tx
            .fast_append()
            .add_data_files(vec![small_a.clone(), small_b.clone()]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        // Rewrite: merge small files into one big one
        let merged = file_with_records("test/merged.parquet", spec_id, 30);
        let tx = Transaction::new(&table);
        let action = tx
            .rewrite()
            .add_data_files(vec![merged.clone()])
            .delete_data_files(vec![small_a.clone(), small_b.clone()]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        let snapshot = table.metadata().current_snapshot().unwrap();
        let summary = &snapshot.summary().additional_properties;

        // Verify added record/file counts in summary
        assert_eq!(
            summary.get("added-data-files").map(|s| s.as_str()),
            Some("1"),
            "Should report 1 added file"
        );
        assert_eq!(
            summary.get("added-records").map(|s| s.as_str()),
            Some("30"),
            "Should report 30 added records"
        );

        // Note: deleted-data-files and deleted-records are not explicitly tracked
        // in the snapshot summary by SnapshotProducer. The summary collector only
        // counts added files. Delete counts would need to be added to the
        // SnapshotSummaryCollector in a future enhancement.
    }

    /// Attempting to delete a file that was already deleted (not alive) should error.
    #[tokio::test]
    async fn test_rewrite_delete_already_deleted_file() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let spec_id = table.metadata().default_partition_spec_id();

        // Append a file
        let file_a = file("test/a.parquet", spec_id);
        let tx = Transaction::new(&table);
        let action = tx.fast_append().add_data_files(vec![file_a.clone()]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        // First rewrite: delete file_a
        let file_a2 = file("test/a2.parquet", spec_id);
        let tx = Transaction::new(&table);
        let action = tx
            .rewrite()
            .add_data_files(vec![file_a2.clone()])
            .delete_data_files(vec![file_a.clone()]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        // Second rewrite: try to delete file_a again (it's already Deleted, not alive)
        let file_a3 = file("test/a3.parquet", spec_id);
        let tx = Transaction::new(&table);
        let action = tx
            .rewrite()
            .add_data_files(vec![file_a3.clone()])
            .delete_data_files(vec![file_a.clone()]);
        let tx = action.apply(tx).unwrap();
        let result = tx.commit(&catalog).await;

        assert!(
            result.is_err(),
            "Should error when deleting an already-deleted file"
        );
    }

    // ========================================================================
    // Format-version-specific tests
    // ========================================================================

    /// Shared logic: basic add+delete rewrite works on any format version.
    async fn assert_basic_add_and_delete(catalog: &impl Catalog, table: crate::table::Table) {
        let spec_id = table.metadata().default_partition_spec_id();

        let original = file("test/original.parquet", spec_id);
        let tx = Transaction::new(&table);
        let action = tx.fast_append().add_data_files(vec![original.clone()]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(catalog).await.unwrap();

        let replacement = file("test/replacement.parquet", spec_id);
        let tx = Transaction::new(&table);
        let action = tx
            .rewrite()
            .add_data_files(vec![replacement.clone()])
            .delete_data_files(vec![original.clone()]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(catalog).await.unwrap();

        let entries = collect_entries(&table).await;
        assert!(
            entries.iter().any(|(p, s, _, _)| p == "test/original.parquet" && *s == ManifestStatus::Deleted),
            "Original should be Deleted, entries: {entries:?}"
        );
        assert!(
            entries.iter().any(|(p, s, _, _)| p == "test/replacement.parquet" && *s == ManifestStatus::Added),
            "Replacement should be Added, entries: {entries:?}"
        );
        let snapshot = table.metadata().current_snapshot().unwrap();
        assert_eq!(snapshot.summary().operation, Operation::Replace);
    }

    /// Shared logic: delete-not-found errors on any format version.
    async fn assert_delete_not_found_errors(catalog: &impl Catalog, table: crate::table::Table) {
        let spec_id = table.metadata().default_partition_spec_id();

        let file_a = file("test/a.parquet", spec_id);
        let tx = Transaction::new(&table);
        let action = tx.fast_append().add_data_files(vec![file_a.clone()]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(catalog).await.unwrap();

        let nonexistent = file("test/ghost.parquet", spec_id);
        let replacement = file("test/replacement.parquet", spec_id);
        let tx = Transaction::new(&table);
        let action = tx
            .rewrite()
            .add_data_files(vec![replacement])
            .delete_data_files(vec![nonexistent]);
        let tx = action.apply(tx).unwrap();
        let result = tx.commit(catalog).await;
        assert!(result.is_err(), "Should error when delete file not found");
    }

    #[tokio::test]
    async fn test_rewrite_v2_basic_add_and_delete() {
        let catalog = new_memory_catalog().await;
        let table = make_v2_minimal_table_in_catalog(&catalog).await;
        assert_eq!(table.metadata().format_version(), crate::spec::FormatVersion::V2);
        assert_basic_add_and_delete(&catalog, table).await;
    }

    /// V2: Verify sequence numbers are properly set for V2 manifest entries.
    #[tokio::test]
    async fn test_rewrite_v2_sequence_numbers() {
        let catalog = new_memory_catalog().await;
        let table = make_v2_minimal_table_in_catalog(&catalog).await;
        let spec_id = table.metadata().default_partition_spec_id();

        // Append two files in separate commits to get distinct sequence numbers
        let file_a = file("test/a.parquet", spec_id);
        let tx = Transaction::new(&table);
        let action = tx.fast_append().add_data_files(vec![file_a.clone()]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        let file_b = file("test/b.parquet", spec_id);
        let tx = Transaction::new(&table);
        let action = tx.fast_append().add_data_files(vec![file_b.clone()]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        // Rewrite: delete file_a, add replacement
        let replacement = file("test/replacement.parquet", spec_id);
        let tx = Transaction::new(&table);
        let action = tx
            .rewrite()
            .add_data_files(vec![replacement.clone()])
            .delete_data_files(vec![file_a.clone()]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        // Load the rewritten manifest and verify the deleted entry has a sequence number
        let snapshot = table.metadata().current_snapshot().unwrap();
        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();

        let mut found_deleted = false;
        for mf in manifest_list.entries() {
            let manifest = mf.load_manifest(table.file_io()).await.unwrap();
            for entry in manifest.entries() {
                if entry.file_path() == "test/a.parquet" {
                    assert_eq!(entry.status(), ManifestStatus::Deleted);
                    assert!(
                        entry.sequence_number().is_some(),
                        "V2: Deleted entry must have a sequence number"
                    );
                    found_deleted = true;
                }
            }
        }
        assert!(found_deleted, "Should have found the deleted entry");
    }

    /// V2: Multiple deletes across manifests should work with V2 format.
    #[tokio::test]
    async fn test_rewrite_v2_deletes_across_manifests() {
        let catalog = new_memory_catalog().await;
        let table = make_v2_minimal_table_in_catalog(&catalog).await;
        let spec_id = table.metadata().default_partition_spec_id();

        // Append files in separate commits → separate manifests
        let file_a = file("test/a.parquet", spec_id);
        let tx = Transaction::new(&table);
        let action = tx.fast_append().add_data_files(vec![file_a.clone()]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        let file_b = file("test/b.parquet", spec_id);
        let tx = Transaction::new(&table);
        let action = tx.fast_append().add_data_files(vec![file_b.clone()]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        // Delete both files from different manifests in one rewrite
        let merged = file("test/merged.parquet", spec_id);
        let tx = Transaction::new(&table);
        let action = tx
            .rewrite()
            .add_data_files(vec![merged.clone()])
            .delete_data_files(vec![file_a.clone(), file_b.clone()]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        let entries = collect_entries(&table).await;

        let deleted_count = entries.iter().filter(|(_, s, _, _)| *s == ManifestStatus::Deleted).count();
        assert_eq!(deleted_count, 2, "V2: Both files should be Deleted");

        let added_count = entries.iter().filter(|(_, s, _, _)| *s == ManifestStatus::Added).count();
        assert_eq!(added_count, 1, "V2: One merged file should be Added");
    }

    #[tokio::test]
    async fn test_rewrite_v2_delete_file_not_found() {
        let catalog = new_memory_catalog().await;
        let table = make_v2_minimal_table_in_catalog(&catalog).await;
        assert_delete_not_found_errors(&catalog, table).await;
    }

    // ---- V1 Format Version Tests ----

    #[tokio::test]
    async fn test_rewrite_v1_basic_add_and_delete() {
        let catalog = new_memory_catalog().await;
        let table = make_v1_minimal_table_in_catalog(&catalog).await;
        assert_eq!(table.metadata().format_version(), crate::spec::FormatVersion::V1);
        assert_basic_add_and_delete(&catalog, table).await;
    }

    /// V1: Sequence numbers should always be 0 in V1 tables.
    #[tokio::test]
    async fn test_rewrite_v1_sequence_numbers_are_zero() {
        let catalog = new_memory_catalog().await;
        let table = make_v1_minimal_table_in_catalog(&catalog).await;
        let spec_id = table.metadata().default_partition_spec_id();

        // V1: next_sequence_number() always returns INITIAL_SEQUENCE_NUMBER (0)
        assert_eq!(table.metadata().next_sequence_number(), 0);

        // Append files
        let file_a = file("test/a.parquet", spec_id);
        let file_b = file("test/b.parquet", spec_id);
        let tx = Transaction::new(&table);
        let action = tx
            .fast_append()
            .add_data_files(vec![file_a.clone(), file_b.clone()]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        // Rewrite: delete file_a
        let replacement = file("test/replacement.parquet", spec_id);
        let tx = Transaction::new(&table);
        let action = tx
            .rewrite()
            .add_data_files(vec![replacement.clone()])
            .delete_data_files(vec![file_a.clone()]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        // All entries in V1 should have sequence_number = 0
        let snapshot = table.metadata().current_snapshot().unwrap();
        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();

        for mf in manifest_list.entries() {
            let manifest = mf.load_manifest(table.file_io()).await.unwrap();
            for entry in manifest.entries() {
                assert_eq!(
                    entry.sequence_number(),
                    Some(0),
                    "V1: sequence_number should be 0 for entry at {}",
                    entry.file_path()
                );
            }
        }
    }

    #[tokio::test]
    async fn test_rewrite_v1_delete_file_not_found() {
        let catalog = new_memory_catalog().await;
        let table = make_v1_minimal_table_in_catalog(&catalog).await;
        assert_delete_not_found_errors(&catalog, table).await;
    }

    /// V1: Partial deletes within a manifest should preserve non-deleted entries.
    #[tokio::test]
    async fn test_rewrite_v1_partial_deletes_in_manifest() {
        let catalog = new_memory_catalog().await;
        let table = make_v1_minimal_table_in_catalog(&catalog).await;
        let spec_id = table.metadata().default_partition_spec_id();

        // Append three files in one manifest
        let file_a = file("test/a.parquet", spec_id);
        let file_b = file("test/b.parquet", spec_id);
        let file_c = file("test/c.parquet", spec_id);
        let tx = Transaction::new(&table);
        let action = tx
            .fast_append()
            .add_data_files(vec![file_a.clone(), file_b.clone(), file_c.clone()]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        // Delete only file_b
        let replacement = file("test/replacement.parquet", spec_id);
        let tx = Transaction::new(&table);
        let action = tx
            .rewrite()
            .add_data_files(vec![replacement.clone()])
            .delete_data_files(vec![file_b.clone()]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        let entries = collect_entries(&table).await;

        assert!(
            entries.iter().any(|(p, s, _, _)| p == "test/b.parquet" && *s == ManifestStatus::Deleted),
            "V1: file_b should be Deleted"
        );
        assert!(
            entries.iter().any(|(p, s, _, _)| p == "test/a.parquet" && *s == ManifestStatus::Existing),
            "V1: file_a should be Existing"
        );
        assert!(
            entries.iter().any(|(p, s, _, _)| p == "test/c.parquet" && *s == ManifestStatus::Existing),
            "V1: file_c should be Existing"
        );
    }

    // ========================================================================
    // Negative / edge-case tests
    // ========================================================================

    /// Calling delete_data_files without add_data_files should error because
    /// SnapshotProducer requires at least added files or snapshot properties.
    #[tokio::test]
    async fn test_rewrite_delete_only_no_added_files() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let spec_id = table.metadata().default_partition_spec_id();

        // Append a file
        let file_a = file("test/a.parquet", spec_id);
        let tx = Transaction::new(&table);
        let action = tx.fast_append().add_data_files(vec![file_a.clone()]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        // Try to delete without adding any new files
        let tx = Transaction::new(&table);
        let action = tx.rewrite().delete_data_files(vec![file_a.clone()]);
        let tx = action.apply(tx).unwrap();
        let result = tx.commit(&catalog).await;

        assert!(
            result.is_err(),
            "Delete-only rewrite (no added files) should error"
        );
    }
}