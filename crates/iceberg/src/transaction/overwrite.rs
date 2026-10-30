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

use async_trait::async_trait;
use uuid::Uuid;

use crate::error::{Result, invalid_data};
use crate::spec::{DataContentType, DataFile, ManifestContentType, Operation};
use crate::table::Table;
use crate::transaction::action::CommitStatus;
use crate::transaction::snapshot::simple::write_added_manifest;
use crate::transaction::snapshot::{
    SnapshotCommitInput, current_manifests, generate_snapshot_id, manifest_list_path,
    manifest_path, new_manifest_writer, validate_files_not_live, validate_partition_value,
    write_snapshot_commit,
};
use crate::transaction::{ActionCommit, TransactionAction};
use crate::{Error, ErrorKind};

/// Adds data files and removes listed data files by path.
///
/// The operation is `Overwrite` when files are both added and removed, otherwise `Delete` or
/// `Append`. There is no row-filter mode, and files committed concurrently are not checked.
#[derive(Clone)]
pub struct OverwriteAction {
    check_duplicate: bool,
    commit_uuid: Option<Uuid>,
    snapshot_properties: HashMap<String, String>,
    added_data_files: Vec<DataFile>,
    added_paths: HashSet<String>,
    deleted_data_files: Vec<DataFile>,
}

impl OverwriteAction {
    pub(crate) fn new() -> Self {
        Self {
            check_duplicate: true,
            commit_uuid: None,
            snapshot_properties: HashMap::default(),
            added_data_files: vec![],
            added_paths: HashSet::new(),
            deleted_data_files: vec![],
        }
    }

    /// Set whether to reject added paths already live in the table (enabled by default).
    /// Paths deleted in the same overwrite may be re-added.
    pub fn with_check_duplicate(mut self, v: bool) -> Self {
        self.check_duplicate = v;
        self
    }

    /// Add data files to the snapshot. Duplicate paths keep the first occurrence.
    pub fn add_data_files(mut self, data_files: impl IntoIterator<Item = DataFile>) -> Self {
        for data_file in data_files {
            if self.added_paths.insert(data_file.file_path.clone()) {
                self.added_data_files.push(data_file);
            }
        }
        self
    }

    /// Specify data files to be removed from the table in this overwrite.
    ///
    /// Files are matched by path against the current snapshot's live data files.
    /// Their `record_count`, `file_size_in_bytes`, `partition_spec_id` and `partition`
    /// must match the committed entry. Duplicate paths keep the first occurrence.
    ///
    /// # Errors
    ///
    /// The commit fails with [`crate::ErrorKind::DataInvalid`] for non-data files,
    /// paths that are not live in the current snapshot, or mismatched metadata.
    pub fn delete_data_files(mut self, data_files: impl IntoIterator<Item = DataFile>) -> Self {
        self.deleted_data_files.extend(data_files);
        self
    }

    /// Set commit UUID for the snapshot.
    pub fn set_commit_uuid(mut self, commit_uuid: Uuid) -> Self {
        self.commit_uuid = Some(commit_uuid);
        self
    }

    /// Set snapshot summary properties.
    pub fn set_snapshot_properties(mut self, snapshot_properties: HashMap<String, String>) -> Self {
        self.snapshot_properties = snapshot_properties;
        self
    }

    fn validate(&self, table: &Table) -> Result<()> {
        // TODO: Allowing snapshot property setup with no data files is a workaround.
        // For details, please refer to https://github.com/apache/iceberg-rust/issues/1548
        if self.added_data_files.is_empty()
            && self.deleted_data_files.is_empty()
            && self.snapshot_properties.is_empty()
        {
            return Err(Error::new(
                ErrorKind::PreconditionFailed,
                "No added or deleted data files or added snapshot properties found",
            ));
        }
        if let Some(data_file) = self
            .added_data_files
            .iter()
            .chain(&self.deleted_data_files)
            .find(|f| f.content_type() != DataContentType::Data)
        {
            return Err(invalid_data!(
                "Only data content type is allowed for overwrite, got {:?} for {}",
                data_file.content_type(),
                data_file.file_path()
            ));
        }
        let metadata = table.metadata();
        for data_file in &self.added_data_files {
            if metadata.default_partition_spec_id() != data_file.partition_spec_id {
                return Err(invalid_data!(
                    "Data file partition spec id does not match table default partition spec id"
                ));
            }
            validate_partition_value(data_file.partition(), metadata.default_partition_type())?;
        }
        Ok(())
    }
}

#[async_trait]
impl TransactionAction for OverwriteAction {
    type State = OverwriteState;

    fn new_state(&self) -> OverwriteState {
        OverwriteState {
            commit_uuid: self.commit_uuid.unwrap_or_else(Uuid::now_v7),
            snapshot_id: None,
            attempt: 0,
            manifest_counter: 0,
        }
    }

    async fn commit(&self, state: &mut OverwriteState, table: &Table) -> Result<ActionCommit> {
        self.validate(table)?;
        let mut deleted_files = HashMap::new();
        for file in &self.deleted_data_files {
            deleted_files.entry(file.file_path()).or_insert(file);
        }
        if self.check_duplicate {
            let added: Vec<DataFile> = self
                .added_data_files
                .iter()
                .filter(|f| !deleted_files.contains_key(f.file_path()))
                .cloned()
                .collect();
            validate_files_not_live(table, &added).await?;
        }

        let snapshot_id = *state
            .snapshot_id
            .get_or_insert_with(|| generate_snapshot_id(table));
        let mut manifests = vec![];
        // One per tombstone written, so a path live in several manifests counts each copy.
        let mut removed_files = vec![];

        for manifest_file in current_manifests(table).await? {
            if manifest_file.content == ManifestContentType::Deletes || deleted_files.is_empty() {
                manifests.push(manifest_file);
                continue;
            }

            let manifest = table.manifest_reader().read(&manifest_file).await?;
            let live: Vec<_> = manifest.entries().iter().filter(|e| e.is_alive()).collect();
            let mut has_deletes = false;
            for entry in &live {
                let Some(requested) = deleted_files.get(entry.file_path()) else {
                    continue;
                };
                if let Some(mismatch) = metadata_mismatch(requested, entry.data_file()) {
                    return Err(invalid_data!(
                        "Delete file metadata does not match the table: {} ({mismatch})",
                        entry.file_path()
                    ));
                }
                has_deletes = true;
            }
            if !has_deletes {
                manifests.push(manifest_file);
                continue;
            }

            let mut writer = new_manifest_writer(
                table,
                state.next_manifest_path(table)?,
                snapshot_id,
                manifest_file.content,
                manifest.metadata().schema.clone(),
                Arc::new(manifest.metadata().partition_spec.clone()),
            )?;
            // Like Java, only live entries are carried into the rewrite.
            for entry in live {
                if deleted_files.contains_key(entry.file_path()) {
                    removed_files.push(entry.data_file().clone());
                    writer.add_delete_entry(entry.as_ref().clone())?;
                } else {
                    writer.add_existing_entry(entry.as_ref().clone())?;
                }
            }
            manifests.push(writer.write_manifest_file().await?);
        }

        let removed_paths: HashSet<&str> = removed_files.iter().map(|f| f.file_path()).collect();
        let mut missing: Vec<&str> = deleted_files
            .keys()
            .copied()
            .filter(|path| !removed_paths.contains(path))
            .collect();
        if !missing.is_empty() {
            missing.sort_unstable();
            return Err(invalid_data!(
                "Missing required files to delete: {}",
                missing.join(", ")
            ));
        }

        if !self.added_data_files.is_empty() {
            manifests.push(
                write_added_manifest(
                    table,
                    state.next_manifest_path(table)?,
                    snapshot_id,
                    table.metadata().default_partition_spec_id(),
                    &self.added_data_files,
                )
                .await?,
            );
        }

        let operation = match (!self.added_data_files.is_empty(), !removed_files.is_empty()) {
            (true, true) => Operation::Overwrite,
            (false, true) => Operation::Delete,
            // Also a properties-only commit, the workaround from #1548.
            _ => Operation::Append,
        };
        let manifest_list_path =
            manifest_list_path(table, snapshot_id, state.attempt, state.commit_uuid)?;
        state.attempt += 1;
        write_snapshot_commit(table, SnapshotCommitInput {
            snapshot_id,
            manifest_list_path,
            operation,
            properties: self.snapshot_properties.clone(),
            added_files: &self.added_data_files,
            removed_files: &removed_files,
            manifests,
            // Only the listed files are replaced, so the table totals carry over.
            truncate_full_table: false,
        })
        .await
    }

    async fn cleanup(self, _state: OverwriteState, _table: &Table, _status: CommitStatus) {}
}

/// Kept across retries, so no attempt overwrites a file an earlier attempt wrote.
pub(crate) struct OverwriteState {
    commit_uuid: Uuid,
    snapshot_id: Option<i64>,
    attempt: u64,
    manifest_counter: u64,
}

impl OverwriteState {
    fn next_manifest_path(&mut self, table: &Table) -> Result<String> {
        let path = manifest_path(table, self.commit_uuid, self.manifest_counter);
        self.manifest_counter += 1;
        path
    }
}

/// Describes the first field where a requested delete differs from the table's entry.
fn metadata_mismatch(requested: &DataFile, actual: &DataFile) -> Option<String> {
    if requested.record_count != actual.record_count {
        Some(format!(
            "record_count: requested {}, table has {}",
            requested.record_count, actual.record_count
        ))
    } else if requested.file_size_in_bytes != actual.file_size_in_bytes {
        Some(format!(
            "file_size_in_bytes: requested {}, table has {}",
            requested.file_size_in_bytes, actual.file_size_in_bytes
        ))
    } else if requested.partition_spec_id != actual.partition_spec_id {
        Some(format!(
            "partition_spec_id: requested {}, table has {}",
            requested.partition_spec_id, actual.partition_spec_id
        ))
    } else if requested.partition != actual.partition {
        Some(format!(
            "partition: requested {:?}, table has {:?}",
            requested.partition, actual.partition
        ))
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use uuid::Uuid;

    use crate::memory::tests::new_memory_catalog;
    use crate::spec::{
        DataContentType, DataFile, DataFileBuilder, DataFileFormat, FormatVersion, Literal,
        ManifestListWriter, ManifestStatus, ManifestWriterBuilder, NestedField, Operation,
        PrimitiveType, Schema, SnapshotRef, Struct, Transform, Type, UnboundPartitionField,
        UnboundPartitionSpec,
    };
    use crate::table::Table;
    use crate::test_utils::commit_with_fresh_state;
    use crate::transaction::tests::{make_v2_minimal_table, make_v3_minimal_table_in_catalog};
    use crate::transaction::{AddColumn, ApplyTransactionAction, Transaction, TransactionAction};
    use crate::{Catalog, ErrorKind, TableCommit, TableCreation, TableUpdate};

    fn test_data_file(path: &str, partition_spec_id: i32) -> DataFile {
        DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(path.to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(partition_spec_id)
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .build()
            .unwrap()
    }

    async fn current_manifest_entries(table: &Table) -> Vec<(ManifestStatus, String)> {
        let snapshot = table.metadata().current_snapshot().unwrap();
        let manifest_list = table.manifest_list_reader(snapshot).load().await.unwrap();

        let mut entries = vec![];
        for manifest_file in manifest_list.entries() {
            let manifest = table.manifest_reader().read(manifest_file).await.unwrap();
            for entry in manifest.entries() {
                entries.push((entry.status(), entry.file_path().to_string()));
            }
        }
        entries
    }

    #[tokio::test]
    async fn test_overwrite_with_deleted_files() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let spec_id = table.metadata().default_partition_spec_id();

        let original_file1 = test_data_file("test/original1.parquet", spec_id);
        let original_file2 = test_data_file("test/original2.parquet", spec_id);
        let tx = Transaction::new(&table);
        let action = tx
            .fast_append()
            .add_data_files(vec![original_file1.clone(), original_file2.clone()]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        let replacement_file = test_data_file("test/replacement.parquet", spec_id);
        let tx = Transaction::new(&table);
        let action = tx
            .overwrite()
            .add_data_files(vec![replacement_file.clone()])
            .delete_data_files(vec![original_file1.clone(), original_file2.clone()]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        let snapshot = table.metadata().current_snapshot().unwrap();
        assert_eq!(snapshot.summary().operation, Operation::Overwrite);
        let summary = &snapshot.summary().additional_properties;
        assert_eq!(summary.get("deleted-data-files").unwrap(), "2");
        assert_eq!(summary.get("deleted-records").unwrap(), "2");

        let entries = current_manifest_entries(&table).await;
        assert_eq!(3, entries.len(), "{entries:?}");
        for entry in [
            (ManifestStatus::Deleted, "test/original1.parquet"),
            (ManifestStatus::Deleted, "test/original2.parquet"),
            (ManifestStatus::Added, "test/replacement.parquet"),
        ] {
            assert!(
                entries.contains(&(entry.0, entry.1.to_string())),
                "{entries:?}"
            );
        }
    }

    #[tokio::test]
    async fn test_second_overwrite_does_not_resurrect_a_deleted_file() {
        let catalog = new_memory_catalog().await;
        let base = make_v3_minimal_table_in_catalog(&catalog).await;
        let v2 = make_v2_minimal_table();
        let table = catalog
            .create_table(
                base.identifier().namespace(),
                TableCreation::builder()
                    .name("v2".to_string())
                    .schema(v2.metadata().current_schema().as_ref().clone())
                    .partition_spec(v2.metadata().default_partition_spec().as_ref().clone())
                    .format_version(FormatVersion::V2)
                    .build(),
            )
            .await
            .unwrap();
        let spec_id = table.metadata().default_partition_spec_id();

        let file_a = test_data_file("test/a.parquet", spec_id);
        let file_b = test_data_file("test/b.parquet", spec_id);
        let tx = Transaction::new(&table);
        let action = tx
            .fast_append()
            .add_data_files(vec![file_a.clone(), file_b.clone()]);
        let table = action.apply(tx).unwrap().commit(&catalog).await.unwrap();

        let tx = Transaction::new(&table);
        let action = tx
            .overwrite()
            .add_data_files(vec![test_data_file("test/c.parquet", spec_id)])
            .delete_data_files(vec![file_a.clone()]);
        let table = action.apply(tx).unwrap().commit(&catalog).await.unwrap();

        let tx = Transaction::new(&table);
        let action = tx
            .overwrite()
            .add_data_files(vec![test_data_file("test/d.parquet", spec_id)])
            .delete_data_files(vec![file_b.clone()]);
        let table = action.apply(tx).unwrap().commit(&catalog).await.unwrap();

        assert_eq!(current_manifest_entries(&table).await, vec![
            (ManifestStatus::Deleted, "test/b.parquet".to_string()),
            (ManifestStatus::Added, "test/c.parquet".to_string()),
            (ManifestStatus::Added, "test/d.parquet".to_string()),
        ]);
    }

    #[tokio::test]
    async fn test_overwrite_retry_keeps_its_snapshot_id_and_never_reuses_a_manifest_path() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let spec_id = table.metadata().default_partition_spec_id();
        let file_a = test_data_file("test/a.parquet", spec_id);
        let tx = Transaction::new(&table);
        let action = tx.fast_append().add_data_files(vec![file_a.clone()]);
        let table = action.apply(tx).unwrap().commit(&catalog).await.unwrap();

        let action = Transaction::new(&table)
            .overwrite()
            .set_commit_uuid(Uuid::nil())
            .add_data_files(vec![test_data_file("test/b.parquet", spec_id)])
            .delete_data_files(vec![file_a]);
        let mut state = action.new_state();
        let mut attempts = vec![];
        for _ in 0..2 {
            let mut action_commit = action.commit(&mut state, &table).await.unwrap();
            let Some(TableUpdate::AddSnapshot { snapshot }) =
                action_commit.take_updates().into_iter().next()
            else {
                unreachable!()
            };
            let snapshot = SnapshotRef::new(snapshot);
            let manifest_list = table.manifest_list_reader(&snapshot).load().await.unwrap();
            let mut paths: Vec<_> = manifest_list
                .entries()
                .iter()
                .map(|m| m.manifest_path.clone())
                .collect();
            paths.push(snapshot.manifest_list().to_string());
            attempts.push((snapshot.snapshot_id(), paths));
        }

        assert_eq!(attempts[0].0, attempts[1].0);
        assert!(attempts[1].1.iter().all(|p| !attempts[0].1.contains(p)));
    }

    #[tokio::test]
    async fn test_rewritten_manifest_keeps_the_schema_and_spec_it_was_written_with() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let original_spec_id = table.metadata().default_partition_spec_id();
        let original_schema_id = table.metadata().current_schema_id();

        let file_a = test_data_file("test/a.parquet", original_spec_id);
        let tx = Transaction::new(&table);
        let action = tx.fast_append().add_data_files(vec![file_a.clone()]);
        let table = action.apply(tx).unwrap().commit(&catalog).await.unwrap();

        let tx = Transaction::new(&table);
        let action = tx.update_schema().add_column(AddColumn::optional(
            "extra",
            Type::Primitive(PrimitiveType::Int),
        ));
        let table = action.apply(tx).unwrap().commit(&catalog).await.unwrap();

        // There is no transaction action for partition evolution yet, so the spec is replaced
        // on the metadata directly.
        let evolved = table
            .metadata()
            .clone()
            .into_builder(None)
            .add_default_partition_spec(
                UnboundPartitionSpec::builder()
                    .add_partition_field(
                        UnboundPartitionField::builder()
                            .source_ids(vec![2])
                            .name("y")
                            .transform(Transform::Identity)
                            .build()
                            .unwrap(),
                    )
                    .unwrap()
                    .build(),
            )
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        let table = table.with_metadata(Arc::new(evolved));
        assert_ne!(original_schema_id, table.metadata().current_schema_id());
        assert_ne!(
            original_spec_id,
            table.metadata().default_partition_spec_id()
        );

        let tx = Transaction::new(&table);
        let action = tx
            .overwrite()
            .add_data_files(vec![test_data_file(
                "test/b.parquet",
                table.metadata().default_partition_spec_id(),
            )])
            .delete_data_files(vec![file_a.clone()]);
        let mut action_commit = commit_with_fresh_state(action, &table).await.unwrap();
        let updates = action_commit.take_updates();
        let new_snapshot: SnapshotRef = if let TableUpdate::AddSnapshot { snapshot } = &updates[0] {
            SnapshotRef::new(snapshot.clone())
        } else {
            unreachable!()
        };

        let manifest_list = table
            .manifest_list_reader(&new_snapshot)
            .load()
            .await
            .unwrap();
        let mut rewritten = None;
        for manifest_file in manifest_list.entries() {
            let manifest = table.manifest_reader().read(manifest_file).await.unwrap();
            if manifest
                .entries()
                .iter()
                .any(|entry| entry.status() == ManifestStatus::Deleted)
            {
                rewritten = Some((manifest_file.clone(), manifest));
            }
        }
        let (rewritten_file, rewritten_manifest) =
            rewritten.expect("the manifest holding test/a.parquet must have been rewritten");

        assert_eq!(original_schema_id, rewritten_manifest.metadata().schema_id);
        assert_eq!(original_spec_id, rewritten_file.partition_spec_id);
    }

    #[tokio::test]
    async fn test_overwrite_counts_a_duplicated_delete_path_once() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let spec_id = table.metadata().default_partition_spec_id();

        let file_a = test_data_file("test/a.parquet", spec_id);
        let file_b = test_data_file("test/b.parquet", spec_id);
        let tx = Transaction::new(&table);
        let action = tx
            .fast_append()
            .add_data_files(vec![file_a.clone(), file_b.clone()]);
        let table = action.apply(tx).unwrap().commit(&catalog).await.unwrap();

        let tx = Transaction::new(&table);
        let action = tx
            .overwrite()
            .add_data_files(vec![test_data_file("test/c.parquet", spec_id)])
            .delete_data_files(vec![file_a.clone(), file_a.clone()]);
        let table = action.apply(tx).unwrap().commit(&catalog).await.unwrap();

        let summary = table.metadata().current_snapshot().unwrap().summary();
        assert_eq!(
            summary.additional_properties.get("deleted-data-files"),
            Some(&"1".to_string())
        );
        assert_eq!(
            summary.additional_properties.get("deleted-records"),
            Some(&"1".to_string())
        );
        assert_eq!(
            summary.additional_properties.get("total-data-files"),
            Some(&"2".to_string())
        );
    }

    #[tokio::test]
    async fn test_overwrite_deletes_every_live_copy_of_a_path() {
        let catalog = new_memory_catalog().await;
        let mut table = make_v3_minimal_table_in_catalog(&catalog).await;
        let file_a = test_data_file(
            "test/a.parquet",
            table.metadata().default_partition_spec_id(),
        );
        for _ in 0..2 {
            let tx = Transaction::new(&table);
            let action = tx
                .fast_append()
                .with_check_duplicate(false)
                .add_data_files(vec![file_a.clone()]);
            table = action.apply(tx).unwrap().commit(&catalog).await.unwrap();
        }

        let tx = Transaction::new(&table);
        let action = tx.overwrite().delete_data_files(vec![file_a]);
        let table = action.apply(tx).unwrap().commit(&catalog).await.unwrap();

        let summary = table.metadata().current_snapshot().unwrap().summary();
        assert_eq!(summary.additional_properties["deleted-data-files"], "2");
        assert_eq!(summary.additional_properties["total-data-files"], "0");
        assert_eq!(current_manifest_entries(&table).await, [
            (ManifestStatus::Deleted, "test/a.parquet".to_string()),
            (ManifestStatus::Deleted, "test/a.parquet".to_string()),
        ]);
    }

    #[tokio::test]
    async fn test_delete_only_overwrite_reports_a_delete_operation() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let spec_id = table.metadata().default_partition_spec_id();

        let file_a = test_data_file("test/a.parquet", spec_id);
        let file_b = test_data_file("test/b.parquet", spec_id);
        let tx = Transaction::new(&table);
        let action = tx
            .fast_append()
            .add_data_files(vec![file_a.clone(), file_b.clone()]);
        let table = action.apply(tx).unwrap().commit(&catalog).await.unwrap();

        let tx = Transaction::new(&table);
        let action = tx.overwrite().delete_data_files(vec![file_a.clone()]);
        let table = action.apply(tx).unwrap().commit(&catalog).await.unwrap();

        let snapshot = table.metadata().current_snapshot().unwrap();
        assert_eq!(snapshot.summary().operation, Operation::Delete);
        assert_eq!(
            snapshot
                .summary()
                .additional_properties
                .get("deleted-data-files"),
            Some(&"1".to_string())
        );

        let entries = current_manifest_entries(&table).await;
        assert!(
            entries.contains(&(ManifestStatus::Deleted, "test/a.parquet".to_string())),
            "the deleted file must be a tombstone, entries: {entries:?}"
        );
        assert!(
            entries.contains(&(ManifestStatus::Existing, "test/b.parquet".to_string())),
            "the untouched file must stay live, entries: {entries:?}"
        );
    }

    #[tokio::test]
    async fn test_overwrite_rejects_deleting_a_file_the_table_does_not_have() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let spec_id = table.metadata().default_partition_spec_id();

        let file_a = test_data_file("test/a.parquet", spec_id);
        let tx = Transaction::new(&table);
        let action = tx.fast_append().add_data_files(vec![file_a.clone()]);
        let table = action.apply(tx).unwrap().commit(&catalog).await.unwrap();

        let tx = Transaction::new(&table);
        let action = tx
            .overwrite()
            .add_data_files(vec![test_data_file("test/b.parquet", spec_id)])
            .delete_data_files(vec![
                file_a.clone(),
                test_data_file("test/never-committed.parquet", spec_id),
            ]);
        let err = commit_with_fresh_state(action, &table).await.err().unwrap();
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(
            err.to_string()
                .contains("Missing required files to delete: test/never-committed.parquet"),
            "{err}"
        );
    }

    #[tokio::test]
    async fn test_overwrite_deletes_files_after_partition_source_promotion() {
        let catalog = new_memory_catalog().await;
        let base = make_v3_minimal_table_in_catalog(&catalog).await;
        for transform in [Transform::Identity, Transform::Truncate(10)] {
            let schema = Schema::builder()
                .with_fields([NestedField::required(1, "x", PrimitiveType::Int).into()])
                .build()
                .unwrap();
            let spec = UnboundPartitionSpec::builder()
                .add_partition_field(
                    UnboundPartitionField::builder()
                        .source_ids(vec![1])
                        .name("p")
                        .transform(transform)
                        .build()
                        .unwrap(),
                )
                .unwrap()
                .build();
            let table = catalog
                .create_table(
                    base.identifier().namespace(),
                    TableCreation::builder()
                        .name(format!("promoted-{transform}"))
                        .schema(schema)
                        .partition_spec(spec)
                        .format_version(FormatVersion::V3)
                        .build(),
                )
                .await
                .unwrap();
            let file = DataFile {
                partition: Struct::from_iter([Some(Literal::int(300))]),
                ..test_data_file(
                    "test/a.parquet",
                    table.metadata().default_partition_spec_id(),
                )
            };
            let tx = Transaction::new(&table);
            let table = tx
                .fast_append()
                .add_data_files([file.clone()])
                .apply(tx)
                .unwrap()
                .commit(&catalog)
                .await
                .unwrap();
            let schema = Schema::builder()
                .with_fields([NestedField::required(1, "x", PrimitiveType::Long).into()])
                .build()
                .unwrap();
            let table = catalog
                .update_table(
                    TableCommit::builder()
                        .ident(table.identifier().clone())
                        .requirements(vec![])
                        .updates(vec![
                            TableUpdate::AddSchema { schema },
                            TableUpdate::SetCurrentSchema { schema_id: -1 },
                        ])
                        .build(),
                )
                .await
                .unwrap();
            let tx = Transaction::new(&table);
            let table = tx
                .overwrite()
                .delete_data_files([file])
                .apply(tx)
                .unwrap()
                .commit(&catalog)
                .await
                .unwrap();
            let snapshot = table.metadata().current_snapshot().unwrap();
            assert_eq!(snapshot.summary().operation, Operation::Delete);
            assert_eq!(
                snapshot.summary().additional_properties["deleted-records"],
                "1"
            );
            assert_eq!(current_manifest_entries(&table).await, [(
                ManifestStatus::Deleted,
                "test/a.parquet".to_string()
            )]);
        }
    }

    #[tokio::test]
    async fn test_overwrite_readds_a_deleted_path_by_default() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let file = test_data_file(
            "test/a.parquet",
            table.metadata().default_partition_spec_id(),
        );
        let tx = Transaction::new(&table);
        let table = tx
            .fast_append()
            .add_data_files([file.clone()])
            .apply(tx)
            .unwrap()
            .commit(&catalog)
            .await
            .unwrap();
        let tx = Transaction::new(&table);
        let table = tx
            .overwrite()
            .delete_data_files([file.clone()])
            .add_data_files([file])
            .apply(tx)
            .unwrap()
            .commit(&catalog)
            .await
            .unwrap();
        assert_eq!(current_manifest_entries(&table).await, [
            (ManifestStatus::Deleted, "test/a.parquet".to_string()),
            (ManifestStatus::Added, "test/a.parquet".to_string()),
        ]);
    }

    #[tokio::test]
    async fn test_overwrite_rejects_incorrect_delete_metrics() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let spec_id = table.metadata().default_partition_spec_id();
        let file = test_data_file("test/a.parquet", spec_id);
        let tx = Transaction::new(&table);
        let table = tx
            .fast_append()
            .add_data_files(vec![file.clone()])
            .apply(tx)
            .unwrap()
            .commit(&catalog)
            .await
            .unwrap();
        let partition_mismatch = |requested: &Struct| {
            format!(
                "partition: requested {requested:?}, table has {:?}",
                file.partition
            )
        };
        let long_partition = Struct::from_iter([Some(Literal::long(999))]);
        let string_partition = Struct::from_iter([Some(Literal::string("invalid"))]);
        for (incorrect, mismatch) in [
            (
                DataFile {
                    record_count: 99,
                    ..file.clone()
                },
                "record_count: requested 99, table has 1".to_string(),
            ),
            (
                DataFile {
                    file_size_in_bytes: 999,
                    ..file.clone()
                },
                "file_size_in_bytes: requested 999, table has 100".to_string(),
            ),
            (
                DataFile {
                    partition: long_partition.clone(),
                    ..file.clone()
                },
                partition_mismatch(&long_partition),
            ),
            (
                DataFile {
                    partition_spec_id: 99,
                    ..file.clone()
                },
                "partition_spec_id: requested 99, table has 0".to_string(),
            ),
            (
                DataFile {
                    partition: string_partition.clone(),
                    ..file.clone()
                },
                partition_mismatch(&string_partition),
            ),
        ] {
            let action = Transaction::new(&table)
                .overwrite()
                .delete_data_files(vec![incorrect]);
            let err = commit_with_fresh_state(action, &table).await.err().unwrap();
            assert_eq!(err.kind(), ErrorKind::DataInvalid);
            assert!(err.message().contains(&mismatch), "{err}");
        }
    }

    #[tokio::test]
    async fn test_overwrite_rejects_non_data_deletions() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let spec_id = table.metadata().default_partition_spec_id();
        for content in [
            DataContentType::PositionDeletes,
            DataContentType::EqualityDeletes,
        ] {
            let file = DataFile {
                content,
                ..test_data_file("test/deletes.parquet", spec_id)
            };
            let action = Transaction::new(&table)
                .overwrite()
                .delete_data_files(vec![file]);
            let err = commit_with_fresh_state(action, &table).await.err().unwrap();
            assert_eq!(err.kind(), ErrorKind::DataInvalid);
            assert!(err.message().contains("Only data content type is allowed"));
        }
    }

    #[tokio::test]
    async fn test_overwrite_preserves_v3_row_lineage() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let spec_id = table.metadata().default_partition_spec_id();
        let a = test_data_file("test/a.parquet", spec_id);
        let b = test_data_file("test/b.parquet", spec_id);
        let c = test_data_file("test/c.parquet", spec_id);
        let tx = Transaction::new(&table);
        let table = tx
            .fast_append()
            .add_data_files(vec![a, b.clone(), c])
            .apply(tx)
            .unwrap()
            .commit(&catalog)
            .await
            .unwrap();
        let snapshot = table.metadata().current_snapshot().unwrap();
        let manifests = table.manifest_list_reader(snapshot).load().await.unwrap();
        let manifest = table
            .manifest_reader()
            .read(&manifests.entries()[0])
            .await
            .unwrap();
        let original_ids: Vec<_> = manifest
            .entries()
            .iter()
            .map(|e| (e.file_path().to_string(), e.data_file().first_row_id))
            .collect();
        assert!(original_ids.iter().all(|(_, id)| id.is_some()));
        let next_row_id = table.metadata().next_row_id();
        let replacement = test_data_file("test/d.parquet", spec_id);
        let tx = Transaction::new(&table);
        let table = tx
            .overwrite()
            .delete_data_files(vec![b])
            .add_data_files(vec![replacement])
            .apply(tx)
            .unwrap()
            .commit(&catalog)
            .await
            .unwrap();
        let manifests = table
            .manifest_list_reader(table.metadata().current_snapshot().unwrap())
            .load()
            .await
            .unwrap();
        let mut actual_ids = vec![];
        for file in manifests.entries() {
            let manifest = table.manifest_reader().read(file).await.unwrap();
            actual_ids.extend(
                manifest
                    .entries()
                    .iter()
                    .filter(|e| e.is_alive())
                    .map(|e| (e.file_path().to_string(), e.data_file().first_row_id)),
            );
        }
        for (path, id) in original_ids.iter().filter(|(p, _)| p != "test/b.parquet") {
            assert!(actual_ids.contains(&(path.clone(), *id)));
        }
        // The rewritten manifest reserves IDs for its existing rows; their explicit IDs stay unchanged.
        assert!(actual_ids.contains(&(
            "test/d.parquet".to_string(),
            Some((next_row_id + 2).try_into().unwrap())
        )));
        assert_eq!(table.metadata().next_row_id(), next_row_id + 3);
    }

    #[tokio::test]
    async fn test_overwrite_preserves_delete_manifests_and_rejects_their_paths() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let spec_id = table.metadata().default_partition_spec_id();
        let tx = Transaction::new(&table);
        let table = tx
            .fast_append()
            .add_data_files(vec![test_data_file("test/a.parquet", spec_id)])
            .apply(tx)
            .unwrap()
            .commit(&catalog)
            .await
            .unwrap();
        let snapshot = table.metadata().current_snapshot().unwrap();
        let file = test_data_file("test/deletes.parquet", spec_id);
        let output = table
            .file_io()
            .new_output("test/delete-manifest.avro")
            .unwrap();
        let mut writer = ManifestWriterBuilder::new(
            output,
            Some(snapshot.snapshot_id()),
            table.metadata().current_schema().clone(),
            table.metadata().default_partition_spec().as_ref().clone(),
        )
        .build_v3_deletes();
        writer
            .add_file(
                DataFile {
                    content: DataContentType::PositionDeletes,
                    ..file.clone()
                },
                snapshot.sequence_number(),
            )
            .unwrap();
        let delete_manifest = writer.write_manifest_file().await.unwrap();
        let mut manifests: Vec<_> = table
            .manifest_list_reader(snapshot)
            .load()
            .await
            .unwrap()
            .consume_entries()
            .into_iter()
            .collect();
        manifests.push(delete_manifest);
        let mut writer = ManifestListWriter::v3(
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
        writer.add_manifests(manifests.into_iter()).unwrap();
        writer.close().await.unwrap();
        let action = Transaction::new(&table)
            .overwrite()
            .delete_data_files(vec![file]);
        let err = commit_with_fresh_state(action, &table).await.err().unwrap();
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(
            err.message()
                .contains("Missing required files to delete: test/deletes.parquet")
        );

        let original = table
            .manifest_list_reader(table.metadata().current_snapshot().unwrap())
            .load()
            .await
            .unwrap()
            .entries()
            .iter()
            .find(|m| m.manifest_path == "test/delete-manifest.avro")
            .unwrap()
            .clone();
        let tx = Transaction::new(&table);
        let table = tx
            .overwrite()
            .delete_data_files([test_data_file("test/a.parquet", spec_id)])
            .apply(tx)
            .unwrap()
            .commit(&catalog)
            .await
            .unwrap();
        let manifests = table
            .manifest_list_reader(table.metadata().current_snapshot().unwrap())
            .load()
            .await
            .unwrap();
        let preserved = manifests
            .entries()
            .iter()
            .find(|m| m.manifest_path == original.manifest_path)
            .unwrap();
        assert_eq!(preserved, &original);
    }
}
