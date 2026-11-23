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

use async_trait::async_trait;
use uuid::Uuid;

use crate::error::{Result, invalid_data};
use crate::spec::{DataContentType, DataFile, Operation};
use crate::table::Table;
use crate::transaction::action::CommitStatus;
use crate::transaction::snapshot::merging::{MergingIntent, MergingSnapshotProducer};
use crate::transaction::snapshot::validate_files_not_live;
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
    /// Files are matched by path against the current snapshot's live data files, and the
    /// summary counts the committed entries' metrics.
    ///
    /// # Errors
    ///
    /// The commit fails with [`crate::ErrorKind::DataInvalid`] for non-data files and for
    /// paths that are not live in the current snapshot.
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

    fn validate(&self) -> Result<()> {
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
        Ok(())
    }
}

#[async_trait]
impl TransactionAction for OverwriteAction {
    type State = MergingSnapshotProducer;

    fn new_state(&self) -> MergingSnapshotProducer {
        let operation = match (
            self.added_data_files.is_empty(),
            self.deleted_data_files.is_empty(),
        ) {
            (false, false) => Operation::Overwrite,
            (true, false) => Operation::Delete,
            // Also a properties-only commit, the workaround from #1548.
            _ => Operation::Append,
        };
        MergingSnapshotProducer::new(MergingIntent {
            commit_uuid: self.commit_uuid,
            operation,
            snapshot_properties: self.snapshot_properties.clone(),
            added_data_files: self.added_data_files.clone(),
            added_delete_files: vec![],
            removed_data_files: self.deleted_data_files.clone(),
            removed_delete_files: vec![],
            fail_missing_delete_paths: true,
            truncate_full_table: false,
        })
    }

    async fn commit(
        &self,
        state: &mut MergingSnapshotProducer,
        table: &Table,
    ) -> Result<ActionCommit> {
        self.validate()?;
        if self.check_duplicate {
            let deleted: HashSet<&str> = self
                .deleted_data_files
                .iter()
                .map(DataFile::file_path)
                .collect();
            let added: Vec<DataFile> = self
                .added_data_files
                .iter()
                .filter(|f| !deleted.contains(f.file_path()))
                .cloned()
                .collect();
            validate_files_not_live(table, &added).await?;
        }
        state.apply(table).await
    }

    async fn cleanup(self, _state: MergingSnapshotProducer, _table: &Table, _status: CommitStatus) {
        // TODO(#3417): remove the producer's uncommitted artifacts once terminal
        // cleanup is wired up.
    }
}

#[cfg(test)]
mod tests {
    use crate::memory::tests::new_memory_catalog;
    use crate::spec::{
        DataContentType, DataFile, DataFileBuilder, DataFileFormat, Literal, ManifestListWriter,
        ManifestStatus, ManifestWriterBuilder, Operation, Struct,
    };
    use crate::table::Table;
    use crate::test_utils::commit_with_fresh_state;
    use crate::transaction::tests::make_v3_minimal_table_in_catalog;
    use crate::transaction::{ApplyTransactionAction, Transaction};
    use crate::{Catalog, ErrorKind};

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
        entries.sort_by(|a, b| a.1.cmp(&b.1));
        entries
    }

    async fn append(catalog: &impl Catalog, table: &Table, files: Vec<DataFile>) -> Table {
        let tx = Transaction::new(table);
        let action = tx.fast_append().add_data_files(files);
        action.apply(tx).unwrap().commit(catalog).await.unwrap()
    }

    #[tokio::test]
    async fn test_partial_overwrite_then_delete() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let spec_id = table.metadata().default_partition_spec_id();
        let file_a = test_data_file("test/a.parquet", spec_id);
        let file_b = test_data_file("test/b.parquet", spec_id);
        let table = append(&catalog, &table, vec![file_a.clone(), file_b.clone()]).await;

        let tx = Transaction::new(&table);
        let action = tx
            .overwrite()
            .add_data_files(vec![test_data_file("test/c.parquet", spec_id)])
            .delete_data_files(vec![file_a]);
        let table = action.apply(tx).unwrap().commit(&catalog).await.unwrap();

        let snapshot = table.metadata().current_snapshot().unwrap();
        assert_eq!(snapshot.summary().operation, Operation::Overwrite);
        let summary = &snapshot.summary().additional_properties;
        assert_eq!(summary["added-data-files"], "1");
        assert_eq!(summary["deleted-data-files"], "1");
        assert_eq!(summary["deleted-records"], "1");
        assert_eq!(summary["total-data-files"], "2");
        assert_eq!(summary["total-records"], "2");
        assert_eq!(current_manifest_entries(&table).await, [
            (ManifestStatus::Deleted, "test/a.parquet".to_string()),
            (ManifestStatus::Existing, "test/b.parquet".to_string()),
            (ManifestStatus::Added, "test/c.parquet".to_string()),
        ]);

        let tx = Transaction::new(&table);
        let action = tx.overwrite().delete_data_files(vec![file_b]);
        let table = action.apply(tx).unwrap().commit(&catalog).await.unwrap();
        let snapshot = table.metadata().current_snapshot().unwrap();
        assert_eq!(snapshot.summary().operation, Operation::Delete);
        assert_eq!(
            snapshot.summary().additional_properties["total-data-files"],
            "1"
        );
    }

    #[tokio::test]
    async fn test_overwrite_rejects_deleting_a_file_the_table_does_not_have() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let spec_id = table.metadata().default_partition_spec_id();
        let file_a = test_data_file("test/a.parquet", spec_id);
        let table = append(&catalog, &table, vec![file_a.clone()]).await;

        let action = Transaction::new(&table).overwrite().delete_data_files(vec![
            file_a,
            test_data_file("test/never-committed.parquet", spec_id),
        ]);
        let err = commit_with_fresh_state(action, &table).await.err().unwrap();
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(
            err.message().contains("test/never-committed.parquet"),
            "{err}"
        );
    }

    #[tokio::test]
    async fn test_overwrite_rejects_adding_a_live_path_it_does_not_delete() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let spec_id = table.metadata().default_partition_spec_id();
        let file = test_data_file("test/a.parquet", spec_id);
        let table = append(&catalog, &table, vec![file.clone()]).await;

        let action = Transaction::new(&table).overwrite().add_data_files([file]);
        let err = commit_with_fresh_state(action, &table).await.err().unwrap();
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(err.message().contains("test/a.parquet"), "{err}");
    }

    #[tokio::test]
    async fn test_overwrite_readds_a_deleted_path_by_default() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let file = test_data_file(
            "test/a.parquet",
            table.metadata().default_partition_spec_id(),
        );
        let table = append(&catalog, &table, vec![file.clone()]).await;

        let tx = Transaction::new(&table);
        let action = tx
            .overwrite()
            .delete_data_files([file.clone()])
            .add_data_files([file]);
        let table = action.apply(tx).unwrap().commit(&catalog).await.unwrap();
        assert_eq!(current_manifest_entries(&table).await, [
            (ManifestStatus::Deleted, "test/a.parquet".to_string()),
            (ManifestStatus::Added, "test/a.parquet".to_string()),
        ]);
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
    async fn test_overwrite_preserves_delete_manifests_and_rejects_their_paths() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let spec_id = table.metadata().default_partition_spec_id();
        let table = append(&catalog, &table, vec![test_data_file(
            "test/a.parquet",
            spec_id,
        )])
        .await;
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
        assert!(err.message().contains("test/deletes.parquet"), "{err}");

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
        let action = tx
            .overwrite()
            .delete_data_files([test_data_file("test/a.parquet", spec_id)]);
        let table = action.apply(tx).unwrap().commit(&catalog).await.unwrap();
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
