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

//! Persistent producer for snapshot actions that add and remove data and
//! delete files.

use std::collections::{HashMap, HashSet};

use uuid::Uuid;

use super::manifest_filter::ManifestFilterManager;
use super::{
    SnapshotCommitInput, current_manifests, generate_snapshot_id, manifest_list_path,
    manifest_path, validate_partition_value, write_added_manifest, write_snapshot_commit,
};
use crate::error::{Result, invalid_data};
use crate::spec::{DataFile, FormatVersion, ManifestContentType, ManifestFile, Operation};
use crate::table::Table;
use crate::transaction::ActionCommit;

/// What a merging action asks the producer to do.
///
/// Fixed for one logical execution; the producer derives everything else from
/// it and the table of the current attempt. The action validates the
/// table-independent parts (content types, disjoint add/remove sets) before
/// each attempt.
pub(crate) struct MergingIntent {
    /// Overrides the generated commit UUID.
    pub commit_uuid: Option<Uuid>,
    pub operation: Operation,
    pub snapshot_properties: HashMap<String, String>,
    pub added_data_files: Vec<DataFile>,
    pub added_delete_files: Vec<DataFile>,
    pub removed_data_files: Vec<DataFile>,
    pub removed_delete_files: Vec<DataFile>,
    /// Whether the summary resets table totals (a full-table overwrite).
    pub truncate_full_table: bool,
}

/// Retry-persistent state for an action that adds and removes data and delete
/// files.
///
/// One producer lives for one logical execution of an action and is driven once
/// per attempt via [`MergingSnapshotProducer::apply`]. Per attempt it takes the
/// current snapshot's manifests, rewrites those that reference a removed file,
/// appends manifests for the added files, and writes a manifest list. Identity
/// (commit UUID, snapshot id) is fixed on first use; rewritten and added
/// manifests are reused across attempts while their inputs are unchanged.
pub(crate) struct MergingSnapshotProducer {
    intent: MergingIntent,
    /// Fixed when the producer is created, so every attempt shares one UUID.
    commit_uuid: Uuid,
    /// Resolved against the table on the first attempt, then stable.
    snapshot_id: Option<i64>,
    /// Incremented per attempt; distinguishes manifest-list paths.
    attempt: u64,
    /// Numbers every manifest this producer writes, including rewrites. Never
    /// reset, so paths never collide across attempts.
    manifest_counter: u64,
    data_filter: ManifestFilterManager,
    delete_filter: ManifestFilterManager,
    /// Manifests written for the added files, reused across attempts.
    added_manifests: Option<AddedManifests>,
    /// Metadata files written by this producer (manifests and every attempt's
    /// manifest list).
    // TODO(#3417): consumed by terminal cleanup once it is wired up.
    owned_artifacts: HashSet<String>,
}

/// The manifests written for one execution's added data and delete files.
///
/// Reusable while the table's format version is unchanged; see
/// `simple::AddedManifests` for why.
struct AddedManifests {
    manifests: Vec<ManifestFile>,
    format_version: FormatVersion,
}

impl MergingSnapshotProducer {
    pub(crate) fn new(intent: MergingIntent) -> Self {
        Self {
            commit_uuid: intent.commit_uuid.unwrap_or_else(Uuid::now_v7),
            data_filter: ManifestFilterManager::new(
                ManifestContentType::Data,
                &intent.removed_data_files,
            ),
            delete_filter: ManifestFilterManager::new(
                ManifestContentType::Deletes,
                &intent.removed_delete_files,
            ),
            intent,
            snapshot_id: None,
            attempt: 0,
            manifest_counter: 0,
            added_manifests: None,
            owned_artifacts: HashSet::new(),
        }
    }

    /// Produces the resulting manifest set for the current table and writes
    /// this attempt's snapshot.
    pub(crate) async fn apply(&mut self, table: &Table) -> Result<ActionCommit> {
        let snapshot_id = self.snapshot_id(table);

        let manifests = current_manifests(table).await?;

        // Removals first: filtering only touches the current snapshot's
        // manifests, never the ones this producer adds.
        let data = self
            .data_filter
            .filter(
                table,
                snapshot_id,
                self.commit_uuid,
                manifests,
                &mut self.manifest_counter,
            )
            .await?;
        let deletes = self
            .delete_filter
            .filter(
                table,
                snapshot_id,
                self.commit_uuid,
                data.manifests,
                &mut self.manifest_counter,
            )
            .await?;
        let mut manifests = deletes.manifests;
        self.owned_artifacts
            .extend(self.data_filter.owned_artifacts().iter().cloned());
        self.owned_artifacts
            .extend(self.delete_filter.owned_artifacts().iter().cloned());

        manifests.extend(self.added_manifests(table, snapshot_id).await?);

        let manifest_list_path = self.next_manifest_list_path(table, snapshot_id)?;

        let added_files: Vec<DataFile> = self
            .intent
            .added_data_files
            .iter()
            .chain(&self.intent.added_delete_files)
            .cloned()
            .collect();

        write_snapshot_commit(table, SnapshotCommitInput {
            snapshot_id,
            manifest_list_path,
            operation: self.intent.operation.clone(),
            properties: self.intent.snapshot_properties.clone(),
            added_files: &added_files,
            removed_files: &data.removed_files,
            removed_delete_files: &deletes.removed_files,
            manifests,
            truncate_full_table: self.intent.truncate_full_table,
        })
        .await
    }

    /// Checks that every added file is written under the table's default
    /// partition spec with a compatible partition value, and that delete files
    /// are only added to tables whose format supports them.
    ///
    /// Runs against the table the manifests are written for; a later rebase
    /// does not invalidate manifests already written under a spec the table
    /// still has.
    fn validate_added_files(&self, table: &Table) -> Result<()> {
        let metadata = table.metadata();
        if !self.intent.added_delete_files.is_empty()
            && metadata.format_version() == FormatVersion::V1
        {
            return Err(invalid_data!(
                "Cannot add delete files to a format version 1 table"
            ));
        }
        for file in self
            .intent
            .added_data_files
            .iter()
            .chain(&self.intent.added_delete_files)
        {
            // TODO(#3500): accept any spec known to the table and group the
            // files by spec when writing manifests.
            if metadata.default_partition_spec_id() != file.partition_spec_id {
                return Err(invalid_data!(
                    "File partition spec id does not match table default partition spec id"
                ));
            }
            validate_partition_value(file.partition(), metadata.default_partition_type())?;
        }
        Ok(())
    }

    fn snapshot_id(&mut self, table: &Table) -> i64 {
        *self
            .snapshot_id
            .get_or_insert_with(|| generate_snapshot_id(table))
    }

    fn next_manifest_list_path(&mut self, table: &Table, snapshot_id: i64) -> Result<String> {
        let path = manifest_list_path(table, snapshot_id, self.attempt, self.commit_uuid)?;
        self.attempt += 1;
        self.owned_artifacts.insert(path.clone());
        Ok(path)
    }

    /// Returns the manifests for the added files, writing them only if no
    /// reusable set exists for the current format version. Data and delete
    /// files go into separate manifests.
    async fn added_manifests(
        &mut self,
        table: &Table,
        snapshot_id: i64,
    ) -> Result<Vec<ManifestFile>> {
        let format_version = table.metadata().format_version();

        if let Some(cached) = &self.added_manifests
            && cached.format_version == format_version
        {
            return Ok(cached.manifests.clone());
        }

        self.validate_added_files(table)?;

        let mut manifests = Vec::new();
        for (content, files) in [
            (ManifestContentType::Data, &self.intent.added_data_files),
            (
                ManifestContentType::Deletes,
                &self.intent.added_delete_files,
            ),
        ] {
            if files.is_empty() {
                continue;
            }
            // Allocate the path before writing so a partial write is still owned.
            //
            // TODO(#3500): group by partition spec. TODO(#3499): roll at the
            // target size and write concurrently.
            let path = manifest_path(table, self.commit_uuid, self.manifest_counter)?;
            self.manifest_counter += 1;
            self.owned_artifacts.insert(path.clone());
            manifests.push(
                write_added_manifest(
                    table,
                    path,
                    snapshot_id,
                    content,
                    table.metadata().default_partition_spec_id(),
                    files,
                )
                .await?,
            );
        }

        self.added_manifests = Some(AddedManifests {
            manifests: manifests.clone(),
            format_version,
        });
        Ok(manifests)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::super::tests::{append_to, data_file};
    use super::{MergingIntent, MergingSnapshotProducer};
    use crate::spec::{
        DataContentType, DataFile, DataFileBuilder, DataFileFormat, Literal, ManifestContentType,
        ManifestStatus, Operation, SnapshotRef, Struct,
    };
    use crate::table::Table;
    use crate::transaction::tests::{make_v1_table, make_v2_minimal_table};
    use crate::{ErrorKind, TableUpdate};

    fn delete_file(table: &Table, path: &str) -> DataFile {
        DataFileBuilder::default()
            .content(DataContentType::PositionDeletes)
            .file_path(path.to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(50)
            .record_count(1)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .build()
            .unwrap()
    }

    fn intent(operation: Operation) -> MergingIntent {
        MergingIntent {
            commit_uuid: None,
            operation,
            snapshot_properties: HashMap::new(),
            added_data_files: vec![],
            added_delete_files: vec![],
            removed_data_files: vec![],
            removed_delete_files: vec![],
            truncate_full_table: false,
        }
    }

    fn added_snapshot(commit: &mut crate::transaction::ActionCommit) -> SnapshotRef {
        commit
            .take_updates()
            .into_iter()
            .find_map(|u| match u {
                TableUpdate::AddSnapshot { snapshot } => Some(SnapshotRef::new(snapshot)),
                _ => None,
            })
            .expect("apply should add a snapshot")
    }

    /// Reads every (status, path) pair in the manifest at `manifest_path`.
    async fn entries_of(
        table: &Table,
        manifest: &crate::spec::ManifestFile,
    ) -> Vec<(ManifestStatus, String)> {
        let mut entries: Vec<_> = table
            .manifest_reader()
            .read(manifest)
            .await
            .unwrap()
            .entries()
            .iter()
            .map(|e| (e.status(), e.file_path().to_string()))
            .collect();
        entries.sort_by(|a, b| a.1.cmp(&b.1));
        entries
    }

    /// Removing one of two files in a manifest rewrites it with one `Deleted`
    /// and one `Existing` entry; a manifest without removed files is carried
    /// forward by path.
    #[tokio::test]
    async fn test_remove_data_file_rewrites_only_its_manifest() {
        let base = make_v2_minimal_table();
        // Two commits so the current snapshot has two data manifests.
        let table = append_to(&base, vec![
            data_file(&base, "test/a.parquet", 1),
            data_file(&base, "test/b.parquet", 1),
        ])
        .await;
        let table = append_to(&table, vec![data_file(&base, "test/c.parquet", 1)]).await;
        let current = table.metadata().current_snapshot().unwrap();
        let before = table.manifest_list_reader(current).load().await.unwrap();
        assert_eq!(before.entries().len(), 2);
        let untouched = before
            .entries()
            .iter()
            .find(|m| m.added_files_count == Some(1))
            .unwrap()
            .manifest_path
            .clone();

        let mut i = intent(Operation::Delete);
        i.removed_data_files = vec![data_file(&base, "test/a.parquet", 1)];
        let mut producer = MergingSnapshotProducer::new(i);
        let mut commit = producer.apply(&table).await.unwrap();
        let snapshot = added_snapshot(&mut commit);

        let after = table.manifest_list_reader(&snapshot).load().await.unwrap();
        assert_eq!(after.entries().len(), 2);
        assert!(after.entries().iter().any(|m| m.manifest_path == untouched));
        let rewritten = after
            .entries()
            .iter()
            .find(|m| m.added_snapshot_id == snapshot.snapshot_id())
            .expect("a rewritten manifest belongs to the new snapshot");
        assert_eq!(rewritten.deleted_files_count, Some(1));
        assert_eq!(rewritten.existing_files_count, Some(1));
        assert_eq!(entries_of(&table, rewritten).await, vec![
            (ManifestStatus::Deleted, "test/a.parquet".to_string()),
            (ManifestStatus::Existing, "test/b.parquet".to_string()),
        ]);

        let summary = &snapshot.summary().additional_properties;
        assert_eq!(summary.get("deleted-data-files").unwrap(), "1");
        assert_eq!(summary.get("total-data-files").unwrap(), "2");
    }

    /// Added data and delete files go into separate manifests of the right
    /// content type, and the summary counts both.
    #[tokio::test]
    async fn test_add_data_and_delete_files() {
        let table = make_v2_minimal_table();
        let mut i = intent(Operation::Overwrite);
        i.added_data_files = vec![data_file(&table, "test/d.parquet", 3)];
        i.added_delete_files = vec![delete_file(&table, "test/d-deletes.parquet")];
        let mut producer = MergingSnapshotProducer::new(i);
        let mut commit = producer.apply(&table).await.unwrap();
        let snapshot = added_snapshot(&mut commit);

        let list = table.manifest_list_reader(&snapshot).load().await.unwrap();
        let mut contents: Vec<_> = list.entries().iter().map(|m| m.content).collect();
        contents.sort_by_key(|c| *c as i32);
        assert_eq!(contents, vec![
            ManifestContentType::Data,
            ManifestContentType::Deletes
        ]);

        let summary = &snapshot.summary().additional_properties;
        assert_eq!(summary.get("added-data-files").unwrap(), "1");
        assert_eq!(summary.get("added-delete-files").unwrap(), "1");
        assert_eq!(summary.get("added-records").unwrap(), "3");
    }

    /// A retry reuses both the rewritten and the added manifests; only the
    /// manifest list is written again.
    #[tokio::test]
    async fn test_retry_reuses_rewritten_and_added_manifests() {
        let base = make_v2_minimal_table();
        let table = append_to(&base, vec![
            data_file(&base, "test/a.parquet", 1),
            data_file(&base, "test/b.parquet", 1),
        ])
        .await;

        let mut i = intent(Operation::Overwrite);
        i.removed_data_files = vec![data_file(&base, "test/a.parquet", 1)];
        i.added_data_files = vec![data_file(&base, "test/x.parquet", 1)];
        let mut producer = MergingSnapshotProducer::new(i);

        let mut first = producer.apply(&table).await.unwrap();
        let mut second = producer.apply(&table).await.unwrap();
        let first = added_snapshot(&mut first);
        let second = added_snapshot(&mut second);

        assert_eq!(first.snapshot_id(), second.snapshot_id());
        assert_ne!(first.manifest_list(), second.manifest_list());

        let paths = |list: crate::spec::ManifestList| {
            let mut p: Vec<_> = list
                .entries()
                .iter()
                .map(|m| m.manifest_path.clone())
                .collect();
            p.sort();
            p
        };
        let first_paths = paths(table.manifest_list_reader(&first).load().await.unwrap());
        let second_paths = paths(table.manifest_list_reader(&second).load().await.unwrap());
        assert_eq!(first_paths, second_paths);
        // One rewrite plus one added manifest.
        assert_eq!(producer.manifest_counter, 2);
        assert_eq!(producer.attempt, 2);
    }

    /// Removing a file that is not live anywhere fails and names the file.
    #[tokio::test]
    async fn test_remove_missing_file_fails() {
        let base = make_v2_minimal_table();
        let table = append_to(&base, vec![data_file(&base, "test/a.parquet", 1)]).await;

        let mut i = intent(Operation::Delete);
        i.removed_data_files = vec![data_file(&base, "test/nope.parquet", 1)];
        let err = MergingSnapshotProducer::new(i)
            .apply(&table)
            .await
            .err()
            .expect("removing a file that is not live must fail");
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(err.message().contains("test/nope.parquet"));
    }

    /// A file removed by a concurrent commit is no longer live on the rebased
    /// table, so the retry fails rather than silently succeeding.
    #[tokio::test]
    async fn test_rebase_onto_table_without_removed_file_fails() {
        let base = make_v2_minimal_table();
        let s1 = append_to(&base, vec![data_file(&base, "test/a.parquet", 1)]).await;

        let mut i = intent(Operation::Delete);
        i.removed_data_files = vec![data_file(&base, "test/a.parquet", 1)];
        let mut producer = MergingSnapshotProducer::new(i);
        producer.apply(&s1).await.unwrap();

        // Concurrent writer removed `a` first; our retry rebases onto that.
        let mut other = intent(Operation::Delete);
        other.removed_data_files = vec![data_file(&base, "test/a.parquet", 1)];
        let mut other_commit = MergingSnapshotProducer::new(other)
            .apply(&s1)
            .await
            .unwrap();
        let s2_snapshot = added_snapshot(&mut other_commit);
        let s2 = {
            use crate::spec::{MAIN_BRANCH, SnapshotReference, SnapshotRetention};
            let metadata = s1
                .metadata()
                .clone()
                .into_builder(None)
                .add_snapshot(s2_snapshot.as_ref().clone())
                .unwrap()
                .set_ref(
                    MAIN_BRANCH,
                    SnapshotReference::new(
                        s2_snapshot.snapshot_id(),
                        SnapshotRetention::branch(None, None, None),
                    ),
                )
                .unwrap()
                .build()
                .unwrap()
                .metadata;
            s1.clone().with_metadata(std::sync::Arc::new(metadata))
        };

        let err = producer
            .apply(&s2)
            .await
            .err()
            .expect("file already removed concurrently must not be removed twice");
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
    }

    #[tokio::test]
    async fn test_delete_files_rejected_on_v1_table() {
        let table = make_v1_table();
        let mut i = intent(Operation::Overwrite);
        i.added_delete_files = vec![delete_file(&table, "test/d-deletes.parquet")];
        let err = MergingSnapshotProducer::new(i)
            .apply(&table)
            .await
            .err()
            .expect("v1 tables cannot hold delete files");
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
    }
}
