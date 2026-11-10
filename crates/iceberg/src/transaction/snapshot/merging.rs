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
    AddedManifests, SnapshotCommitInput, current_manifests, generate_snapshot_id,
    manifest_list_path, manifest_path, validate_partition_value, write_added_manifest,
    write_snapshot_commit,
};
use crate::error::{Result, invalid_data};
use crate::spec::{
    DataContentType, DataFile, DataFileFormat, FormatVersion, ManifestContentType, ManifestFile,
    Operation,
};
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
    /// Whether a removed file that is no longer live fails the commit.
    ///
    /// Required by actions that must not double-remove (a rewrite whose
    /// replacement files would otherwise duplicate rows). Actions whose
    /// removal is harmless when already done, such as a merge-on-read delete
    /// replacing a position delete file, leave it off so a retry after a
    /// concurrent removal still commits.
    pub fail_missing_delete_paths: bool,
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

impl MergingSnapshotProducer {
    pub(crate) fn new(intent: MergingIntent) -> Self {
        Self {
            commit_uuid: intent.commit_uuid.unwrap_or_else(Uuid::now_v7),
            data_filter: ManifestFilterManager::new(
                ManifestContentType::Data,
                &intent.removed_data_files,
                intent.fail_missing_delete_paths,
            ),
            delete_filter: ManifestFilterManager::new(
                ManifestContentType::Deletes,
                &intent.removed_delete_files,
                intent.fail_missing_delete_paths,
            )
            .with_dangling_dvs_for(&intent.removed_data_files),
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
                &mut self.owned_artifacts,
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
                &mut self.owned_artifacts,
            )
            .await?;
        // A manifest from an earlier snapshot whose entries are all `Deleted`
        // has nothing left to contribute; dropping it keeps the manifest list
        // from growing with every commit. This commit's own rewrites stay even
        // when delete-only, since they record what it removed.
        let mut manifests: Vec<ManifestFile> = deletes
            .manifests
            .into_iter()
            .filter(|m| {
                m.has_added_files() || m.has_existing_files() || m.added_snapshot_id == snapshot_id
            })
            .collect();

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
    /// partition spec with a compatible partition value, and that every added
    /// delete file is in a format the table's version allows.
    ///
    /// Runs against the table the manifests are written for; a later rebase
    /// does not invalidate manifests already written under a spec the table
    /// still has.
    fn validate_added_files(&self, table: &Table) -> Result<()> {
        let metadata = table.metadata();
        let format_version = metadata.format_version();
        for file in &self.intent.added_delete_files {
            validate_delete_file_format(file, format_version)?;
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

/// Checks that a delete file's format is allowed by the table's version: v1
/// has no deletes, v2 has no deletion vectors, and v3 position deletes must be
/// deletion vectors.
fn validate_delete_file_format(file: &DataFile, format_version: FormatVersion) -> Result<()> {
    let is_dv = file.file_format() == DataFileFormat::Puffin;
    match format_version {
        FormatVersion::V1 => Err(invalid_data!(
            "Cannot add delete files to a format version 1 table"
        )),
        FormatVersion::V2 if is_dv => Err(invalid_data!(
            "Cannot add deletion vector {} to a format version 2 table",
            file.file_path()
        )),
        FormatVersion::V3 if file.content_type() == DataContentType::PositionDeletes && !is_dv => {
            Err(invalid_data!(
                "Position deletes must be deletion vectors in a format version 3 table: {}",
                file.file_path()
            ))
        }
        _ => Ok(()),
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
            fail_missing_delete_paths: true,
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

    /// Multi-action replay (RFC §5.2): action A's manifest keeps its path across
    /// attempts, but after a rebase the manifest list assigns it a new sequence
    /// number. Action B's rewrite of that manifest bakes the inherited sequence
    /// numbers into its `Existing`/`Deleted` entries, so B must not reuse a
    /// rewrite made under the previous sequence number.
    #[tokio::test]
    async fn test_rewrite_is_not_reused_when_source_manifest_sequence_number_changes() {
        use super::super::simple::{AppendIntent, SimpleSnapshotProducer};
        use super::super::tests::with_snapshot;

        let s0 = make_v2_minimal_table();

        // Action A appends x and y; action B removes x from A's manifest.
        let mut a = SimpleSnapshotProducer::new(AppendIntent {
            commit_uuid: None,
            snapshot_properties: HashMap::new(),
            data_files: vec![
                data_file(&s0, "test/x.parquet", 1),
                data_file(&s0, "test/y.parquet", 1),
            ],
        });
        let mut b_intent = intent(Operation::Delete);
        b_intent.removed_data_files = vec![data_file(&s0, "test/x.parquet", 1)];
        let mut b = MergingSnapshotProducer::new(b_intent);

        // Attempt 0 on S0.
        let sa0 = added_snapshot(&mut a.apply(&s0).await.unwrap());
        let after_a0 = with_snapshot(&s0, sa0.clone());
        let sb0 = added_snapshot(&mut b.apply(&after_a0).await.unwrap());

        // A concurrent commit lands on S0, taking the next sequence number. The
        // retry rebases onto it: A reuses its manifest but it now inherits a new
        // sequence number, and B sees the same manifest path under it.
        let s1 = append_to(&s0, vec![data_file(&s0, "test/other.parquet", 1)]).await;
        let sa1 = added_snapshot(&mut a.apply(&s1).await.unwrap());
        let after_a1 = with_snapshot(&s1, sa1.clone());
        let sb1 = added_snapshot(&mut b.apply(&after_a1).await.unwrap());
        assert_eq!(sa1.sequence_number(), sa0.sequence_number() + 1);

        // B's rewritten manifest in each attempt.
        let rewritten_in = |table: Table, snapshot: SnapshotRef| async move {
            let list = table.manifest_list_reader(&snapshot).load().await.unwrap();
            let rewritten = list
                .entries()
                .iter()
                .find(|m| m.added_snapshot_id == snapshot.snapshot_id())
                .cloned()
                .unwrap();
            let manifest = table.manifest_reader().read(&rewritten).await.unwrap();
            let y = manifest
                .entries()
                .iter()
                .find(|e| e.file_path() == "test/y.parquet")
                .unwrap();
            (rewritten.manifest_path.clone(), y.sequence_number())
        };
        let (path0, y_seq0) = rewritten_in(after_a0, sb0).await;
        let (path1, y_seq1) = rewritten_in(after_a1, sb1).await;

        // `y` stays live under A's snapshot, whose sequence number moved; the
        // rewrite that keeps it as `Existing` must carry the new one.
        assert_eq!(y_seq0, Some(sa0.sequence_number()));
        assert_eq!(y_seq1, Some(sa1.sequence_number()));
        assert_ne!(
            path0, path1,
            "a rewrite under a different sequence number is a new file"
        );
    }

    /// Removing a data file from a v1 table: v1 entries read with sequence
    /// number 0, which satisfies the writer, and v1 serialization drops the
    /// field again. The rewritten manifest is readable and `b` stays live.
    #[tokio::test]
    async fn test_remove_data_file_from_v1_table() {
        let base = make_v1_table();
        let table = append_to(&base, vec![
            data_file(&base, "test/a.parquet", 1),
            data_file(&base, "test/b.parquet", 1),
        ])
        .await;

        let mut i = intent(Operation::Delete);
        i.removed_data_files = vec![data_file(&base, "test/a.parquet", 1)];
        let mut commit = MergingSnapshotProducer::new(i).apply(&table).await.unwrap();
        let snapshot = added_snapshot(&mut commit);

        let list = table.manifest_list_reader(&snapshot).load().await.unwrap();
        let rewritten = list
            .entries()
            .iter()
            .find(|m| m.added_snapshot_id == snapshot.snapshot_id())
            .unwrap();
        let manifest = table.manifest_reader().read(rewritten).await.unwrap();
        let mut live: Vec<_> = manifest
            .entries()
            .iter()
            .filter(|e| e.is_alive())
            .map(|e| e.file_path().to_string())
            .collect();
        live.sort();
        assert_eq!(live, vec!["test/b.parquet".to_string()]);
        // Both the rewritten entries and the snapshot stay at v1's sequence number 0.
        assert!(
            manifest
                .entries()
                .iter()
                .all(|e| e.sequence_number() == Some(0))
        );
        assert_eq!(snapshot.sequence_number(), 0);
        assert_eq!(
            snapshot
                .summary()
                .additional_properties
                .get("deleted-data-files")
                .unwrap(),
            "1"
        );
    }

    /// A manifest written under an earlier schema is rewritten under the
    /// current one; its partition spec still binds because partition source
    /// columns cannot be dropped.
    #[tokio::test]
    async fn test_rewrite_manifest_written_under_earlier_schema() {
        use crate::spec::{NestedField, PrimitiveType, Type};

        let base = make_v2_minimal_table();
        let table = append_to(&base, vec![
            data_file(&base, "test/a.parquet", 1),
            data_file(&base, "test/b.parquet", 1),
        ])
        .await;

        // Evolve the schema after the manifest was written.
        let new_schema = table
            .metadata()
            .current_schema()
            .as_ref()
            .clone()
            .into_builder()
            .with_fields([
                NestedField::optional(100, "added", Type::Primitive(PrimitiveType::Int)).into(),
            ])
            .build()
            .unwrap();
        let metadata = table
            .metadata()
            .clone()
            .into_builder(None)
            .add_current_schema(new_schema)
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        let evolved = table.clone().with_metadata(std::sync::Arc::new(metadata));
        assert_ne!(
            evolved.metadata().current_schema_id(),
            table.metadata().current_schema_id()
        );

        let mut i = intent(Operation::Delete);
        i.removed_data_files = vec![data_file(&base, "test/a.parquet", 1)];
        let mut commit = MergingSnapshotProducer::new(i)
            .apply(&evolved)
            .await
            .unwrap();
        let snapshot = added_snapshot(&mut commit);

        let list = evolved
            .manifest_list_reader(&snapshot)
            .load()
            .await
            .unwrap();
        let rewritten = list
            .entries()
            .iter()
            .find(|m| m.added_snapshot_id == snapshot.snapshot_id())
            .unwrap();
        assert_eq!(
            rewritten.partition_spec_id,
            table.metadata().default_partition_spec_id()
        );
        assert_eq!(entries_of(&evolved, rewritten).await, vec![
            (ManifestStatus::Deleted, "test/a.parquet".to_string()),
            (ManifestStatus::Existing, "test/b.parquet".to_string()),
        ]);
        // Partition values survive the rewrite under the new schema.
        let manifest = evolved.manifest_reader().read(rewritten).await.unwrap();
        for entry in manifest.entries() {
            assert_eq!(
                entry.data_file().partition(),
                &Struct::from_iter([Some(Literal::long(300))])
            );
        }
    }

    /// A manifest's partition values are encoded for the partition type its
    /// spec had when it was written. Promoting the source column (`int` to
    /// `long`) changes that type under the current schema; the rewrite must
    /// keep the entries readable and their partition values intact.
    #[tokio::test]
    async fn test_rewrite_manifest_after_partition_source_type_promotion() {
        use crate::TableIdent;
        use crate::io::FileIO;
        use crate::spec::{
            FormatVersion, NestedField, PartitionSpec, PrimitiveType, Schema, SortOrder,
            TableMetadataBuilder, Transform, Type,
        };
        use crate::test_utils::test_runtime;

        let int_schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "x", Type::Primitive(PrimitiveType::Int)).into(),
            ])
            .build()
            .unwrap();
        let spec = PartitionSpec::builder(int_schema.clone())
            .add_partition_field("x", "x", Transform::Identity)
            .unwrap()
            .build()
            .unwrap();
        let metadata = TableMetadataBuilder::new(
            int_schema,
            spec,
            SortOrder::unsorted_order(),
            "memory:///table".to_string(),
            FormatVersion::V2,
            HashMap::new(),
        )
        .unwrap()
        .build()
        .unwrap()
        .metadata;
        let base = Table::builder()
            .metadata(metadata)
            .metadata_location("memory:///table/metadata/v1.json")
            .identifier(TableIdent::from_strs(["ns", "t"]).unwrap())
            .file_io(FileIO::new_with_memory())
            .runtime(test_runtime())
            .build()
            .unwrap();

        let int_file = |path: &str| {
            DataFileBuilder::default()
                .content(DataContentType::Data)
                .file_path(path.to_string())
                .file_format(DataFileFormat::Parquet)
                .file_size_in_bytes(100)
                .record_count(1)
                .partition_spec_id(base.metadata().default_partition_spec_id())
                .partition(Struct::from_iter([Some(Literal::int(7))]))
                .build()
                .unwrap()
        };
        let table = append_to(&base, vec![
            int_file("test/a.parquet"),
            int_file("test/b.parquet"),
        ])
        .await;

        // Promote x: int -> long.
        let long_schema = Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![
                NestedField::required(1, "x", Type::Primitive(PrimitiveType::Long)).into(),
            ])
            .build()
            .unwrap();
        let metadata = table
            .metadata()
            .clone()
            .into_builder(None)
            .add_current_schema(long_schema)
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        let promoted = table.clone().with_metadata(std::sync::Arc::new(metadata));

        let mut i = intent(Operation::Delete);
        i.removed_data_files = vec![int_file("test/a.parquet")];
        let mut commit = MergingSnapshotProducer::new(i)
            .apply(&promoted)
            .await
            .unwrap();
        let snapshot = added_snapshot(&mut commit);

        let list = promoted
            .manifest_list_reader(&snapshot)
            .load()
            .await
            .unwrap();
        let rewritten = list
            .entries()
            .iter()
            .find(|m| m.added_snapshot_id == snapshot.snapshot_id())
            .unwrap();
        assert_eq!(entries_of(&promoted, rewritten).await, vec![
            (ManifestStatus::Deleted, "test/a.parquet".to_string()),
            (ManifestStatus::Existing, "test/b.parquet".to_string()),
        ]);
        let manifest = promoted.manifest_reader().read(rewritten).await.unwrap();
        for entry in manifest.entries() {
            assert_eq!(
                entry.data_file().partition(),
                &Struct::from_iter([Some(Literal::int(7))])
            );
        }
    }

    /// On v3, existing entries keep the row ids they were assigned when first
    /// committed, and the rewritten manifest gets its own range from the list
    /// writer.
    #[tokio::test]
    async fn test_rewrite_preserves_first_row_ids_on_v3() {
        use crate::transaction::tests::make_v3_minimal_table;

        let base = make_v3_minimal_table();
        let table = append_to(&base, vec![
            data_file(&base, "test/a.parquet", 5),
            data_file(&base, "test/b.parquet", 7),
        ])
        .await;
        let before = table.metadata().current_snapshot().unwrap();
        let before_list = table.manifest_list_reader(before).load().await.unwrap();
        let before_manifest = table
            .manifest_reader()
            .read(&before_list.entries()[0])
            .await
            .unwrap();
        let b_row_id = before_manifest
            .entries()
            .iter()
            .find(|e| e.file_path() == "test/b.parquet")
            .unwrap()
            .data_file()
            .first_row_id();
        assert_eq!(b_row_id, Some(5));

        let mut i = intent(Operation::Delete);
        i.removed_data_files = vec![data_file(&base, "test/a.parquet", 5)];
        let mut commit = MergingSnapshotProducer::new(i).apply(&table).await.unwrap();
        let snapshot = added_snapshot(&mut commit);

        let list = table.manifest_list_reader(&snapshot).load().await.unwrap();
        let rewritten = list
            .entries()
            .iter()
            .find(|m| m.added_snapshot_id == snapshot.snapshot_id())
            .unwrap();
        let manifest = table.manifest_reader().read(rewritten).await.unwrap();
        let b = manifest
            .entries()
            .iter()
            .find(|e| e.file_path() == "test/b.parquet")
            .unwrap();
        assert_eq!(b.data_file().first_row_id(), b_row_id);
        // The new snapshot starts its range where the table's next_row_id was.
        assert_eq!(
            snapshot.first_row_id(),
            Some(table.metadata().next_row_id())
        );
    }

    /// Every manifest the producer writes, including rewrites, is recorded as
    /// owned before the write starts.
    #[tokio::test]
    async fn test_owned_artifacts_cover_rewrites_and_added_manifests() {
        let base = make_v2_minimal_table();
        let table = append_to(&base, vec![data_file(&base, "test/a.parquet", 1)]).await;

        let mut i = intent(Operation::Overwrite);
        i.removed_data_files = vec![data_file(&base, "test/a.parquet", 1)];
        i.added_data_files = vec![data_file(&base, "test/x.parquet", 1)];
        let mut producer = MergingSnapshotProducer::new(i);
        let mut commit = producer.apply(&table).await.unwrap();
        let snapshot = added_snapshot(&mut commit);

        let list = table.manifest_list_reader(&snapshot).load().await.unwrap();
        for m in list.entries() {
            if m.added_snapshot_id == snapshot.snapshot_id() {
                assert!(producer.owned_artifacts.contains(&m.manifest_path));
            }
        }
        assert!(producer.owned_artifacts.contains(snapshot.manifest_list()));
        // One rewrite, one added manifest, one manifest list.
        assert_eq!(producer.owned_artifacts.len(), 3);
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

    /// A deletion vector for `data_path`: one blob of a Puffin file, located
    /// by offset and size.
    fn deletion_vector(table: &Table, path: &str, data_path: &str, offset: i64) -> DataFile {
        DataFileBuilder::default()
            .content(DataContentType::PositionDeletes)
            .file_path(path.to_string())
            .file_format(DataFileFormat::Puffin)
            .file_size_in_bytes(200)
            .record_count(1)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .referenced_data_file(Some(data_path.to_string()))
            .content_offset(Some(offset))
            .content_size_in_bytes(Some(10))
            .build()
            .unwrap()
    }

    fn equality_delete_file(table: &Table, path: &str) -> DataFile {
        DataFileBuilder::default()
            .content(DataContentType::EqualityDeletes)
            .file_path(path.to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(50)
            .record_count(1)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .equality_ids(Some(vec![1]))
            .build()
            .unwrap()
    }

    #[tokio::test]
    async fn test_deletion_vector_rejected_on_v2_table() {
        let table = make_v2_minimal_table();
        let mut i = intent(Operation::Overwrite);
        i.added_delete_files = vec![deletion_vector(
            &table,
            "test/dv.puffin",
            "test/d.parquet",
            4,
        )];
        let err = MergingSnapshotProducer::new(i)
            .apply(&table)
            .await
            .err()
            .expect("v2 tables cannot hold deletion vectors");
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(err.message().contains("test/dv.puffin"));
    }

    #[tokio::test]
    async fn test_position_delete_file_rejected_on_v3_table() {
        use crate::transaction::tests::make_v3_minimal_table;

        let table = make_v3_minimal_table();
        let mut i = intent(Operation::Overwrite);
        i.added_delete_files = vec![delete_file(&table, "test/d-deletes.parquet")];
        let err = MergingSnapshotProducer::new(i)
            .apply(&table)
            .await
            .err()
            .expect("v3 position deletes must be deletion vectors");
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(err.message().contains("test/d-deletes.parquet"));
    }

    /// v3 still accepts equality delete files and deletion vectors.
    #[tokio::test]
    async fn test_v3_accepts_equality_deletes_and_deletion_vectors() {
        use crate::transaction::tests::make_v3_minimal_table;

        let table = make_v3_minimal_table();
        let mut i = intent(Operation::Overwrite);
        i.added_delete_files = vec![
            equality_delete_file(&table, "test/eq.parquet"),
            deletion_vector(&table, "test/dv.puffin", "test/d.parquet", 4),
        ];
        let mut commit = MergingSnapshotProducer::new(i).apply(&table).await.unwrap();
        let snapshot = added_snapshot(&mut commit);
        assert_eq!(
            snapshot
                .summary()
                .additional_properties
                .get("added-delete-files")
                .unwrap(),
            "2"
        );
    }

    /// Two deletion vectors share one Puffin file. Removing one marks only
    /// that blob's entry `Deleted`; the other stays `Existing`.
    #[tokio::test]
    async fn test_remove_one_deletion_vector_keeps_others_in_same_puffin_file() {
        use crate::transaction::tests::make_v3_minimal_table;

        let base = make_v3_minimal_table();
        let dv_a = deletion_vector(&base, "test/dvs.puffin", "test/a.parquet", 4);
        let dv_b = deletion_vector(&base, "test/dvs.puffin", "test/b.parquet", 14);

        let mut add = intent(Operation::Overwrite);
        add.added_data_files = vec![data_file(&base, "test/a.parquet", 1)];
        add.added_delete_files = vec![dv_a.clone(), dv_b.clone()];
        let mut commit = MergingSnapshotProducer::new(add)
            .apply(&base)
            .await
            .unwrap();
        let table = super::super::tests::with_snapshot(&base, added_snapshot(&mut commit));

        let mut remove = intent(Operation::Delete);
        remove.removed_delete_files = vec![dv_a.clone()];
        let mut commit = MergingSnapshotProducer::new(remove)
            .apply(&table)
            .await
            .unwrap();
        let snapshot = added_snapshot(&mut commit);

        let list = table.manifest_list_reader(&snapshot).load().await.unwrap();
        let rewritten = list
            .entries()
            .iter()
            .find(|m| {
                m.added_snapshot_id == snapshot.snapshot_id()
                    && m.content == ManifestContentType::Deletes
            })
            .expect("the delete manifest is rewritten");
        let manifest = table.manifest_reader().read(rewritten).await.unwrap();
        let mut entries: Vec<_> = manifest
            .entries()
            .iter()
            .map(|e| (e.status(), e.data_file().content_offset()))
            .collect();
        entries.sort_by_key(|(_, offset)| *offset);
        assert_eq!(entries, vec![
            (ManifestStatus::Deleted, dv_a.content_offset()),
            (ManifestStatus::Existing, dv_b.content_offset()),
        ]);
        let summary = &snapshot.summary().additional_properties;
        assert_eq!(summary.get("removed-delete-files").unwrap(), "1");
        assert_eq!(summary.get("total-delete-files").unwrap(), "1");
    }

    /// Removing a data file on v3 also marks its deletion vector `Deleted`,
    /// even though the intent names no delete files. A DV for another data
    /// file in the same Puffin file stays `Existing`, and a removed data file
    /// without a DV is fine under `fail_missing_delete_paths`.
    #[tokio::test]
    async fn test_remove_data_file_also_removes_its_deletion_vector() {
        use super::super::tests::with_snapshot;
        use crate::transaction::tests::make_v3_minimal_table;

        let base = make_v3_minimal_table();
        let dv_a = deletion_vector(&base, "test/dvs.puffin", "test/a.parquet", 4);
        let dv_b = deletion_vector(&base, "test/dvs.puffin", "test/b.parquet", 14);

        let mut add = intent(Operation::Overwrite);
        add.added_data_files = vec![
            data_file(&base, "test/a.parquet", 1),
            data_file(&base, "test/b.parquet", 1),
            data_file(&base, "test/c.parquet", 1),
        ];
        add.added_delete_files = vec![dv_a.clone(), dv_b.clone()];
        let mut commit = MergingSnapshotProducer::new(add)
            .apply(&base)
            .await
            .unwrap();
        let table = with_snapshot(&base, added_snapshot(&mut commit));

        let mut remove = intent(Operation::Delete);
        remove.removed_data_files = vec![
            data_file(&base, "test/a.parquet", 1),
            data_file(&base, "test/c.parquet", 1),
        ];
        let mut commit = MergingSnapshotProducer::new(remove)
            .apply(&table)
            .await
            .unwrap();
        let snapshot = added_snapshot(&mut commit);

        let list = table.manifest_list_reader(&snapshot).load().await.unwrap();
        let rewritten_deletes = list
            .entries()
            .iter()
            .find(|m| {
                m.added_snapshot_id == snapshot.snapshot_id()
                    && m.content == ManifestContentType::Deletes
            })
            .expect("the delete manifest is rewritten for the dangling DV");
        let manifest = table
            .manifest_reader()
            .read(rewritten_deletes)
            .await
            .unwrap();
        let mut entries: Vec<_> = manifest
            .entries()
            .iter()
            .map(|e| (e.status(), e.data_file().referenced_data_file()))
            .collect();
        entries.sort_by_key(|(_, data)| data.clone());
        assert_eq!(entries, vec![
            (ManifestStatus::Deleted, Some("test/a.parquet".to_string())),
            (ManifestStatus::Existing, Some("test/b.parquet".to_string())),
        ]);

        let summary = &snapshot.summary().additional_properties;
        assert_eq!(summary.get("deleted-data-files").unwrap(), "2");
        assert_eq!(summary.get("removed-delete-files").unwrap(), "1");
        assert_eq!(summary.get("total-data-files").unwrap(), "1");
        assert_eq!(summary.get("total-delete-files").unwrap(), "1");
    }

    /// With missing-file failures off, a file removed by a concurrent commit
    /// is skipped and the attempt commits; nothing is rewritten for it.
    #[tokio::test]
    async fn test_missing_removed_file_is_skipped_when_not_failing() {
        let base = make_v2_minimal_table();
        let table = append_to(&base, vec![data_file(&base, "test/a.parquet", 1)]).await;

        let mut i = intent(Operation::Delete);
        i.removed_data_files = vec![data_file(&base, "test/nope.parquet", 1)];
        i.fail_missing_delete_paths = false;
        let mut producer = MergingSnapshotProducer::new(i);
        let mut commit = producer.apply(&table).await.unwrap();
        let snapshot = added_snapshot(&mut commit);

        let list = table.manifest_list_reader(&snapshot).load().await.unwrap();
        assert!(
            list.entries()
                .iter()
                .all(|m| m.added_snapshot_id != snapshot.snapshot_id())
        );
        assert_eq!(producer.manifest_counter, 0);
        let summary = &snapshot.summary().additional_properties;
        assert_eq!(summary.get("total-data-files").unwrap(), "1");
        assert!(!summary.contains_key("deleted-data-files"));
    }

    /// A concurrent format upgrade invalidates both caches: the rewrite, whose
    /// entries were written without v2 sequence numbers, and the added
    /// manifest, whose entries must now inherit one.
    #[tokio::test]
    async fn test_rewrite_and_added_manifest_are_redone_when_format_version_changes() {
        use crate::spec::FormatVersion;

        let base = make_v1_table();
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
        let first = added_snapshot(&mut first);
        assert_eq!(producer.manifest_counter, 2);

        let metadata = table
            .metadata()
            .clone()
            .into_builder(None)
            .upgrade_format_version(FormatVersion::V2)
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        let upgraded = table.clone().with_metadata(std::sync::Arc::new(metadata));

        let mut second = producer.apply(&upgraded).await.unwrap();
        let second = added_snapshot(&mut second);
        assert_eq!(producer.manifest_counter, 4);

        let own_paths = |list: crate::spec::ManifestList, snapshot_id: i64| {
            let mut p: Vec<_> = list
                .entries()
                .iter()
                .filter(|m| m.added_snapshot_id == snapshot_id)
                .map(|m| m.manifest_path.clone())
                .collect();
            p.sort();
            p
        };
        let first_list = table.manifest_list_reader(&first).load().await.unwrap();
        let second_list = upgraded.manifest_list_reader(&second).load().await.unwrap();
        let first_paths = own_paths(first_list, first.snapshot_id());
        let second_paths = own_paths(second_list.clone(), second.snapshot_id());
        assert_eq!(first_paths.len(), 2);
        assert_eq!(second_paths.len(), 2);
        assert!(first_paths.iter().all(|p| !second_paths.contains(p)));

        // Under v2 the added entry inherits the new sequence number, while the
        // rewritten entries keep the v1 data's sequence number 0.
        for m in second_list
            .entries()
            .iter()
            .filter(|m| m.added_snapshot_id == second.snapshot_id())
        {
            let manifest = upgraded.manifest_reader().read(m).await.unwrap();
            for entry in manifest.entries() {
                let expected = if entry.file_path() == "test/x.parquet" {
                    second.sequence_number()
                } else {
                    0
                };
                assert_eq!(
                    entry.sequence_number(),
                    Some(expected),
                    "{}",
                    entry.file_path()
                );
            }
        }
    }

    /// A manifest left with only `Deleted` entries by an earlier snapshot is
    /// dropped by the next commit; the one this commit writes is kept.
    #[tokio::test]
    async fn test_delete_only_manifest_from_earlier_snapshot_is_dropped() {
        use super::super::tests::with_snapshot;

        let base = make_v2_minimal_table();
        let table = append_to(&base, vec![data_file(&base, "test/a.parquet", 1)]).await;

        let mut remove = intent(Operation::Delete);
        remove.removed_data_files = vec![data_file(&base, "test/a.parquet", 1)];
        let mut commit = MergingSnapshotProducer::new(remove)
            .apply(&table)
            .await
            .unwrap();
        let s1 = added_snapshot(&mut commit);
        let s1_list = table.manifest_list_reader(&s1).load().await.unwrap();
        assert_eq!(s1_list.entries().len(), 1);
        assert_eq!(s1_list.entries()[0].deleted_files_count, Some(1));
        assert_eq!(s1_list.entries()[0].existing_files_count, Some(0));
        let table = with_snapshot(&table, s1);

        let mut add = intent(Operation::Append);
        add.added_data_files = vec![data_file(&base, "test/b.parquet", 1)];
        let mut commit = MergingSnapshotProducer::new(add)
            .apply(&table)
            .await
            .unwrap();
        let s2 = added_snapshot(&mut commit);
        let s2_list = table.manifest_list_reader(&s2).load().await.unwrap();
        assert_eq!(s2_list.entries().len(), 1);
        assert_eq!(s2_list.entries()[0].added_snapshot_id, s2.snapshot_id());
        assert_eq!(
            s2.summary()
                .additional_properties
                .get("total-data-files")
                .unwrap(),
            "1"
        );
    }
}
