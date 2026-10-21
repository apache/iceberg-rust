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

//! Persistent producer for append-shaped snapshot actions.

use std::collections::{HashMap, HashSet};

use futures::TryStreamExt;
use futures::stream::FuturesUnordered;
use uuid::Uuid;

use crate::error::{Result, invalid_data};
use crate::spec::{
    DataContentType, DataFile, FormatVersion, ManifestContentType, ManifestEntry, ManifestFile,
    ManifestStatus, Operation,
};
use crate::table::Table;
use crate::transaction::ActionCommit;
use crate::transaction::snapshot_helpers::{
    SnapshotCommitInput, current_manifests, generate_snapshot_id, manifest_list_path,
    manifest_path, new_manifest_writer, validate_partition_value, write_snapshot_commit,
};
use crate::{Error, ErrorKind};

/// Retry-persistent state for an append-shaped action: added data files go
/// into new manifests, and the current snapshot's manifests are carried
/// forward untouched (no filtering, no merging).
///
/// One producer lives for one logical execution of an action. It is driven
/// once per attempt via [`SimpleSnapshotProducer::apply`]: logical identity
/// (commit UUID, snapshot id) is fixed on first use, the manifests written for
/// the added files are reused across attempts while their semantic inputs are
/// unchanged, and every attempt writes a fresh manifest list under a distinct
/// path.
pub(crate) struct SimpleSnapshotProducer {
    /// Fixed when the producer is created, so every attempt shares one UUID.
    commit_uuid: Uuid,
    /// Resolved against the table on the first attempt, then stable.
    snapshot_id: Option<i64>,
    /// Incremented per attempt; distinguishes manifest-list paths.
    attempt: u64,
    /// Numbers this producer's manifests. Never reset, so reused manifests keep
    /// their paths and new ones never collide with earlier attempts.
    manifest_counter: u64,
    /// Manifests written for the added files, reused across attempts.
    added_manifests: Option<AddedManifests>,
    /// Metadata files written by this producer (manifests and every attempt's
    /// manifest list).
    // TODO(#3417): cleanup is not implemented yet; terminal cleanup consumes
    // this set.
    #[allow(dead_code)]
    owned_artifacts: HashSet<String>,
}

/// The manifests written for one append's added files.
///
/// Reusable across attempts as long as the format version is unchanged: a v1
/// manifest entry has no sequence number of its own and cannot inherit the one
/// a v2 manifest list assigns after an upgrade. The added files and their
/// partition specs are fixed by the action, and a manifest's embedded schema
/// is only used to bind its partition spec, so neither needs a guard (matching
/// Java's `FastAppend`).
struct AddedManifests {
    /// One manifest today. Several once added files are grouped by partition
    /// spec (#3500) or rolled over at a target size (#3499).
    manifests: Vec<ManifestFile>,
    format_version: FormatVersion,
}

impl SimpleSnapshotProducer {
    /// Creates a producer. `commit_uuid` overrides the generated commit UUID.
    pub(crate) fn new(commit_uuid: Option<Uuid>) -> Self {
        Self {
            commit_uuid: commit_uuid.unwrap_or_else(Uuid::now_v7),
            snapshot_id: None,
            attempt: 0,
            manifest_counter: 0,
            added_manifests: None,
            owned_artifacts: HashSet::new(),
        }
    }

    /// Checks that every file is a data file written under the table's default
    /// partition spec with a compatible partition value.
    pub(crate) fn validate_added_data_files(
        &self,
        table: &Table,
        data_files: &[DataFile],
    ) -> Result<()> {
        let metadata = table.metadata();
        for data_file in data_files {
            if data_file.content_type() != DataContentType::Data {
                return Err(invalid_data!(
                    "Only data content type is allowed for fast append"
                ));
            }
            // TODO(#3500): accept any spec known to the table and group the
            // files by spec when writing manifests, instead of requiring the
            // default spec.
            if metadata.default_partition_spec_id() != data_file.partition_spec_id {
                return Err(invalid_data!(
                    "Data file partition spec id does not match table default partition spec id"
                ));
            }
            validate_partition_value(data_file.partition(), metadata.default_partition_type())?;
        }
        Ok(())
    }

    /// Fails if any of `data_files` is already live in the current snapshot.
    ///
    /// This must re-run against each attempt's base, since a concurrent commit
    /// may have added one of these files. Manifest reads go through the table's
    /// object cache, so unchanged manifests are not re-read on retry.
    pub(crate) async fn validate_duplicate_files(
        &self,
        table: &Table,
        data_files: &[DataFile],
    ) -> Result<()> {
        let Some(current_snapshot) = table.metadata().current_snapshot() else {
            return Ok(());
        };

        let new_files: HashSet<&str> = data_files.iter().map(|df| df.file_path()).collect();
        if new_files.is_empty() {
            return Ok(());
        }

        let manifest_list = table.manifest_list_reader(current_snapshot).load().await?;
        let runtime = table.runtime();
        let object_cache = table.object_cache();

        let new_files_ref = &new_files;
        let referenced_files: Vec<String> = manifest_list
            .consume_entries()
            .into_iter()
            .map(|entry| {
                let object_cache = object_cache.clone();
                runtime
                    .io()
                    .spawn(async move { object_cache.get_manifest(&entry).await })
            })
            .collect::<FuturesUnordered<_>>()
            .try_fold(Vec::new(), |mut acc, manifest| async move {
                acc.extend(
                    manifest?
                        .entries()
                        .iter()
                        .filter(|e| new_files_ref.contains(e.file_path()) && e.is_alive())
                        .map(|e| e.file_path().to_string()),
                );
                Ok(acc)
            })
            .await?;

        if !referenced_files.is_empty() {
            return Err(invalid_data!(
                "Cannot add files that are already referenced by table, files: {}",
                referenced_files.join(", ")
            ));
        }

        Ok(())
    }

    /// Runs one attempt: produces the resulting manifest set for the current
    /// table and writes this attempt's snapshot.
    pub(crate) async fn apply(
        &mut self,
        table: &Table,
        properties: HashMap<String, String>,
        data_files: &[DataFile],
    ) -> Result<ActionCommit> {
        // TODO: Allowing snapshot property setup with no added data files is a workaround.
        // We should clean it up after all necessary actions are supported.
        // For details, please refer to https://github.com/apache/iceberg-rust/issues/1548
        if data_files.is_empty() && properties.is_empty() {
            return Err(Error::new(
                ErrorKind::PreconditionFailed,
                "No added data files or added snapshot properties found when write a manifest file",
            ));
        }

        let snapshot_id = self.snapshot_id(table);

        let mut manifests = current_manifests(table).await?;
        if !data_files.is_empty() {
            manifests.extend(self.added_manifests(table, snapshot_id, data_files).await?);
        }

        let manifest_list_path = self.next_manifest_list_path(table, snapshot_id)?;

        write_snapshot_commit(table, SnapshotCommitInput {
            snapshot_id,
            manifest_list_path,
            operation: Operation::Append,
            properties,
            added_files: data_files,
            removed_files: &[],
            manifests,
            truncate_full_table: false,
        })
        .await
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
    /// reusable set exists for the current format version.
    async fn added_manifests(
        &mut self,
        table: &Table,
        snapshot_id: i64,
        data_files: &[DataFile],
    ) -> Result<Vec<ManifestFile>> {
        let metadata = table.metadata();
        let format_version = metadata.format_version();

        if let Some(cached) = &self.added_manifests
            && cached.format_version == format_version
        {
            return Ok(cached.manifests.clone());
        }

        // Allocate every path before writing, so each is recorded as owned
        // even if the write fails part-way: the path is write-once and a
        // partial file is an orphan to clean up like any other.
        //
        // TODO(#3500): group `data_files` by partition spec and write one
        // manifest per group.
        // TODO(#3499): roll a group over at the target manifest size, and
        // write the resulting manifests concurrently.
        let path = manifest_path(table, self.commit_uuid, self.manifest_counter)?;
        self.manifest_counter += 1;
        self.owned_artifacts.insert(path.clone());

        let manifests = vec![
            write_added_manifest(
                table,
                path,
                snapshot_id,
                metadata.default_partition_spec_id(),
                data_files,
            )
            .await?,
        ];

        self.added_manifests = Some(AddedManifests {
            manifests: manifests.clone(),
            format_version,
        });
        Ok(manifests)
    }
}

/// Writes one data manifest at `path` for `data_files`, all of which must
/// belong to `partition_spec_id`, and returns its manifest-list entry.
async fn write_added_manifest(
    table: &Table,
    path: String,
    snapshot_id: i64,
    partition_spec_id: i32,
    data_files: &[DataFile],
) -> Result<ManifestFile> {
    let metadata = table.metadata();
    let partition_spec = metadata
        .partition_spec_by_id(partition_spec_id)
        .ok_or_else(|| invalid_data!("Unknown partition spec id {partition_spec_id}"))?;

    let mut writer = new_manifest_writer(
        table,
        path,
        snapshot_id,
        ManifestContentType::Data,
        metadata.current_schema().clone(),
        partition_spec.clone(),
    )?;
    let format_version = metadata.format_version();
    for data_file in data_files {
        let builder = ManifestEntry::builder()
            .status(ManifestStatus::Added)
            .data_file(data_file.clone());
        let entry = if format_version == FormatVersion::V1 {
            builder.snapshot_id(snapshot_id).build()
        } else {
            // For format version > 1, the snapshot id is inherited from the
            // manifest list, so a rebase does not require rewriting the manifest.
            builder.build()
        };
        writer.add_entry(entry)?;
    }
    writer.write_manifest_file().await
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use uuid::Uuid;

    use super::SimpleSnapshotProducer;
    use crate::spec::{
        DataContentType, DataFile, DataFileBuilder, DataFileFormat, FormatVersion, Literal,
        MAIN_BRANCH, NestedField, PrimitiveType, SnapshotRef, SnapshotReference, SnapshotRetention,
        Struct, Type,
    };
    use crate::table::Table;
    use crate::transaction::tests::{make_v1_table, make_v2_minimal_table};
    use crate::{ErrorKind, TableUpdate};

    fn data_file(table: &Table, path: &str) -> DataFile {
        DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(path.to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .build()
            .unwrap()
    }

    fn added_snapshot(commit: &mut crate::transaction::ActionCommit) -> SnapshotRef {
        commit
            .take_updates()
            .into_iter()
            .find_map(|u| match u {
                TableUpdate::AddSnapshot { snapshot } => Some(SnapshotRef::new(snapshot)),
                _ => None,
            })
            .expect("append should add a snapshot")
    }

    /// The minimal table has no current snapshot, so `current_manifests` is
    /// empty and the only manifest in the result is the one we wrote.
    #[tokio::test]
    async fn test_retry_reuses_identity_and_added_manifest() {
        let table = make_v2_minimal_table();
        let requested_uuid = Uuid::now_v7();
        let mut producer = SimpleSnapshotProducer::new(Some(requested_uuid));
        let files = vec![data_file(&table, "test/1.parquet")];

        let mut first = producer
            .apply(&table, HashMap::new(), &files)
            .await
            .unwrap();
        let mut second = producer
            .apply(&table, HashMap::new(), &files)
            .await
            .unwrap();

        let first_snapshot = added_snapshot(&mut first);
        let second_snapshot = added_snapshot(&mut second);

        // Identity is stable across attempts.
        assert_eq!(producer.commit_uuid, requested_uuid);
        assert_eq!(first_snapshot.snapshot_id(), second_snapshot.snapshot_id());
        assert_eq!(producer.snapshot_id, Some(first_snapshot.snapshot_id()));

        // Each attempt writes its own manifest list under a distinct path.
        assert_ne!(
            first_snapshot.manifest_list(),
            second_snapshot.manifest_list()
        );
        assert!(first_snapshot.manifest_list().contains("-0-"));
        assert!(second_snapshot.manifest_list().contains("-1-"));

        // Both manifest lists point at the same, single added manifest.
        let first_list = table
            .manifest_list_reader(&first_snapshot)
            .load()
            .await
            .unwrap();
        let second_list = table
            .manifest_list_reader(&second_snapshot)
            .load()
            .await
            .unwrap();
        assert_eq!(first_list.entries().len(), 1);
        assert_eq!(second_list.entries().len(), 1);
        assert_eq!(
            first_list.entries()[0].manifest_path,
            second_list.entries()[0].manifest_path
        );
        assert_eq!(producer.manifest_counter, 1);

        // One manifest plus two manifest lists are owned.
        assert_eq!(producer.owned_artifacts.len(), 3);
    }

    #[tokio::test]
    /// A manifest's embedded schema only binds its partition spec, so a
    /// concurrent schema change does not invalidate it (as in Java).
    async fn test_added_manifest_is_reused_across_schema_change() {
        let table = make_v2_minimal_table();
        let mut producer = SimpleSnapshotProducer::new(None);
        let files = vec![data_file(&table, "test/1.parquet")];

        let mut first = producer
            .apply(&table, HashMap::new(), &files)
            .await
            .unwrap();
        let first_snapshot = added_snapshot(&mut first);

        // Rebase onto a table whose current schema changed.
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
        let rebased = table.clone().with_metadata(Arc::new(metadata));

        let mut second = producer
            .apply(&rebased, HashMap::new(), &files)
            .await
            .unwrap();
        let second_snapshot = added_snapshot(&mut second);

        let first_list = table
            .manifest_list_reader(&first_snapshot)
            .load()
            .await
            .unwrap();
        let second_list = rebased
            .manifest_list_reader(&second_snapshot)
            .load()
            .await
            .unwrap();
        assert_eq!(
            first_list.entries()[0].manifest_path,
            second_list.entries()[0].manifest_path
        );
        assert_eq!(producer.manifest_counter, 1);
    }

    /// A v1 manifest entry carries no sequence number and is read back as 0,
    /// so it cannot inherit the sequence number a v2 manifest list assigns.
    /// Reusing it after a format upgrade would give the added files
    /// `sequence_number = 0`, so the upgrade must invalidate the cached manifest.
    #[tokio::test]
    async fn test_added_manifest_is_rewritten_when_format_version_changes() {
        let table = make_v1_table();
        let mut producer = SimpleSnapshotProducer::new(None);
        let files = vec![data_file(&table, "test/1.parquet")];

        let mut first = producer
            .apply(&table, HashMap::new(), &files)
            .await
            .unwrap();
        let first_snapshot = added_snapshot(&mut first);

        // A concurrent commit upgraded the table; schema and spec are unchanged.
        let metadata = table
            .metadata()
            .clone()
            .into_builder(None)
            .upgrade_format_version(FormatVersion::V2)
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        let upgraded = table.clone().with_metadata(Arc::new(metadata));
        assert_eq!(
            upgraded.metadata().current_schema_id(),
            table.metadata().current_schema_id()
        );

        let mut second = producer
            .apply(&upgraded, HashMap::new(), &files)
            .await
            .unwrap();
        let second_snapshot = added_snapshot(&mut second);

        let first_list = table
            .manifest_list_reader(&first_snapshot)
            .load()
            .await
            .unwrap();
        let second_list = upgraded
            .manifest_list_reader(&second_snapshot)
            .load()
            .await
            .unwrap();
        let first_added = first_list
            .entries()
            .iter()
            .find(|m| m.added_snapshot_id == first_snapshot.snapshot_id())
            .unwrap();
        let second_added = second_list
            .entries()
            .iter()
            .find(|m| m.added_snapshot_id == second_snapshot.snapshot_id())
            .unwrap();
        assert_ne!(
            first_added.manifest_path, second_added.manifest_path,
            "a manifest is encoded for one format version, so an upgrade must invalidate reuse"
        );
        assert_eq!(producer.manifest_counter, 2);

        // The rewritten manifest's entries inherit the v2 sequence number.
        let manifest = upgraded.manifest_reader().read(second_added).await.unwrap();
        assert_eq!(
            manifest.entries()[0].sequence_number(),
            Some(second_snapshot.sequence_number())
        );
        assert_ne!(second_snapshot.sequence_number(), 0);
    }

    #[tokio::test]
    async fn test_rejects_non_data_content() {
        let table = make_v2_minimal_table();
        let mut file = data_file(&table, "test/1.parquet");
        file.content = DataContentType::EqualityDeletes;

        let producer = SimpleSnapshotProducer::new(None);
        let err = producer
            .validate_added_data_files(&table, &[file])
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
    }

    #[tokio::test]
    async fn test_rejects_empty_append() {
        let table = make_v2_minimal_table();
        let mut producer = SimpleSnapshotProducer::new(None);
        let err = producer
            .apply(&table, HashMap::new(), &[])
            .await
            .err()
            .expect("empty append should be rejected");
        assert_eq!(err.kind(), ErrorKind::PreconditionFailed);
    }

    #[tokio::test]
    async fn test_duplicate_check_sees_files_added_by_rebase() {
        // First attempt: table has no snapshot, so nothing is a duplicate.
        let table = make_v2_minimal_table();
        let producer = SimpleSnapshotProducer::new(None);
        let files = vec![data_file(&table, "test/dup.parquet")];
        producer
            .validate_duplicate_files(&table, &files)
            .await
            .unwrap();

        // A concurrent writer commits the same file; our next attempt rebases
        // onto that snapshot and must detect the duplicate.
        let mut other = SimpleSnapshotProducer::new(None);
        let mut commit = other.apply(&table, HashMap::new(), &files).await.unwrap();
        let snapshot = added_snapshot(&mut commit);
        let metadata = table
            .metadata()
            .clone()
            .into_builder(None)
            .add_snapshot(snapshot.as_ref().clone())
            .unwrap()
            .set_ref(
                MAIN_BRANCH,
                SnapshotReference::new(
                    snapshot.snapshot_id(),
                    SnapshotRetention::branch(None, None, None),
                ),
            )
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        let rebased = table.clone().with_metadata(Arc::new(metadata));

        let err = producer
            .validate_duplicate_files(&rebased, &files)
            .await
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(err.message().contains("test/dup.parquet"));

        // Sanity: a different file is not flagged.
        producer
            .validate_duplicate_files(&rebased, &[data_file(&table, "test/other.parquet")])
            .await
            .unwrap();
    }
}
