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

use uuid::Uuid;

use super::{
    SnapshotCommitInput, current_manifests, generate_snapshot_id, manifest_list_path,
    manifest_path, validate_partition_value, write_added_manifest, write_snapshot_commit,
};
use crate::error::{Result, invalid_data};
use crate::spec::{DataFile, FormatVersion, ManifestContentType, ManifestFile, Operation};
use crate::table::Table;
use crate::transaction::ActionCommit;

/// What an append-shaped action asks the producer to do.
///
/// Fixed for one logical execution; the producer derives everything else from
/// it and the table of the current attempt.
pub(crate) struct AppendIntent {
    /// Overrides the generated commit UUID.
    pub commit_uuid: Option<Uuid>,
    pub snapshot_properties: HashMap<String, String>,
    /// Data files to add. The action validates them before each attempt.
    pub data_files: Vec<DataFile>,
}

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
    intent: AppendIntent,
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
/// Reusable while the table's format version is unchanged. Per spec, sequence
/// number inheritance does not apply to v1 manifests, so one written before a
/// concurrent upgrade cannot carry the v2 sequence number a retry assigns and
/// must be rewritten. The schema is not a guard: the manifest's embedded schema
/// describes the files it was written for, which do not change.
struct AddedManifests {
    /// One manifest today. Several once added files are grouped by partition
    /// spec (#3500) or rolled over at a target size (#3499).
    manifests: Vec<ManifestFile>,
    format_version: FormatVersion,
}

impl SimpleSnapshotProducer {
    pub(crate) fn new(intent: AppendIntent) -> Self {
        Self {
            commit_uuid: intent.commit_uuid.unwrap_or_else(Uuid::now_v7),
            intent,
            snapshot_id: None,
            attempt: 0,
            manifest_counter: 0,
            added_manifests: None,
            owned_artifacts: HashSet::new(),
        }
    }

    /// Produces the resulting manifest set for the current
    /// table and writes this attempt's snapshot.
    pub(crate) async fn apply(&mut self, table: &Table) -> Result<ActionCommit> {
        let snapshot_id = self.snapshot_id(table);

        let mut manifests = current_manifests(table).await?;
        if !self.intent.data_files.is_empty() {
            manifests.extend(self.added_manifests(table, snapshot_id).await?);
        }

        let manifest_list_path = self.next_manifest_list_path(table, snapshot_id)?;

        write_snapshot_commit(table, SnapshotCommitInput {
            snapshot_id,
            manifest_list_path,
            operation: Operation::Append,
            properties: self.intent.snapshot_properties.clone(),
            added_files: &self.intent.data_files,
            removed_files: &[],
            removed_delete_files: &[],
            manifests,
            truncate_full_table: false,
        })
        .await
    }

    /// Checks that every file is written under the table's default partition
    /// spec with a compatible partition value.
    ///
    /// Runs against the table the manifests are written for; a later rebase
    /// does not invalidate manifests already written under a spec the table
    /// still has.
    fn validate_added_data_files(&self, table: &Table) -> Result<()> {
        let metadata = table.metadata();
        for data_file in &self.intent.data_files {
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
    ) -> Result<Vec<ManifestFile>> {
        let format_version = table.metadata().format_version();

        if let Some(cached) = &self.added_manifests
            && cached.format_version == format_version
        {
            return Ok(cached.manifests.clone());
        }

        self.validate_added_data_files(table)?;

        // Allocate every path before writing, so each is recorded as owned
        // even if the write fails part-way: the path is write-once and a
        // partial file is an orphan to clean up like any other.
        //
        // TODO(#3500): group the files by partition spec and write one
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
                ManifestContentType::Data,
                table.metadata().default_partition_spec_id(),
                &self.intent.data_files,
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use uuid::Uuid;

    use super::{AppendIntent, SimpleSnapshotProducer};
    use crate::TableUpdate;
    use crate::spec::{
        DataContentType, DataFile, DataFileBuilder, DataFileFormat, FormatVersion, Literal,
        NestedField, PrimitiveType, SnapshotRef, Struct, Type,
    };
    use crate::table::Table;
    use crate::transaction::tests::{make_v1_table, make_v2_minimal_table};

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

    fn producer(data_files: Vec<DataFile>) -> SimpleSnapshotProducer {
        producer_with_uuid(None, data_files)
    }

    fn producer_with_uuid(
        commit_uuid: Option<Uuid>,
        data_files: Vec<DataFile>,
    ) -> SimpleSnapshotProducer {
        SimpleSnapshotProducer::new(AppendIntent {
            commit_uuid,
            snapshot_properties: HashMap::new(),
            data_files,
        })
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
        let mut producer = producer_with_uuid(Some(requested_uuid), vec![data_file(
            &table,
            "test/1.parquet",
        )]);

        let mut first = producer.apply(&table).await.unwrap();
        let mut second = producer.apply(&table).await.unwrap();

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

    /// The manifest's embedded schema still describes the same files after a
    /// concurrent schema change, so the manifest stays valid (as in Java).
    #[tokio::test]
    async fn test_added_manifest_is_reused_across_schema_change() {
        let table = make_v2_minimal_table();
        let mut producer = producer(vec![data_file(&table, "test/1.parquet")]);

        let mut first = producer.apply(&table).await.unwrap();
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

        let mut second = producer.apply(&rebased).await.unwrap();
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

    /// Per spec, a v1 manifest's entries always read with sequence number 0, so
    /// a v1 manifest written before a concurrent format upgrade cannot carry the
    /// sequence number the retry's v2 manifest list assigns. The upgrade must
    /// invalidate the cached manifest.
    #[tokio::test]
    async fn test_added_manifest_is_rewritten_when_format_version_changes() {
        let table = make_v1_table();
        let mut producer = producer(vec![data_file(&table, "test/1.parquet")]);

        let mut first = producer.apply(&table).await.unwrap();
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

        let mut second = producer.apply(&upgraded).await.unwrap();
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
        assert_ne!(first_added.manifest_path, second_added.manifest_path);
        assert_eq!(producer.manifest_counter, 2);

        // The rewritten v2 manifest's entries inherit the v2 sequence number.
        let manifest = upgraded.manifest_reader().read(second_added).await.unwrap();
        assert_eq!(
            manifest.entries()[0].sequence_number(),
            Some(second_snapshot.sequence_number())
        );
        assert_ne!(second_snapshot.sequence_number(), 0);
    }
}
