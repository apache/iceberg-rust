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

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use uuid::Uuid;

use crate::error::Result;
use crate::spec::{DataFile, ManifestEntry, ManifestFile, Operation};
use crate::table::Table;
use crate::transaction::snapshot::{
    DefaultManifestProcess, SnapshotProduceOperation, SnapshotProducer,
};
use crate::transaction::{ActionCommit, TransactionAction};

/// FastAppendAction is a transaction action for fast append data files to the table.
pub struct FastAppendAction {
    check_duplicate: bool,
    // below are properties used to create SnapshotProducer when commit
    commit_uuid: Option<Uuid>,
    key_metadata: Option<Vec<u8>>,
    snapshot_properties: HashMap<String, String>,
    added_data_files: Vec<DataFile>,
}

impl FastAppendAction {
    pub(crate) fn new() -> Self {
        Self {
            check_duplicate: true,
            commit_uuid: None,
            key_metadata: None,
            snapshot_properties: HashMap::default(),
            added_data_files: vec![],
        }
    }

    /// Set whether to check duplicate files
    pub fn with_check_duplicate(mut self, v: bool) -> Self {
        self.check_duplicate = v;
        self
    }

    /// Add data files to the snapshot.
    pub fn add_data_files(mut self, data_files: impl IntoIterator<Item = DataFile>) -> Self {
        self.added_data_files.extend(data_files);
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
impl TransactionAction for FastAppendAction {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        let snapshot_producer = SnapshotProducer::new(
            table,
            self.commit_uuid.unwrap_or_else(Uuid::now_v7),
            self.key_metadata.clone(),
            self.snapshot_properties.clone(),
            self.added_data_files.clone(),
        );

        // validate added files
        snapshot_producer.validate_added_data_files()?;

        // Checks duplicate files
        if self.check_duplicate {
            snapshot_producer.validate_duplicate_files().await?;
        }

        snapshot_producer
            .commit(FastAppendOperation, DefaultManifestProcess)
            .await
    }
}

struct FastAppendOperation;

impl SnapshotProduceOperation for FastAppendOperation {
    fn operation(&self) -> Operation {
        Operation::Append
    }

    async fn delete_entries(
        &self,
        _snapshot_produce: &SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestEntry>> {
        Ok(vec![])
    }

    async fn delete_files(
        &self,
        _snapshot_produce: &SnapshotProducer<'_>,
    ) -> Result<Vec<DataFile>> {
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

        // Carry EVERY prior manifest forward, UNFILTERED — Java parity (`FastAppend.apply`,
        // `core/FastAppend.java`): `manifests.addAll(snapshot.allManifests(ops().io()))` with NO
        // predicate, and `BaseSnapshot.allManifests` returns the manifest list verbatim. The
        // non-merging fast append keeps the manifest-list STRUCTURE intact, including a manifest left
        // ALL-DELETED (every entry a tombstone) by a prior copy-on-write delete that emptied it.
        //
        // The previous `has_added_files() || has_existing_files()` filter DROPPED such all-tombstone
        // manifests, diverging from Java on any append-after-emptying-delete history (the new
        // snapshot's manifest list omitted a manifest Java keeps referenced, and the dropped manifest
        // — now unreferenced — could be GC'd, a manifest-LIST structure divergence visible to interop
        // and to tombstone-visibility consumers). Fixed 2026-06-11 (Wave-4 Group O / A3 STOP-finding).
        //
        // NOTE: the MERGING append (`MergeAppendAction`) does NOT carry all-tombstone manifests — Java
        // `MergingSnapshotProducer.apply` filters its carried set through `shouldKeep = hasAddedFiles
        // || hasExistingFiles || snapshotId() == snapshotId()`, which drops them. That filter is
        // intentionally kept in `merge_append.rs`; only the non-merging fast append carries unfiltered.
        Ok(manifest_list.entries().to_vec())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use crate::spec::{
        DataContentType, DataFileBuilder, DataFileFormat, Literal, MAIN_BRANCH, Struct,
    };
    use crate::transaction::tests::make_v2_minimal_table;
    use crate::transaction::{Transaction, TransactionAction};
    use crate::{TableRequirement, TableUpdate};

    #[tokio::test]
    async fn test_empty_data_append_action() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);
        let action = tx.fast_append().add_data_files(vec![]);
        assert!(Arc::new(action).commit(&table).await.is_err());
    }

    #[tokio::test]
    async fn test_set_snapshot_properties() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);

        let mut snapshot_properties = HashMap::new();
        snapshot_properties.insert("key".to_string(), "val".to_string());

        let data_file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("test/1.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .build()
            .unwrap();

        let action = tx
            .fast_append()
            .set_snapshot_properties(snapshot_properties)
            .add_data_files(vec![data_file]);
        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();

        // Check customized properties is contained in snapshot summary properties.
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

        // Java parity (SnapshotSummary.Builder, trusted partition metrics): a per-file producer
        // commit carries `changed-partition-count` — one appended file touches exactly ONE
        // partition. Pins the PRODUCER wiring of the 2026-06-10 summary fix offline (the
        // metadata-level interop harness pins it end-to-end).
        assert_eq!(
            new_snapshot
                .summary()
                .additional_properties
                .get("changed-partition-count")
                .unwrap(),
            "1"
        );
    }

    #[tokio::test]
    async fn test_append_snapshot_properties() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);

        let mut snapshot_properties = HashMap::new();
        snapshot_properties.insert("key".to_string(), "val".to_string());

        let action = tx
            .fast_append()
            .set_snapshot_properties(snapshot_properties);
        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();

        // Check customized properties is contained in snapshot summary properties.
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
    async fn test_fast_append_file_with_incompatible_partition_value() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);
        let action = tx.fast_append();

        // check add data file with incompatible partition value
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

        let action = action.add_data_files(vec![data_file.clone()]);

        assert!(Arc::new(action).commit(&table).await.is_err());
    }

    #[tokio::test]
    async fn test_fast_append() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);
        let action = tx.fast_append();

        let data_file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("test/3.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .build()
            .unwrap();

        let action = action.add_data_files(vec![data_file.clone()]);
        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();
        let requirements = action_commit.take_requirements();

        // check updates and requirements
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

        // check manifest list
        let new_snapshot = if let TableUpdate::AddSnapshot { snapshot } = &updates[0] {
            snapshot
        } else {
            unreachable!()
        };
        let manifest_list = new_snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();
        assert_eq!(1, manifest_list.entries().len());
        assert_eq!(
            manifest_list.entries()[0].sequence_number,
            new_snapshot.sequence_number()
        );

        // check manifest
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

    // -----------------------------------------------------------------------------------------------
    // All-tombstone-manifest carry parity (Java `FastAppend.apply` → `snapshot.allManifests`, UNFILTERED).
    //
    // Reproduction shape (the A3 STOP-finding): commit A adds one file `d1` to one manifest; commit B
    // (a copy-on-write delete of `d1`) rewrites that manifest to ALL-tombstone (1 Deleted entry, 0
    // live); commit C = fast_append `d2`. Java's manifest list keeps the all-tombstone manifest;
    // Rust's pre-fix `existing_manifest` filter (`has_added || has_existing`) DROPPED it.
    // -----------------------------------------------------------------------------------------------

    use crate::Catalog;
    use crate::memory::tests::new_memory_catalog;
    use crate::spec::{ManifestContentType, ManifestFile, ManifestStatus};
    use crate::table::Table;
    use crate::transaction::ApplyTransactionAction;
    use crate::transaction::tests::make_v2_minimal_table_in_catalog;

    /// A data file routed to partition `x = 0` (the V2 minimal table is identity(x), spec id 0).
    fn tombstone_data_file(path: &str) -> DataFileBuilder {
        let mut builder = DataFileBuilder::default();
        builder
            .content(DataContentType::Data)
            .file_path(path.to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(0)
            .partition(Struct::from_iter([Some(Literal::long(0))]));
        builder
    }

    /// Commit A (append d1) → commit B (delete d1, an emptying copy-on-write delete) and return the
    /// updated table plus the FULL all-tombstone `ManifestFile` (so callers can field-compare the carried
    /// copy against this original), asserting B's manifest list does contain it.
    async fn append_then_emptying_delete(
        catalog: &impl Catalog,
        table: &Table,
    ) -> (Table, ManifestFile) {
        // Commit A: fast_append d1 → one manifest, one Added entry.
        let d1 = tombstone_data_file("test/d1.parquet").build().unwrap();
        let tx = Transaction::new(table);
        let action = tx.fast_append().add_data_files(vec![d1]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(catalog).await.unwrap();

        // Commit B: delete d1 (copy-on-write) → the manifest is rewritten to {d1: Deleted}, 0 live.
        let tx = Transaction::new(&table);
        let action = tx
            .delete_files()
            .delete_files(vec!["test/d1.parquet".to_string()]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(catalog).await.unwrap();

        // The rewritten manifest is all-tombstone: 0 added, 0 existing, 1 deleted — it IS in B's list.
        let snapshot = table.metadata().current_snapshot().unwrap();
        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();
        let tombstone = manifest_list
            .entries()
            .iter()
            .find(|entry| {
                entry.content == ManifestContentType::Data
                    && !entry.has_added_files()
                    && !entry.has_existing_files()
                    && entry.has_deleted_files()
            })
            .expect("commit B leaves an all-tombstone data manifest in its manifest list");
        (table, tombstone.clone())
    }

    // FAIL-BEFORE / PASS-AFTER (the A3 reproduction). After append → emptying-delete → fast_append, the
    // NEW snapshot's manifest list FILE (re-parsed from disk, not the in-memory object) must CONTAIN the
    // all-tombstone manifest path. Pre-fix the `existing_manifest` filter dropped it; this asserts it is
    // carried forward (Java `FastAppend.apply` → `allManifests`, unfiltered).
    //
    // Risk pinned: a manifest-LIST structure divergence on append-after-emptying-delete history — the
    // dropped all-tombstone manifest is unreferenced (and GC-eligible) under the old behavior, where Java
    // keeps it referenced. Byte-level interop and tombstone-visibility consumers diverge.
    #[tokio::test]
    async fn test_fast_append_carries_all_tombstone_manifest_forward_on_disk() {
        let catalog = new_memory_catalog().await;
        let table = make_v2_minimal_table_in_catalog(&catalog).await;
        let (table, original_tombstone) = append_then_emptying_delete(&catalog, &table).await;
        let tombstone_path = original_tombstone.manifest_path.clone();

        // Commit C: fast_append d2.
        let d2 = tombstone_data_file("test/d2.parquet").build().unwrap();
        let tx = Transaction::new(&table);
        let action = tx.fast_append().add_data_files(vec![d2]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        // RE-PARSE the new snapshot's manifest list FILE from disk (not the in-memory ManifestFile
        // objects) and assert it CONTAINS the all-tombstone manifest path.
        let snapshot = table.metadata().current_snapshot().unwrap();
        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();
        let carried_paths: Vec<&str> = manifest_list
            .entries()
            .iter()
            .map(|entry| entry.manifest_path.as_str())
            .collect();
        assert!(
            carried_paths.contains(&tombstone_path.as_str()),
            "fast_append must carry the all-tombstone manifest forward (Java allManifests is \
             unfiltered); manifest list = {carried_paths:?}, missing {tombstone_path}"
        );

        // ENTRY-COPY FIDELITY (the re-inheritance corruption class): the carried manifest-list entry is
        // FIELD-IDENTICAL to the SAME entry in the prior (commit-B) manifest list — Java's `allManifests`
        // carry re-references the manifest VERBATIM (no `copyOf`/`withSnapshotId` restamp; that restamp is
        // only on the unported `appendManifest()` path). Critically this pins `added_snapshot_id`: a
        // restamp to the new snapshot id would corrupt re-inheritance and silently break the
        // `added_*_files_after` concurrent-conflict window. Full struct `==` covers added_snapshot_id,
        // sequence_number, min_sequence_number, every added/existing/deleted count, and partition summaries.
        let carried_tombstone = manifest_list
            .entries()
            .iter()
            .find(|entry| entry.manifest_path == tombstone_path)
            .unwrap();
        assert_eq!(
            carried_tombstone, &original_tombstone,
            "the carried all-tombstone manifest-list entry must be field-identical to the prior list's \
             entry (verbatim Java `allManifests` carry — no added_snapshot_id/seq/count/partition restamp)"
        );

        // And it is still all-tombstone after the carry (carried verbatim, not rewritten).
        assert!(
            !carried_tombstone.has_added_files()
                && !carried_tombstone.has_existing_files()
                && carried_tombstone.has_deleted_files(),
            "the carried manifest is still all-tombstone (0 added, 0 existing, 1 deleted)"
        );
    }

    // SCAN-BEHAVIOR PIN: carrying the all-tombstone manifest forward does NOT resurrect the deleted row.
    // The scan view (live file paths) after the carry is exactly {d2} — d1 stays deleted. Risk pinned:
    // the carry must be a pure manifest-LIST structure fix, never a live-set change (a regression here
    // would resurrect deleted data — the worst silent-corruption class for a maintenance/append op).
    #[tokio::test]
    async fn test_fast_append_carried_tombstone_does_not_resurrect_deleted_rows() {
        use std::collections::HashSet;

        let catalog = new_memory_catalog().await;
        let table = make_v2_minimal_table_in_catalog(&catalog).await;
        let (table, _original_tombstone) = append_then_emptying_delete(&catalog, &table).await;

        // After the emptying delete, d1 is gone from the live set.
        assert_eq!(
            live_data_file_paths(&table).await,
            HashSet::new(),
            "after the emptying delete, no data file is live"
        );

        // Commit C: fast_append d2.
        let d2 = tombstone_data_file("test/d2.parquet").build().unwrap();
        let tx = Transaction::new(&table);
        let action = tx.fast_append().add_data_files(vec![d2]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        // d2 is live; d1 stays deleted (the carried tombstone does NOT resurrect it).
        assert_eq!(
            live_data_file_paths(&table).await,
            HashSet::from(["test/d2.parquet".to_string()]),
            "only d2 is live; the carried all-tombstone manifest does not resurrect d1"
        );
    }

    // MERGE_APPEND DOCUMENTED-PARITY PIN: the MERGING append (`merge_append` / Java `newAppend` →
    // `MergingSnapshotProducer.apply`) DROPS the all-tombstone manifest via Java's `shouldKeep =
    // hasAddedFiles || hasExistingFiles || snapshotId() == snapshotId()` predicate (the third clause is
    // unreachable for a pure append — no carried manifest was written by the not-yet-committed snapshot).
    // Rust merge_append's `has_added || has_existing` filter MATCHES Java's `shouldKeep` minus that
    // unreachable clause ⇒ no divergence. This pins the deliberate asymmetry: fast_append CARRIES the
    // all-tombstone manifest (above), merge_append DROPS it. (Bytecode-settled NO-FIX for #2.)
    #[tokio::test]
    async fn test_merge_append_drops_all_tombstone_manifest_unlike_fast_append() {
        let catalog = new_memory_catalog().await;
        let table = make_v2_minimal_table_in_catalog(&catalog).await;
        let (table, original_tombstone) = append_then_emptying_delete(&catalog, &table).await;
        let tombstone_path = original_tombstone.manifest_path.clone();

        // Commit C: merge_append d2 (with default merge settings — min-count 100 ⇒ no merge fires; the
        // drop comes from `existing_manifest`, not from the merge step).
        let d2 = tombstone_data_file("test/d2.parquet").build().unwrap();
        let tx = Transaction::new(&table);
        let action = tx.merge_append().add_data_files(vec![d2]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        let snapshot = table.metadata().current_snapshot().unwrap();
        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();
        let carried_paths: Vec<&str> = manifest_list
            .entries()
            .iter()
            .map(|entry| entry.manifest_path.as_str())
            .collect();
        assert!(
            !carried_paths.contains(&tombstone_path.as_str()),
            "merge_append DROPS the all-tombstone manifest (Java `shouldKeep`); manifest list = \
             {carried_paths:?} must not contain {tombstone_path}"
        );
    }

    /// The set of live (Added or Existing) DATA-file paths across the current snapshot (what a scan reads).
    async fn live_data_file_paths(table: &Table) -> std::collections::HashSet<String> {
        let snapshot = table.metadata().current_snapshot().unwrap();
        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();
        let mut live = std::collections::HashSet::new();
        for manifest_file in manifest_list.entries() {
            if manifest_file.content != ManifestContentType::Data {
                continue;
            }
            let manifest = manifest_file.load_manifest(table.file_io()).await.unwrap();
            for entry in manifest.entries() {
                if entry.is_alive() && entry.status() != ManifestStatus::Deleted {
                    live.insert(entry.file_path().to_string());
                }
            }
        }
        live
    }
}
