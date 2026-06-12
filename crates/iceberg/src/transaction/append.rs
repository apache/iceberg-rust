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
    /// Stage the produced snapshot for write-audit-publish instead of moving `main` (Java
    /// `SnapshotProducer.stageOnly()`). See [`FastAppendAction::stage_only`].
    stage_only: bool,
}

impl FastAppendAction {
    pub(crate) fn new() -> Self {
        Self {
            check_duplicate: true,
            commit_uuid: None,
            key_metadata: None,
            snapshot_properties: HashMap::default(),
            added_data_files: vec![],
            stage_only: false,
        }
    }

    /// Set whether to check duplicate files
    pub fn with_check_duplicate(mut self, v: bool) -> Self {
        self.check_duplicate = v;
        self
    }

    /// STAGE this append for write-audit-publish (WAP) instead of publishing it to `main` (Java
    /// `SnapshotProducer.stageOnly()`, exposed on the `SnapshotUpdate` interface). When called, committing
    /// this action ADDS the new snapshot to table metadata but moves NO ref: `current-snapshot-id`, the
    /// `main` ref, and the snapshot-log are left UNCHANGED, so readers continue to see the pre-staging data
    /// and the staged snapshot is invisible until a later [`crate::transaction::Transaction::cherry_pick`]
    /// publishes it. The staged snapshot still consumes a sequence number exactly like a normal commit.
    ///
    /// A WAP audit job stamps the staged snapshot's `wap.id` via [`Self::set_snapshot_properties`] (the
    /// Rust equivalent of Java's engine-side `set(SnapshotSummary.STAGED_WAP_ID_PROP, ...)`); cherry-pick
    /// then carries it forward as `published-wap-id`.
    pub fn stage_only(mut self) -> Self {
        self.stage_only = true;
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
        )
        .with_stage_only(self.stage_only);

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

    // ===============================================================================================
    // stage_only() — the write-audit-publish (WAP) staging path (Java `SnapshotProducer.stageOnly()`).
    //
    // A staged commit ADDS its snapshot to table metadata but moves NO ref: the update set is
    // `AddSnapshot` ALONE (no `SetSnapshotRef`), so `current-snapshot-id`, the `main` ref, and the
    // snapshot-log are unchanged ON DISK. The snapshot still consumes a sequence number exactly like a
    // normal commit (Java `apply()` assigns `base.nextSequenceNumber()` regardless of the flag), and is
    // published later via cherry-pick. These tests re-parse the COMMITTED metadata (the catalog-returned
    // table is the re-parsed on-disk metadata) so the pins are on-disk, not in-memory shape.
    // ===============================================================================================

    use std::collections::HashSet;

    use crate::spec::{DataFile, Operation};
    use crate::transaction::tests::make_v3_minimal_table_in_catalog;

    /// A data file routed to partition `x = part_value` (V2/V3 minimal table is identity(x), spec id 0).
    fn part_data_file(path: &str, part_value: i64) -> DataFile {
        DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(path.to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(0)
            .partition(Struct::from_iter([Some(Literal::long(part_value))]))
            .build()
            .unwrap()
    }

    /// Fast-append `files` (publishing to `main`) and return the updated table.
    async fn append_published(
        catalog: &impl Catalog,
        table: &Table,
        files: Vec<DataFile>,
    ) -> Table {
        let tx = Transaction::new(table);
        let action = tx.fast_append().add_data_files(files);
        let tx = action.apply(tx).unwrap();
        tx.commit(catalog).await.unwrap()
    }

    /// THE STAGING CROWN JEWEL (on-disk). A `fast_append().stage_only()` with a `wap.id` summary property:
    /// re-parse the committed metadata and assert the new snapshot EXISTS in `snapshots`, but
    /// `current-snapshot-id` is UNCHANGED, the `main` ref is UNCHANGED, the snapshot-log has NO entry for
    /// the staged snapshot (Java does NOT advance the snapshot log for a staged snapshot — bytecode:
    /// `addSnapshot` touches neither `snapshotLog` nor `currentSnapshotId`), the staged snapshot's summary
    /// carries `wap.id`, and its sequence number == the sequence number a normal commit would assign
    /// (`base.next_sequence_number()`, Java's `apply()` is stageOnly-independent).
    ///
    /// Risk pinned: a staging that moves a ref (publishes unaudited data) or mangles the snapshot-log /
    /// current-snapshot-id (corrupts the table for every reader). This is THE load-bearing WAP-write pin.
    #[tokio::test]
    async fn test_fast_append_stage_only_adds_snapshot_without_moving_main_on_disk() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        // Publish a base snapshot so there IS a current-snapshot-id / main ref / snapshot-log entry to pin
        // as UNCHANGED across the staging.
        let table = append_published(&catalog, &table, vec![part_data_file(
            "test/base.parquet",
            9,
        )])
        .await;
        let base_current_id = table.metadata().current_snapshot_id();
        let base_main_ref = table
            .metadata()
            .snapshot_for_ref(MAIN_BRANCH)
            .unwrap()
            .snapshot_id();
        let base_snapshot_log = table.metadata().history().to_vec();
        let base_snapshot_count = table.metadata().snapshots().count();
        // The sequence number a NORMAL next commit would assign — what the staged snapshot must also get.
        let expected_seq = table.metadata().next_sequence_number();

        // STAGE an append with a wap.id (the WAP audit-job shape).
        let mut wap_properties = HashMap::new();
        wap_properties.insert("wap.id".to_string(), "wap-staged-1".to_string());
        let tx = Transaction::new(&table);
        let action = tx
            .fast_append()
            .set_snapshot_properties(wap_properties)
            .stage_only()
            .add_data_files(vec![part_data_file("test/staged.parquet", 0)]);
        let tx = action.apply(tx).unwrap();
        let staged_table = tx.commit(&catalog).await.unwrap();

        // RE-PARSE the committed metadata from the catalog (on-disk truth, not the in-memory builder result).
        let reloaded = catalog.load_table(staged_table.identifier()).await.unwrap();
        let metadata = reloaded.metadata();

        // A new snapshot EXISTS in `snapshots`.
        assert_eq!(
            metadata.snapshots().count(),
            base_snapshot_count + 1,
            "the staged snapshot must be added to the table's snapshots"
        );
        let staged_id = staged_table.metadata().current_snapshot_id();
        // (`staged_table` is the in-memory builder result; its current id is also the BASE — see below. We
        // identify the staged snapshot as the one not in the base.)
        let staged_snapshot = metadata
            .snapshots()
            .find(|s| Some(s.snapshot_id()) != base_current_id)
            .expect("the staged snapshot must be present in metadata");

        // current-snapshot-id UNCHANGED (still the base).
        assert_eq!(
            metadata.current_snapshot_id(),
            base_current_id,
            "stage_only must NOT advance current-snapshot-id"
        );
        // The in-memory builder result agrees (the staging did not move main there either).
        assert_eq!(
            staged_id, base_current_id,
            "stage_only leaves the returned table's main at the base"
        );

        // main ref UNCHANGED.
        assert_eq!(
            metadata
                .snapshot_for_ref(MAIN_BRANCH)
                .unwrap()
                .snapshot_id(),
            base_main_ref,
            "stage_only must NOT move the main ref"
        );

        // snapshot-log UNCHANGED — no entry for the staged snapshot (Java does not advance the log).
        assert_eq!(
            metadata.history().to_vec(),
            base_snapshot_log,
            "stage_only must NOT add a snapshot-log entry (Java addSnapshot leaves snapshotLog untouched)"
        );
        assert!(
            !metadata
                .history()
                .iter()
                .any(|entry| entry.snapshot_id == staged_snapshot.snapshot_id()),
            "the snapshot-log must carry NO entry for the staged snapshot"
        );

        // The staged snapshot's summary carries wap.id.
        assert_eq!(
            staged_snapshot
                .summary()
                .additional_properties
                .get("wap.id")
                .map(String::as_str),
            Some("wap-staged-1"),
            "the staged snapshot's summary must carry the wap.id"
        );

        // Sequence number == what a normal commit would assign (Java apply() is stageOnly-independent).
        assert_eq!(
            staged_snapshot.sequence_number(),
            expected_seq,
            "a staged snapshot consumes a sequence number exactly like a normal commit"
        );
    }

    /// A scan after staging returns the PRE-staging data — refs untouched ⇒ readers are unaffected. The
    /// live file set of the current snapshot after a staged append is exactly the base file set; the staged
    /// file is NOT visible.
    ///
    /// Risk pinned: a staging that leaks the staged (unaudited) data into the readable table.
    #[tokio::test]
    async fn test_fast_append_stage_only_scan_returns_pre_staging_data() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let table = append_published(&catalog, &table, vec![part_data_file(
            "test/base.parquet",
            9,
        )])
        .await;

        // Stage an append.
        let tx = Transaction::new(&table);
        let action = tx
            .fast_append()
            .stage_only()
            .add_data_files(vec![part_data_file("test/staged.parquet", 0)]);
        let tx = action.apply(tx).unwrap();
        let staged_table = tx.commit(&catalog).await.unwrap();

        // The current snapshot's live set (what a reader sees) is exactly the base — the staged file is absent.
        let reloaded = catalog.load_table(staged_table.identifier()).await.unwrap();
        assert_eq!(
            live_data_file_paths(&reloaded).await,
            HashSet::from(["test/base.parquet".to_string()]),
            "the readable (current-snapshot) live set must be the pre-staging data; the staged file is hidden"
        );
    }

    /// E2E: stage an append (wap.id), then cherry-pick the staged snapshot — the publish makes the staged
    /// data visible. Because the staged snapshot's parent == head, this is a FAST-FORWARD: the staged
    /// snapshot itself becomes current verbatim (no new snapshot, no `published-wap-id` rename), so it keeps
    /// its ORIGINAL `wap.id`. Proves the stage_only WRITE path produces a snapshot the existing cherry-pick
    /// PUBLISH machinery consumes.
    ///
    /// Risk pinned: a staged snapshot the publish path can't consume (a broken WAP write/publish handshake).
    #[tokio::test]
    async fn test_fast_append_stage_only_then_cherry_pick_publishes_with_wap_id() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let table = append_published(&catalog, &table, vec![part_data_file(
            "test/base.parquet",
            9,
        )])
        .await;
        let base_id = table.metadata().current_snapshot_id().unwrap();

        // Stage an append (parent == base == head ⇒ cherry-pick will FAST-FORWARD to it, publishing verbatim).
        let mut wap_properties = HashMap::new();
        wap_properties.insert("wap.id".to_string(), "wap-e2e".to_string());
        let tx = Transaction::new(&table);
        let action = tx
            .fast_append()
            .set_snapshot_properties(wap_properties)
            .stage_only()
            .add_data_files(vec![part_data_file("test/staged.parquet", 0)]);
        let tx = action.apply(tx).unwrap();
        let staged_table = tx.commit(&catalog).await.unwrap();

        // Identify the staged snapshot id (the one that is not the base and not on main).
        let reloaded = catalog.load_table(staged_table.identifier()).await.unwrap();
        let staged_id = reloaded
            .metadata()
            .snapshots()
            .map(|s| s.snapshot_id())
            .find(|id| *id != base_id)
            .expect("staged snapshot present");
        // main is still at the base (not published yet).
        assert_eq!(reloaded.metadata().current_snapshot_id(), Some(base_id));

        // Publish via cherry-pick. Parent == head ⇒ fast-forward: main moves to the staged snapshot AS-IS.
        let tx = Transaction::new(&reloaded);
        let action = tx.cherry_pick(staged_id);
        let tx = action.apply(tx).unwrap();
        let published = tx.commit(&catalog).await.unwrap();

        // main is now the staged snapshot; the staged data is visible.
        assert_eq!(
            published.metadata().current_snapshot_id(),
            Some(staged_id),
            "cherry-pick fast-forwards main to the staged snapshot"
        );
        assert!(
            live_data_file_paths(&published)
                .await
                .contains("test/staged.parquet"),
            "the staged data is visible after the publish"
        );
        // A fast-forward publishes the staged snapshot verbatim, so its summary carries the ORIGINAL wap.id
        // (the staged snapshot is its own published snapshot — no new snapshot, no `published-wap-id` rename).
        assert_eq!(
            published
                .metadata()
                .current_snapshot()
                .unwrap()
                .summary()
                .additional_properties
                .get("wap.id")
                .map(String::as_str),
            Some("wap-e2e"),
            "the fast-forward-published staged snapshot keeps its wap.id"
        );
    }

    /// MULTIPLE staged snapshots coexist: stage two appends off the same base; neither is visible on main
    /// (both invisible to a reader), and each is independently cherry-pickable. This pins that staging is
    /// additive — two staged snapshots can sit in metadata without colliding.
    ///
    /// Java constraint note: each is a distinct dangling snapshot off the base. We stage A, then re-base off
    /// the still-unmoved main and stage B. Both have parent == base; after publishing A (fast-forward),
    /// B's parent (base) is no longer head, so B REPLAYS — still publishable, in either order.
    ///
    /// Risk pinned: a second staged commit clobbering the first (lost staged snapshot) or making it
    /// un-publishable.
    #[tokio::test]
    async fn test_two_staged_snapshots_coexist_and_each_publishes() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let table = append_published(&catalog, &table, vec![part_data_file(
            "test/base.parquet",
            9,
        )])
        .await;
        let base_id = table.metadata().current_snapshot_id().unwrap();

        // Stage A.
        let tx = Transaction::new(&table);
        let action = tx
            .fast_append()
            .stage_only()
            .add_data_files(vec![part_data_file("test/staged_a.parquet", 0)]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        // Stage B (main still at base).
        let tx = Transaction::new(&table);
        let action = tx
            .fast_append()
            .stage_only()
            .add_data_files(vec![part_data_file("test/staged_b.parquet", 1)]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        let reloaded = catalog.load_table(table.identifier()).await.unwrap();
        // main is STILL the base — neither staged snapshot is visible.
        assert_eq!(reloaded.metadata().current_snapshot_id(), Some(base_id));
        assert_eq!(
            live_data_file_paths(&reloaded).await,
            HashSet::from(["test/base.parquet".to_string()]),
            "neither staged snapshot is visible on main"
        );

        // Both staged snapshots exist in metadata (base + A + B = 3 snapshots).
        let staged_ids: Vec<i64> = reloaded
            .metadata()
            .snapshots()
            .map(|s| s.snapshot_id())
            .filter(|id| *id != base_id)
            .collect();
        assert_eq!(
            staged_ids.len(),
            2,
            "both staged snapshots coexist in metadata"
        );

        // Find which staged snapshot carries which file (order-independent).
        let staged_a = find_staged_with_file(&reloaded, base_id, "test/staged_a.parquet").await;
        let staged_b = find_staged_with_file(&reloaded, base_id, "test/staged_b.parquet").await;

        // Publish A (parent == base == head ⇒ fast-forward).
        let tx = Transaction::new(&reloaded);
        let tx = tx.cherry_pick(staged_a).apply(tx).unwrap();
        let after_a = tx.commit(&catalog).await.unwrap();
        assert_eq!(after_a.metadata().current_snapshot_id(), Some(staged_a));
        assert!(
            live_data_file_paths(&after_a)
                .await
                .contains("test/staged_a.parquet")
        );

        // Publish B (parent == base, now NOT head ⇒ replays into a new snapshot). Still publishable.
        let tx = Transaction::new(&after_a);
        let tx = tx.cherry_pick(staged_b).apply(tx).unwrap();
        let after_b = tx.commit(&catalog).await.unwrap();
        let live = live_data_file_paths(&after_b).await;
        assert!(
            live.contains("test/staged_a.parquet") && live.contains("test/staged_b.parquet"),
            "both staged snapshots' data is visible after both publishes: {live:?}"
        );
    }

    /// REVERSE-ORDER coexist publish: stage A then B (as above), but publish B FIRST (fast-forward, B's
    /// parent == base == head) then A (replay, A's parent base is no longer head). Pins that the
    /// coexist+publish is symmetric — neither stage order forces a particular publish order, and both
    /// staged snapshots' data lands regardless. Risk pinned: an asymmetry where only one publish order works.
    #[tokio::test]
    async fn test_two_staged_snapshots_publish_in_reverse_order() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let table = append_published(&catalog, &table, vec![part_data_file(
            "test/base.parquet",
            9,
        )])
        .await;
        let base_id = table.metadata().current_snapshot_id().unwrap();

        // Stage A then B (both parent == base; main stays at base).
        let tx = Transaction::new(&table);
        let tx = tx
            .fast_append()
            .stage_only()
            .add_data_files(vec![part_data_file("test/staged_a.parquet", 0)])
            .apply(tx)
            .unwrap();
        let table = tx.commit(&catalog).await.unwrap();
        let tx = Transaction::new(&table);
        let tx = tx
            .fast_append()
            .stage_only()
            .add_data_files(vec![part_data_file("test/staged_b.parquet", 1)])
            .apply(tx)
            .unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        let reloaded = catalog.load_table(table.identifier()).await.unwrap();
        assert_eq!(reloaded.metadata().current_snapshot_id(), Some(base_id));
        let staged_a = find_staged_with_file(&reloaded, base_id, "test/staged_a.parquet").await;
        let staged_b = find_staged_with_file(&reloaded, base_id, "test/staged_b.parquet").await;

        // Publish B FIRST (fast-forward: B's parent == base == head).
        let tx = Transaction::new(&reloaded);
        let tx = tx.cherry_pick(staged_b).apply(tx).unwrap();
        let after_b = tx.commit(&catalog).await.unwrap();
        assert_eq!(
            after_b.metadata().current_snapshot_id(),
            Some(staged_b),
            "publishing B first fast-forwards main to B"
        );

        // Publish A SECOND (replay: A's parent base is no longer head).
        let tx = Transaction::new(&after_b);
        let tx = tx.cherry_pick(staged_a).apply(tx).unwrap();
        let after_a = tx.commit(&catalog).await.unwrap();
        let live = live_data_file_paths(&after_a).await;
        assert!(
            live.contains("test/staged_a.parquet") && live.contains("test/staged_b.parquet"),
            "both staged snapshots' data is visible after the reverse-order publishes: {live:?}"
        );
    }

    /// Find the staged snapshot (not the base) whose own manifests contain `file_path`.
    async fn find_staged_with_file(table: &Table, base_id: i64, file_path: &str) -> i64 {
        for snapshot in table.metadata().snapshots() {
            if snapshot.snapshot_id() == base_id {
                continue;
            }
            let manifest_list = snapshot
                .load_manifest_list(table.file_io(), table.metadata())
                .await
                .unwrap();
            for manifest_file in manifest_list.entries() {
                if manifest_file.added_snapshot_id != snapshot.snapshot_id() {
                    continue;
                }
                let manifest = manifest_file.load_manifest(table.file_io()).await.unwrap();
                if manifest
                    .entries()
                    .iter()
                    .any(|e| e.file_path() == file_path)
                {
                    return snapshot.snapshot_id();
                }
            }
        }
        panic!("no staged snapshot carries {file_path}");
    }

    /// RETENTION: a staged-but-never-published snapshot is EXPIRABLE once aged (ExpireSnapshots removes it
    /// per the normal unreferenced-snapshot rule — a staged snapshot is referenced by NO ref). Stage a
    /// snapshot, then expire with a cutoff in the future ⇒ the staged snapshot is removed; the base (on main)
    /// survives.
    ///
    /// Risk pinned: a staged snapshot becoming un-collectable garbage (a permanent metadata leak) — or,
    /// inversely, expiry refusing to touch it. Java treats a staged (unreferenced) snapshot as a normal
    /// unreferenced-snapshot expiry candidate.
    #[tokio::test]
    async fn test_staged_unpublished_snapshot_is_expirable_once_aged() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let table = append_published(&catalog, &table, vec![part_data_file(
            "test/base.parquet",
            9,
        )])
        .await;
        let base_id = table.metadata().current_snapshot_id().unwrap();

        // Stage an append (it is unreferenced — no ref points at it).
        let tx = Transaction::new(&table);
        let action = tx
            .fast_append()
            .stage_only()
            .add_data_files(vec![part_data_file("test/staged.parquet", 0)]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();
        let reloaded = catalog.load_table(table.identifier()).await.unwrap();
        let staged_id = reloaded
            .metadata()
            .snapshots()
            .map(|s| s.snapshot_id())
            .find(|id| *id != base_id)
            .expect("staged snapshot present");
        assert_eq!(reloaded.metadata().snapshots().count(), 2);

        // Expire with a cutoff far in the future so the staged (unreferenced) snapshot ages out. main and
        // its ancestry (the base) are retained as a branch head; the staged snapshot is not referenced.
        let far_future = i64::MAX / 2;
        let tx = Transaction::new(&reloaded);
        let action = tx
            .expire_snapshots()
            .expire_older_than(far_future)
            .retain_last(1);
        let tx = action.apply(tx).unwrap();
        let expired = tx.commit(&catalog).await.unwrap();
        let expired_reloaded = catalog.load_table(expired.identifier()).await.unwrap();

        let surviving_ids: HashSet<i64> = expired_reloaded
            .metadata()
            .snapshots()
            .map(|s| s.snapshot_id())
            .collect();
        assert!(
            !surviving_ids.contains(&staged_id),
            "the staged-but-unpublished snapshot must be expired once aged (unreferenced)"
        );
        assert!(
            surviving_ids.contains(&base_id),
            "the base snapshot on main survives expiry"
        );
        // main is intact.
        assert_eq!(
            expired_reloaded.metadata().current_snapshot_id(),
            Some(base_id)
        );
    }

    /// MUTATION-BAIT. Pins that the crown jewel actually catches a staging that ALSO emits the ref update.
    /// This builds the SAME staged commit but ALSO publishes (a normal append), and asserts the table DID
    /// move main / advance current-snapshot-id — proving that the crown jewel's "UNCHANGED" assertions would
    /// FAIL if `stage_only()` were neutered to emit the `SetSnapshotRef` (i.e. behave like a normal append).
    ///
    /// Risk pinned: a stage_only that silently degrades to a publish (the mutation `if !self.stage_only` →
    /// `if true` in the producer). Under that mutation, a staged append moves main exactly like this
    /// published append does — so the crown jewel's current-id/main-ref/snapshot-log assertions flip and fail.
    #[tokio::test]
    async fn test_published_append_moves_main_proving_stage_only_assertions_are_load_bearing() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let table = append_published(&catalog, &table, vec![part_data_file(
            "test/base.parquet",
            9,
        )])
        .await;
        let base_id = table.metadata().current_snapshot_id();
        let base_log_len = table.metadata().history().len();

        // A NORMAL (non-staged) append — the behavior the stage_only mutation would degrade into.
        let published = append_published(&catalog, &table, vec![part_data_file(
            "test/pub.parquet",
            0,
        )])
        .await;
        let reloaded = catalog.load_table(published.identifier()).await.unwrap();

        // It MOVED main + advanced the snapshot-log — the exact opposite of the crown-jewel pins. So if
        // stage_only emitted the ref update, the crown jewel would see these and fail.
        assert_ne!(
            reloaded.metadata().current_snapshot_id(),
            base_id,
            "a PUBLISHED append moves current-snapshot-id (the mutation would make stage_only do this)"
        );
        assert_eq!(
            reloaded.metadata().history().len(),
            base_log_len + 1,
            "a PUBLISHED append adds a snapshot-log entry (the mutation would make stage_only do this)"
        );
    }

    // Concurrent publish vs staged commit, BOTH orders. A staged commit asserts ONLY UuidMatch (UUID never
    // changes), so it must NOT conflict with a concurrent normal publish that moved main — in EITHER order.
    // Java derives only AssertTableUUID for an AddSnapshot-only update set (no AddSnapshot case in
    // UpdateRequirements.Builder.update; 1.10.0 bytecode-confirmed). Risk pinned: an over-strict staged
    // requirement that spuriously conflicts with a concurrent publish (false-positive commit failure).
    #[tokio::test]
    async fn test_stage_only_does_not_conflict_with_concurrent_publish_stage_second() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let table = append_published(&catalog, &table, vec![part_data_file(
            "test/base.parquet",
            9,
        )])
        .await;
        let base_id = table.metadata().current_snapshot_id();

        // Plan a staged action off `table` (head == base).
        let tx_staged = Transaction::new(&table);
        let staged_action = tx_staged
            .fast_append()
            .stage_only()
            .add_data_files(vec![part_data_file("test/staged.parquet", 0)]);
        let tx_staged = staged_action.apply(tx_staged).unwrap();

        // A CONCURRENT normal publish lands first, moving main off base.
        let published = append_published(&catalog, &table, vec![part_data_file(
            "test/pub.parquet",
            1,
        )])
        .await;
        let published_id = published.metadata().current_snapshot_id();
        assert_ne!(published_id, base_id, "concurrent publish moved main");

        // Now commit the staged action. With only UuidMatch, it must SUCCEED despite main having moved.
        let staged_table = tx_staged
            .commit(&catalog)
            .await
            .expect("staged commit must succeed after a concurrent publish (UuidMatch only)");

        let reloaded = catalog.load_table(staged_table.identifier()).await.unwrap();
        // main is STILL the concurrent publish (the staged commit did not move it).
        assert_eq!(
            reloaded.metadata().current_snapshot_id(),
            published_id,
            "staged commit must NOT move main; the concurrent publish stays current"
        );
        // The staged snapshot was added (base + publish + staged = 3).
        assert_eq!(reloaded.metadata().snapshots().count(), 3);
        // The staged file is NOT live (still staged).
        assert!(
            !live_data_file_paths(&reloaded)
                .await
                .contains("test/staged.parquet"),
            "the staged file stays hidden after the concurrent publish + stage"
        );
        // The published file IS live.
        assert!(
            live_data_file_paths(&reloaded)
                .await
                .contains("test/pub.parquet")
        );
    }

    /// The reverse of the above: stage first, then a normal publish lands. The publish's
    /// `RefSnapshotIdMatch(main=base)` must still hold because staging did not move main, so the publish
    /// succeeds. Risk pinned: a staged commit that silently moved main would make a subsequent publish's
    /// ref guard fail (a staged write leaking into the ref that a later publish trips over).
    #[tokio::test]
    async fn test_stage_only_does_not_block_subsequent_publish_stage_first() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let table = append_published(&catalog, &table, vec![part_data_file(
            "test/base.parquet",
            9,
        )])
        .await;
        let base_id = table.metadata().current_snapshot_id();

        // Stage first (main stays at base).
        let tx = Transaction::new(&table);
        let action = tx
            .fast_append()
            .stage_only()
            .add_data_files(vec![part_data_file("test/staged.parquet", 0)]);
        let tx = action.apply(tx).unwrap();
        let after_stage = tx.commit(&catalog).await.unwrap();
        assert_eq!(
            after_stage.metadata().current_snapshot_id(),
            base_id,
            "staging did not move main"
        );

        // Now a NORMAL publish off the staged table — its RefSnapshotIdMatch(main=base) must still hold
        // (staging did not move main), so the publish SUCCEEDS.
        let published = append_published(&catalog, &after_stage, vec![part_data_file(
            "test/pub.parquet",
            1,
        )])
        .await;
        let reloaded = catalog.load_table(published.identifier()).await.unwrap();
        assert_ne!(
            reloaded.metadata().current_snapshot_id(),
            base_id,
            "the publish after a stage moves main"
        );
        // All three snapshots exist; the published file is live, the staged file is not.
        assert_eq!(reloaded.metadata().snapshots().count(), 3);
        let live = live_data_file_paths(&reloaded).await;
        assert!(live.contains("test/pub.parquet"));
        assert!(!live.contains("test/staged.parquet"));
    }

    /// A staged commit that RE-BASES after a concurrent publish landed must recompute BOTH the sequence
    /// number AND the parent against the REFRESHED base. Java `apply()` refreshes then reads
    /// `base.nextSequenceNumber()` + `latestSnapshot(base, main)` unconditionally (no stageOnly branch;
    /// 1.10.0 bytecode), so a staged retry against a moved head gets the NEW seq and the NEW parent.
    /// Risk pinned: a stale seq or stale parent on retry — on-disk corruption (a staged snapshot whose
    /// seq <= last_sequence_number, or whose parent points at a superseded head).
    #[tokio::test]
    async fn test_stage_only_rebases_seq_and_parent_after_concurrent_publish() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let table = append_published(&catalog, &table, vec![part_data_file(
            "test/base.parquet",
            9,
        )])
        .await;
        let base_id = table.metadata().current_snapshot_id();
        // The seq a staged commit off `base` ALONE would get.
        let seq_if_no_concurrent = table.metadata().next_sequence_number();

        // Plan a staged action off the stale `table` (head == base).
        let tx_staged = Transaction::new(&table);
        let staged_action = tx_staged
            .fast_append()
            .stage_only()
            .add_data_files(vec![part_data_file("test/staged.parquet", 0)]);
        let tx_staged = staged_action.apply(tx_staged).unwrap();

        // A concurrent publish lands, bumping last_sequence_number and moving main.
        let published = append_published(&catalog, &table, vec![part_data_file(
            "test/pub.parquet",
            1,
        )])
        .await;
        let published_id = published.metadata().current_snapshot_id().unwrap();
        let published_seq = published
            .metadata()
            .current_snapshot()
            .unwrap()
            .sequence_number();
        // The seq a staged commit off the REFRESHED base should get == published_seq + 1.
        let expected_staged_seq = published.metadata().next_sequence_number();
        assert!(
            expected_staged_seq > seq_if_no_concurrent,
            "the concurrent publish advanced the next sequence number"
        );

        // Commit the staged action: it re-bases onto the refreshed head (the publish).
        let staged_table = tx_staged.commit(&catalog).await.unwrap();
        let reloaded = catalog.load_table(staged_table.identifier()).await.unwrap();

        // Find the staged snapshot (the one that's neither base nor publish).
        let staged_snapshot = reloaded
            .metadata()
            .snapshots()
            .find(|s| Some(s.snapshot_id()) != base_id && s.snapshot_id() != published_id)
            .expect("staged snapshot present after rebase");

        // SEQ recomputed against the REFRESHED base (NOT the stale base's next seq).
        assert_eq!(
            staged_snapshot.sequence_number(),
            expected_staged_seq,
            "staged retry must take the REFRESHED base's next sequence number (got stale seq = corruption)"
        );
        assert!(
            staged_snapshot.sequence_number() > published_seq,
            "the staged snapshot's seq must be strictly greater than the concurrent publish's seq"
        );
        // PARENT recomputed to the refreshed head (the concurrent publish), matching Java
        // latestSnapshot(base, main) on the refreshed base.
        assert_eq!(
            staged_snapshot.parent_snapshot_id(),
            Some(published_id),
            "the staged snapshot's parent must be the refreshed head (the concurrent publish), not the stale base"
        );
        // main is STILL the publish (staging moved nothing).
        assert_eq!(
            reloaded.metadata().current_snapshot_id(),
            Some(published_id),
            "the staged retry did not move main"
        );
    }

    /// EXACT UPDATE-SET + REQUIREMENT-SET pin (the wire-level Java-parity oracle, mirroring `test_fast_append`
    /// for the staged path). A staged `fast_append` emits EXACTLY one update — `AddSnapshot` ALONE (no
    /// `SetSnapshotRef`) — and EXACTLY one requirement — `UuidMatch` ALONE (no `RefSnapshotIdMatch`). This is
    /// what Java's `UpdateRequirements.forUpdateTable(base, [AddSnapshot])` derives: `AssertTableUUID` only
    /// (the `Builder.update` dispatcher has NO `AddSnapshot` case — 1.10.0 bytecode). This pins the
    /// requirement-set at the SOURCE (the ActionCommit), where the end-to-end concurrency tests cannot — the
    /// retry/rebase machinery recomputes a `RefSnapshotIdMatch` against the refreshed base, so an over-strict
    /// staged requirement is invisible end-to-end in MemoryCatalog but DIVERGES from Java's REST wire protocol.
    ///
    /// Risk pinned: an over-strict staged commit emitting `RefSnapshotIdMatch` (a REST-interop divergence:
    /// Java derives no ref requirement for an AddSnapshot-only update) OR an under-strict one dropping the
    /// UuidMatch (a staged snapshot clobbering a different table's metadata).
    #[tokio::test]
    async fn test_stage_only_emits_add_snapshot_alone_with_uuid_match_only() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let table = append_published(&catalog, &table, vec![part_data_file(
            "test/base.parquet",
            9,
        )])
        .await;

        let tx = Transaction::new(&table);
        let action = tx
            .fast_append()
            .stage_only()
            .add_data_files(vec![part_data_file("test/staged.parquet", 0)]);
        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();
        let requirements = action_commit.take_requirements();

        // EXACTLY one update: AddSnapshot alone (no SetSnapshotRef).
        assert_eq!(
            updates.len(),
            1,
            "staged commit must emit exactly one update"
        );
        assert!(
            matches!(&updates[0], TableUpdate::AddSnapshot { .. }),
            "the sole staged update must be AddSnapshot, got {:?}",
            updates[0]
        );

        // EXACTLY one requirement: UuidMatch alone (no RefSnapshotIdMatch) — Java's AssertTableUUID-only.
        assert_eq!(
            requirements,
            vec![TableRequirement::UuidMatch {
                uuid: table.metadata().uuid()
            }],
            "the staged requirement-set must be UuidMatch ALONE (Java forUpdateTable for AddSnapshot-only)"
        );
    }

    /// wap.id rides the set_snapshot_properties channel and coexists with producer-generated summary keys
    /// (operation, added counts) — no clobbering in EITHER direction. Risk pinned: the wap.id channel
    /// silently overwriting a producer count, or a producer count overwriting wap.id.
    #[tokio::test]
    async fn test_stage_only_wap_id_coexists_with_producer_summary_keys() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let table = append_published(&catalog, &table, vec![part_data_file(
            "test/base.parquet",
            9,
        )])
        .await;
        let base_current_id = table.metadata().current_snapshot_id();

        let mut props = HashMap::new();
        props.insert("wap.id".to_string(), "wap-coexist-1".to_string());
        let tx = Transaction::new(&table);
        let action = tx
            .fast_append()
            .set_snapshot_properties(props)
            .stage_only()
            .add_data_files(vec![part_data_file("test/staged.parquet", 0)]);
        let tx = action.apply(tx).unwrap();
        let staged_table = tx.commit(&catalog).await.unwrap();
        let reloaded = catalog.load_table(staged_table.identifier()).await.unwrap();

        let staged_snapshot = reloaded
            .metadata()
            .snapshots()
            .find(|s| Some(s.snapshot_id()) != base_current_id)
            .unwrap();
        let summary = staged_snapshot.summary();

        // The producer-generated operation field is intact (NOT clobbered by wap.id).
        assert_eq!(
            summary.operation,
            Operation::Append,
            "wap.id must not clobber the snapshot operation"
        );
        // wap.id is present in the additional properties.
        assert_eq!(
            summary
                .additional_properties
                .get("wap.id")
                .map(String::as_str),
            Some("wap-coexist-1")
        );
        // Producer-generated count keys coexist (the staged append added exactly one data file).
        assert_eq!(
            summary
                .additional_properties
                .get("added-data-files")
                .map(String::as_str),
            Some("1"),
            "the producer-generated added-data-files key coexists with wap.id"
        );
        // The wap.id did NOT overwrite any producer count (sanity: the two key namespaces are disjoint).
        assert!(
            summary.additional_properties.contains_key("added-records"),
            "producer record-count key present alongside wap.id"
        );
    }

    /// Reader-invisibility breadth. A staged snapshot is invisible to the DEFAULT scan (refs untouched) but
    /// READABLE by explicit snapshot id (time-travel), matching Java's posture — a staged snapshot IS a
    /// valid time-travel target, it just isn't the table default. Also pins the inspect posture: the
    /// `snapshots` metadata-table source includes it; the `history` (snapshot-log) source does not.
    /// Risk pinned: a staged snapshot leaking into the default reader, or being unreachable by explicit id.
    #[tokio::test]
    async fn test_stage_only_snapshot_readable_by_explicit_id_but_not_default_scan() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let table = append_published(&catalog, &table, vec![part_data_file(
            "test/base.parquet",
            9,
        )])
        .await;
        let base_id = table.metadata().current_snapshot_id();

        let tx = Transaction::new(&table);
        let action = tx
            .fast_append()
            .stage_only()
            .add_data_files(vec![part_data_file("test/staged.parquet", 0)]);
        let tx = action.apply(tx).unwrap();
        let staged_table = tx.commit(&catalog).await.unwrap();
        let reloaded = catalog.load_table(staged_table.identifier()).await.unwrap();

        let staged_id = reloaded
            .metadata()
            .snapshots()
            .map(|s| s.snapshot_id())
            .find(|id| Some(*id) != base_id)
            .unwrap();

        // Default scan (current snapshot) does NOT see the staged file.
        assert!(
            !live_data_file_paths(&reloaded)
                .await
                .contains("test/staged.parquet"),
            "default scan must not see the staged data"
        );

        // Explicit time-travel by the staged snapshot id DOES build a scan (Java allows reading a staged
        // snapshot by explicit id), and the staged snapshot's live file set contains the staged data.
        reloaded
            .scan()
            .snapshot_id(staged_id)
            .build()
            .expect("scan by an explicit staged snapshot id must build");
        let staged_snapshot = reloaded.metadata().snapshot_by_id(staged_id).unwrap();
        let manifest_list = staged_snapshot
            .load_manifest_list(reloaded.file_io(), reloaded.metadata())
            .await
            .unwrap();
        let mut staged_paths: HashSet<String> = HashSet::new();
        for manifest_file in manifest_list.entries() {
            if manifest_file.content != ManifestContentType::Data {
                continue;
            }
            let manifest = manifest_file
                .load_manifest(reloaded.file_io())
                .await
                .unwrap();
            for entry in manifest.entries() {
                if entry.is_alive() && entry.status() != ManifestStatus::Deleted {
                    staged_paths.insert(entry.file_path().to_string());
                }
            }
        }
        assert!(
            staged_paths.contains("test/staged.parquet"),
            "the staged snapshot (readable by explicit id) carries the staged data: {staged_paths:?}"
        );

        // inspect posture: the `snapshots` metadata-table source includes the staged snapshot; the `history`
        // (snapshot-log) source does NOT (Java: history derives from snapshotLog; snapshots iterates all).
        let snapshot_table_ids: HashSet<i64> = reloaded
            .metadata()
            .snapshots()
            .map(|s| s.snapshot_id())
            .collect();
        assert!(
            snapshot_table_ids.contains(&staged_id),
            "the `snapshots` metadata table must include the staged snapshot"
        );
        assert!(
            !reloaded
                .metadata()
                .history()
                .iter()
                .any(|entry| entry.snapshot_id == staged_id),
            "the `history` metadata table (snapshot-log) must NOT include the staged snapshot"
        );
    }
}
