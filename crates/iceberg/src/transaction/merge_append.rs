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
    MergeManifestProcess, SnapshotProduceOperation, SnapshotProducer,
};
use crate::transaction::{ActionCommit, TransactionAction};

/// A transaction action that appends data files in MERGE mode (Java `MergeAppend` /
/// `AppendFiles` "merge" mode).
///
/// Like [`FastAppendAction`](crate::transaction::append::FastAppendAction) it adds data files in a single
/// `Append` snapshot, but instead of always writing one new manifest it bin-packs the table's manifests
/// per partition spec and MERGES small bins into a single manifest (Java `ManifestMergeManager`), so the
/// manifest count stays bounded as the table accumulates appends. The merge thresholds are read from the
/// table properties (`commit.manifest.target-size-bytes`, `commit.manifest.min-count-to-merge`,
/// `commit.manifest-merge.enabled`); when merging is disabled this behaves exactly like a fast append.
pub struct MergeAppendAction {
    check_duplicate: bool,
    // Below: the inputs used to build the SnapshotProducer at commit time.
    commit_uuid: Option<Uuid>,
    key_metadata: Option<Vec<u8>>,
    snapshot_properties: HashMap<String, String>,
    added_data_files: Vec<DataFile>,
}

impl MergeAppendAction {
    pub(crate) fn new() -> Self {
        Self {
            check_duplicate: true,
            commit_uuid: None,
            key_metadata: None,
            snapshot_properties: HashMap::default(),
            added_data_files: vec![],
        }
    }

    /// Set whether to check that the added files are not already referenced by the table.
    pub fn with_check_duplicate(mut self, v: bool) -> Self {
        self.check_duplicate = v;
        self
    }

    /// Add data files to the snapshot.
    pub fn add_data_files(mut self, data_files: impl IntoIterator<Item = DataFile>) -> Self {
        self.added_data_files.extend(data_files);
        self
    }

    /// Set the commit UUID for the snapshot (used to name the manifest files it writes).
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
impl TransactionAction for MergeAppendAction {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        let snapshot_producer = SnapshotProducer::new(
            table,
            self.commit_uuid.unwrap_or_else(Uuid::now_v7),
            self.key_metadata.clone(),
            self.snapshot_properties.clone(),
            self.added_data_files.clone(),
        );

        // Validate the added files (same contract as fast append: data content + matching default spec).
        snapshot_producer.validate_added_data_files()?;

        // Reject files already referenced by the table.
        if self.check_duplicate {
            snapshot_producer.validate_duplicate_files().await?;
        }

        // Read the merge configuration from the table properties and drive the producer with the merging
        // manifest post-processor (the ONLY difference from `FastAppendAction`, which uses the
        // pass-through `DefaultManifestProcess`).
        let merge_process = MergeManifestProcess::new(table.metadata().properties());

        snapshot_producer
            .commit(MergeAppendOperation, merge_process)
            .await
    }
}

/// The snapshot-produce operation for a merging append: an `Append` that adds files and never removes any.
///
/// Behaviorally identical to `append.rs`'s fast-append operation — both carry forward every existing
/// manifest with live files and record `Operation::Append`. It is duplicated here (rather than shared)
/// to keep the merge action self-contained; the merging behavior lives entirely in the
/// [`MergeManifestProcess`] post-processor, not the operation.
struct MergeAppendOperation;

impl SnapshotProduceOperation for MergeAppendOperation {
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

        Ok(manifest_list
            .entries()
            .iter()
            .filter(|entry| entry.has_added_files() || entry.has_existing_files())
            .cloned()
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use crate::Catalog;
    use crate::memory::tests::new_memory_catalog;
    use crate::spec::{
        DataContentType, DataFile, DataFileBuilder, DataFileFormat, Literal, Operation, Struct,
        TableProperties,
    };
    use crate::table::Table;
    use crate::transaction::tests::make_v3_minimal_table_in_catalog;
    use crate::transaction::{ApplyTransactionAction, Transaction};

    /// Build a data file routed to partition `(x = part_value)` of the V3 minimal table's spec id 0
    /// (identity(x)), with a unique path.
    fn data_file(path: &str, part_value: i64) -> DataFile {
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

    /// Build a data file for partition spec id 1 — the spec produced by evolving the default spec with an
    /// extra identity(y) field. Its partition tuple is `(x, y)`.
    fn data_file_spec1(path: &str, x: i64, y: i64) -> DataFile {
        DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(path.to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(1)
            .partition(Struct::from_iter([
                Some(Literal::long(x)),
                Some(Literal::long(y)),
            ]))
            .build()
            .unwrap()
    }

    /// Collect the set of live (Added or Existing) data-file paths across the table's current snapshot —
    /// the real correctness signal (exactly what a scan would read). Data loss shrinks this set;
    /// duplication is detected by comparing against the cumulative appended set.
    async fn live_file_paths(table: &Table) -> HashSet<String> {
        let snapshot = table
            .metadata()
            .current_snapshot()
            .expect("table should have a current snapshot");
        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .expect("manifest list should load");

        let mut live = HashSet::new();
        for manifest_file in manifest_list.entries() {
            let manifest = manifest_file
                .load_manifest(table.file_io())
                .await
                .expect("manifest should load");
            for entry in manifest.entries() {
                if entry.is_alive() {
                    live.insert(entry.file_path().to_string());
                }
            }
        }
        live
    }

    /// Total count of LIVE entries across the current snapshot's manifests (NOT de-duplicated by path) —
    /// the duplication detector: if a merge copies an entry twice, this exceeds the appended count even
    /// when [`live_file_paths`] (a set) would hide it.
    async fn live_entry_count(table: &Table) -> usize {
        let snapshot = table.metadata().current_snapshot().unwrap();
        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();
        let mut count = 0;
        for manifest_file in manifest_list.entries() {
            let manifest = manifest_file.load_manifest(table.file_io()).await.unwrap();
            count += manifest.entries().iter().filter(|e| e.is_alive()).count();
        }
        count
    }

    /// Number of manifests in the table's current snapshot.
    async fn manifest_count(table: &Table) -> usize {
        let snapshot = table.metadata().current_snapshot().unwrap();
        snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap()
            .entries()
            .len()
    }

    /// Return `(snapshot_id, sequence_number, file_sequence_number)` of the LIVE entry for `path`.
    async fn entry_provenance(
        table: &Table,
        path: &str,
    ) -> (Option<i64>, Option<i64>, Option<i64>) {
        let snapshot = table.metadata().current_snapshot().unwrap();
        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();
        for manifest_file in manifest_list.entries() {
            let manifest = manifest_file.load_manifest(table.file_io()).await.unwrap();
            for entry in manifest.entries() {
                if entry.is_alive() && entry.file_path() == path {
                    return (
                        entry.snapshot_id(),
                        entry.sequence_number(),
                        entry.file_sequence_number,
                    );
                }
            }
        }
        panic!("no live entry for {path}");
    }

    /// Fast-append the given files in one commit (builds one new manifest, no merge).
    async fn fast_append(catalog: &impl Catalog, table: &Table, files: Vec<DataFile>) -> Table {
        let tx = Transaction::new(table);
        let action = tx.fast_append().add_data_files(files);
        let tx = action.apply(tx).unwrap();
        tx.commit(catalog).await.unwrap()
    }

    /// Merge-append the given files in one commit (builds the new manifest, then merges per the table's
    /// `commit.manifest.*` properties).
    async fn merge_append(catalog: &impl Catalog, table: &Table, files: Vec<DataFile>) -> Table {
        let tx = Transaction::new(table);
        let action = tx.merge_append().add_data_files(files);
        let tx = action.apply(tx).unwrap();
        tx.commit(catalog).await.unwrap()
    }

    /// Set `commit.manifest.min-count-to-merge` (and optionally disable merging) on the table.
    async fn set_merge_props(
        catalog: &impl Catalog,
        table: &Table,
        min_count: Option<usize>,
        merge_enabled: Option<bool>,
    ) -> Table {
        let tx = Transaction::new(table);
        let mut action = tx.update_table_properties();
        if let Some(min_count) = min_count {
            action = action.set(
                TableProperties::PROPERTY_MANIFEST_MIN_MERGE_COUNT.to_string(),
                min_count.to_string(),
            );
        }
        if let Some(enabled) = merge_enabled {
            action = action.set(
                TableProperties::PROPERTY_MANIFEST_MERGE_ENABLED.to_string(),
                enabled.to_string(),
            );
        }
        let tx = action.apply(tx).unwrap();
        tx.commit(catalog).await.unwrap()
    }

    /// KEY TEST — risk: DATA LOSS / DUPLICATION. With `min-count-to-merge = 2`, build N = 3 separate data
    /// manifests via fast-append, then `merge_append` a 4th batch. The resulting snapshot must have FEWER
    /// manifests than the naive N + 1 = 4, the SCAN live set must equal ALL appended rows (nothing lost),
    /// and the live ENTRY count must equal the path count (nothing duplicated).
    #[tokio::test]
    async fn test_merge_append_reduces_manifest_count_and_preserves_live_scan() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let table = set_merge_props(&catalog, &table, Some(2), None).await;

        // Three fast-appends → three separate data manifests.
        let table = fast_append(&catalog, &table, vec![data_file("test/a.parquet", 0)]).await;
        let table = fast_append(&catalog, &table, vec![data_file("test/b.parquet", 0)]).await;
        let table = fast_append(&catalog, &table, vec![data_file("test/c.parquet", 0)]).await;
        assert_eq!(
            manifest_count(&table).await,
            3,
            "three fast-appends must build three separate manifests"
        );

        // Merge-append a 4th batch. The bin holds the new manifest + 3 old (4 ≥ min-count 2) → MERGED.
        let table = merge_append(&catalog, &table, vec![data_file("test/d.parquet", 0)]).await;

        let manifests_after = manifest_count(&table).await;
        assert!(
            manifests_after < 4,
            "merge append must produce FEWER manifests than the naive N+1=4, got {manifests_after}"
        );
        assert_eq!(
            manifests_after, 1,
            "all four same-spec manifests pack into one bin (≥ min-count) and merge into one"
        );

        let expected: HashSet<String> = [
            "test/a.parquet",
            "test/b.parquet",
            "test/c.parquet",
            "test/d.parquet",
        ]
        .into_iter()
        .map(String::from)
        .collect();
        assert_eq!(
            live_file_paths(&table).await,
            expected,
            "no row may be lost by the merge"
        );
        assert_eq!(
            live_entry_count(&table).await,
            4,
            "no row may be duplicated by the merge"
        );
        assert_eq!(
            table
                .metadata()
                .current_snapshot()
                .unwrap()
                .summary()
                .operation,
            Operation::Append
        );
    }

    /// Risk: MERGE-DISABLED REGRESSION. With `commit.manifest-merge.enabled = false`, `merge_append` must
    /// behave exactly like a fast append — it must NOT merge, so the manifest count keeps growing — while
    /// still preserving the live set.
    #[tokio::test]
    async fn test_merge_disabled_behaves_like_fast_append() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        // Low min-count so merging WOULD fire if it were enabled — isolate the disable switch.
        let table = set_merge_props(&catalog, &table, Some(2), Some(false)).await;

        let table = fast_append(&catalog, &table, vec![data_file("test/a.parquet", 0)]).await;
        let table = fast_append(&catalog, &table, vec![data_file("test/b.parquet", 0)]).await;
        assert_eq!(manifest_count(&table).await, 2);

        // Merge-append with merging disabled → a third manifest, NOT a merge.
        let table = merge_append(&catalog, &table, vec![data_file("test/c.parquet", 0)]).await;
        assert_eq!(
            manifest_count(&table).await,
            3,
            "with merging disabled, merge_append must NOT merge (one new manifest, like fast-append)"
        );
        assert_eq!(
            live_file_paths(&table).await,
            ["test/a.parquet", "test/b.parquet", "test/c.parquet"]
                .into_iter()
                .map(String::from)
                .collect()
        );
    }

    /// Risk: CROSS-SPEC MERGE (partition mis-typing). Manifests of DIFFERENT partition specs must NEVER be
    /// merged into one manifest. Build a spec-0 manifest, evolve the spec to id 1, build a spec-1 manifest,
    /// then `merge_append` a spec-1 batch with a low min-count. The spec-1 manifests may merge among
    /// themselves, but the spec-0 manifest must stay a SEPARATE manifest (its spec id preserved), and the
    /// live set must be intact.
    #[tokio::test]
    async fn test_merge_append_does_not_merge_across_partition_specs() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let table = set_merge_props(&catalog, &table, Some(2), None).await;

        // One spec-0 manifest.
        let table = fast_append(&catalog, &table, vec![data_file("test/spec0.parquet", 0)]).await;

        // Evolve the default spec to add identity(y) → new default spec id 1.
        let tx = Transaction::new(&table);
        let action = tx.update_partition_spec().add_field("y");
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();
        assert_eq!(table.metadata().default_partition_spec_id(), 1);

        // One spec-1 manifest via fast-append, then a spec-1 merge-append.
        let table = fast_append(&catalog, &table, vec![data_file_spec1(
            "test/spec1a.parquet",
            0,
            7,
        )])
        .await;
        let table = merge_append(&catalog, &table, vec![data_file_spec1(
            "test/spec1b.parquet",
            0,
            7,
        )])
        .await;

        // Inspect the resulting manifests: every manifest is single-spec; the spec-0 manifest is NOT
        // merged with any spec-1 manifest.
        let manifest_list = table
            .metadata()
            .current_snapshot()
            .unwrap()
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();
        let mut spec_ids: Vec<i32> = manifest_list
            .entries()
            .iter()
            .map(|m| m.partition_spec_id)
            .collect();
        spec_ids.sort_unstable();
        assert!(
            spec_ids.contains(&0),
            "the spec-0 manifest must survive as its own manifest, not be folded into a spec-1 merge"
        );
        assert!(
            spec_ids.contains(&1),
            "the spec-1 manifests must be present"
        );

        // Each manifest carries exactly ONE spec id (a merge never mixed two specs into one manifest).
        for manifest_file in manifest_list.entries() {
            let manifest = manifest_file.load_manifest(table.file_io()).await.unwrap();
            assert!(
                manifest
                    .entries()
                    .iter()
                    .all(|e| e.data_file().partition_spec_id == manifest_file.partition_spec_id),
                "a merged manifest must not contain entries from a different partition spec"
            );
        }

        assert_eq!(
            live_file_paths(&table).await,
            [
                "test/spec0.parquet",
                "test/spec1a.parquet",
                "test/spec1b.parquet"
            ]
            .into_iter()
            .map(String::from)
            .collect()
        );
    }

    /// Risk: PROVENANCE CORRUPTION. When the merge copies an entry from an OLD committed manifest into the
    /// merged manifest, it must copy it as `Existing` carrying its ORIGINAL snapshot id + data/file
    /// sequence numbers — NOT re-stamped with the merge snapshot's id/seq. A wrong data-sequence number
    /// silently breaks merge-on-read delete application and incremental scans. The path-set / count tests
    /// all pass under a re-stamp, so only this test catches it.
    #[tokio::test]
    async fn test_merge_append_preserves_merged_entry_provenance() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let table = set_merge_props(&catalog, &table, Some(2), None).await;

        // A in snapshot S1 (data seq 1), B in snapshot S2 (data seq 2) — distinct provenance.
        let table = fast_append(&catalog, &table, vec![data_file("test/a.parquet", 0)]).await;
        let s1 = table.metadata().current_snapshot().unwrap().snapshot_id();
        let (a_snap, a_seq, a_fseq) = entry_provenance(&table, "test/a.parquet").await;
        assert_eq!(a_snap, Some(s1));

        let table = fast_append(&catalog, &table, vec![data_file("test/b.parquet", 0)]).await;
        let s2 = table.metadata().current_snapshot().unwrap().snapshot_id();
        let (b_snap, b_seq, b_fseq) = entry_provenance(&table, "test/b.parquet").await;
        assert_eq!(b_snap, Some(s2));
        assert_ne!(a_seq, b_seq, "A and B must have different data seq numbers");

        // Merge-append C → A's and B's manifests are merged into one; their entries become Existing and
        // MUST keep their original provenance.
        let table = merge_append(&catalog, &table, vec![data_file("test/c.parquet", 0)]).await;
        let s3 = table.metadata().current_snapshot().unwrap().snapshot_id();
        assert_ne!(s3, s1);
        assert_ne!(s3, s2);
        assert_eq!(
            manifest_count(&table).await,
            1,
            "A, B and C merge into one manifest"
        );

        // A keeps S1 + its original seqs.
        let (a2_snap, a2_seq, a2_fseq) = entry_provenance(&table, "test/a.parquet").await;
        assert_eq!(
            a2_snap,
            Some(s1),
            "merged-forward A must keep its ORIGINAL snapshot id, not S3"
        );
        assert_eq!(
            a2_seq, a_seq,
            "merged-forward A must keep its ORIGINAL data seq"
        );
        assert_eq!(
            a2_fseq, a_fseq,
            "merged-forward A must keep its ORIGINAL file seq"
        );

        // B keeps S2 + its original seqs.
        let (b2_snap, b2_seq, b2_fseq) = entry_provenance(&table, "test/b.parquet").await;
        assert_eq!(
            b2_snap,
            Some(s2),
            "merged-forward B must keep its ORIGINAL snapshot id, not S3"
        );
        assert_eq!(
            b2_seq, b_seq,
            "merged-forward B must keep its ORIGINAL data seq"
        );
        assert_eq!(
            b2_fseq, b_fseq,
            "merged-forward B must keep its ORIGINAL file seq"
        );

        // C (this snapshot's add) belongs to S3 and inherits S3's (new, higher) sequence number.
        let (c_snap, c_seq, _c_fseq) = entry_provenance(&table, "test/c.parquet").await;
        assert_eq!(
            c_snap,
            Some(s3),
            "the newly-added C belongs to the merge snapshot S3"
        );
        assert!(
            c_seq > a_seq && c_seq > b_seq,
            "C's data seq (the new snapshot's) must exceed the merged-forward entries'"
        );
    }

    /// Risk: an over-eager merge that REWRITES a bin that should be left alone. A single-manifest bin must
    /// be carried forward unchanged (Java `bin.size() == 1`). A `merge_append` on an EMPTY table writes
    /// exactly one new manifest, which forms a size-1 bin and must NOT be rewritten — its path is the
    /// freshly-written manifest, and the live set is the appended file.
    #[tokio::test]
    async fn test_merge_append_leaves_single_manifest_group_untouched() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let table = set_merge_props(&catalog, &table, Some(2), None).await;

        let table = merge_append(&catalog, &table, vec![data_file("test/only.parquet", 0)]).await;

        assert_eq!(
            manifest_count(&table).await,
            1,
            "a single new manifest is a size-1 bin and must be kept as-is"
        );
        let snapshot = table.metadata().current_snapshot().unwrap();
        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();
        // The lone manifest holds the just-added file as an ADDED entry (it was not rewritten to Existing).
        let manifest = manifest_list.entries()[0]
            .load_manifest(table.file_io())
            .await
            .unwrap();
        assert_eq!(manifest.entries().len(), 1);
        assert_eq!(manifest.entries()[0].file_path(), "test/only.parquet");
        assert!(
            manifest.entries()[0].status() == crate::spec::ManifestStatus::Added,
            "a size-1 bin must be carried forward unchanged (the entry stays Added, not rewritten)"
        );
        assert_eq!(
            live_file_paths(&table).await,
            ["test/only.parquet"]
                .into_iter()
                .map(String::from)
                .collect()
        );
    }

    /// Risk: BELOW-THRESHOLD MERGE. A bin that contains the new manifest but has fewer than
    /// `min-count-to-merge` manifests must be left SPLIT (Java `bin.contains(first) && bin.size() <
    /// minCountToMerge`). With min-count = 5 and only 2 manifests (1 old + the new), no merge happens.
    #[tokio::test]
    async fn test_merge_append_below_min_count_does_not_merge() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let table = set_merge_props(&catalog, &table, Some(5), None).await;

        let table = fast_append(&catalog, &table, vec![data_file("test/a.parquet", 0)]).await;
        // bin = [old A manifest, new B manifest], size 2 < min-count 5, contains the new manifest → split.
        let table = merge_append(&catalog, &table, vec![data_file("test/b.parquet", 0)]).await;

        assert_eq!(
            manifest_count(&table).await,
            2,
            "a bin with the new manifest below min-count must NOT be merged"
        );
        assert_eq!(
            live_file_paths(&table).await,
            ["test/a.parquet", "test/b.parquet"]
                .into_iter()
                .map(String::from)
                .collect()
        );
    }

    /// Risk: DATA LOSS / DUPLICATION (exact-union). 3 distinct-file manifests + a 4th merge-append
    /// batch of 3 files → the merged manifest's LIVE entry list must equal the EXACT union of all
    /// appended files: count == 6 (no dup) AND path set == the 6 appended paths (no loss). Cross-checks
    /// that the entry-count (not de-duplicated) and the path-set agree, which they only do iff every
    /// entry appears exactly once. A multi-file batch widens the KEY test's single-file batches.
    #[tokio::test]
    async fn test_merge_append_merged_entries_equal_exact_union() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let table = set_merge_props(&catalog, &table, Some(2), None).await;

        let table = fast_append(&catalog, &table, vec![data_file("u/a.parquet", 0)]).await;
        let table = fast_append(&catalog, &table, vec![data_file("u/b.parquet", 0)]).await;
        let table = fast_append(&catalog, &table, vec![data_file("u/c.parquet", 0)]).await;
        let table = merge_append(&catalog, &table, vec![
            data_file("u/d.parquet", 0),
            data_file("u/e.parquet", 0),
            data_file("u/f.parquet", 0),
        ])
        .await;

        let expected: HashSet<String> = ["u/a", "u/b", "u/c", "u/d", "u/e", "u/f"]
            .iter()
            .map(|s| format!("{s}.parquet"))
            .collect();
        let live = live_file_paths(&table).await;
        assert_eq!(
            live, expected,
            "exact union of paths (no loss, no spurious)"
        );
        assert_eq!(
            live_entry_count(&table).await,
            6,
            "exactly 6 live entries (no duplication)"
        );
        // Everything merged into one manifest (4 same-spec manifests, all >= min-count 2).
        assert_eq!(manifest_count(&table).await, 1);
    }

    /// Risk: WRONG THRESHOLD (off-by-one at the boundary). EXACT min-count boundary: min-count = 3;
    /// bin = [2 old manifests + the new manifest] = size 3. Java's keep-separate condition is
    /// `bin.contains(first) && bin.size() < minCountToMerge`; size 3 is NOT < 3, so the bin MERGES.
    /// Pins that `==` merges (the strict `<`), and that the new-manifest-LAST assembly (vs Java's
    /// new-FIRST) does not shift the decision — the check is an order-independent membership + length.
    #[tokio::test]
    async fn test_merge_append_at_exact_min_count_boundary_merges() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let table = set_merge_props(&catalog, &table, Some(3), None).await;

        let table = fast_append(&catalog, &table, vec![data_file("bnd/a.parquet", 0)]).await;
        let table = fast_append(&catalog, &table, vec![data_file("bnd/b.parquet", 0)]).await;
        assert_eq!(manifest_count(&table).await, 2);
        // bin = [old A, old B, new C] size 3 == min-count 3 → merges (size 3 is not < 3).
        let table = merge_append(&catalog, &table, vec![data_file("bnd/c.parquet", 0)]).await;
        assert_eq!(
            manifest_count(&table).await,
            1,
            "exactly min-count manifests including the new one MUST merge (bin.size == min-count)"
        );
        assert_eq!(
            live_file_paths(&table).await,
            ["bnd/a.parquet", "bnd/b.parquet", "bnd/c.parquet"]
                .into_iter()
                .map(String::from)
                .collect()
        );
        assert_eq!(live_entry_count(&table).await, 3);
    }

    /// Risk: WRONG THRESHOLD (merges one too eagerly). One-below boundary: min-count = 4; bin =
    /// [2 old + new] = size 3 < 4 → kept separate (no merge). Confirms the boundary is exactly at
    /// min-count, not min-count-1 (pairs with the exact-boundary test above).
    #[tokio::test]
    async fn test_merge_append_one_below_min_count_does_not_merge() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let table = set_merge_props(&catalog, &table, Some(4), None).await;

        let table = fast_append(&catalog, &table, vec![data_file("bel/a.parquet", 0)]).await;
        let table = fast_append(&catalog, &table, vec![data_file("bel/b.parquet", 0)]).await;
        let table = merge_append(&catalog, &table, vec![data_file("bel/c.parquet", 0)]).await;
        assert_eq!(
            manifest_count(&table).await,
            3,
            "size 3 < min-count 4 with the new manifest present → no merge"
        );
        assert_eq!(live_entry_count(&table).await, 3);
    }
}
