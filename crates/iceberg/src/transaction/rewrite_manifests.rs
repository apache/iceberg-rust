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

use crate::error::Result;
use crate::spec::{DataFile, ManifestContentType, ManifestEntry, ManifestFile, Operation};
use crate::table::Table;
use crate::transaction::snapshot::{
    RewriteManifestProcess, SnapshotProduceOperation, SnapshotProducer,
};
use crate::transaction::{ActionCommit, TransactionAction};

/// A predicate deciding whether an existing [`ManifestFile`] is rewritten (`true`) or kept as-is
/// (`false`) — Java `RewriteManifests.rewriteIf(Predicate<ManifestFile>)`.
pub type ManifestPredicate = Arc<dyn Fn(&ManifestFile) -> bool + Send + Sync>;

/// A transaction action that REORGANIZES a table's manifests without changing any data (Java
/// `RewriteManifests` / `BaseRewriteManifests`).
///
/// On commit it produces a new `Operation::Replace` snapshot whose DATA manifests are a reclustering of
/// the existing live DATA manifests: the SAME data files, regrouped. By default every live DATA manifest
/// is rewritten and its live entries are clustered by PARTITION — entries with the same
/// `(partition_spec_id, partition)` are co-located into the same new manifest (the maintenance default;
/// Java's `clusterByFunc` defaults to partition). No data file is added or removed, so the table's live
/// (scannable) file set is byte-identical before and after; only the manifest organization changes.
///
/// DELETE manifests are carried forward unchanged. [`rewrite_if`](Self::rewrite_if) restricts the rewrite
/// to the DATA manifests matching a predicate (the rest are kept as-is — Java `keepActiveManifests`).
///
/// **Out of scope (deferred):** the arbitrary `clusterBy(Function<DataFile, Object>)` custom cluster
/// function (this action does the partition-clustering default), explicit `addManifest`/`deleteManifest`,
/// DELETE-manifest reclustering, and the `manifests-created`/`-replaced`/`-kept` summary fields.
pub struct RewriteManifestsAction {
    commit_uuid: Option<Uuid>,
    key_metadata: Option<Vec<u8>>,
    snapshot_properties: HashMap<String, String>,
    /// The `rewriteIf` predicate; `None` ⇒ rewrite every DATA manifest (the default).
    predicate: Option<ManifestPredicate>,
}

impl RewriteManifestsAction {
    pub(crate) fn new() -> Self {
        Self {
            commit_uuid: None,
            key_metadata: None,
            snapshot_properties: HashMap::default(),
            predicate: None,
        }
    }

    /// Restrict the rewrite to the DATA manifests for which `predicate` returns `true`; DATA manifests for
    /// which it returns `false` (and all DELETE manifests) are carried forward unchanged (Java
    /// `RewriteManifests.rewriteIf`). When this is not set, every DATA manifest is rewritten.
    pub fn rewrite_if(
        mut self,
        predicate: impl Fn(&ManifestFile) -> bool + Send + Sync + 'static,
    ) -> Self {
        self.predicate = Some(Arc::new(predicate));
        self
    }

    /// Set the commit UUID for the snapshot (used to name the manifest files it writes).
    pub fn set_commit_uuid(mut self, commit_uuid: Uuid) -> Self {
        self.commit_uuid = Some(commit_uuid);
        self
    }

    /// Set key metadata for the manifest files this action writes.
    pub fn set_key_metadata(mut self, key_metadata: Vec<u8>) -> Self {
        self.key_metadata = Some(key_metadata);
        self
    }

    /// Set snapshot summary properties.
    pub fn set_snapshot_properties(mut self, snapshot_properties: HashMap<String, String>) -> Self {
        self.snapshot_properties = snapshot_properties;
        self
    }

    /// Resolve the set of DATA-manifest paths to rewrite against the current snapshot, applying the
    /// `rewrite_if` predicate (default: every DATA manifest). DELETE manifests are never targeted here —
    /// they are carried forward by the process. Returns an empty set when the table has no current
    /// snapshot.
    async fn resolve_rewrite_targets(&self, table: &Table) -> Result<HashSet<String>> {
        let Some(snapshot) = table.metadata().current_snapshot() else {
            return Ok(HashSet::new());
        };
        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), &table.metadata_ref())
            .await?;

        let mut targets = HashSet::new();
        for manifest in manifest_list.entries() {
            if manifest.content != ManifestContentType::Data {
                continue;
            }
            let matches = match &self.predicate {
                Some(predicate) => predicate(manifest),
                None => true,
            };
            if matches {
                targets.insert(manifest.manifest_path.clone());
            }
        }
        Ok(targets)
    }
}

#[async_trait]
impl TransactionAction for RewriteManifestsAction {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        // Decide which DATA manifests to rewrite up front (the predicate runs against the current
        // snapshot's manifest list).
        let rewrite_targets = self.resolve_rewrite_targets(table).await?;

        // A reorganization adds no data files; the producer is built with an empty added set and the
        // manifest-reorg flag so the empty-commit precondition is relaxed.
        let snapshot_producer = SnapshotProducer::new(
            table,
            self.commit_uuid.unwrap_or_else(Uuid::now_v7),
            self.key_metadata.clone(),
            self.snapshot_properties.clone(),
            vec![],
        )
        .with_manifest_reorg(true);

        let process = RewriteManifestProcess::new(table.metadata().properties(), rewrite_targets);

        snapshot_producer
            .commit(RewriteManifestsOperation, process)
            .await
    }
}

/// The snapshot-produce operation for a manifest reorganization: a `Replace` that adds and removes no data
/// files. Its `existing_manifest` exposes EVERY current live manifest (DATA candidates to recluster +
/// DELETE manifests to carry forward) to the [`RewriteManifestProcess`], which does the regrouping (the
/// keep/rewrite split is made there from the `rewrite_targets` the process was built with).
struct RewriteManifestsOperation;

impl SnapshotProduceOperation for RewriteManifestsOperation {
    fn operation(&self) -> Operation {
        // Java `BaseRewriteManifests.operation()` == `DataOperations.REPLACE`.
        Operation::Replace
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

        // Hand EVERY live manifest to the process: live DATA manifests are reclustered, DELETE manifests
        // and non-target DATA manifests are carried forward unchanged.
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
        DataContentType, DataFile, DataFileBuilder, DataFileFormat, Literal, ManifestStatus,
        Operation, Struct,
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

    /// Collect the set of live (Added or Existing) data-file paths across the table's current snapshot —
    /// exactly what a scan would read. A reorg that drops an entry shrinks this set; a reorg that adds or
    /// loses a file changes it. The reorg contract is that this set is IDENTICAL before and after.
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
    /// the DUPLICATION detector: if a reorg copies an entry twice, this exceeds the path count even when
    /// the path SET (which de-dups) would hide it.
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

    /// Number of DATA manifests in the table's current snapshot.
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

    /// For each manifest, the SET of distinct partition x-values among its live entries. A
    /// partition-clustered reorg produces one entry per set (each manifest holds a single partition).
    async fn manifest_partition_value_sets(table: &Table) -> Vec<HashSet<i64>> {
        let snapshot = table.metadata().current_snapshot().unwrap();
        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();
        let mut result = Vec::new();
        for manifest_file in manifest_list.entries() {
            let manifest = manifest_file.load_manifest(table.file_io()).await.unwrap();
            let mut values: HashSet<i64> = HashSet::new();
            for entry in manifest.entries() {
                if !entry.is_alive() {
                    continue;
                }
                if let Some(Literal::Primitive(crate::spec::PrimitiveLiteral::Long(value))) = entry
                    .data_file()
                    .partition()
                    .fields()
                    .first()
                    .and_then(|v| v.as_ref())
                {
                    values.insert(*value);
                }
            }
            result.push(values);
        }
        result
    }

    /// Fast-append the given files in one commit (builds one new manifest).
    async fn fast_append(catalog: &impl Catalog, table: &Table, files: Vec<DataFile>) -> Table {
        let tx = Transaction::new(table);
        let action = tx.fast_append().add_data_files(files);
        let tx = action.apply(tx).unwrap();
        tx.commit(catalog).await.unwrap()
    }

    /// Reorganize the table's manifests (default partition clustering, all DATA manifests).
    async fn rewrite_manifests(catalog: &impl Catalog, table: &Table) -> Table {
        let tx = Transaction::new(table);
        let action = tx.rewrite_manifests();
        let tx = action.apply(tx).unwrap();
        tx.commit(catalog).await.unwrap()
    }

    /// KEY TEST — risks: DATA LOSS / DUPLICATION / PROVENANCE CORRUPTION / WRONG CLUSTERING. Build a table
    /// whose data files span 2 partitions SCATTERED across several manifests (each fast-append interleaves
    /// the two partitions in one manifest, so a partition's files are split across manifests). After
    /// `rewrite_manifests`:
    /// - the SCAN live set is byte-identical to before (no file lost / duplicated / spurious);
    /// - the live ENTRY count equals the path count (no duplication a path-set would hide);
    /// - the snapshot operation is `Replace`;
    /// - every entry keeps its ORIGINAL provenance (snapshot id + both sequence numbers);
    /// - the new manifests are PARTITION-LOCAL: each holds entries of a SINGLE partition value.
    #[tokio::test]
    async fn test_rewrite_manifests_preserves_scan_and_clusters_by_partition() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        // Three fast-appends, each interleaving partition 0 and partition 1 → three manifests, and BOTH
        // partitions are spread across all three (the scatter the reorg must fix).
        let table = fast_append(&catalog, &table, vec![
            data_file("p/a0.parquet", 0),
            data_file("p/a1.parquet", 1),
        ])
        .await;
        let s1 = table.metadata().current_snapshot().unwrap().snapshot_id();
        let table = fast_append(&catalog, &table, vec![
            data_file("p/b0.parquet", 0),
            data_file("p/b1.parquet", 1),
        ])
        .await;
        let s2 = table.metadata().current_snapshot().unwrap().snapshot_id();
        let table = fast_append(&catalog, &table, vec![
            data_file("p/c0.parquet", 0),
            data_file("p/c1.parquet", 1),
        ])
        .await;
        let s3 = table.metadata().current_snapshot().unwrap().snapshot_id();
        assert_eq!(
            manifest_count(&table).await,
            3,
            "three appends → three manifests"
        );

        let before: HashSet<String> = live_file_paths(&table).await;
        assert_eq!(before.len(), 6, "six live files across two partitions");
        // Capture each file's provenance before the reorg.
        let prov_a0 = entry_provenance(&table, "p/a0.parquet").await;
        let prov_b1 = entry_provenance(&table, "p/b1.parquet").await;
        let prov_c0 = entry_provenance(&table, "p/c0.parquet").await;

        let table = rewrite_manifests(&catalog, &table).await;

        // RISK: data loss / duplication / spurious file — the live set must be byte-identical.
        assert_eq!(
            live_file_paths(&table).await,
            before,
            "the reorg must not lose, duplicate, or add any data file"
        );
        // RISK: duplication a path-set hides — exactly six live entries.
        assert_eq!(
            live_entry_count(&table).await,
            6,
            "exactly six live entries (no entry copied twice)"
        );

        // RISK: wrong operation — a reorg is a Replace.
        assert_eq!(
            table
                .metadata()
                .current_snapshot()
                .unwrap()
                .summary()
                .operation,
            Operation::Replace
        );

        // RISK: provenance corruption — each entry keeps its ORIGINAL snapshot id + both sequence numbers,
        // NOT the reorg snapshot's. (The reorg snapshot id differs from s1/s2/s3.)
        let reorg_id = table.metadata().current_snapshot().unwrap().snapshot_id();
        assert_ne!(reorg_id, s1);
        assert_ne!(reorg_id, s2);
        assert_ne!(reorg_id, s3);
        assert_eq!(
            entry_provenance(&table, "p/a0.parquet").await,
            prov_a0,
            "a0 must keep its original snapshot id + seqs after the reorg"
        );
        assert_eq!(entry_provenance(&table, "p/b1.parquet").await, prov_b1);
        assert_eq!(entry_provenance(&table, "p/c0.parquet").await, prov_c0);

        // RISK: wrong clustering — after the reorg each manifest holds a SINGLE partition value.
        let per_manifest = manifest_partition_value_sets(&table).await;
        assert_eq!(
            per_manifest.len(),
            2,
            "two partitions → two reclustered manifests (each partition co-located)"
        );
        for value_set in &per_manifest {
            assert_eq!(
                value_set.len(),
                1,
                "each reclustered manifest must hold entries of exactly one partition, got {value_set:?}"
            );
        }
        // The two manifests cover exactly partitions {0} and {1}.
        let mut covered: Vec<i64> = per_manifest
            .iter()
            .flat_map(|s| s.iter().copied())
            .collect();
        covered.sort_unstable();
        assert_eq!(covered, vec![0, 1]);

        // Every reclustered entry is written as Existing (provenance-preserving copy), never re-Added.
        let snapshot = table.metadata().current_snapshot().unwrap();
        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();
        for manifest_file in manifest_list.entries() {
            let manifest = manifest_file.load_manifest(table.file_io()).await.unwrap();
            for entry in manifest.entries() {
                assert_eq!(
                    entry.status(),
                    ManifestStatus::Existing,
                    "a reclustered entry must be copied as Existing, not re-Added"
                );
            }
        }
    }

    /// Risk: REWRITE-IF OVER-REACH. `rewrite_if` selecting a SUBSET of DATA manifests must leave the
    /// non-matching ones untouched (same manifest path, carried forward) while still rewriting the
    /// selected ones — and the live set stays identical.
    #[tokio::test]
    async fn test_rewrite_if_subset_leaves_unselected_manifests_untouched() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        // Three single-file manifests; we will rewrite only the first.
        let table = fast_append(&catalog, &table, vec![data_file("s/a.parquet", 0)]).await;
        let table = fast_append(&catalog, &table, vec![data_file("s/b.parquet", 0)]).await;
        let table = fast_append(&catalog, &table, vec![data_file("s/c.parquet", 0)]).await;

        // Record the manifest path that holds `b` — it must survive the reorg unchanged.
        let snapshot = table.metadata().current_snapshot().unwrap();
        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();
        let mut b_manifest_path = None;
        let mut a_manifest_path = None;
        for manifest_file in manifest_list.entries() {
            let manifest = manifest_file.load_manifest(table.file_io()).await.unwrap();
            for entry in manifest.entries() {
                if entry.file_path() == "s/b.parquet" {
                    b_manifest_path = Some(manifest_file.manifest_path.clone());
                }
                if entry.file_path() == "s/a.parquet" {
                    a_manifest_path = Some(manifest_file.manifest_path.clone());
                }
            }
        }
        let b_manifest_path = b_manifest_path.unwrap();
        let a_manifest_path = a_manifest_path.unwrap();

        let before = live_file_paths(&table).await;

        // Rewrite ONLY the manifest that contains `a`.
        let tx = Transaction::new(&table);
        let action = tx
            .rewrite_manifests()
            .rewrite_if(move |m| m.manifest_path == a_manifest_path);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        // Live set unchanged.
        assert_eq!(
            live_file_paths(&table).await,
            before,
            "a subset rewrite must not change the live set"
        );
        assert_eq!(live_entry_count(&table).await, 3);

        // The `b` manifest (not selected) is carried forward with its ORIGINAL path.
        let snapshot = table.metadata().current_snapshot().unwrap();
        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();
        let surviving_paths: HashSet<String> = manifest_list
            .entries()
            .iter()
            .map(|m| m.manifest_path.clone())
            .collect();
        assert!(
            surviving_paths.contains(&b_manifest_path),
            "the unselected manifest must be carried forward unchanged (same path)"
        );
        assert_eq!(
            table
                .metadata()
                .current_snapshot()
                .unwrap()
                .summary()
                .operation,
            Operation::Replace
        );
    }

    /// Risk: REORG-ONLY COMMIT REJECTED. A manifest reorganization adds and removes NO data files and sets
    /// no properties — the producer's empty-commit precondition must NOT reject it (the manifest list still
    /// changes). Pins that the precondition relaxation actually fires.
    #[tokio::test]
    async fn test_reorg_only_commit_is_allowed() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let table = fast_append(&catalog, &table, vec![
            data_file("r/a.parquet", 0),
            data_file("r/b.parquet", 1),
        ])
        .await;
        let before = live_file_paths(&table).await;

        // A reorg with no added/removed files commits successfully (does not error as an empty commit).
        let table = rewrite_manifests(&catalog, &table).await;

        assert_eq!(
            live_file_paths(&table).await,
            before,
            "reorg-only commit must preserve the live set"
        );
        assert_eq!(
            table
                .metadata()
                .current_snapshot()
                .unwrap()
                .summary()
                .operation,
            Operation::Replace,
            "reorg-only commit produces a Replace snapshot"
        );
    }

    /// Risk: NO-OP REORG corrupts an already-clustered table. A table that is already optimally clustered
    /// (one manifest per partition) must survive a reorg as a sane no-op-ish reorg: a NEW Replace snapshot,
    /// the same live set, no data change, provenance preserved, still one manifest per partition.
    #[tokio::test]
    async fn test_rewrite_manifests_on_already_clustered_table_is_sane_noop() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        // Two appends, each a SINGLE partition → already one manifest per partition.
        let table = fast_append(&catalog, &table, vec![
            data_file("k/a0.parquet", 0),
            data_file("k/b0.parquet", 0),
        ])
        .await;
        let table = fast_append(&catalog, &table, vec![
            data_file("k/a1.parquet", 1),
            data_file("k/b1.parquet", 1),
        ])
        .await;
        let before = live_file_paths(&table).await;
        let prov_a0 = entry_provenance(&table, "k/a0.parquet").await;
        let old_snapshot_id = table.metadata().current_snapshot().unwrap().snapshot_id();

        let table = rewrite_manifests(&catalog, &table).await;

        // A new snapshot is produced even when the clustering does not change.
        assert_ne!(
            table.metadata().current_snapshot().unwrap().snapshot_id(),
            old_snapshot_id,
            "a reorg always produces a new Replace snapshot"
        );
        assert_eq!(
            live_file_paths(&table).await,
            before,
            "no data change on an already-clustered reorg"
        );
        assert_eq!(live_entry_count(&table).await, 4);
        // Still partition-local: each manifest one partition value.
        let per_manifest = manifest_partition_value_sets(&table).await;
        assert_eq!(per_manifest.len(), 2);
        for value_set in &per_manifest {
            assert_eq!(value_set.len(), 1);
        }
        // Provenance preserved.
        assert_eq!(entry_provenance(&table, "k/a0.parquet").await, prov_a0);
    }

    /// Risk: SEQUENCE-NUMBER RE-STAMP (the worst reorg bug — breaks merge-on-read delete application).
    /// Each append's data file carries a DISTINCT, NON-ZERO data sequence number (1, 2, 3 for three
    /// sequential V3 appends); the reorg snapshot is seq 4. A reorg that re-stamps an entry to the reorg
    /// snapshot's seq (4) would make existing position-deletes (`data_seq <= delete_seq`) stop applying,
    /// resurrecting deleted rows. This pins that every entry keeps its EXACT original data + file
    /// sequence number — proving the provenance assertion is not trivially passing on `None`/`Some(0)`.
    #[tokio::test]
    async fn test_rewrite_manifests_preserves_distinct_nonzero_sequence_numbers() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let table = fast_append(&catalog, &table, vec![data_file("z/a0.parquet", 0)]).await;
        let table = fast_append(&catalog, &table, vec![data_file("z/b0.parquet", 0)]).await;
        let table = fast_append(&catalog, &table, vec![data_file("z/a1.parquet", 1)]).await;

        let pa = entry_provenance(&table, "z/a0.parquet").await;
        let pb = entry_provenance(&table, "z/b0.parquet").await;
        let pc = entry_provenance(&table, "z/a1.parquet").await;
        // The three appends produce data/file sequence numbers 1, 2, 3 — distinct and non-zero.
        assert_eq!(pa.1, Some(1), "a0 data seq is the first append's seq");
        assert_eq!(pa.2, Some(1), "a0 file seq is the first append's seq");
        assert_eq!(pb.1, Some(2), "b0 data seq is the second append's seq");
        assert_eq!(pc.1, Some(3), "a1 data seq is the third append's seq");

        let table = rewrite_manifests(&catalog, &table).await;
        assert_eq!(
            table
                .metadata()
                .current_snapshot()
                .unwrap()
                .sequence_number(),
            4,
            "reorg is the 4th commit → seq 4 (the value a re-stamp would wrongly write)"
        );
        // A re-stamp would set these to seq 4; preserved means they stay 1 / 2 / 3 (data AND file seq).
        assert_eq!(entry_provenance(&table, "z/a0.parquet").await, pa);
        assert_eq!(entry_provenance(&table, "z/b0.parquet").await, pb);
        assert_eq!(entry_provenance(&table, "z/a1.parquet").await, pc);
    }

    /// Risk: DELETE MANIFEST DROPPED / RE-STAMPED BY A REORG — silent merge-on-read corruption. A reorg
    /// targets only DATA manifests; every DELETE manifest (carrying position/equality deletes) must be
    /// carried forward UNCHANGED (same path, same entry provenance), or an existing delete would stop
    /// applying after the reorg and deleted rows would resurrect. Builds a table with a DATA manifest and
    /// a DELETE manifest (via `row_delta`), reorganizes, and asserts the DELETE manifest survives with its
    /// original path and the delete entry keeps its original snapshot id + both sequence numbers.
    #[tokio::test]
    async fn test_rewrite_manifests_carries_delete_manifests_forward_unchanged() {
        use crate::spec::ManifestContentType;

        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        // Append a data file, then add a position-delete via row_delta — this writes a DELETE manifest.
        let table = fast_append(&catalog, &table, vec![data_file("d/data.parquet", 0)]).await;
        let delete_file = DataFileBuilder::default()
            .content(DataContentType::PositionDeletes)
            .file_path("d/pos-del.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(0)
            .partition(Struct::from_iter([Some(Literal::long(0))]))
            .build()
            .unwrap();
        let table = {
            let tx = Transaction::new(&table);
            let action = tx.row_delta().add_deletes(vec![delete_file]);
            let tx = action.apply(tx).unwrap();
            tx.commit(&catalog).await.unwrap()
        };

        // Record the DELETE manifest's path + the delete entry's provenance BEFORE the reorg.
        let (delete_manifest_path_before, delete_prov_before) = {
            let snapshot = table.metadata().current_snapshot().unwrap();
            let manifest_list = snapshot
                .load_manifest_list(table.file_io(), table.metadata())
                .await
                .unwrap();
            let mut found = None;
            for manifest_file in manifest_list.entries() {
                if manifest_file.content != ManifestContentType::Deletes {
                    continue;
                }
                let manifest = manifest_file.load_manifest(table.file_io()).await.unwrap();
                for entry in manifest.entries() {
                    if entry.is_alive() && entry.file_path() == "d/pos-del.parquet" {
                        found = Some((
                            manifest_file.manifest_path.clone(),
                            (
                                entry.snapshot_id(),
                                entry.sequence_number(),
                                entry.file_sequence_number,
                            ),
                        ));
                    }
                }
            }
            found.expect("a DELETE manifest with the position-delete must exist before the reorg")
        };

        let before_live = live_file_paths(&table).await;
        let table = rewrite_manifests(&catalog, &table).await;

        // The live DATA set is unchanged (the delete file is not a "live data path").
        assert_eq!(
            live_file_paths(&table).await,
            before_live,
            "the reorg must not change the live data set"
        );

        // The DELETE manifest survives with its ORIGINAL path, and the delete entry keeps its provenance.
        let snapshot = table.metadata().current_snapshot().unwrap();
        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();
        let mut delete_manifest_path_after = None;
        let mut delete_prov_after = None;
        for manifest_file in manifest_list.entries() {
            if manifest_file.content != ManifestContentType::Deletes {
                continue;
            }
            let manifest = manifest_file.load_manifest(table.file_io()).await.unwrap();
            for entry in manifest.entries() {
                if entry.is_alive() && entry.file_path() == "d/pos-del.parquet" {
                    delete_manifest_path_after = Some(manifest_file.manifest_path.clone());
                    delete_prov_after = Some((
                        entry.snapshot_id(),
                        entry.sequence_number(),
                        entry.file_sequence_number,
                    ));
                }
            }
        }
        assert_eq!(
            delete_manifest_path_after.as_ref(),
            Some(&delete_manifest_path_before),
            "the DELETE manifest must be carried forward with its ORIGINAL path (not rewritten)"
        );
        assert_eq!(
            delete_prov_after,
            Some(delete_prov_before),
            "the delete entry must keep its original snapshot id + both sequence numbers after the reorg"
        );
    }
}
