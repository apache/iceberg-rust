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

//! This module contains the merge-append action (Java `MergeAppend` / `ManifestMergeManager`).
//!
//! [`MergeAppendAction`] is the "minimal number of manifest files" append: it appends data files in
//! one `Operation::Append` snapshot exactly like [`crate::transaction::append::FastAppendAction`], but
//! then BIN-PACKS the resulting manifest list and MERGES the small manifests into fewer, larger ones
//! (Java `MergeAppend extends MergingSnapshotProducer<AppendFiles>`, `core/MergeAppend.java`). Java's
//! `Table.newAppend()` returns this MERGING producer; `newFastAppend()` returns the non-merging one —
//! this fork's [`crate::transaction::Transaction::fast_append`] is `newFastAppend`, and
//! [`crate::transaction::Transaction::merge_append`] is `newAppend`.
//!
//! ## The merge contract (Java `ManifestMergeManager`, `core/ManifestMergeManager.java`)
//!
//! After the producer writes the new added-data manifest and carries the existing manifests forward,
//! [`MergeManifestProcess`] runs the manager (`mergeManifests` L79-94):
//!
//! 1. **Disabled / empty short-circuit** (L80-83): if `commit.manifest-merge.enabled` is `false` or
//!    the list is empty, return it UNCHANGED.
//! 2. **Group by partition spec id, REVERSE-sorted** (`groupBySpec` L129-137): manifests are grouped
//!    by spec id into a `TreeMap` with `Comparator.reverseOrder()` — higher spec ids are processed
//!    (and emitted) first. Manifests of different spec ids are NEVER merged together (a merged manifest
//!    carries exactly one spec id).
//! 3. **Bin-pack each group** (`mergeGroup` L140-185): `ListPacker(targetSizeBytes, lookback=1,
//!    largestBinFirst=false).packEnd(group, ManifestFile::length)` packs the group by manifest length
//!    from the END (so the under-filled bin is the FIRST one — it gets merged on the next append). Then
//!    per bin:
//!    - `bin.size() == 1` ⇒ keep the single manifest as-is (no rewrite).
//!    - the bin CONTAINS the FIRST manifest (this commit's new added manifest) AND `bin.size() <
//!      minCountToMerge` ⇒ keep ALL manifests in the bin as-is (the min-count gate applies ONLY to the
//!      bin holding the in-memory/new manifest, so a large old manifest can't block merging older
//!      groups, L175-181).
//!    - else ⇒ MERGE the bin into one manifest (`createManifest`).
//! 4. **`createManifest`** (L187-239): per entry of each merged manifest —
//!    - `DELETED` ⇒ kept ONLY if `entry.snapshotId() == this snapshot` (`writer.delete`); older
//!      tombstones from previous snapshots are SUPPRESSED (L203-208).
//!    - `ADDED` and `entry.snapshotId() == this snapshot` ⇒ `writer.add` (stays Added; re-inherits the
//!      new snapshot's sequence number).
//!    - else ⇒ `writer.existing` (provenance preserved: original snapshot id + data/file sequence
//!      numbers, status Existing). RE-STAMPING a carried entry's provenance is the #1 silent-corruption
//!      class.
//!
//! The apply order mirrors Java `MergingSnapshotProducer.apply` L1007-1008: the NEW added-data manifest
//! is FIRST, then the existing manifests in their current order, then `mergeManager.mergeManifests`.
//!
//! ## Physics: the new added manifest read back before commit
//!
//! The new added-data manifest is read BACK (via [`ManifestFile::load_manifest`]) to merge it, while the
//! snapshot is still UNCOMMITTED — so its manifest-list entry carries `sequence_number ==
//! UNASSIGNED_SEQUENCE_NUMBER` (-1). [`crate::spec::ManifestEntry::inherit_data`] then makes each
//! `Added` entry inherit `Some(-1)` as its data/file sequence number (the `status == Added` branch
//! inherits regardless of the manifest seq). When that entry is re-routed through the merged writer's
//! [`crate::spec::ManifestWriter::add_entry`], the writer STRIPS the negative seq back to `None` (its
//! `sequence_number().is_some_and(|n| n >= 0)` test is false for -1), so the merged manifest stores the
//! re-added entry with a NULL sequence number ON DISK — it re-inherits the new snapshot's REAL sequence
//! number at commit (same as a fast append). Carried-forward entries from COMMITTED manifests have real
//! `Some(seq)` values, so [`crate::spec::ManifestWriter::add_existing_entry`] writes those EXPLICITLY on
//! disk (status Existing, original snapshot id, original seqs). The merged manifest's `added_snapshot_id`
//! is the new snapshot id (the cluster writer stamps it), so the manifest-list writer's
//! `assign_sequence_numbers` legally stamps the new sequence number onto the merged manifest-list entry.
//!
//! ## Adaptations / deviations from Java (each named here per the parity contract)
//!
//! - **Delete-manifest merging is deferred.** Java runs a SEPARATE `deleteMergeManager.mergeManifests`
//!   over the delete manifests (`MergingSnapshotProducer.apply` L1023). This port carries every DELETE
//!   manifest forward UNCHANGED (appended after the merged data manifests) — a merge_append never adds a
//!   delete file, so there is at most the outstanding set, and carrying them forward is correctness-safe
//!   (the conservative direction shared by every delete-bearing action in this fork). Merging them is a
//!   future increment.
//! - **The retry cache + `cleanUncommitted` + `replacedManifestsCount` are not ported.** Java caches
//!   merged manifests across commit retries (`mergedManifests`), deletes orphaned merged manifests on a
//!   failed attempt (`cleanUncommitted`), and tracks a replaced-manifest count for `CommitMetrics`. The
//!   Rust producer recomputes the merge from the refreshed base on every attempt (no cross-attempt state)
//!   and there is NO orphan-file cleanup anywhere in the Rust producer (a known fork-wide gap, not
//!   merge-specific). `replacedManifestsCount` feeds `CommitMetrics`, not the snapshot summary.
//! - **`appendManifest` is not ported.** Java `MergeAppend.appendManifest` (L52-64) adds a pre-built
//!   manifest of NEW files (rejecting existing/deleted files — the OPPOSITE of
//!   `RewriteManifests.addManifest`, which rejects ADDED files). This fork's `fast_append` has no
//!   `add_manifest` either, so this is a documented parity gap, not a regression.
//! - **No `manifests-created/-kept/-replaced` summary keys.** Java's `MergingSnapshotProducer.apply`
//!   appends those three keys via `buildManifestCountSummary` (`SnapshotProducer.java` L716-733). The Rust
//!   [`crate::transaction::snapshot::SnapshotProducer::summary`] does not emit them, and this manager does
//!   not inject them, so a merge_append's snapshot summary has the SAME SHAPE as a fast_append's (only
//!   `RewriteManifests` sets those keys in this fork). Documented divergence from Java.
//! - **`scanManifestsWith(ExecutorService)` parallelism is not ported.** Java bin-merges in parallel
//!   (`Tasks.range(...).executeWith(workerPool)`); the Rust path is sequential async.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use uuid::Uuid;

use crate::error::Result;
use crate::spec::{
    DataFile, ManifestContentType, ManifestEntry, ManifestFile, ManifestStatus, Operation,
    TableProperties,
};
use crate::table::Table;
use crate::transaction::snapshot::{ManifestProcess, SnapshotProduceOperation, SnapshotProducer};
use crate::transaction::{ActionCommit, TransactionAction};

/// A transaction action that appends data files in one `Operation::Append` snapshot and then MERGES the
/// resulting manifest list into a minimal number of manifests (Java `MergeAppend` / `Table.newAppend()`).
///
/// Create one with [`crate::transaction::Transaction::merge_append`]. It mirrors
/// [`crate::transaction::append::FastAppendAction`]'s public surface
/// ([`MergeAppendAction::add_data_files`], [`MergeAppendAction::set_commit_uuid`],
/// [`MergeAppendAction::set_key_metadata`], [`MergeAppendAction::set_snapshot_properties`],
/// [`MergeAppendAction::with_check_duplicate`]); the only difference is that on commit it bin-packs and
/// merges manifests per the three `commit.manifest*` table properties (see the module doc). The live
/// file set (the paths a scan reads) is identical to what the equivalent fast append would produce.
pub struct MergeAppendAction {
    check_duplicate: bool,
    // below are properties used to create SnapshotProducer when commit
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

    /// Set whether to check duplicate files (mirrors `FastAppendAction::with_check_duplicate`).
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
impl TransactionAction for MergeAppendAction {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        let snapshot_producer = SnapshotProducer::new(
            table,
            self.commit_uuid.unwrap_or_else(Uuid::now_v7),
            self.key_metadata.clone(),
            self.snapshot_properties.clone(),
            self.added_data_files.clone(),
        );

        // Validate added files (identical to fast append — only DATA content, matching spec, valid
        // partition values).
        snapshot_producer.validate_added_data_files()?;

        // Check duplicate files (identical to fast append).
        if self.check_duplicate {
            snapshot_producer.validate_duplicate_files().await?;
        }

        // Read the three merge properties from the table at commit time (Java
        // `ManifestMergeManager` ctor args, from `TableProperties`).
        let merge_settings = MergeSettings::from_table(table);
        let merge_process =
            MergeManifestProcess::new(snapshot_producer.snapshot_id(), merge_settings);

        snapshot_producer
            .commit(MergeAppendOperation, merge_process)
            .await
    }
}

/// The three `commit.manifest*` settings that drive the merge, read from the table properties at commit
/// time (Java `ManifestMergeManager` constructor: `targetSizeBytes`, `minCountToMerge`, `mergeEnabled`).
#[derive(Debug, Clone, Copy)]
struct MergeSettings {
    /// `commit.manifest.target-size-bytes` (default 8 MB) — the bin-packing target weight.
    target_size_bytes: u64,
    /// `commit.manifest.min-count-to-merge` (default 100) — the new-manifest bin is merged only at/above
    /// this many manifests.
    min_count_to_merge: u32,
    /// `commit.manifest-merge.enabled` (default true) — when false the manifest list is returned as-is.
    merge_enabled: bool,
}

impl MergeSettings {
    fn from_table(table: &Table) -> Self {
        let properties = table.metadata().properties();

        let target_size_bytes = properties
            .get(TableProperties::PROPERTY_COMMIT_MANIFEST_TARGET_SIZE_BYTES)
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(TableProperties::PROPERTY_COMMIT_MANIFEST_TARGET_SIZE_BYTES_DEFAULT);

        let min_count_to_merge = properties
            .get(TableProperties::PROPERTY_COMMIT_MANIFEST_MIN_COUNT_TO_MERGE)
            .and_then(|value| value.parse::<u32>().ok())
            .unwrap_or(TableProperties::PROPERTY_COMMIT_MANIFEST_MIN_COUNT_TO_MERGE_DEFAULT);

        let merge_enabled = properties
            .get(TableProperties::PROPERTY_COMMIT_MANIFEST_MERGE_ENABLED)
            .and_then(|value| value.parse::<bool>().ok())
            .unwrap_or(TableProperties::PROPERTY_COMMIT_MANIFEST_MERGE_ENABLED_DEFAULT);

        Self {
            target_size_bytes,
            min_count_to_merge,
            merge_enabled,
        }
    }
}

/// The [`SnapshotProduceOperation`] for [`MergeAppendAction`] — identical to the fast-append operation
/// (records `Operation::Append`, removes no files, carries forward every existing manifest with live
/// files). The MERGE happens in [`MergeManifestProcess`], not here.
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
        // Carry forward every existing manifest that still has live files (Java
        // `MergingSnapshotProducer.apply`'s `shouldKeep = hasAddedFiles || hasExistingFiles ||
        // snapshotId() == snapshotId()`). Mirrors `FastAppendOperation::existing_manifest` exactly so
        // the carried set is byte-identical; the merge step then decides which of these to combine.
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

/// The [`ManifestProcess`] that bin-packs and merges the snapshot's manifest list (Java
/// `ManifestMergeManager`, run from `MergingSnapshotProducer.apply`).
pub(crate) struct MergeManifestProcess {
    /// The new snapshot's id — the producer's `snapshot_id()`. Used to identify this commit's new added
    /// manifest (the Java `first` manifest) and to route entries (this-snapshot Added/Deleted vs carried).
    snapshot_id: i64,
    settings: MergeSettings,
}

impl MergeManifestProcess {
    fn new(snapshot_id: i64, settings: MergeSettings) -> Self {
        Self {
            snapshot_id,
            settings,
        }
    }

    /// Split the input manifest list into (data, deletes), reorder the DATA manifests so this commit's
    /// new added manifest is FIRST (Java `Iterables.concat(prepareNewDataManifests(), filtered)`), and
    /// return them alongside the delete manifests (carried forward unchanged).
    ///
    /// The producer's `manifest_file` builds the list as `[carried existing..., new added data manifest]`
    /// (the added manifest is PUSHED last). Java puts the new data manifest FIRST, which the bin-packer's
    /// "bin containing `first`" min-count rule depends on. For merge_append the new added manifest is the
    /// UNIQUE data manifest whose `added_snapshot_id == this snapshot` (no rewritten manifests exist — the
    /// delete set is always empty, so no manifest was rewritten with this snapshot id). We assert that
    /// uniqueness rather than trust it.
    fn split_and_reorder(
        &self,
        manifests: Vec<ManifestFile>,
    ) -> (Vec<ManifestFile>, Vec<ManifestFile>) {
        let mut new_added_data: Vec<ManifestFile> = Vec::new();
        let mut existing_data: Vec<ManifestFile> = Vec::new();
        let mut delete_manifests: Vec<ManifestFile> = Vec::new();

        for manifest in manifests {
            match manifest.content {
                ManifestContentType::Deletes => delete_manifests.push(manifest),
                ManifestContentType::Data => {
                    if manifest.added_snapshot_id == self.snapshot_id {
                        new_added_data.push(manifest);
                    } else {
                        existing_data.push(manifest);
                    }
                }
            }
        }

        // For merge_append the new added manifest is unambiguous: the producer writes at most one added
        // DATA manifest and rewrites NONE (the delete set is always empty). A future merging caller with
        // rewritten manifests would need a richer "first" identification; flag it loudly here instead of
        // silently producing the wrong order.
        debug_assert!(
            new_added_data.len() <= 1,
            "merge_append expects at most one new added data manifest (got {})",
            new_added_data.len()
        );

        // New added data manifest FIRST, then existing data manifests in their current order (Java
        // `concat(prepareNewDataManifests(), filtered)`).
        let mut data_manifests = new_added_data;
        data_manifests.extend(existing_data);

        (data_manifests, delete_manifests)
    }

    /// Run the merge over the DATA manifests (Java `ManifestMergeManager.mergeManifests` over the data
    /// manager). Returns the merged data manifest list in Java's order (higher spec id first; within a
    /// spec id, the bin order).
    async fn merge_data_manifests(
        &self,
        snapshot_producer: &mut SnapshotProducer<'_>,
        data_manifests: Vec<ManifestFile>,
    ) -> Result<Vec<ManifestFile>> {
        // Disabled / empty short-circuit (Java `mergeManifests` L80-83).
        if !self.settings.merge_enabled || data_manifests.is_empty() {
            return Ok(data_manifests);
        }

        // Java's `first` is the unconditional STREAM HEAD (`ManifestFile first = manifestIter.next()`,
        // ManifestMergeManager L85) — NOT "the new manifest". After `split_and_reorder` the head is the
        // new added manifest when one exists; for an empty-data merging append (a properties-only
        // commit) the head is the first EXISTING manifest, and Java still gives ITS bin the min-count
        // protection. Gating this on `added_snapshot_id == self.snapshot_id` would drop the protection
        // for every bin on the empty-data path and merge manifests Java keeps (audit fix 2026-06-10).
        let first_manifest_path = data_manifests
            .first()
            .map(|manifest| manifest.manifest_path.clone());

        // Group by partition spec id, REVERSE-sorted (Java `groupBySpec` L129-137: a TreeMap with
        // `Comparator.reverseOrder()` ⇒ higher spec ids first). Preserve the within-group order.
        let mut groups: HashMap<i32, Vec<ManifestFile>> = HashMap::new();
        let mut spec_order: Vec<i32> = Vec::new();
        for manifest in data_manifests {
            let spec_id = manifest.partition_spec_id;
            groups.entry(spec_id).or_insert_with(|| {
                spec_order.push(spec_id);
                Vec::new()
            });
            groups
                .get_mut(&spec_id)
                .expect("group was just inserted")
                .push(manifest);
        }
        spec_order.sort_unstable_by(|a, b| b.cmp(a)); // reverse order

        let mut merged: Vec<ManifestFile> = Vec::new();
        for spec_id in spec_order {
            let group = groups.remove(&spec_id).expect("spec id came from the keys");
            let group_result = self
                .merge_group(
                    snapshot_producer,
                    spec_id,
                    group,
                    first_manifest_path.as_deref(),
                )
                .await?;
            merged.extend(group_result);
        }

        Ok(merged)
    }

    /// Bin-pack and merge one spec-id group (Java `mergeGroup` L140-185).
    async fn merge_group(
        &self,
        snapshot_producer: &mut SnapshotProducer<'_>,
        spec_id: i32,
        group: Vec<ManifestFile>,
        first_manifest_path: Option<&str>,
    ) -> Result<Vec<ManifestFile>> {
        // ListPacker(targetSizeBytes, lookback=1, largestBinFirst=false).packEnd(group,
        // ManifestFile::length) (Java L146-148). The weight is the manifest's on-disk length.
        let bins = bin_packing::pack_end(group, self.settings.target_size_bytes, |manifest| {
            manifest.manifest_length.max(0) as u64
        });

        let mut output: Vec<ManifestFile> = Vec::new();
        for bin in bins {
            let bin_contains_first = first_manifest_path.is_some_and(|first_path| {
                bin.iter()
                    .any(|manifest| manifest.manifest_path == first_path)
            });
            match bin_disposition(
                bin.len(),
                bin_contains_first,
                self.settings.min_count_to_merge,
            ) {
                BinDisposition::Keep => output.extend(bin),
                BinDisposition::Merge => {
                    let merged = self
                        .create_manifest(snapshot_producer, spec_id, &bin)
                        .await?;
                    output.push(merged);
                }
            }
        }

        Ok(output)
    }

    /// Merge `bin` into a single manifest (Java `createManifest` L187-239). Per entry of each source
    /// manifest, route via the three-way rule (this-snapshot DELETED → delete; this-snapshot ADDED →
    /// add; else → existing). Older tombstones (DELETED by a previous snapshot) are SUPPRESSED.
    async fn create_manifest(
        &self,
        snapshot_producer: &mut SnapshotProducer<'_>,
        spec_id: i32,
        bin: &[ManifestFile],
    ) -> Result<ManifestFile> {
        let mut writer = snapshot_producer.new_cluster_manifest_writer(spec_id)?;

        for manifest_file in bin {
            let manifest = manifest_file
                .load_manifest(snapshot_producer.table.file_io())
                .await?;
            for entry in manifest.entries() {
                let entry = entry.as_ref().clone();
                match entry.status() {
                    ManifestStatus::Deleted => {
                        // Suppress deletes from previous snapshots: only files DELETED by THIS snapshot
                        // are carried into the merged manifest (Java L203-208). For merge_append this is
                        // unreachable (an append never deletes a file), but the routing is kept faithful.
                        if entry.snapshot_id() == Some(self.snapshot_id) {
                            writer.add_delete_entry(entry)?;
                        }
                    }
                    ManifestStatus::Added if entry.snapshot_id() == Some(self.snapshot_id) => {
                        // Adds from THIS snapshot stay adds (Java L209-211). `add_entry` re-stamps the
                        // entry to Added + this snapshot and strips the inherited `Some(-1)` seq back to
                        // `None` so it re-inherits the new snapshot's real seq at commit (see the Physics
                        // section in the module doc).
                        writer.add_entry(entry)?;
                    }
                    _ => {
                        // Everything else (older Added, or Existing) becomes an Existing entry with its
                        // ORIGINAL provenance preserved (Java L212-214, `writer.existing(entry)`). This is
                        // the load-bearing invariant — re-stamping here is the silent-corruption class.
                        writer.add_existing_entry(entry)?;
                    }
                }
            }
        }

        writer.write_manifest_file().await
    }
}

impl ManifestProcess for MergeManifestProcess {
    async fn process_manifests(
        &self,
        snapshot_produce: &mut SnapshotProducer<'_>,
        manifests: Vec<ManifestFile>,
    ) -> Result<Vec<ManifestFile>> {
        // Split DATA from DELETE manifests and reorder DATA so the new added manifest is first.
        let (data_manifests, delete_manifests) = self.split_and_reorder(manifests);

        // Merge the data manifests (Java `mergeManager.mergeManifests(unmergedManifests)`).
        let merged_data = self
            .merge_data_manifests(snapshot_produce, data_manifests)
            .await?;

        // Output order: merged data manifests (bin order), then the delete manifests carried unchanged
        // (Java appends `deleteMergeManager.mergeManifests(unmergedDeleteManifests)` after the data
        // manifests; this port leaves the delete manifests un-merged — see the module doc).
        let mut result = merged_data;
        result.extend(delete_manifests);
        Ok(result)
    }
}

/// What `merge_group` does with a single bin (Java `mergeGroup` L162-181, factored out so the rule is
/// unit-testable without driving real manifest lengths).
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
enum BinDisposition {
    /// Keep every manifest in the bin as-is (no rewrite).
    Keep,
    /// Merge the whole bin into one manifest.
    Merge,
}

/// Decide a bin's disposition (Java `mergeGroup` L162-181):
/// - `bin_len == 1` ⇒ keep the single manifest as-is.
/// - the bin CONTAINS the FIRST (new added) manifest AND `bin_len < min_count_to_merge` ⇒ keep ALL (the
///   min-count gate applies ONLY to the new-manifest bin, so a big old manifest can't block merging
///   older groups).
/// - otherwise ⇒ merge.
fn bin_disposition(
    bin_len: usize,
    bin_contains_first: bool,
    min_count_to_merge: u32,
) -> BinDisposition {
    if bin_len == 1 {
        return BinDisposition::Keep;
    }
    if bin_contains_first && (bin_len as u32) < min_count_to_merge {
        return BinDisposition::Keep;
    }
    BinDisposition::Merge
}

/// A faithful port of Java `BinPacking.ListPacker.packEnd` (`core/util/BinPacking.java`) for the
/// lookback-1, non-largest-bin-first case the manifest merge uses.
///
/// `pack_end` packs `items` into bins of total weight `<= target_weight` BY PACKING FROM THE END: it
/// reverses the input, runs the greedy first-fit packer (lookback 1 ⇒ a single open bin), then reverses
/// each bin's items AND the list of bins so the original order is restored. The "from the end" property
/// is what makes the UNDER-FILLED bin the FIRST one in the output (Java `mergeGroup`'s comment), so the
/// small leftover bin is the one merged on the next append.
///
/// This module ports the general `PackingIterable`/`PackingIterator` algorithm (configurable lookback,
/// `largest_bin_first`, `max_items_per_bin`), not just the lookback-1 special case, so the bin-packing
/// can be unit-tested against hand-computed Java outcomes independent of the merge manager.
mod bin_packing {
    /// Pack `items` into weight-bounded bins from the END, restoring the original order (Java
    /// `ListPacker.packEnd(items, weightFunc)` with `lookback = 1`, `largest_bin_first = false`,
    /// `max_items_per_bin = u64::MAX`).
    pub(super) fn pack_end<T>(
        items: Vec<T>,
        target_weight: u64,
        weight_func: impl Fn(&T) -> u64,
    ) -> Vec<Vec<T>> {
        pack_end_with(items, target_weight, 1, false, u64::MAX, weight_func)
    }

    /// The general `packEnd` (Java `ListPacker.packEnd` with all `PackingIterable` knobs exposed): pack
    /// `reverse(items)` with the greedy packer, then `reverse` each bin and the bin list.
    pub(super) fn pack_end_with<T>(
        mut items: Vec<T>,
        target_weight: u64,
        lookback: usize,
        largest_bin_first: bool,
        max_items_per_bin: u64,
        weight_func: impl Fn(&T) -> u64,
    ) -> Vec<Vec<T>> {
        items.reverse();
        let mut bins = pack(
            items,
            target_weight,
            lookback,
            largest_bin_first,
            max_items_per_bin,
            weight_func,
        );
        for bin in &mut bins {
            bin.reverse();
        }
        bins.reverse();
        bins
    }

    /// The forward greedy packer — Java `BinPacking.PackingIterator.next` materialized eagerly. With a
    /// `lookback` window of open bins, each item is placed in the FIRST open bin that can still hold it
    /// (`bin_weight + weight <= target_weight && bin_size < max_items_per_bin`); when a new bin is opened
    /// and the open-bin count exceeds `lookback`, the oldest (or largest, if `largest_bin_first`) open bin
    /// is emitted. At the end the remaining open bins are emitted in order.
    ///
    /// `lookback` must be `>= 1` (Java `Preconditions.checkArgument(lookback > 0)`); the callers here
    /// always pass a positive value, so an invalid lookback is a programming error (asserted).
    pub(super) fn pack<T>(
        items: Vec<T>,
        target_weight: u64,
        lookback: usize,
        largest_bin_first: bool,
        max_items_per_bin: u64,
        weight_func: impl Fn(&T) -> u64,
    ) -> Vec<Vec<T>> {
        assert!(lookback >= 1, "bin look-back size must be greater than 0");

        let mut closed_bins: Vec<Vec<T>> = Vec::new();
        // Open bins, oldest first. Each is (items, accumulated_weight).
        let mut open_bins: Vec<(Vec<T>, u64)> = Vec::new();

        for item in items {
            let weight = weight_func(&item);

            // findBin: the first open bin that can still hold this item (Java `findBin`,
            // `bin.weight + weight <= targetWeight`). Saturating add: weights come from the
            // UNTRUSTED `manifest_length` field of a manifest list read from storage, and a hostile
            // value near u64::MAX must not panic (debug) or wrap into "fits" (release) — saturation
            // makes an absurd sum simply never fit, which opens a fresh bin (audit hardening
            // 2026-06-10; identical to Java for every realistic weight).
            let target_bin = open_bins.iter().position(|(bin_items, bin_weight)| {
                bin_weight.saturating_add(weight) <= target_weight
                    && (bin_items.len() as u64) < max_items_per_bin
            });

            match target_bin {
                Some(index) => {
                    let (bin_items, bin_weight) = &mut open_bins[index];
                    bin_items.push(item);
                    *bin_weight = bin_weight.saturating_add(weight);
                }
                None => {
                    // Open a new bin for this item.
                    open_bins.push((vec![item], weight));

                    // If we now exceed the lookback window, emit one open bin (Java
                    // `bins.removeFirst()` / `removeLargestBin`).
                    if open_bins.len() > lookback {
                        let remove_index = if largest_bin_first {
                            largest_bin_index(&open_bins)
                        } else {
                            0
                        };
                        let (bin_items, _) = open_bins.remove(remove_index);
                        closed_bins.push(bin_items);
                    }
                }
            }
        }

        // Emit the remaining open bins in order (Java drains `bins.removeFirst()` until empty).
        for (bin_items, _) in open_bins {
            closed_bins.push(bin_items);
        }

        closed_bins
    }

    /// The index of the open bin with the greatest accumulated weight (Java `removeLargestBin` ⇒
    /// `Collections.max(bins, comparingLong(Bin::weight))`). Ties take the FIRST max, matching Java's
    /// `Collections.max` (which returns the first element that is not less than every other).
    fn largest_bin_index<T>(open_bins: &[(Vec<T>, u64)]) -> usize {
        let mut max_index = 0;
        let mut max_weight = open_bins[0].1;
        for (index, (_, weight)) in open_bins.iter().enumerate().skip(1) {
            if *weight > max_weight {
                max_weight = *weight;
                max_index = index;
            }
        }
        max_index
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        // Risk: the lookback-1 packEnd order semantics drift from Java — items must pack from the END so
        // the under-filled bin is FIRST. Weights [3,3,3,3] target 6 ⇒ reversed [3,3,3,3] packs into
        // [[3,3],[3,3]]; reversing restores [[3,3],[3,3]]. Exact-boundary: a bin holding two 3s == target
        // 6 (canAdd uses `<=`), so each bin holds exactly two.
        #[test]
        fn test_pack_end_exact_boundary_pairs() {
            let bins = pack_end(vec![3u64, 3, 3, 3], 6, |w| *w);
            assert_eq!(bins, vec![vec![3, 3], vec![3, 3]]);
        }

        // Risk: an item exactly at the target on its own opens its own bin, and the order is preserved.
        // Weights [4,3,3] target 6: reversed [3,3,4]; first 3 → bin A(3); second 3 fits A → A(6); 4 does
        // not fit A (6+4>6) → new bin B(4). Open bins after loop: [A=[3,3], B=[4]] (lookback 1 ⇒ when B
        // opened, A was emitted first). Reversing each bin + the list ⇒ [[4],[3,3]].
        #[test]
        fn test_pack_end_under_filled_bin_is_first() {
            let bins = pack_end(vec![4u64, 3, 3], 6, |w| *w);
            assert_eq!(bins, vec![vec![4], vec![3, 3]]);
        }

        // Risk: a single item is its own bin (the bin.size()==1 keep-as-is path upstream depends on this).
        #[test]
        fn test_pack_end_single_item() {
            let bins = pack_end(vec![5u64], 6, |w| *w);
            assert_eq!(bins, vec![vec![5]]);
        }

        // Risk: everything fits in one bin when the target is large — the merge-everything case. Weights
        // [1,1,1] target 100 ⇒ one bin [1,1,1], order preserved.
        #[test]
        fn test_pack_end_all_in_one_bin() {
            let bins = pack_end(vec![1u64, 1, 1], 100, |w| *w);
            assert_eq!(bins, vec![vec![1, 1, 1]]);
        }

        // Risk: an item heavier than the target still gets its own bin (Java never drops an item — canAdd
        // is false for an empty bin too, so a new bin is opened and the over-weight item sits alone).
        #[test]
        fn test_pack_end_over_weight_item_alone() {
            let bins = pack_end(vec![1u64, 10, 1], 6, |w| *w);
            // reversed [1,10,1]: 1→A(1); 10 doesn't fit A(1+10>6) → emit A, B(10); 1 doesn't fit
            // B(10+1>6) → emit B, C(1). closed=[[1],[10]], open=[[1]] ⇒ [[1],[10],[1]]. reverse each
            // (no-op) + list ⇒ [[1],[10],[1]].
            assert_eq!(bins, vec![vec![1], vec![10], vec![1]]);
        }

        // Risk: reverse-order preservation — the forward `pack` (not packEnd) packs from the FRONT.
        // Weights [3,3,3,3] target 6 ⇒ [[3,3],[3,3]] (front packing, lookback 1).
        #[test]
        fn test_pack_forward_pairs() {
            let bins = pack(vec![3u64, 3, 3, 3], 6, 1, false, u64::MAX, |w| *w);
            assert_eq!(bins, vec![vec![3, 3], vec![3, 3]]);
        }

        // Risk: max_items_per_bin caps a bin even when weight would allow more (Java `binSize < maxSize`).
        // Weights [1,1,1,1] target 100, max 2 ⇒ [[1,1],[1,1]] despite the huge target.
        #[test]
        fn test_pack_respects_max_items_per_bin() {
            let bins = pack(vec![1u64, 1, 1, 1], 100, 1, false, 2, |w| *w);
            assert_eq!(bins, vec![vec![1, 1], vec![1, 1]]);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::{BinDisposition, bin_disposition};
    use crate::Catalog;
    use crate::memory::tests::new_memory_catalog;
    use crate::spec::{
        DataContentType, DataFile, DataFileBuilder, DataFileFormat, Literal, ManifestContentType,
        ManifestFile, ManifestStatus, Operation, Struct,
    };
    use crate::table::Table;
    use crate::transaction::tests::make_v3_minimal_table_in_catalog;
    use crate::transaction::{ApplyTransactionAction, Transaction};

    // -------------------------------------------------------------------------------------------------
    // Bin-rule unit tests (Java mergeGroup L162-181) — deterministic, no real manifest lengths.
    // -------------------------------------------------------------------------------------------------

    // Risk: a single-manifest bin is wrongly merged (would rewrite a manifest for no reason) — Java
    // L162-166 keeps it.
    #[test]
    fn test_bin_disposition_size_one_is_kept() {
        assert_eq!(bin_disposition(1, false, 2), BinDisposition::Keep);
        assert_eq!(
            bin_disposition(1, true, 2),
            BinDisposition::Keep,
            "even the bin WITH first is kept at size 1"
        );
    }

    // Risk: the min-count gate fires on a bin WITHOUT `first` — it must NOT (Java L172-181 applies the
    // gate only to the bin containing the new manifest). A 2-manifest old-only bin merges below min-count.
    #[test]
    fn test_bin_disposition_two_without_first_merges_below_min_count() {
        assert_eq!(
            bin_disposition(2, false, 100),
            BinDisposition::Merge,
            "a >=2 bin WITHOUT first merges even far below min-count"
        );
    }

    // Risk: the bin WITH `first` merges below min-count (the in-memory manifest must accumulate to the
    // threshold first — Java L172-181). Below the threshold it is kept; at/above it merges. Strict
    // boundary (== min_count): bin_len 2 with min_count 2 must MERGE (the gate is `<`, not `<=`).
    #[test]
    fn test_bin_disposition_with_first_respects_min_count_on_the_boundary() {
        assert_eq!(
            bin_disposition(2, true, 3),
            BinDisposition::Keep,
            "bin WITH first below min-count (2 < 3) is kept"
        );
        assert_eq!(
            bin_disposition(2, true, 2),
            BinDisposition::Merge,
            "bin WITH first AT min-count (2 == 2, gate is strict `<`) merges"
        );
        assert_eq!(
            bin_disposition(3, true, 2),
            BinDisposition::Merge,
            "bin WITH first above min-count merges"
        );
    }

    // -------------------------------------------------------------------------------------------------
    // Fixtures (mirror the rewrite_manifests.rs / row_delta.rs helpers).
    // -------------------------------------------------------------------------------------------------

    /// A data file routed to partition `x = part_value` (the V3 minimal table is identity(x), spec id 0).
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

    /// Fast-append `files` in one commit and return the updated table (a non-merging append, so each
    /// commit produces its own manifest — the small manifests a merge_append later combines).
    async fn fast_append(catalog: &impl Catalog, table: &Table, files: Vec<DataFile>) -> Table {
        let tx = Transaction::new(table);
        let action = tx.fast_append().add_data_files(files);
        let tx = action.apply(tx).unwrap();
        tx.commit(catalog).await.unwrap()
    }

    /// Merge-append `files` in one commit and return the updated table.
    async fn merge_append(catalog: &impl Catalog, table: &Table, files: Vec<DataFile>) -> Table {
        let tx = Transaction::new(table);
        let action = tx.merge_append().add_data_files(files);
        let tx = action.apply(tx).unwrap();
        tx.commit(catalog).await.unwrap()
    }

    /// Set a table property through the catalog so the action reads it from the refreshed base.
    async fn set_table_property(
        catalog: &impl Catalog,
        table: &Table,
        key: &str,
        value: &str,
    ) -> Table {
        let tx = Transaction::new(table);
        let action = tx
            .update_table_properties()
            .set(key.to_string(), value.to_string());
        let tx = action.apply(tx).unwrap();
        tx.commit(catalog).await.unwrap()
    }

    /// The current snapshot's full manifest list (data + deletes).
    async fn current_manifests(table: &Table) -> Vec<ManifestFile> {
        let snapshot = table.metadata().current_snapshot().unwrap();
        snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap()
            .entries()
            .to_vec()
    }

    /// The set of live (Added or Existing) data-file paths across the current snapshot (what a scan reads).
    async fn live_file_paths(table: &Table) -> HashSet<String> {
        let snapshot = table.metadata().current_snapshot().unwrap();
        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();
        let mut live = HashSet::new();
        for manifest_file in manifest_list.entries() {
            let manifest = manifest_file.load_manifest(table.file_io()).await.unwrap();
            for entry in manifest.entries() {
                if entry.is_alive() {
                    live.insert(entry.file_path().to_string());
                }
            }
        }
        live
    }

    /// Return (status, snapshot_id, data_seq, file_seq) of the live entry for `path` (panics if absent).
    async fn live_entry(
        table: &Table,
        path: &str,
    ) -> (ManifestStatus, Option<i64>, Option<i64>, Option<i64>) {
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
                        entry.status(),
                        entry.snapshot_id(),
                        entry.sequence_number(),
                        entry.file_sequence_number,
                    );
                }
            }
        }
        panic!("no live entry for {path}");
    }

    /// Read a snapshot summary property, defaulting to `None` when absent.
    fn summary_prop(table: &Table, prop: &str) -> Option<String> {
        table
            .metadata()
            .current_snapshot()
            .unwrap()
            .summary()
            .additional_properties
            .get(prop)
            .cloned()
    }

    /// The number of DATA-content manifests in the current snapshot.
    async fn data_manifest_count(table: &Table) -> usize {
        current_manifests(table)
            .await
            .iter()
            .filter(|m| m.content == ManifestContentType::Data)
            .count()
    }

    // 1. BELOW-THRESHOLD PASSTHROUGH (Risk: merge fires below min-count). Two fast appends + a
    // merge_append with DEFAULT properties (min-count 100) → no merge: three data manifests survive, and
    // each pre-existing entry keeps its ORIGINAL adding snapshot id (carried byte-unchanged).
    #[tokio::test]
    async fn test_merge_append_below_min_count_does_not_merge() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        let table = fast_append(&catalog, &table, vec![data_file("test/a.parquet", 0)]).await;
        let sa = table.metadata().current_snapshot().unwrap().snapshot_id();
        let table = fast_append(&catalog, &table, vec![data_file("test/b.parquet", 0)]).await;
        let sb = table.metadata().current_snapshot().unwrap().snapshot_id();

        // merge_append a third file with DEFAULT properties: min-count is 100, so nothing merges.
        let table = merge_append(&catalog, &table, vec![data_file("test/c.parquet", 0)]).await;

        assert_eq!(
            data_manifest_count(&table).await,
            3,
            "below the default min-count, the three data manifests are not merged"
        );
        // The two pre-existing entries keep their ORIGINAL adding snapshot ids (carried unchanged, not
        // re-stamped to the merge snapshot).
        let (status_a, snap_a, _, _) = live_entry(&table, "test/a.parquet").await;
        let (status_b, snap_b, _, _) = live_entry(&table, "test/b.parquet").await;
        assert_eq!(snap_a, Some(sa), "a keeps its original adding snapshot id");
        assert_eq!(snap_b, Some(sb), "b keeps its original adding snapshot id");
        // Carried-but-not-merged manifests keep their entries with the ORIGINAL status (here: Added, the
        // status a fast-append entry has in its own un-rewritten manifest).
        assert_eq!(status_a, ManifestStatus::Added);
        assert_eq!(status_b, ManifestStatus::Added);
        // The new file is Added by the merge snapshot.
        let merge_snap = table.metadata().current_snapshot().unwrap().snapshot_id();
        let (status_c, snap_c, _, _) = live_entry(&table, "test/c.parquet").await;
        assert_eq!(status_c, ManifestStatus::Added);
        assert_eq!(snap_c, Some(merge_snap));
        // Live set is the union.
        assert_eq!(
            live_file_paths(&table).await,
            HashSet::from([
                "test/a.parquet".to_string(),
                "test/b.parquet".to_string(),
                "test/c.parquet".to_string(),
            ])
        );
    }

    // 2. AT-THRESHOLD MERGE WITH PROVENANCE (the crown jewel — docs/testing.md write-action pin #2). With
    // min-count-to-merge=2, two fast appends + a merge_append → ONE merged data manifest; carried entries
    // are Existing + ORIGINAL snapshot id + EXPLICIT original data seq ON DISK (raw avro); this-commit
    // entries are Added + NULL seq on disk; live set unchanged; summary keys == fast_append shape.
    #[tokio::test]
    async fn test_merge_append_at_threshold_merges_and_preserves_provenance() {
        use crate::spec::Manifest;

        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let table =
            set_table_property(&catalog, &table, "commit.manifest.min-count-to-merge", "2").await;

        // Two separate fast appends ⇒ two source manifests, each entry inheriting a distinct seq.
        let table = fast_append(&catalog, &table, vec![data_file("test/a.parquet", 0)]).await;
        let sa = table.metadata().current_snapshot().unwrap().snapshot_id();
        let (_, _, a_seq, a_fseq) = live_entry(&table, "test/a.parquet").await;
        let table = fast_append(&catalog, &table, vec![data_file("test/b.parquet", 0)]).await;
        let sb = table.metadata().current_snapshot().unwrap().snapshot_id();
        let (_, _, b_seq, b_fseq) = live_entry(&table, "test/b.parquet").await;
        assert!(a_seq.is_some() && a_fseq.is_some() && b_seq.is_some() && b_fseq.is_some());

        // merge_append a third file: the new added manifest's bin reaches size 3 (>= min-count 2) ⇒ merge.
        let table = merge_append(&catalog, &table, vec![data_file("test/c.parquet", 0)]).await;
        let merge_snap = table.metadata().current_snapshot().unwrap().snapshot_id();

        // Exactly one data manifest now (all three combined).
        let manifests = current_manifests(&table).await;
        let data_manifests: Vec<&ManifestFile> = manifests
            .iter()
            .filter(|m| m.content == ManifestContentType::Data)
            .collect();
        assert_eq!(
            data_manifests.len(),
            1,
            "at/above min-count the three data manifests merge into one"
        );
        let merged_list_seq = data_manifests[0].sequence_number;

        // Live set unchanged (scan view).
        assert_eq!(
            live_file_paths(&table).await,
            HashSet::from([
                "test/a.parquet".to_string(),
                "test/b.parquet".to_string(),
                "test/c.parquet".to_string(),
            ])
        );

        // a and b: Existing + ORIGINAL snapshot id + ORIGINAL seqs (via load_manifest inheritance view).
        let (status_a, snap_a, seq_a, fseq_a) = live_entry(&table, "test/a.parquet").await;
        assert_eq!(status_a, ManifestStatus::Existing, "a becomes Existing");
        assert_eq!(snap_a, Some(sa), "a keeps ORIGINAL adding snapshot id");
        assert_eq!(seq_a, a_seq, "a keeps ORIGINAL data seq");
        assert_eq!(fseq_a, a_fseq, "a keeps ORIGINAL file seq");
        let (status_b, snap_b, seq_b, fseq_b) = live_entry(&table, "test/b.parquet").await;
        assert_eq!(status_b, ManifestStatus::Existing, "b becomes Existing");
        assert_eq!(snap_b, Some(sb), "b keeps ORIGINAL adding snapshot id");
        assert_eq!(seq_b, b_seq, "b keeps ORIGINAL data seq");
        assert_eq!(fseq_b, b_fseq, "b keeps ORIGINAL file seq");

        // c: Added by the merge snapshot, inheriting the merge snapshot's seq.
        let (status_c, snap_c, seq_c, _) = live_entry(&table, "test/c.parquet").await;
        assert_eq!(status_c, ManifestStatus::Added, "c stays Added");
        assert_eq!(snap_c, Some(merge_snap), "c is added by the merge snapshot");
        assert_eq!(
            seq_c,
            Some(merged_list_seq),
            "c re-inherits the merge snapshot's seq"
        );

        // RAW AVRO ON-DISK PIN: carried entries store ORIGINAL seqs EXPLICITLY (not null ⇒ no
        // re-inheritance to the higher merge seq); the this-commit entry stores NULL ⇒ re-inherits.
        let bytes = table
            .file_io()
            .new_input(&data_manifests[0].manifest_path)
            .unwrap()
            .read()
            .await
            .unwrap();
        let (_, raw_entries) = Manifest::try_from_avro_bytes(&bytes).unwrap();
        assert_eq!(
            raw_entries.len(),
            3,
            "all three entries in the merged manifest"
        );
        for entry in &raw_entries {
            match entry.file_path() {
                "test/a.parquet" => {
                    assert_eq!(entry.status(), ManifestStatus::Existing);
                    assert_eq!(
                        entry.sequence_number(),
                        a_seq,
                        "a: original data seq stored EXPLICITLY on disk"
                    );
                    assert_eq!(entry.file_sequence_number, a_fseq);
                    assert_ne!(
                        entry.sequence_number(),
                        Some(merged_list_seq),
                        "a's on-disk seq must NOT be the merge snapshot's seq (the resurrection bug)"
                    );
                }
                "test/b.parquet" => {
                    assert_eq!(entry.status(), ManifestStatus::Existing);
                    assert_eq!(entry.sequence_number(), b_seq);
                    assert_eq!(entry.file_sequence_number, b_fseq);
                }
                "test/c.parquet" => {
                    assert_eq!(entry.status(), ManifestStatus::Added);
                    assert_eq!(
                        entry.sequence_number(),
                        None,
                        "c: this-commit added entry stores NULL seq on disk ⇒ re-inherits at commit"
                    );
                    assert_eq!(entry.file_sequence_number, None);
                }
                other => panic!("unexpected entry {other}"),
            }
        }

        // SUMMARY SHAPE == fast_append: no merge-specific keys; totals correct.
        assert_eq!(
            table
                .metadata()
                .current_snapshot()
                .unwrap()
                .summary()
                .operation,
            Operation::Append,
            "merge_append records Operation::Append"
        );
        assert_eq!(
            summary_prop(&table, "manifests-created"),
            None,
            "merge_append emits NO manifests-created key (fast_append summary shape)"
        );
        assert_eq!(summary_prop(&table, "manifests-kept"), None);
        assert_eq!(summary_prop(&table, "manifests-replaced"), None);
        assert_eq!(
            summary_prop(&table, "total-data-files").as_deref(),
            Some("3"),
            "cumulative total-data-files is correct"
        );
        assert_eq!(
            summary_prop(&table, "total-records").as_deref(),
            Some("3"),
            "cumulative total-records is correct"
        );
    }

    // 3. PROPERTY-DISABLED PASSTHROUGH (Risk: merge fires when disabled). merge-enabled=false + min-count=2
    // ⇒ no merge even though the threshold is met.
    #[tokio::test]
    async fn test_merge_append_disabled_does_not_merge() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let table =
            set_table_property(&catalog, &table, "commit.manifest.min-count-to-merge", "2").await;
        let table =
            set_table_property(&catalog, &table, "commit.manifest-merge.enabled", "false").await;

        let table = fast_append(&catalog, &table, vec![data_file("test/a.parquet", 0)]).await;
        let table = fast_append(&catalog, &table, vec![data_file("test/b.parquet", 0)]).await;
        let table = merge_append(&catalog, &table, vec![data_file("test/c.parquet", 0)]).await;

        assert_eq!(
            data_manifest_count(&table).await,
            3,
            "merge-enabled=false leaves all three manifests un-merged"
        );
        assert_eq!(
            live_file_paths(&table).await,
            HashSet::from([
                "test/a.parquet".to_string(),
                "test/b.parquet".to_string(),
                "test/c.parquet".to_string(),
            ])
        );
    }

    // 4. OLD-TOMBSTONE SUPPRESSION (Java createManifest L203-208). A delete_files commit rewrites a
    // manifest that carries BOTH a live entry (b) AND a prior-snapshot DELETED tombstone (a). Because it
    // still has a live (Existing) entry it SURVIVES `existing_manifest`'s `has_added/existing_files`
    // filter — so it reaches the merge. A min-count=2 merge_append then merges it; the prior-snapshot
    // tombstone for `a` must be SUPPRESSED (absent from EVERY merged manifest's raw entries — its adding
    // snapshot is the delete snapshot, not the merge snapshot); live set + scan unchanged.
    //
    // NOTE: `a` and `b` MUST share one source manifest (one commit), so the rewritten manifest keeps b
    // live AND carries a's tombstone. Two separate commits would leave a's tombstone in its OWN rewritten
    // manifest with NO live files, which `existing_manifest` drops before the merge ever sees it (so the
    // suppression path would never run and the test would be vacuous).
    #[tokio::test]
    async fn test_merge_append_suppresses_old_tombstones() {
        use crate::spec::Manifest;

        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let table =
            set_table_property(&catalog, &table, "commit.manifest.min-count-to-merge", "2").await;

        // a and b in ONE commit (one source manifest), then DELETE a — the delete rewrites that manifest
        // to {a: Deleted (tombstone), b: Existing}. The tombstone's adding snapshot is the delete snapshot.
        let table = fast_append(&catalog, &table, vec![
            data_file("test/a.parquet", 0),
            data_file("test/b.parquet", 0),
        ])
        .await;
        let tx = Transaction::new(&table);
        let action = tx
            .delete_files()
            .delete_files(vec!["test/a.parquet".to_string()]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();
        let delete_snap = table.metadata().current_snapshot().unwrap().snapshot_id();

        // Sanity: a is gone from the live set; the rewritten manifest carries BOTH b (live) AND a's
        // tombstone (adding snapshot = delete snapshot), so it survives `existing_manifest`'s live filter.
        assert_eq!(
            live_file_paths(&table).await,
            HashSet::from(["test/b.parquet".to_string()]),
            "after delete, only b is live"
        );
        let mut tombstone_with_live = false;
        for manifest_file in current_manifests(&table).await {
            let manifest = manifest_file.load_manifest(table.file_io()).await.unwrap();
            let has_a_tombstone = manifest.entries().iter().any(|entry| {
                entry.file_path() == "test/a.parquet" && entry.status() == ManifestStatus::Deleted
            });
            let has_b_live = manifest
                .entries()
                .iter()
                .any(|entry| entry.file_path() == "test/b.parquet" && entry.is_alive());
            if has_a_tombstone {
                let tombstone = manifest
                    .entries()
                    .iter()
                    .find(|entry| entry.file_path() == "test/a.parquet")
                    .unwrap();
                assert_eq!(
                    tombstone.snapshot_id(),
                    Some(delete_snap),
                    "the tombstone's adding snapshot is the delete snapshot"
                );
                tombstone_with_live = has_b_live;
            }
        }
        assert!(
            tombstone_with_live,
            "the rewritten manifest carries a's tombstone alongside live b (so it reaches the merge)"
        );

        // merge_append a new file with min-count=2 ⇒ the data manifests merge (the rewritten manifest +
        // the new added manifest land in one >=2 bin); the prior-snapshot tombstone for a is SUPPRESSED.
        let table = merge_append(&catalog, &table, vec![data_file("test/c.parquet", 0)]).await;

        let manifests = current_manifests(&table).await;
        let data_manifests: Vec<&ManifestFile> = manifests
            .iter()
            .filter(|m| m.content == ManifestContentType::Data)
            .collect();
        assert_eq!(
            data_manifests.len(),
            1,
            "the data manifests merged into one at min-count 2"
        );

        // Across EVERY merged data manifest's raw entries, a's tombstone is absent (older tombstone
        // suppressed — Java L203-208). Reading raw avro bypasses inheritance so we see exactly what was
        // written.
        for data_manifest in &data_manifests {
            let bytes = table
                .file_io()
                .new_input(&data_manifest.manifest_path)
                .unwrap()
                .read()
                .await
                .unwrap();
            let (_, raw_entries) = Manifest::try_from_avro_bytes(&bytes).unwrap();
            assert!(
                raw_entries
                    .iter()
                    .all(|e| e.file_path() != "test/a.parquet"),
                "the prior-snapshot DELETED tombstone for a is suppressed from the merged manifest"
            );
        }

        // Live set + scan unchanged: b and c live, a still gone.
        assert_eq!(
            live_file_paths(&table).await,
            HashSet::from(["test/b.parquet".to_string(), "test/c.parquet".to_string()]),
            "a stays deleted, b and c live"
        );
    }

    // 5. MULTI-SPEC SEPARATION (Java groupBySpec L129-137, reverse TreeMap). Files under spec 0 and spec 1
    // with min-count=2 ⇒ merge groups NEVER cross spec ids; output ordered higher-spec-first.
    #[tokio::test]
    async fn test_merge_append_never_merges_across_specs_higher_spec_first() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let table =
            set_table_property(&catalog, &table, "commit.manifest.min-count-to-merge", "2").await;

        // Two files under spec 0 (so its group has 2 ⇒ mergeable).
        let table = fast_append(&catalog, &table, vec![data_file("test/spec0a.parquet", 0)]).await;
        let table = fast_append(&catalog, &table, vec![data_file("test/spec0b.parquet", 0)]).await;

        // Evolve the partition spec: add identity(y) ⇒ new default spec (spec 1).
        let tx = Transaction::new(&table);
        let action = tx.update_partition_spec().add_field("y");
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();
        let new_spec_id = table.metadata().default_partition_spec_id();
        assert_ne!(new_spec_id, 0);

        // A file under spec 1.
        let spec1_file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("test/spec1a.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(new_spec_id)
            .partition(Struct::from_iter([
                Some(Literal::long(0)),
                Some(Literal::long(0)),
            ]))
            .build()
            .unwrap();
        let table = fast_append(&catalog, &table, vec![spec1_file]).await;

        // merge_append the NEW file under the new default spec (spec 1). The new added manifest is spec 1.
        let spec1_new = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("test/spec1b.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(new_spec_id)
            .partition(Struct::from_iter([
                Some(Literal::long(0)),
                Some(Literal::long(0)),
            ]))
            .build()
            .unwrap();
        let table = merge_append(&catalog, &table, vec![spec1_new]).await;

        // Every merged DATA manifest carries exactly one spec id (no cross-spec merge).
        let manifests = current_manifests(&table).await;
        let data_manifests: Vec<&ManifestFile> = manifests
            .iter()
            .filter(|m| m.content == ManifestContentType::Data)
            .collect();
        for manifest in &data_manifests {
            let loaded = manifest.load_manifest(table.file_io()).await.unwrap();
            let specs: HashSet<i32> = loaded
                .entries()
                .iter()
                .map(|e| e.data_file().partition_spec_id)
                .collect();
            assert_eq!(
                specs.len(),
                1,
                "a merged manifest must carry exactly one partition spec id"
            );
            assert!(specs.contains(&manifest.partition_spec_id));
        }

        // Output ordered higher-spec-first (Java reverse TreeMap): the FIRST data manifest is spec 1.
        assert!(
            data_manifests[0].partition_spec_id >= new_spec_id,
            "the higher spec id group is emitted first (got {})",
            data_manifests[0].partition_spec_id
        );

        // Live set is all four files.
        assert_eq!(
            live_file_paths(&table).await,
            HashSet::from([
                "test/spec0a.parquet".to_string(),
                "test/spec0b.parquet".to_string(),
                "test/spec1a.parquet".to_string(),
                "test/spec1b.parquet".to_string(),
            ])
        );
    }

    // 6. BIN RULE — SIZE-1 KEEP (Java mergeGroup L162-166), deterministic manager-level pin. With a
    // 1-byte target every manifest is its OWN bin (no two manifests fit in 1 byte), so EVERY bin is
    // size-1 ⇒ NOTHING merges even though min-count=2 is met. This pins that the target-size is consulted
    // (a tiny target forces single-manifest bins) AND that size-1 bins are kept as-is.
    //
    // The "≥2 bin WITHOUT first merges below min-count" and "bin WITH first below min-count is kept"
    // sub-rules are pinned deterministically at the `bin_disposition` unit level (see the unit tests),
    // because driving them through real manifest lengths is brittle (manifest avro length varies by a few
    // bytes per commit, so length arithmetic on the target is flaky). The unit-level pin is exact.
    #[tokio::test]
    async fn test_merge_append_tiny_target_keeps_all_size_one_bins() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let table =
            set_table_property(&catalog, &table, "commit.manifest.min-count-to-merge", "2").await;
        // A 1-byte target: no two single-entry manifests can share a bin ⇒ every bin is size 1.
        let table =
            set_table_property(&catalog, &table, "commit.manifest.target-size-bytes", "1").await;

        let table = fast_append(&catalog, &table, vec![data_file("test/a.parquet", 0)]).await;
        let table = fast_append(&catalog, &table, vec![data_file("test/b.parquet", 0)]).await;
        let table = merge_append(&catalog, &table, vec![data_file("test/c.parquet", 0)]).await;

        assert_eq!(
            data_manifest_count(&table).await,
            3,
            "a 1-byte target makes every manifest its own size-1 bin ⇒ no merge despite min-count=2"
        );
        assert_eq!(
            live_file_paths(&table).await,
            HashSet::from([
                "test/a.parquet".to_string(),
                "test/b.parquet".to_string(),
                "test/c.parquet".to_string(),
            ])
        );
    }

    // 7. MoR SAFETY (crown-jewel style). A merge_append on a table with a delete manifest carries the
    // delete manifest UNCHANGED (count pin) and the scan STILL applies the delete after the merge.
    #[tokio::test]
    async fn test_merge_append_carries_delete_manifest_and_delete_still_applies() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let table =
            set_table_property(&catalog, &table, "commit.manifest.min-count-to-merge", "2").await;

        // Real data + a real position delete (mirrors rewrite_manifests test 5).
        let real_data =
            write_real_data_file(&table, "rows.parquet", 0, &[(0, 10), (0, 20), (0, 30)]).await;
        let data_path = real_data.file_path().to_string();
        let table = fast_append(&catalog, &table, vec![real_data]).await;
        let real_data2 = write_real_data_file(&table, "rows2.parquet", 0, &[(0, 40)]).await;
        let table = fast_append(&catalog, &table, vec![real_data2]).await;

        // row_delta a position delete dropping position 1 of rows.parquet (y=20).
        let delete_file = write_real_position_delete(&table, 0, &[(data_path.clone(), 1)]).await;
        let tx = Transaction::new(&table);
        let action = tx.row_delta().add_deletes(vec![delete_file]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        let delete_manifest_count_before = current_manifests(&table)
            .await
            .iter()
            .filter(|m| m.content == ManifestContentType::Deletes)
            .count();
        assert_eq!(
            delete_manifest_count_before, 1,
            "one delete manifest exists"
        );
        assert_eq!(
            scan_y_values(&table).await,
            HashSet::from([10, 30, 40]),
            "the delete drops y=20 before the merge"
        );

        // merge_append a new data file with min-count 2 ⇒ data manifests merge; the delete manifest is
        // carried UNCHANGED.
        let real_data3 = write_real_data_file(&table, "rows3.parquet", 0, &[(0, 50)]).await;
        let table = fast_append(&catalog, &table, vec![real_data3]).await;
        let real_data4 = write_real_data_file(&table, "rows4.parquet", 0, &[(0, 60)]).await;
        let table = merge_append(&catalog, &table, vec![real_data4]).await;

        let delete_manifests: Vec<ManifestFile> = current_manifests(&table)
            .await
            .into_iter()
            .filter(|m| m.content == ManifestContentType::Deletes)
            .collect();
        assert_eq!(
            delete_manifests.len(),
            1,
            "the delete manifest is carried forward unchanged (count pin)"
        );

        // The scan STILL drops y=20 after the merge (delete still applies).
        assert_eq!(
            scan_y_values(&table).await,
            HashSet::from([10, 30, 40, 50, 60]),
            "after the merge the position delete still drops y=20"
        );
    }

    // 8. CUMULATIVE TOTALS (docs/testing.md pin #3). append → append → merge_append → totals correct.
    #[tokio::test]
    async fn test_merge_append_cumulative_totals_correct() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let table =
            set_table_property(&catalog, &table, "commit.manifest.min-count-to-merge", "2").await;

        let table = fast_append(&catalog, &table, vec![data_file("test/a.parquet", 0)]).await;
        let table = fast_append(&catalog, &table, vec![data_file("test/b.parquet", 0)]).await;
        let table = merge_append(&catalog, &table, vec![data_file("test/c.parquet", 0)]).await;

        assert_eq!(
            summary_prop(&table, "total-data-files").as_deref(),
            Some("3"),
            "three data files total after two appends + one merge_append"
        );
        assert_eq!(
            summary_prop(&table, "total-records").as_deref(),
            Some("3"),
            "three records total"
        );
        // The added-this-commit counters reflect ONLY the merge_append's added file (not the merged-in
        // carried files).
        assert_eq!(
            summary_prop(&table, "added-data-files").as_deref(),
            Some("1"),
            "the merge_append added exactly one file this commit"
        );
    }

    // 9. EMPTY APPEND is rejected (mirrors fast_append's empty-append rejection — the producer's
    // empty-commit precondition).
    #[tokio::test]
    async fn test_merge_append_empty_is_rejected() {
        use std::sync::Arc;

        use crate::transaction::TransactionAction;

        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let tx = Transaction::new(&table);
        let action = tx.merge_append().add_data_files(vec![]);
        assert!(
            Arc::new(action).commit(&table).await.is_err(),
            "a merge_append with no data files and no properties must be rejected"
        );
    }

    // -------------------------------------------------------------------------------------------------
    // Real data / delete / scan helpers (copied from rewrite_manifests.rs test fixtures).
    // -------------------------------------------------------------------------------------------------

    async fn write_real_data_file(
        table: &Table,
        file_name: &str,
        part_value: i64,
        rows: &[(i64, i64)],
    ) -> DataFile {
        use std::sync::Arc;

        use arrow_array::{ArrayRef, Int64Array, RecordBatch};

        use crate::arrow::schema_to_arrow_schema;
        use crate::writer::file_writer::{FileWriter, FileWriterBuilder, ParquetWriterBuilder};

        let schema = table.metadata().current_schema();
        let arrow_schema = Arc::new(schema_to_arrow_schema(schema).unwrap());
        let xs: Vec<i64> = rows.iter().map(|(x, _)| *x).collect();
        let ys: Vec<i64> = rows.iter().map(|(_, y)| *y).collect();
        let zs: Vec<i64> = rows.iter().map(|(_, y)| *y * 10).collect();
        let batch = RecordBatch::try_new(arrow_schema, vec![
            Arc::new(Int64Array::from(xs)) as ArrayRef,
            Arc::new(Int64Array::from(ys)) as ArrayRef,
            Arc::new(Int64Array::from(zs)) as ArrayRef,
        ])
        .unwrap();

        let file_path = format!("{}/data/{}", table.metadata().location(), file_name);
        let output = table.file_io().new_output(file_path).unwrap();
        let parquet_builder = ParquetWriterBuilder::new(
            parquet::file::properties::WriterProperties::builder().build(),
            schema.clone(),
        );
        let mut writer = parquet_builder.build(output).await.unwrap();
        writer.write(&batch).await.unwrap();
        let mut builder = writer.close().await.unwrap().into_iter().next().unwrap();
        builder
            .content(DataContentType::Data)
            .partition_spec_id(0)
            .partition(Struct::from_iter([Some(Literal::long(part_value))]))
            .build()
            .unwrap()
    }

    async fn write_real_position_delete(
        table: &Table,
        part_value: i64,
        deletes: &[(String, i64)],
    ) -> DataFile {
        use std::sync::Arc;

        use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};

        use crate::spec::PartitionKey;
        use crate::writer::base_writer::position_delete_writer::{
            PositionDeleteFileWriterBuilder, PositionDeleteWriterConfig,
        };
        use crate::writer::file_writer::ParquetWriterBuilder;
        use crate::writer::file_writer::location_generator::{
            DefaultFileNameGenerator, DefaultLocationGenerator,
        };
        use crate::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
        use crate::writer::{IcebergWriter, IcebergWriterBuilder};

        let config = PositionDeleteWriterConfig::new().unwrap();
        let location_gen = DefaultLocationGenerator::new(table.metadata().clone()).unwrap();
        let file_name_gen = DefaultFileNameGenerator::new(
            "pos-del".to_string(),
            Some(uuid::Uuid::now_v7().to_string()),
            DataFileFormat::Parquet,
        );
        let parquet_builder = ParquetWriterBuilder::new(
            parquet::file::properties::WriterProperties::builder().build(),
            config.schema().clone(),
        );
        let rolling = RollingFileWriterBuilder::new_with_default_file_size(
            parquet_builder,
            table.file_io().clone(),
            location_gen,
            file_name_gen,
        );
        let partition_key = PartitionKey::new(
            table.metadata().default_partition_spec().as_ref().clone(),
            table.metadata().current_schema().clone(),
            Struct::from_iter([Some(Literal::long(part_value))]),
        );
        let mut writer = PositionDeleteFileWriterBuilder::new(rolling, config.clone())
            .build(Some(partition_key))
            .await
            .unwrap();

        let paths: Vec<&str> = deletes.iter().map(|(p, _)| p.as_str()).collect();
        let positions: Vec<i64> = deletes.iter().map(|(_, pos)| *pos).collect();
        let batch = RecordBatch::try_new(config.arrow_schema().clone(), vec![
            Arc::new(StringArray::from(paths)) as ArrayRef,
            Arc::new(Int64Array::from(positions)) as ArrayRef,
        ])
        .unwrap();
        writer.write(batch).await.unwrap();
        writer.close().await.unwrap().into_iter().next().unwrap()
    }

    async fn scan_y_values(table: &Table) -> HashSet<i64> {
        use arrow_array::{Int64Array, RecordBatch};
        use futures::TryStreamExt;

        let stream = table
            .scan()
            .select(["y"])
            .build()
            .unwrap()
            .to_arrow()
            .await
            .unwrap();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
        let mut values = HashSet::new();
        for batch in batches {
            let col = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for i in 0..col.len() {
                values.insert(col.value(i));
            }
        }
        values
    }

    // AUDIT pin (2026-06-10): Java's `first` is the unconditional STREAM HEAD
    // (`ManifestFile first = manifestIter.next()`, ManifestMergeManager L85) — for a merging append
    // that adds NO data files (a properties-only commit, allowed by the producer when snapshot
    // properties are non-empty) the head is the first EXISTING manifest, and ITS bin gets the
    // min-count protection. With the default min-count (100) a properties-only merge_append must
    // therefore KEEP the table's small manifests un-merged. Risk pinned: a `first` identification
    // gated on `added_snapshot_id == this snapshot` yields `None` here, drops the protection for
    // every bin, and merges manifests Java would keep.
    #[tokio::test]
    async fn test_properties_only_merge_append_keeps_first_existing_bin_below_min_count() {
        use std::collections::HashMap;

        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let table = fast_append(&catalog, &table, vec![data_file("test/audit-a.parquet", 0)]).await;
        let table = fast_append(&catalog, &table, vec![data_file("test/audit-b.parquet", 0)]).await;
        assert_eq!(current_manifests(&table).await.len(), 2);

        // Properties-only merging append: no data files, default merge settings (min-count 100).
        let tx = Transaction::new(&table);
        let action = tx.merge_append().set_snapshot_properties(HashMap::from([(
            "audit-pin".to_string(),
            "true".to_string(),
        )]));
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        // Java parity: the single bin contains `first` (the first existing manifest) and 2 < 100,
        // so BOTH manifests are kept — no merge fires.
        let manifests = current_manifests(&table).await;
        assert_eq!(
            manifests.len(),
            2,
            "a properties-only merge_append below min-count must keep the existing manifests un-merged (Java stream-head `first`)"
        );
        assert_eq!(
            live_file_paths(&table).await,
            HashSet::from([
                "test/audit-a.parquet".to_string(),
                "test/audit-b.parquet".to_string()
            ])
        );
    }
}
