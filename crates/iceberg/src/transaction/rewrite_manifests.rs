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

//! This module contains the rewrite-manifests action (manifest re-organization, NOT data change).
//!
//! [`RewriteManifestsAction`] re-organizes a table's MANIFEST files without changing the set of live
//! data files: it produces an `Operation::Replace` snapshot whose LIVE FILE SET IS IDENTICAL to the
//! base snapshot's — only the grouping of entries into manifests changes (Java `BaseRewriteManifests`,
//! which extends `SnapshotProducer<RewriteManifests>`, NOT `MergingSnapshotProducer`). It is the commit
//! primitive a manifest-compaction / re-clustering job uses.
//!
//! Two modes (a single commit may use both):
//! - **Clustered rewrite** ([`RewriteManifestsAction::cluster_by`]): every current DATA manifest that
//!   matches [`RewriteManifestsAction::rewrite_if`] is re-read and its LIVE entries are re-grouped into
//!   new manifests keyed by `(cluster_key, partition_spec_id)`. Each re-grouped entry is written via the
//!   manifest writer's EXISTING-entry path — **the load-bearing invariant: its original `snapshot_id` +
//!   data sequence number + file sequence number are preserved and its status becomes `Existing`** (Java
//!   `ManifestWriter.existing(entry)`, `core/BaseRewriteManifests.java` L372). Re-stamping provenance
//!   here is the #1 silent-corruption class — it breaks merge-on-read delete application and incremental
//!   scans.
//! - **Explicit replacement** ([`RewriteManifestsAction::add_manifest`] /
//!   [`RewriteManifestsAction::delete_manifest`]): replace a specific current manifest with an
//!   externally-prepared one carrying the SAME active files.
//!
//! **Operation recorded:** always [`Operation::Replace`] (Java `BaseRewriteManifests.operation()`
//! returns `DataOperations.REPLACE`, L87-89).
//!
//! **Delete manifests are immune:** a `Deletes`-content manifest is never re-clustered (Java
//! `containsDeletes(manifest)` ⇒ kept, L252). It is carried forward byte-identical, so any outstanding
//! merge-on-read deletes still apply after the rewrite. (The clustered rewrite only touches DATA
//! manifests; their live data entries keep their provenance, so `data_seq <= delete_seq` still holds.)
//!
//! ## Adaptations / deviations from Java (each one named here per the parity contract)
//!
//! - **String cluster key.** Java `clusterBy(Function<DataFile, Object>)` clusters on `Object` equality.
//!   This port takes `Fn(&DataFile) -> String`: a `String` key covers the practical cluster keys (a
//!   partition value rendered to a string, a bucket id, etc.) and gives a `Hash + Eq` key for the
//!   per-cluster writer map without an `Any`-downcast dance. Documented on [`RewriteManifestsAction::cluster_by`].
//! - **Estimated-length size rolling.** Java rolls a cluster writer to a new manifest when
//!   `writer.length() >= manifestTargetSizeBytes` (L368). The Rust [`crate::spec::ManifestWriter`] BUFFERS
//!   its entries and exposes NO incremental on-disk length, so this port uses an ESTIMATED-length proxy:
//!   for each appended entry it adds the source manifest's average per-entry byte size
//!   (`source.manifest_length / source-entry-count`) to a running estimate and rolls when the estimate
//!   reaches the target. This only shifts the roll POINTS, never correctness (every live entry is written
//!   exactly once with preserved provenance regardless of which manifest it lands in); with the default
//!   8 MB target, rolling is rare. See [`ClusterWriters`].
//! - **V1 `add_manifest` is unsupported.** On a V2+ table an added manifest can inherit the new snapshot
//!   id (Java `canInheritSnapshotId()` ⇒ `addedManifests.add(manifest)`, L143-144). On a V1 table Java
//!   instead COPIES the manifest, re-stamping every entry with the new snapshot id
//!   (`copyManifest` / `rewrittenAddedManifests`, L145-167). That copy path is deferred; this action
//!   rejects [`RewriteManifestsAction::add_manifest`] on a V1 table with [`ErrorKind::FeatureUnsupported`].
//! - **No-current-snapshot is a clean error, not a panic.** Java reads `base.currentSnapshot().allManifests`
//!   (L171) and NPEs on an empty table. This port returns a clean [`ErrorKind::DataInvalid`] instead.
//! - **`scanManifestsWith(ExecutorService)` is not ported.** Java parallelizes the per-manifest rewrite
//!   over a worker pool (L248-249). The Rust path is sequential async; there is no executor surface.
//! - **`requiresRewrite` statefulness is dropped.** Java caches `rewrittenManifests` across commit retries
//!   and short-circuits a full rewrite when the cached set is still valid (L206-219). The Rust action holds
//!   no cross-attempt state and recomputes the rewrite from the refreshed base on every commit attempt,
//!   which is semantically equivalent (the cache is a retry optimization, not a behavior).

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use uuid::Uuid;

use crate::error::{Error, ErrorKind, Result};
use crate::spec::{
    DataFile, FormatVersion, ManifestContentType, ManifestEntry, ManifestFile, Operation,
    TableProperties, UNASSIGNED_SEQUENCE_NUMBER, UNASSIGNED_SNAPSHOT_ID,
};
use crate::table::Table;
use crate::transaction::snapshot::{
    DefaultManifestProcess, SnapshotProduceOperation, SnapshotProducer,
};
use crate::transaction::{ActionCommit, TransactionAction};

/// A function that maps a [`DataFile`] to its cluster key (Java `clusterBy(Function<DataFile, Object>)`).
/// See the module doc for why this is a `String` rather than Java's `Object`.
type ClusterByFunction = Arc<dyn Fn(&DataFile) -> String + Send + Sync>;

/// A predicate deciding whether a current [`ManifestFile`] should be rewritten (Java
/// `rewriteIf(Predicate<ManifestFile>)`). `None` ⇒ every data manifest is rewritten (Java default).
type RewriteIfPredicate = Arc<dyn Fn(&ManifestFile) -> bool + Send + Sync>;

/// A transaction action that re-organizes a table's manifests without changing its live data files,
/// producing one `Operation::Replace` snapshot (the manifest-compaction commit primitive — Java
/// `BaseRewriteManifests`).
///
/// Create one with [`crate::transaction::Transaction::rewrite_manifests`]. Configure it with
/// [`RewriteManifestsAction::cluster_by`] (re-cluster matching data manifests) and/or
/// [`RewriteManifestsAction::add_manifest`] + [`RewriteManifestsAction::delete_manifest`] (explicit
/// replacement). A no-op rewrite (no cluster fn, no add/delete) is allowed and keeps every manifest
/// as-is (Java allows it).
///
/// The live file set (the set of paths a scan would read) is IDENTICAL before and after the rewrite;
/// re-clustered entries keep their original provenance (snapshot id + sequence numbers). See the module
/// doc for the full Java contract and the documented adaptations.
pub struct RewriteManifestsAction {
    /// The cluster-key function (Java `clusterByFunc`). `None` ⇒ no clustered rewrite is performed.
    cluster_by: Option<ClusterByFunction>,
    /// The rewrite predicate (Java `predicate`). `None` ⇒ all data manifests match (rewrite all).
    rewrite_if: Option<RewriteIfPredicate>,
    /// Manifests to delete explicitly (Java `deletedManifests`), matched against the current snapshot by
    /// path. Each must carry a balancing [`Self::added_manifests`] entry or `validateFilesCounts` fires.
    deleted_manifests: Vec<ManifestFile>,
    /// Manifests to add explicitly (Java `addedManifests`); each must carry only existing entries with an
    /// unassigned snapshot id + sequence number (validated in [`Self::add_manifest`]).
    added_manifests: Vec<ManifestFile>,
    /// User-supplied snapshot summary properties (Java `RewriteManifests.set`).
    snapshot_properties: HashMap<String, String>,
    commit_uuid: Option<Uuid>,
    key_metadata: Option<Vec<u8>>,
}

impl RewriteManifestsAction {
    pub(crate) fn new() -> Self {
        Self {
            cluster_by: None,
            rewrite_if: None,
            deleted_manifests: vec![],
            added_manifests: vec![],
            snapshot_properties: HashMap::default(),
            commit_uuid: None,
            key_metadata: None,
        }
    }

    /// Cluster live data-file entries into new manifests by the key `func` returns (Java
    /// `RewriteManifests.clusterBy(Function<DataFile, Object>)`). Every current DATA manifest matching
    /// [`Self::rewrite_if`] is re-read and its live entries are re-grouped, keyed by
    /// `(func(file), manifest.partition_spec_id)` (Java `Pair.of(key, partitionSpecId)`, L340-343).
    ///
    /// The key is a `String` (a documented adaptation of Java's `Object` — see the module doc); it must
    /// be deterministic for a given file. Re-grouped entries preserve their original provenance.
    pub fn cluster_by(
        mut self,
        func: impl Fn(&DataFile) -> String + Send + Sync + 'static,
    ) -> Self {
        self.cluster_by = Some(Arc::new(func));
        self
    }

    /// Restrict which current manifests are rewritten (Java `RewriteManifests.rewriteIf`). A manifest is
    /// rewritten only when `predicate` returns `true`; manifests that do not match are kept byte-identical.
    /// When this is not set, every (data) manifest is rewritten (Java default, L282-284).
    pub fn rewrite_if(
        mut self,
        predicate: impl Fn(&ManifestFile) -> bool + Send + Sync + 'static,
    ) -> Self {
        self.rewrite_if = Some(Arc::new(predicate));
        self
    }

    /// Delete a specific manifest from the table (Java `RewriteManifests.deleteManifest`). The manifest
    /// must be present in the current snapshot (validated at commit by path — Java
    /// `validateDeletedManifests`, L286-298) and its active files must be re-added via a balancing
    /// [`Self::add_manifest`], or the conservation check (`validateFilesCounts`) rejects the commit.
    pub fn delete_manifest(mut self, manifest: ManifestFile) -> Self {
        // Java `deletedManifests` is a SET with path-based equality (`Sets.newHashSet` +
        // `GenericManifestFile` path equality, L55) — a duplicate delete of the same manifest is a
        // no-op. Deduplicate by path here so the duplicate cannot double-count the replaced side of
        // `validateFilesCounts` (audit fix 2026-06-10).
        if !self
            .deleted_manifests
            .iter()
            .any(|existing| existing.manifest_path == manifest.manifest_path)
        {
            self.deleted_manifests.push(manifest);
        }
        self
    }

    /// Add an externally-prepared manifest to the table (Java `RewriteManifests.addManifest`). The
    /// manifest must contain ONLY existing entries — it cannot carry added files, deleted files, an
    /// assigned snapshot id, or an assigned sequence number (Java `addManifest` preconditions, L134-141):
    ///
    /// - **Added files** (`has_added_files`) ⇒ "Cannot add manifest with added files".
    /// - **Deleted files** (`has_deleted_files`) ⇒ "Cannot add manifest with deleted files".
    /// - **Assigned snapshot id** (`added_snapshot_id != -1` /
    ///   [`UNASSIGNED_SNAPSHOT_ID`]) ⇒ "Snapshot id must be assigned during commit" (Java checks
    ///   `snapshotId() == null || snapshotId() == -1`; the Rust [`ManifestFile`] stores a non-optional
    ///   `added_snapshot_id`, so the representable check is `== UNASSIGNED_SNAPSHOT_ID`).
    /// - **Assigned sequence number** (`sequence_number != -1` /
    ///   [`UNASSIGNED_SEQUENCE_NUMBER`]) ⇒ "Sequence must be assigned during commit".
    ///
    /// **V1 tables are unsupported** ([`ErrorKind::FeatureUnsupported`]): on V2+ the manifest inherits the
    /// new snapshot id (Java `canInheritSnapshotId()`); on V1 Java instead COPIES the manifest, a path this
    /// action defers (see the module doc).
    ///
    /// The precondition checks return their error eagerly so the caller learns immediately. (Java throws in
    /// `addManifest` itself.)
    pub fn add_manifest(mut self, manifest: ManifestFile) -> Result<Self> {
        if manifest.has_added_files() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Cannot add manifest with added files",
            ));
        }
        if manifest.has_deleted_files() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Cannot add manifest with deleted files",
            ));
        }
        if manifest.added_snapshot_id != UNASSIGNED_SNAPSHOT_ID {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Snapshot id must be assigned during commit",
            ));
        }
        if manifest.sequence_number != UNASSIGNED_SEQUENCE_NUMBER {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Sequence must be assigned during commit",
            ));
        }
        self.added_manifests.push(manifest);
        Ok(self)
    }

    /// Merge a property into the snapshot summary (Java `RewriteManifests.set`).
    pub fn set(mut self, property: impl Into<String>, value: impl Into<String>) -> Self {
        self.snapshot_properties
            .insert(property.into(), value.into());
        self
    }

    /// Set the commit UUID for the snapshot (otherwise a fresh v7 UUID is generated).
    pub fn set_commit_uuid(mut self, commit_uuid: Uuid) -> Self {
        self.commit_uuid = Some(commit_uuid);
        self
    }

    /// Set key metadata for manifest files.
    pub fn set_key_metadata(mut self, key_metadata: Vec<u8>) -> Self {
        self.key_metadata = Some(key_metadata);
        self
    }
}

#[async_trait]
impl TransactionAction for RewriteManifestsAction {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        // V1 `add_manifest` is unsupported (the Java `copyManifest` legacy path is deferred — module doc).
        // This feature-level rejection is independent of table state, so it fires BEFORE the
        // no-current-snapshot check below.
        if !self.added_manifests.is_empty()
            && table.metadata().format_version() == FormatVersion::V1
        {
            return Err(Error::new(
                ErrorKind::FeatureUnsupported,
                "add_manifest on a V1 table is not supported: the no-inheritance copyManifest path \
                 (which re-stamps every entry with the new snapshot id) is not yet implemented",
            ));
        }

        // No-current-snapshot ⇒ clean error (Java reads `base.currentSnapshot().allManifests` and NPEs;
        // we fail loudly — see the module doc).
        let Some(current_snapshot) = table.metadata().current_snapshot() else {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Cannot rewrite manifests: table has no current snapshot",
            ));
        };
        let current_snapshot_id = current_snapshot.snapshot_id();

        // The manifest target size (Java `getManifestTargetSizeBytes`).
        let target_size_bytes = table
            .metadata()
            .properties()
            .get(TableProperties::PROPERTY_COMMIT_MANIFEST_TARGET_SIZE_BYTES)
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(TableProperties::PROPERTY_COMMIT_MANIFEST_TARGET_SIZE_BYTES_DEFAULT);

        // Construct the producer FIRST (empty added files — this action adds no data files); the rewrite
        // borrows its writer machinery (Java `BaseRewriteManifests extends SnapshotProducer`).
        let mut snapshot_producer = SnapshotProducer::new(
            table,
            self.commit_uuid.unwrap_or_else(Uuid::now_v7),
            self.key_metadata.clone(),
            self.snapshot_properties.clone(),
            vec![],
        );

        // Load the FULL manifest list — DATA and DELETE (Java `allManifests`, L171); `current_data_manifests`
        // would drop the delete manifests, which must be carried forward so deletes still apply.
        let current_manifests = current_snapshot
            .load_manifest_list(table.file_io(), &table.metadata_ref())
            .await?
            .entries()
            .to_vec();

        // validateDeletedManifests (Java L286-298): every delete_manifest arg must be present in the
        // current manifest list (matched by path — ManifestFile equality is path-based in Java).
        self.validate_deleted_manifests(&current_manifests, current_snapshot_id)?;

        // Partition current manifests into KEPT vs REWRITTEN and produce the new cluster manifests.
        let rewrite_outcome = self
            .perform_rewrite(
                &mut snapshot_producer,
                &current_manifests,
                target_size_bytes,
            )
            .await?;

        // validateFilesCounts (Java L300-329): Σ active files over created == Σ over replaced.
        validate_files_counts(
            &rewrite_outcome.new_manifests,
            &self.added_manifests,
            &rewrite_outcome.rewritten_manifests,
            &self.deleted_manifests,
        )?;

        // Stamp every externally-added manifest with the new snapshot id (Java `withSnapshotId`, L184-187)
        // — required by `ManifestListWriter::assign_sequence_numbers` (an unassigned-seq manifest whose
        // `added_snapshot_id != snapshot_id` is an error). New cluster manifests already carry it from the
        // writer.
        let new_snapshot_id = snapshot_producer.snapshot_id();
        let mut added_manifests = self.added_manifests.clone();
        for manifest in &mut added_manifests {
            manifest.added_snapshot_id = new_snapshot_id;
        }

        // Compose the final list NEW MANIFESTS FIRST, then added, then kept (Java L189-194 puts the new
        // ones at the beginning).
        let mut final_manifests = rewrite_outcome.new_manifests;
        final_manifests.extend(added_manifests);
        final_manifests.extend(rewrite_outcome.kept_manifests);

        // Summary counts (Java `summary()`, L98-112). These also satisfy the empty-commit precondition in
        // `SnapshotProducer::manifest_file` (this action adds no data files), so they MUST be set.
        let created_count = rewrite_outcome.new_manifest_count + self.added_manifests.len();
        let replaced_count =
            rewrite_outcome.rewritten_manifests.len() + self.deleted_manifests.len();
        snapshot_producer.extend_snapshot_properties([
            ("manifests-created".to_string(), created_count.to_string()),
            (
                "manifests-kept".to_string(),
                rewrite_outcome.kept_count.to_string(),
            ),
            ("manifests-replaced".to_string(), replaced_count.to_string()),
            (
                "entries-processed".to_string(),
                rewrite_outcome.entries_processed.to_string(),
            ),
        ]);

        snapshot_producer
            .commit(
                RewriteManifestsOperation {
                    existing_manifests: final_manifests,
                },
                DefaultManifestProcess,
            )
            .await
    }
}

/// The result of partitioning + re-clustering the current manifests.
struct RewriteOutcome {
    /// The newly-written cluster manifests (already carry the new snapshot id from the writer).
    new_manifests: Vec<ManifestFile>,
    /// The number of new cluster manifests (== `new_manifests.len()`, kept explicit for the summary so a
    /// later refactor cannot desync it from the byte vec).
    new_manifest_count: usize,
    /// The current manifests that were rewritten (their entries went into `new_manifests`) — the
    /// "replaced" side of the conservation check, alongside the explicitly-deleted manifests.
    rewritten_manifests: Vec<ManifestFile>,
    /// The current manifests carried forward unchanged (predicate-false, delete-content, or no cluster fn).
    kept_manifests: Vec<ManifestFile>,
    /// The number of kept manifests (== `kept_manifests.len()`, for the summary).
    kept_count: usize,
    /// Total live entries re-clustered (Java `entryCount` / `entries-processed`).
    entries_processed: u64,
}

impl RewriteManifestsAction {
    /// validateDeletedManifests (Java `BaseRewriteManifests.validateDeletedManifests`, L286-298): every
    /// `delete_manifest` argument must be present in the current snapshot's manifest list, matched by path
    /// (Java ManifestFile equality is path-based). The first missing one errors with Java's message shape.
    fn validate_deleted_manifests(
        &self,
        current_manifests: &[ManifestFile],
        current_snapshot_id: i64,
    ) -> Result<()> {
        let current_paths: HashSet<&str> = current_manifests
            .iter()
            .map(|manifest| manifest.manifest_path.as_str())
            .collect();

        for deleted in &self.deleted_manifests {
            if !current_paths.contains(deleted.manifest_path.as_str()) {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Deleted manifest {} could not be found in the latest snapshot {}",
                        deleted.manifest_path, current_snapshot_id
                    ),
                ));
            }
        }

        Ok(())
    }

    /// Partition the current manifests and (when a cluster function is set) re-cluster the matching data
    /// manifests' live entries into new manifests.
    ///
    /// Mirrors Java `performRewrite` (L239-276) + `keepActiveManifests` (L221-229). The deleted set is
    /// excluded up front (Java `remainingManifests`, L242-245). For each remaining manifest:
    /// - **no cluster function** ⇒ every non-deleted manifest is KEPT as-is (Java `requiresRewrite` returns
    ///   `false` when `clusterByFunc == null`, so `keepActiveManifests` runs);
    /// - **delete-content manifest** OR **`rewrite_if` predicate false** ⇒ KEPT as-is (Java
    ///   `containsDeletes(manifest) || !matchesPredicate(manifest)`, L252);
    /// - otherwise ⇒ REWRITTEN: each LIVE entry (`is_alive`; Java `liveEntries()` skips DELETED) is appended
    ///   to the cluster writer for `(cluster_by(file), manifest.partition_spec_id)` via `add_existing_entry`
    ///   (provenance preserved).
    async fn perform_rewrite(
        &self,
        snapshot_producer: &mut SnapshotProducer<'_>,
        current_manifests: &[ManifestFile],
        target_size_bytes: u64,
    ) -> Result<RewriteOutcome> {
        let deleted_paths: HashSet<&str> = self
            .deleted_manifests
            .iter()
            .map(|manifest| manifest.manifest_path.as_str())
            .collect();

        let mut kept_manifests: Vec<ManifestFile> = Vec::new();
        let mut rewritten_manifests: Vec<ManifestFile> = Vec::new();
        let mut cluster_writers = ClusterWriters::new(target_size_bytes);
        let mut entries_processed: u64 = 0;

        for manifest_file in current_manifests {
            if deleted_paths.contains(manifest_file.manifest_path.as_str()) {
                // Explicitly deleted — replaced by an added_manifest; never kept, never rewritten here.
                continue;
            }

            let should_rewrite = self.cluster_by.is_some()
                && manifest_file.content == ManifestContentType::Data
                && self
                    .rewrite_if
                    .as_ref()
                    .map(|predicate| predicate(manifest_file))
                    .unwrap_or(true);

            if !should_rewrite {
                // KEPT as-is: no cluster fn, a delete-content manifest, or the predicate said keep.
                kept_manifests.push(manifest_file.clone());
                continue;
            }

            // REWRITTEN: re-cluster its live entries (provenance preserved).
            let cluster_by = self
                .cluster_by
                .as_ref()
                .expect("should_rewrite implies cluster_by is set");
            let manifest = manifest_file
                .load_manifest(snapshot_producer.table.file_io())
                .await?;
            let per_entry_size_estimate = estimate_per_entry_size(manifest_file);

            for entry in manifest.entries() {
                if !entry.is_alive() {
                    continue;
                }
                let entry = entry.as_ref().clone();
                let cluster_key = cluster_by(entry.data_file());
                cluster_writers
                    .append(
                        snapshot_producer,
                        cluster_key,
                        manifest_file.partition_spec_id,
                        entry,
                        per_entry_size_estimate,
                    )
                    .await?;
                entries_processed += 1;
            }

            rewritten_manifests.push(manifest_file.clone());
        }

        let new_manifests = cluster_writers.finish().await?;
        let new_manifest_count = new_manifests.len();
        let kept_count = kept_manifests.len();

        Ok(RewriteOutcome {
            new_manifests,
            new_manifest_count,
            rewritten_manifests,
            kept_manifests,
            kept_count,
            entries_processed,
        })
    }
}

/// The per-`(cluster_key, partition_spec_id)` manifest writers, with the estimated-length size-rolling
/// proxy for Java's `writer.length() >= manifestTargetSizeBytes` (L368).
///
/// Each key owns ONE open writer plus a running byte-size ESTIMATE. When appending an entry would not yet
/// be reached, it accumulates onto the open writer; once the estimate reaches `target_size_bytes` the open
/// writer is sealed into a finished [`ManifestFile`] and a fresh writer takes over (the roll). All finished
/// manifests are returned by [`ClusterWriters::finish`].
///
/// **Why an estimate (documented divergence):** [`crate::spec::ManifestWriter`] buffers entries and
/// exposes no incremental on-disk length, so the exact `writer.length()` Java rolls on is unavailable until
/// the writer is closed. The per-entry estimate (the source manifest's average entry size) only shifts the
/// roll POINTS — every live entry is still written exactly once with preserved provenance — so it cannot
/// affect correctness, only the manifest-count distribution. With the default 8 MB target rolling is rare.
struct ClusterWriters {
    target_size_bytes: u64,
    /// Per key: the open writer's accumulated size estimate.
    open_estimates: HashMap<(String, i32), u64>,
    /// Per key: the open writer (taken out + replaced on a roll).
    open_writers: HashMap<(String, i32), crate::spec::ManifestWriter>,
    /// Sealed manifests, in append order.
    finished: Vec<ManifestFile>,
}

impl ClusterWriters {
    fn new(target_size_bytes: u64) -> Self {
        Self {
            target_size_bytes,
            open_estimates: HashMap::new(),
            open_writers: HashMap::new(),
            finished: Vec::new(),
        }
    }

    /// Append one live entry to the writer for `(cluster_key, partition_spec_id)`, rolling to a new
    /// manifest first if the open writer's estimated size has reached the target (Java `WriterWrapper.addEntry`
    /// L365-373: roll on `writer.length() >= target`, then `writer.existing(entry)`).
    async fn append(
        &mut self,
        snapshot_producer: &mut SnapshotProducer<'_>,
        cluster_key: String,
        partition_spec_id: i32,
        entry: ManifestEntry,
        per_entry_size_estimate: u64,
    ) -> Result<()> {
        let key = (cluster_key, partition_spec_id);

        // Roll BEFORE appending if the current open writer has reached the target (Java rolls on the
        // entry that would tip it over; here the estimate is checked against the same threshold).
        if let Some(estimate) = self.open_estimates.get(&key).copied()
            && estimate >= self.target_size_bytes
            && let Some(writer) = self.open_writers.remove(&key)
        {
            let manifest = writer.write_manifest_file().await?;
            self.finished.push(manifest);
            self.open_estimates.remove(&key);
        }

        // Ensure an open writer exists for this key.
        if !self.open_writers.contains_key(&key) {
            let writer = snapshot_producer.new_cluster_manifest_writer(partition_spec_id)?;
            self.open_writers.insert(key.clone(), writer);
            self.open_estimates.insert(key.clone(), 0);
        }

        // Append the entry as an EXISTING entry — preserves snapshot id + both sequence numbers, status
        // becomes Existing (Java `writer.existing(entry)`). THE load-bearing provenance invariant.
        let writer = self
            .open_writers
            .get_mut(&key)
            .expect("writer was just inserted for this key");
        writer.add_existing_entry(entry)?;
        // Saturating add: the per-entry estimate derives from the UNTRUSTED `manifest_length` of a
        // manifest list read from storage; a hostile value must not panic (debug) or wrap the
        // estimate back to small (release). Saturation just pins the estimate at the ceiling, which
        // rolls the writer — harmless (audit hardening 2026-06-10).
        let estimate = self.open_estimates.entry(key).or_insert(0);
        *estimate = estimate.saturating_add(per_entry_size_estimate);

        Ok(())
    }

    /// Seal every still-open writer and return all finished manifests in deterministic order (sorted by
    /// `(cluster_key, partition_spec_id)`), then the rolled ones already in append order. Java closes all
    /// writers in `performRewrite`'s `finally` (L273-275).
    async fn finish(mut self) -> Result<Vec<ManifestFile>> {
        // Sort the still-open keys for a deterministic manifest ordering across runs (the per-attempt
        // HashMap iteration order is otherwise nondeterministic; the live set is identical regardless).
        let mut open_keys: Vec<(String, i32)> = self.open_writers.keys().cloned().collect();
        open_keys.sort();

        let mut result = std::mem::take(&mut self.finished);
        for key in open_keys {
            if let Some(writer) = self.open_writers.remove(&key) {
                let manifest = writer.write_manifest_file().await?;
                result.push(manifest);
            }
        }
        Ok(result)
    }
}

/// validateFilesCounts (Java `BaseRewriteManifests.validateFilesCounts`, L300-329): the total active-file
/// count (added + existing) across the CREATED manifests (new cluster manifests + explicitly-added
/// manifests) must equal the count across the REPLACED manifests (rewritten + explicitly-deleted). A `None`
/// count field on any manifest errors ("Missing file counts in {path}"); a mismatch errors with Java's exact
/// message ("Replaced and created manifests must have the same number of active files: {new} (new), {old} (old)").
fn validate_files_counts(
    new_manifests: &[ManifestFile],
    added_manifests: &[ManifestFile],
    rewritten_manifests: &[ManifestFile],
    deleted_manifests: &[ManifestFile],
) -> Result<()> {
    let created_count = active_files_count(new_manifests.iter().chain(added_manifests.iter()))?;
    let replaced_count =
        active_files_count(rewritten_manifests.iter().chain(deleted_manifests.iter()))?;

    if created_count != replaced_count {
        return Err(Error::new(
            ErrorKind::DataInvalid,
            format!(
                "Replaced and created manifests must have the same number of active files: {created_count} (new), {replaced_count} (old)"
            ),
        ));
    }

    Ok(())
}

/// Sum the active-file count (added + existing) over `manifests` (Java `activeFilesCount`, L316-329). A
/// `None` `added_files_count` / `existing_files_count` is a hard error (Java
/// `Preconditions.checkNotNull(..., "Missing file counts in %s", manifest.path())`).
fn active_files_count<'a>(manifests: impl Iterator<Item = &'a ManifestFile>) -> Result<u64> {
    let mut total: u64 = 0;
    for manifest in manifests {
        let added = manifest.added_files_count.ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("Missing file counts in {}", manifest.manifest_path),
            )
        })?;
        let existing = manifest.existing_files_count.ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("Missing file counts in {}", manifest.manifest_path),
            )
        })?;
        total += u64::from(added) + u64::from(existing);
    }
    Ok(total)
}

/// The estimated per-entry byte size of `manifest_file` — its `manifest_length` divided by its total entry
/// count (added + existing + deleted). Used by the size-rolling proxy (see [`ClusterWriters`]). Defaults to
/// 0 when the manifest reports no entries (it then contributes nothing to the estimate, so its writer never
/// rolls on it alone — harmless, since an empty manifest yields no appended entries anyway).
fn estimate_per_entry_size(manifest_file: &ManifestFile) -> u64 {
    let entry_count = u64::from(manifest_file.added_files_count.unwrap_or(0))
        + u64::from(manifest_file.existing_files_count.unwrap_or(0))
        + u64::from(manifest_file.deleted_files_count.unwrap_or(0));
    if entry_count == 0 {
        return 0;
    }
    (manifest_file.manifest_length.max(0) as u64) / entry_count
}

/// The [`SnapshotProduceOperation`] for [`RewriteManifestsAction`].
///
/// Records [`Operation::Replace`] (Java `BaseRewriteManifests.operation()` = `DataOperations.REPLACE`),
/// removes no data files, and returns the PRECOMPUTED final manifest list (new cluster manifests + added +
/// kept) as the existing-manifest set. The producer's `manifest_file` feeds this straight through
/// [`DefaultManifestProcess`] (no per-manifest filtering — `delete_files` is empty), so the snapshot's
/// manifest list IS the precomputed list.
struct RewriteManifestsOperation {
    existing_manifests: Vec<ManifestFile>,
}

impl SnapshotProduceOperation for RewriteManifestsOperation {
    fn operation(&self) -> Operation {
        // Java `BaseRewriteManifests.operation()` returns `DataOperations.REPLACE` (L87-89).
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
        // A manifest rewrite removes no data files (the live set is unchanged), so there is nothing for
        // the producer's by-path `process_deletes` to do.
        Ok(vec![])
    }

    async fn existing_manifest(
        &self,
        _snapshot_produce: &SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestFile>> {
        // The precomputed final list (new manifests first, then added, then kept — Java L189-194).
        Ok(self.existing_manifests.clone())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::RewriteManifestsAction;
    use crate::memory::tests::new_memory_catalog;
    use crate::spec::{
        DataContentType, DataFile, DataFileBuilder, DataFileFormat, Literal, ManifestContentType,
        ManifestEntry, ManifestFile, ManifestStatus, Operation, Struct, UNASSIGNED_SEQUENCE_NUMBER,
        UNASSIGNED_SNAPSHOT_ID,
    };
    use crate::table::Table;
    use crate::transaction::tests::{
        make_v2_minimal_table_in_catalog, make_v3_minimal_table_in_catalog,
    };
    use crate::transaction::{ApplyTransactionAction, Transaction};
    use crate::{Catalog, ErrorKind};

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

    /// Fast-append `files` in one commit and return the updated table.
    async fn append_files(catalog: &impl Catalog, table: &Table, files: Vec<DataFile>) -> Table {
        let tx = Transaction::new(table);
        let action = tx.fast_append().add_data_files(files);
        let tx = action.apply(tx).unwrap();
        tx.commit(catalog).await.unwrap()
    }

    /// The set of live (Added or Existing) data-file paths across the current snapshot (what a scan reads).
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

    /// Set a table property through the catalog so the rewrite reads it from the refreshed base.
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

    // 1. THE PROVENANCE PIN (docs/testing.md write-action pin #2): three separate fast-append commits
    // (3 manifests) → cluster by a CONSTANT key → ONE data manifest whose every entry is Existing AND keeps
    // its ORIGINAL adding snapshot id + data/file sequence numbers. Re-stamping (add_entry) would replace
    // them with the rewrite snapshot — the #1 corruption class.
    #[tokio::test]
    async fn test_rewrite_manifests_preserves_per_entry_provenance_into_one_manifest() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        // Three separate commits ⇒ three manifests, each with a distinct adding snapshot + seq.
        let table = append_files(&catalog, &table, vec![data_file("test/a.parquet", 0)]).await;
        let sa = table.metadata().current_snapshot().unwrap().snapshot_id();
        let (_, _, a_seq, a_fseq) = live_entry(&table, "test/a.parquet").await;

        let table = append_files(&catalog, &table, vec![data_file("test/b.parquet", 0)]).await;
        let sb = table.metadata().current_snapshot().unwrap().snapshot_id();
        let (_, _, b_seq, b_fseq) = live_entry(&table, "test/b.parquet").await;

        let table = append_files(&catalog, &table, vec![data_file("test/c.parquet", 0)]).await;
        let sc = table.metadata().current_snapshot().unwrap().snapshot_id();
        let (_, _, c_seq, c_fseq) = live_entry(&table, "test/c.parquet").await;

        assert_eq!(
            current_manifests(&table).await.len(),
            3,
            "three source manifests"
        );

        // Cluster everything to a constant key ⇒ ONE output manifest.
        let tx = Transaction::new(&table);
        let action = tx.rewrite_manifests().cluster_by(|_| "all".to_string());
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        let manifests = current_manifests(&table).await;
        assert_eq!(
            manifests.len(),
            1,
            "all entries clustered into one manifest"
        );
        assert_eq!(
            manifests[0].content,
            ManifestContentType::Data,
            "the clustered manifest is a data manifest"
        );

        // Each entry: status Existing + ORIGINAL provenance preserved (NOT re-stamped to the rewrite snapshot).
        for (path, src_snap, src_seq, src_fseq) in [
            ("test/a.parquet", sa, a_seq, a_fseq),
            ("test/b.parquet", sb, b_seq, b_fseq),
            ("test/c.parquet", sc, c_seq, c_fseq),
        ] {
            let (status, snap, seq, fseq) = live_entry(&table, path).await;
            assert_eq!(status, ManifestStatus::Existing, "{path} becomes Existing");
            assert_eq!(
                snap,
                Some(src_snap),
                "{path} keeps its ORIGINAL adding snapshot id"
            );
            assert_eq!(
                seq, src_seq,
                "{path} keeps its ORIGINAL data sequence number"
            );
            assert_eq!(
                fseq, src_fseq,
                "{path} keeps its ORIGINAL file sequence number"
            );
        }
    }

    // 1b. THE TWO-LEVEL-INHERITANCE PIN (point 1, second half): the entries in the REWRITTEN manifest
    // must carry their ORIGINAL data/file sequence numbers EXPLICITLY ON DISK — not null. If the rewrite
    // wrote null on-disk seqs, re-loading the rewritten manifest under the NEW (higher-seq) snapshot's
    // manifest-list entry would RE-INHERIT the new higher seq (entry.rs `inherit_data`), silently lifting
    // every re-clustered data entry's data_seq above any older position delete ⇒ deleted rows resurrect.
    // We read the rewritten manifest's RAW avro bytes (bypassing `load_manifest`'s inheritance) and assert
    // the on-disk seqs equal the originals, proving the rewrite materialized provenance, not deferred it.
    #[tokio::test]
    async fn test_rewritten_manifest_stores_original_seqs_explicitly_on_disk() {
        use crate::spec::Manifest;

        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        // Two separate commits ⇒ two source manifests, each entry inheriting a distinct seq at read time.
        let table = append_files(&catalog, &table, vec![data_file("test/a.parquet", 0)]).await;
        let (_, _, a_seq, a_fseq) = live_entry(&table, "test/a.parquet").await;
        let table = append_files(&catalog, &table, vec![data_file("test/b.parquet", 0)]).await;
        let (_, _, b_seq, b_fseq) = live_entry(&table, "test/b.parquet").await;
        // The originals must be real (non-null) seqs for this assertion to be meaningful.
        assert!(a_seq.is_some() && a_fseq.is_some() && b_seq.is_some() && b_fseq.is_some());

        // Cluster both into ONE rewritten manifest carried by a NEW (strictly higher-seq) snapshot.
        let tx = Transaction::new(&table);
        let action = tx.rewrite_manifests().cluster_by(|_| "all".to_string());
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        let manifests = current_manifests(&table).await;
        assert_eq!(
            manifests.len(),
            1,
            "all entries clustered into one manifest"
        );
        let new_manifest_list_seq = manifests[0].sequence_number;

        // Read the RAW avro bytes of the rewritten manifest — `try_from_avro_bytes` does NOT run
        // inheritance, so we see exactly what was written to disk.
        let bytes = table
            .file_io()
            .new_input(&manifests[0].manifest_path)
            .unwrap()
            .read()
            .await
            .unwrap();
        let (_, raw_entries) = Manifest::try_from_avro_bytes(&bytes).unwrap();
        assert_eq!(
            raw_entries.len(),
            2,
            "both live entries in the rewritten manifest"
        );

        for entry in &raw_entries {
            let (expect_seq, expect_fseq) = if entry.file_path() == "test/a.parquet" {
                (a_seq, a_fseq)
            } else {
                (b_seq, b_fseq)
            };
            // The data sequence number is stored EXPLICITLY (Some) and equals the original — NOT null
            // (which would re-inherit) and NOT the new manifest-list seq.
            assert_eq!(
                entry.sequence_number(),
                expect_seq,
                "{}: original data seq stored explicitly on disk",
                entry.file_path()
            );
            assert_eq!(
                entry.file_sequence_number,
                expect_fseq,
                "{}: original file seq stored explicitly on disk",
                entry.file_path()
            );
            assert_ne!(
                entry.sequence_number(),
                Some(new_manifest_list_seq),
                "{}: data seq must NOT be the new rewrite snapshot's seq (that is the resurrection bug)",
                entry.file_path()
            );
        }
    }

    // 2. Live-set unchanged + operation is Replace.
    #[tokio::test]
    async fn test_rewrite_manifests_keeps_live_set_and_records_replace() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let table = append_files(&catalog, &table, vec![data_file("test/a.parquet", 0)]).await;
        let table = append_files(&catalog, &table, vec![data_file("test/b.parquet", 0)]).await;
        let before = live_file_paths(&table).await;

        let tx = Transaction::new(&table);
        let action = tx.rewrite_manifests().cluster_by(|_| "k".to_string());
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        assert_eq!(
            table
                .metadata()
                .current_snapshot()
                .unwrap()
                .summary()
                .operation,
            Operation::Replace
        );
        assert_eq!(
            live_file_paths(&table).await,
            before,
            "live set is identical before/after"
        );
    }

    // 3. Summary parity: exact manifests-created/-kept/-replaced/entries-processed; no partition-summaries.
    #[tokio::test]
    async fn test_rewrite_manifests_summary_counts_exact_and_no_partition_summaries() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        // Two commits ⇒ two source data manifests, two entries.
        let table = append_files(&catalog, &table, vec![data_file("test/a.parquet", 0)]).await;
        let table = append_files(&catalog, &table, vec![data_file("test/b.parquet", 0)]).await;

        let tx = Transaction::new(&table);
        let action = tx.rewrite_manifests().cluster_by(|_| "k".to_string());
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        // 2 rewritten → 1 new manifest; 0 kept; 2 entries processed.
        assert_eq!(
            summary_prop(&table, "manifests-created").as_deref(),
            Some("1")
        );
        assert_eq!(summary_prop(&table, "manifests-kept").as_deref(), Some("0"));
        assert_eq!(
            summary_prop(&table, "manifests-replaced").as_deref(),
            Some("2")
        );
        assert_eq!(
            summary_prop(&table, "entries-processed").as_deref(),
            Some("2")
        );

        // No partition-summaries: data did not change, so no per-partition (`partitions.*`) summaries are
        // emitted and the `partition-summaries-included` marker is absent (Java sets the partition-summary
        // limit to 0). (The producer's summary collector still records `changed-partition-count=0`, which is
        // faithful to Java's `SnapshotSummary` collector run with no tracked partition changes — it is the
        // per-partition summary BLOCK that the rewrite must not emit.)
        let summary = table
            .metadata()
            .current_snapshot()
            .unwrap()
            .summary()
            .clone();
        assert!(
            !summary
                .additional_properties
                .keys()
                .any(|key| key.starts_with("partitions.")),
            "a manifest rewrite emits no per-partition summaries (partitions.* keys)"
        );
        assert!(
            !summary
                .additional_properties
                .contains_key("partition-summaries-included"),
            "a manifest rewrite emits no partition-summaries-included marker"
        );
    }

    // 3b. COMPUTED-COUNT PRECEDENCE (point 7): Java `BaseRewriteManifests.summary()` puts user `set()`
    // values into the same `SnapshotSummary.Builder`, then OVERWRITES the four count keys with the
    // computed values (L98-112, last-write-wins). Rust must match: a user `set("manifests-created", ...)`
    // is taken at producer construction, then `extend_snapshot_properties` injects the computed count,
    // and `HashMap::extend` overwrites it. The computed count MUST win; a user-supplied value MUST NOT
    // leak into the count key. (A non-count user property is preserved untouched.)
    #[tokio::test]
    async fn test_user_set_does_not_override_computed_manifest_counts() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let table = append_files(&catalog, &table, vec![data_file("test/a.parquet", 0)]).await;
        let table = append_files(&catalog, &table, vec![data_file("test/b.parquet", 0)]).await;

        let tx = Transaction::new(&table);
        let action = tx
            .rewrite_manifests()
            .cluster_by(|_| "k".to_string())
            // A bogus user-supplied value for a COMPUTED count key — must be overwritten.
            .set("manifests-created", "999")
            // A non-count user property — must survive untouched.
            .set("my-custom-key", "kept");
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        // The computed count (2 rewritten → 1 new manifest) wins, NOT the user's "999".
        assert_eq!(
            summary_prop(&table, "manifests-created").as_deref(),
            Some("1"),
            "the computed manifests-created count overrides the user-set value (Java last-write-wins)"
        );
        // The non-count user property is preserved.
        assert_eq!(
            summary_prop(&table, "my-custom-key").as_deref(),
            Some("kept"),
            "a non-count user property set() survives into the summary"
        );
    }

    // 4. rewrite_if scoping: a predicate-false manifest is carried byte-identical (same path + snapshot id).
    #[tokio::test]
    async fn test_rewrite_if_keeps_predicate_false_manifest_byte_identical() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        // A in its own manifest, B in its own manifest.
        let table = append_files(&catalog, &table, vec![data_file("test/a.parquet", 0)]).await;
        let table = append_files(&catalog, &table, vec![data_file("test/b.parquet", 0)]).await;

        // Identify A's source manifest by which manifest contains a.parquet.
        let manifests = current_manifests(&table).await;
        let mut a_manifest_path = None;
        for manifest_file in &manifests {
            let manifest = manifest_file.load_manifest(table.file_io()).await.unwrap();
            if manifest
                .entries()
                .iter()
                .any(|e| e.file_path() == "test/a.parquet")
            {
                a_manifest_path = Some(manifest_file.manifest_path.clone());
            }
        }
        let a_manifest_path = a_manifest_path.unwrap();
        let a_added_snapshot_id = manifests
            .iter()
            .find(|m| m.manifest_path == a_manifest_path)
            .unwrap()
            .added_snapshot_id;

        // Rewrite only the manifest that does NOT contain a.parquet (keep A's manifest as-is).
        let keep_path = a_manifest_path.clone();
        let tx = Transaction::new(&table);
        let action = tx
            .rewrite_manifests()
            .cluster_by(|_| "k".to_string())
            .rewrite_if(move |m| m.manifest_path != keep_path);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        // A's manifest is still present byte-identical (same path + same added_snapshot_id — not rewritten).
        let after = current_manifests(&table).await;
        let carried = after.iter().find(|m| m.manifest_path == a_manifest_path);
        assert!(
            carried.is_some(),
            "the predicate-false manifest is carried forward unchanged"
        );
        assert_eq!(
            carried.unwrap().added_snapshot_id,
            a_added_snapshot_id,
            "the carried manifest keeps its original added_snapshot_id (not rewritten)"
        );
        // The live set is still {A, B}.
        assert_eq!(
            live_file_paths(&table).await,
            HashSet::from(["test/a.parquet".to_string(), "test/b.parquet".to_string()])
        );
    }

    // 4b. MULTI-SPEC CLUSTERING AXIS (point 2): Java keys writers by `Pair.of(key, partitionSpecId)` and
    // writes each with `specsById.get(partitionSpecId)` (BaseRewriteManifests L340-343). With a CONSTANT
    // cluster key spanning entries from TWO different partition spec ids, the rewrite must produce TWO
    // output manifests — one per spec id — each carrying its own source spec id and preserved provenance,
    // never cross-merging specs into one writer (which would write corrupt partition tuples). The live set
    // is unchanged. A `(key, spec_id)`-keyed writer map is what makes this correct; a key-only map fails.
    #[tokio::test]
    async fn test_cluster_by_constant_across_two_specs_yields_one_manifest_per_spec() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        // Append a file under the original spec 0 (identity(x), 1-field partition).
        let table = append_files(&catalog, &table, vec![data_file("test/spec0.parquet", 0)]).await;

        // Evolve the partition spec: add identity(y) ⇒ a NEW default spec (spec 1, identity(x)+identity(y)).
        let tx = Transaction::new(&table);
        let action = tx.update_partition_spec().add_field("y");
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();
        let new_spec_id = table.metadata().default_partition_spec_id();
        assert_ne!(
            new_spec_id, 0,
            "spec evolution must create a new default spec"
        );

        // Append a file under the NEW default spec (2-field partition (x, y), spec id = new_spec_id).
        let spec1_file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("test/spec1.parquet".to_string())
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
        let table = append_files(&catalog, &table, vec![spec1_file]).await;

        // Two source data manifests, one per spec id.
        let before = current_manifests(&table).await;
        assert_eq!(before.len(), 2, "two source manifests (one per spec)");
        let before_specs: HashSet<i32> = before.iter().map(|m| m.partition_spec_id).collect();
        assert_eq!(before_specs, HashSet::from([0, new_spec_id]));

        // Capture original provenance of each entry to assert it survives the rewrite.
        let (_, spec0_snap, spec0_seq, spec0_fseq) = live_entry(&table, "test/spec0.parquet").await;
        let (_, spec1_snap, spec1_seq, spec1_fseq) = live_entry(&table, "test/spec1.parquet").await;

        // Cluster by a CONSTANT key — the ONLY thing distinguishing the two writers is the spec id.
        let tx = Transaction::new(&table);
        let action = tx.rewrite_manifests().cluster_by(|_| "same".to_string());
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        // TWO output manifests, one per spec id (a key-only writer map would merge into ONE here).
        let after = current_manifests(&table).await;
        let data_manifests: Vec<&ManifestFile> = after
            .iter()
            .filter(|m| m.content == ManifestContentType::Data)
            .collect();
        assert_eq!(
            data_manifests.len(),
            2,
            "a constant key over two specs must still yield one manifest per spec id, got {}",
            data_manifests.len()
        );
        let after_specs: HashSet<i32> =
            data_manifests.iter().map(|m| m.partition_spec_id).collect();
        assert_eq!(
            after_specs,
            HashSet::from([0, new_spec_id]),
            "each output manifest carries one of the two source spec ids"
        );

        // Live set unchanged, and each entry kept its ORIGINAL spec id + provenance.
        assert_eq!(
            live_file_paths(&table).await,
            HashSet::from([
                "test/spec0.parquet".to_string(),
                "test/spec1.parquet".to_string(),
            ])
        );
        for (path, src_snap, src_seq, src_fseq, src_spec) in [
            ("test/spec0.parquet", spec0_snap, spec0_seq, spec0_fseq, 0),
            (
                "test/spec1.parquet",
                spec1_snap,
                spec1_seq,
                spec1_fseq,
                new_spec_id,
            ),
        ] {
            let (status, snap, seq, fseq) = live_entry(&table, path).await;
            assert_eq!(status, ManifestStatus::Existing, "{path} becomes Existing");
            assert_eq!(snap, src_snap, "{path} keeps its original snapshot id");
            assert_eq!(seq, src_seq, "{path} keeps its original data seq");
            assert_eq!(fseq, src_fseq, "{path} keeps its original file seq");
            // The entry lives in the manifest carrying its own source spec id.
            let owning = data_manifests
                .iter()
                .find(|m| m.partition_spec_id == src_spec)
                .expect("a manifest with the entry's source spec id exists");
            let loaded = owning.load_manifest(table.file_io()).await.unwrap();
            assert!(
                loaded.entries().iter().any(|e| e.file_path() == path),
                "{path} lives in the manifest for spec {src_spec}"
            );
        }
    }

    // 5. Delete manifests are immune: a merge-on-read table's Deletes manifest is KEPT, data re-clustered,
    // and a post-rewrite scan still applies the delete (live rows unchanged).
    #[tokio::test]
    async fn test_rewrite_manifests_keeps_delete_manifest_and_delete_still_applies() {
        let catalog = new_memory_catalog().await;
        let table = make_v2_minimal_table_in_catalog(&catalog).await;

        // Write a real data file (5 rows, partition x=0) and append it.
        let real_data = write_real_data_file(&table, "rows.parquet", 0, &[
            (0, 10),
            (0, 20),
            (0, 30),
            (0, 40),
            (0, 50),
        ])
        .await;
        let data_path = real_data.file_path().to_string();
        let table = append_files(&catalog, &table, vec![real_data]).await;

        // Append a second data file so there are two DATA manifests to re-cluster.
        let real_data2 =
            write_real_data_file(&table, "rows2.parquet", 0, &[(0, 60), (0, 70)]).await;
        let table = append_files(&catalog, &table, vec![real_data2]).await;

        // row_delta a position delete dropping positions 1 and 3 of rows.parquet (y=20, y=40).
        let delete_file = write_real_position_delete(&table, 0, &[
            (data_path.clone(), 1),
            (data_path.clone(), 3),
        ])
        .await;
        let tx = Transaction::new(&table);
        let action = tx.row_delta().add_deletes(vec![delete_file]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        // Sanity: a delete-content manifest exists, and the scan already drops y=20,40.
        let before_manifests = current_manifests(&table).await;
        assert!(
            before_manifests
                .iter()
                .any(|m| m.content == ManifestContentType::Deletes),
            "the table carries a delete manifest"
        );
        assert_eq!(
            scan_y_values(&table).await,
            HashSet::from([10, 30, 50, 60, 70]),
            "before the rewrite, the delete drops y=20 and y=40"
        );

        // Cluster-rewrite the data manifests.
        let tx = Transaction::new(&table);
        let action = tx.rewrite_manifests().cluster_by(|_| "all".to_string());
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        // The Deletes-content manifest is kept; the data manifests were re-clustered.
        let after_manifests = current_manifests(&table).await;
        let delete_manifests: Vec<&ManifestFile> = after_manifests
            .iter()
            .filter(|m| m.content == ManifestContentType::Deletes)
            .collect();
        assert_eq!(
            delete_manifests.len(),
            1,
            "exactly one delete manifest, carried forward"
        );
        // It is byte-identical to the pre-rewrite delete manifest (same path).
        let before_delete_path = before_manifests
            .iter()
            .find(|m| m.content == ManifestContentType::Deletes)
            .unwrap()
            .manifest_path
            .clone();
        assert_eq!(
            delete_manifests[0].manifest_path, before_delete_path,
            "the delete manifest is carried forward byte-identical (same path, not rewritten)"
        );

        // The post-rewrite scan STILL drops y=20 and y=40 — provenance preserved ⇒ delete still applies.
        assert_eq!(
            scan_y_values(&table).await,
            HashSet::from([10, 30, 50, 60, 70]),
            "after the rewrite, the delete still applies (live rows unchanged)"
        );
    }

    // 6. validateDeletedManifests fires: delete_manifest of a manifest absent from the current snapshot →
    // exact Java message.
    #[tokio::test]
    async fn test_delete_manifest_not_in_current_snapshot_errors() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let table = append_files(&catalog, &table, vec![data_file("test/a.parquet", 0)]).await;
        let snapshot_id = table.metadata().current_snapshot().unwrap().snapshot_id();

        // A bogus ManifestFile whose path is not in the current snapshot.
        let bogus = bogus_manifest_file("s3://nowhere/does-not-exist-m0.avro", 1, 0);

        let tx = Transaction::new(&table);
        let action = tx.rewrite_manifests().delete_manifest(bogus);
        let tx = action.apply(tx).unwrap();
        let error = tx
            .commit(&catalog)
            .await
            .expect_err("deleting an absent manifest must error");
        assert_eq!(error.kind(), ErrorKind::DataInvalid);
        let expected = format!(
            "Deleted manifest s3://nowhere/does-not-exist-m0.avro could not be found in the latest snapshot {snapshot_id}"
        );
        assert_eq!(
            error.message(),
            expected,
            "exact Java validateDeletedManifests message"
        );
    }

    // 7. Count conservation fires: delete_manifest without a balancing add → exact message; table unchanged.
    #[tokio::test]
    async fn test_delete_manifest_without_balancing_add_errors_on_counts() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let table = append_files(&catalog, &table, vec![data_file("test/a.parquet", 0)]).await;

        // The real manifest in the current snapshot (it has 1 active file).
        let manifests = current_manifests(&table).await;
        let real_manifest = manifests[0].clone();

        let tx = Transaction::new(&table);
        let action = tx.rewrite_manifests().delete_manifest(real_manifest);
        let tx = action.apply(tx).unwrap();
        let error = tx
            .commit(&catalog)
            .await
            .expect_err("deleting without re-adding the files must fail the count check");
        assert_eq!(error.kind(), ErrorKind::DataInvalid);
        assert_eq!(
            error.message(),
            "Replaced and created manifests must have the same number of active files: 0 (new), 1 (old)"
        );

        // The table is unchanged.
        let reloaded = catalog.load_table(table.identifier()).await.unwrap();
        assert_eq!(
            live_file_paths(&reloaded).await,
            HashSet::from(["test/a.parquet".to_string()])
        );
    }

    // AUDIT pin (2026-06-10): Java's `deletedManifests` is a SET with path-based equality
    // (`Sets.newHashSet` + `GenericManifestFile` path equality) — passing the SAME manifest to
    // `delete_manifest` twice is a no-op and must count it ONCE on the replaced side of the
    // conservation check. Risk pinned: a Vec-backed deleted set double-counts the duplicate and
    // spuriously rejects with "0 (new), 2 (old)" where Java reports 1.
    #[tokio::test]
    async fn test_duplicate_delete_manifest_counts_once_java_set_semantics() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let table = append_files(&catalog, &table, vec![data_file("test/a.parquet", 0)]).await;

        let manifests = current_manifests(&table).await;
        let real_manifest = manifests[0].clone();

        let tx = Transaction::new(&table);
        let action = tx
            .rewrite_manifests()
            .delete_manifest(real_manifest.clone())
            .delete_manifest(real_manifest);
        let tx = action.apply(tx).unwrap();
        let error = tx
            .commit(&catalog)
            .await
            .expect_err("still fails conservation (nothing re-added), but with the DEDUPED count");
        assert_eq!(error.kind(), ErrorKind::DataInvalid);
        assert_eq!(
            error.message(),
            "Replaced and created manifests must have the same number of active files: 0 (new), 1 (old)",
            "the duplicate delete_manifest must count once (Java Set semantics), not twice"
        );
    }

    // 8. add_manifest preconditions: each of the four rejections.
    #[tokio::test]
    async fn test_add_manifest_preconditions_each_rejection() {
        // `add_manifest` returns Result<Self, Error>; Self has no Debug, so extract the error by match.
        fn add_manifest_error(manifest: ManifestFile) -> crate::Error {
            match RewriteManifestsAction::new().add_manifest(manifest) {
                Ok(_) => panic!("the precondition should have rejected this manifest"),
                Err(error) => error,
            }
        }

        // added files present.
        let with_added = bogus_manifest_file("s3://x/m-added.avro", 1, 0);
        let error = add_manifest_error(with_added);
        assert_eq!(error.kind(), ErrorKind::DataInvalid);
        assert_eq!(error.message(), "Cannot add manifest with added files");

        // deleted files present (no added, but deleted > 0).
        let mut with_deleted = bogus_manifest_file("s3://x/m-deleted.avro", 0, 0);
        with_deleted.added_files_count = Some(0);
        with_deleted.deleted_files_count = Some(1);
        let error = add_manifest_error(with_deleted);
        assert_eq!(error.message(), "Cannot add manifest with deleted files");

        // assigned snapshot id (no added/deleted files, but added_snapshot_id != -1).
        let mut with_snapshot = bogus_manifest_file("s3://x/m-snap.avro", 0, 0);
        with_snapshot.added_files_count = Some(0);
        with_snapshot.deleted_files_count = Some(0);
        with_snapshot.added_snapshot_id = 12345;
        let error = add_manifest_error(with_snapshot);
        assert_eq!(
            error.message(),
            "Snapshot id must be assigned during commit"
        );

        // assigned sequence number (no added/deleted files, snapshot unassigned, but sequence_number != -1).
        let mut with_seq = bogus_manifest_file("s3://x/m-seq.avro", 0, 0);
        with_seq.added_files_count = Some(0);
        with_seq.deleted_files_count = Some(0);
        with_seq.added_snapshot_id = UNASSIGNED_SNAPSHOT_ID;
        with_seq.sequence_number = 7;
        let error = add_manifest_error(with_seq);
        assert_eq!(error.message(), "Sequence must be assigned during commit");
    }

    // 9. add/delete replacement round-trip: externally write a manifest of Existing entries carrying the same
    // active files as a deleted manifest → commits; scan unchanged; the added manifest's list entry gets the
    // NEW snapshot's added_snapshot_id + sequence number.
    #[tokio::test]
    async fn test_add_delete_manifest_round_trip() {
        use crate::spec::ManifestWriterBuilder;

        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let table = append_files(&catalog, &table, vec![data_file("test/a.parquet", 0)]).await;

        let manifests = current_manifests(&table).await;
        let old_manifest = manifests[0].clone();
        // Read its single live entry; we'll rewrite it into our own manifest as an Existing entry.
        let loaded = old_manifest.load_manifest(table.file_io()).await.unwrap();
        let live: Vec<ManifestEntry> = loaded
            .entries()
            .iter()
            .filter(|e| e.is_alive())
            .map(|e| e.as_ref().clone())
            .collect();
        assert_eq!(live.len(), 1);
        let (_, src_snap, src_seq, src_fseq) = live_entry(&table, "test/a.parquet").await;

        // Externally write a NEW manifest containing the same active file as an Existing entry, with the
        // snapshot id UNASSIGNED on the manifest (it will be stamped during commit).
        let metadata = table.metadata();
        let manifest_path = format!("{}/metadata/external-add-m0.avro", metadata.location());
        let output = table.file_io().new_output(&manifest_path).unwrap();
        let mut writer = ManifestWriterBuilder::new(
            output,
            None, // unassigned snapshot id ⇒ added_snapshot_id = UNASSIGNED_SNAPSHOT_ID
            None,
            metadata.current_schema().clone(),
            metadata.default_partition_spec().as_ref().clone(),
        )
        .build_v3_data();
        writer.add_existing_entry(live[0].clone()).unwrap();
        let external_manifest = writer.write_manifest_file().await.unwrap();
        assert_eq!(external_manifest.added_snapshot_id, UNASSIGNED_SNAPSHOT_ID);
        assert_eq!(
            external_manifest.sequence_number,
            UNASSIGNED_SEQUENCE_NUMBER
        );

        // Replace: delete the old manifest, add ours.
        let tx = Transaction::new(&table);
        let action = tx
            .rewrite_manifests()
            .delete_manifest(old_manifest.clone())
            .add_manifest(external_manifest)
            .unwrap();
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();
        let new_snapshot_id = table.metadata().current_snapshot().unwrap().snapshot_id();

        // The live set is unchanged, the old manifest is gone, and a.parquet keeps its ORIGINAL provenance.
        assert_eq!(
            live_file_paths(&table).await,
            HashSet::from(["test/a.parquet".to_string()])
        );
        let after = current_manifests(&table).await;
        assert!(
            !after
                .iter()
                .any(|m| m.manifest_path == old_manifest.manifest_path),
            "the deleted manifest is gone from the new snapshot"
        );
        // The added manifest's list entry carries the NEW snapshot id + an assigned sequence number.
        let added_entry = after
            .iter()
            .find(|m| m.manifest_path.ends_with("external-add-m0.avro"))
            .expect("the added manifest is in the new snapshot");
        assert_eq!(
            added_entry.added_snapshot_id, new_snapshot_id,
            "the added manifest is stamped with the NEW snapshot id (Java withSnapshotId)"
        );
        assert_ne!(
            added_entry.sequence_number, UNASSIGNED_SEQUENCE_NUMBER,
            "the manifest-list writer assigned the added manifest a real sequence number"
        );
        // The entry's own provenance is unchanged (Existing entry keeps source snapshot + seqs).
        let (status, snap, seq, fseq) = live_entry(&table, "test/a.parquet").await;
        assert_eq!(status, ManifestStatus::Existing);
        assert_eq!(
            snap, src_snap,
            "the carried entry keeps its ORIGINAL adding snapshot id"
        );
        assert_eq!(
            seq, src_seq,
            "the carried entry keeps its ORIGINAL data seq"
        );
        assert_eq!(
            fseq, src_fseq,
            "the carried entry keeps its ORIGINAL file seq"
        );
    }

    // 10. Size rolling: a tiny target-size on the table makes one cluster key yield MULTIPLE output manifests.
    #[tokio::test]
    async fn test_tiny_target_size_rolls_into_multiple_manifests() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        // Three separate commits ⇒ three source manifests, each with a nonzero manifest_length.
        let table = append_files(&catalog, &table, vec![data_file("test/a.parquet", 0)]).await;
        let table = append_files(&catalog, &table, vec![data_file("test/b.parquet", 0)]).await;
        let table = append_files(&catalog, &table, vec![data_file("test/c.parquet", 0)]).await;

        // Set the target size to 1 byte: every appended entry's estimate (a full manifest_length, since each
        // source manifest has one entry) reaches the threshold ⇒ the writer rolls after each entry.
        let table =
            set_table_property(&catalog, &table, "commit.manifest.target-size-bytes", "1").await;

        let tx = Transaction::new(&table);
        let action = tx.rewrite_manifests().cluster_by(|_| "all".to_string());
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        // One cluster key, but the tiny target rolls into multiple data manifests.
        let data_manifest_count = current_manifests(&table)
            .await
            .iter()
            .filter(|m| m.content == ManifestContentType::Data)
            .count();
        assert!(
            data_manifest_count > 1,
            "a 1-byte target must roll the single cluster into multiple manifests, got {data_manifest_count}"
        );
        // The live set is still {A, B, C}: rolling never loses an entry.
        assert_eq!(
            live_file_paths(&table).await,
            HashSet::from([
                "test/a.parquet".to_string(),
                "test/b.parquet".to_string(),
                "test/c.parquet".to_string(),
            ])
        );
    }

    // 11a. Empty table → clean DataInvalid error.
    #[tokio::test]
    async fn test_rewrite_manifests_on_empty_table_errors() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        let tx = Transaction::new(&table);
        let action = tx.rewrite_manifests().cluster_by(|_| "k".to_string());
        let tx = action.apply(tx).unwrap();
        let error = tx
            .commit(&catalog)
            .await
            .expect_err("a rewrite on a table with no snapshot must error cleanly");
        assert_eq!(error.kind(), ErrorKind::DataInvalid);
        assert!(
            error.message().contains("no current snapshot"),
            "unexpected message: {}",
            error.message()
        );
    }

    // 11b. No-op rewrite (no cluster fn, no add/delete) → commits with all manifests kept (Java allows it).
    #[tokio::test]
    async fn test_rewrite_manifests_no_op_keeps_all_manifests() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let table = append_files(&catalog, &table, vec![data_file("test/a.parquet", 0)]).await;
        let table = append_files(&catalog, &table, vec![data_file("test/b.parquet", 0)]).await;
        let before = live_file_paths(&table).await;
        let before_count = current_manifests(&table).await.len();

        let tx = Transaction::new(&table);
        let action = tx.rewrite_manifests();
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        assert_eq!(
            table
                .metadata()
                .current_snapshot()
                .unwrap()
                .summary()
                .operation,
            Operation::Replace
        );
        assert_eq!(
            live_file_paths(&table).await,
            before,
            "no-op keeps the live set"
        );
        assert_eq!(
            summary_prop(&table, "manifests-kept").as_deref(),
            Some(before_count.to_string().as_str()),
            "all source manifests are kept"
        );
        assert_eq!(
            summary_prop(&table, "manifests-created").as_deref(),
            Some("0")
        );
        assert_eq!(
            summary_prop(&table, "manifests-replaced").as_deref(),
            Some("0")
        );
        assert_eq!(
            summary_prop(&table, "entries-processed").as_deref(),
            Some("0")
        );
    }

    // 12. Cumulative totals (docs/testing.md pin #3): append → append → rewrite_manifests leaves
    // total-data-files / total-records unchanged through the rewrite.
    #[tokio::test]
    async fn test_rewrite_manifests_preserves_cumulative_totals() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let table = append_files(&catalog, &table, vec![data_file("test/a.parquet", 0)]).await;
        let table = append_files(&catalog, &table, vec![data_file("test/b.parquet", 0)]).await;
        let total_files_before = summary_prop(&table, "total-data-files");
        let total_records_before = summary_prop(&table, "total-records");
        assert_eq!(total_files_before.as_deref(), Some("2"));
        assert_eq!(total_records_before.as_deref(), Some("2"));

        let tx = Transaction::new(&table);
        let action = tx.rewrite_manifests().cluster_by(|_| "k".to_string());
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        assert_eq!(
            summary_prop(&table, "total-data-files"),
            total_files_before,
            "a manifest rewrite does not change total-data-files"
        );
        assert_eq!(
            summary_prop(&table, "total-records"),
            total_records_before,
            "a manifest rewrite does not change total-records"
        );
    }

    // 13. V1 add_manifest → FeatureUnsupported.
    #[tokio::test]
    async fn test_add_manifest_on_v1_table_unsupported() {
        use std::sync::Arc;

        use crate::transaction::TransactionAction;
        use crate::transaction::tests::make_v1_table;

        let table = make_v1_table();
        // A valid (precondition-passing) added manifest: no added/deleted files, unassigned snapshot + seq.
        let mut manifest = bogus_manifest_file("s3://x/v1-add-m0.avro", 0, 0);
        manifest.added_files_count = Some(0);
        manifest.existing_files_count = Some(0);
        manifest.deleted_files_count = Some(0);
        manifest.added_snapshot_id = UNASSIGNED_SNAPSHOT_ID;
        manifest.sequence_number = UNASSIGNED_SEQUENCE_NUMBER;

        let action = RewriteManifestsAction::new()
            .add_manifest(manifest)
            .unwrap();
        let error = match Arc::new(action).commit(&table).await {
            Ok(_) => panic!("add_manifest on a V1 table must be FeatureUnsupported"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), ErrorKind::FeatureUnsupported);
        assert!(
            error.message().contains("V1 table is not supported"),
            "unexpected message: {}",
            error.message()
        );
    }

    // -------------------------------------------------------------------------------------------------
    // Helpers that build real data / delete files + scan (mirrors the row_delta.rs fixtures).
    // -------------------------------------------------------------------------------------------------

    /// A bogus [`ManifestFile`] with the given path + (added, deleted) file counts — for precondition /
    /// validateDeletedManifests tests that never load it.
    fn bogus_manifest_file(path: &str, added_files: u32, deleted_files: u32) -> ManifestFile {
        ManifestFile {
            manifest_path: path.to_string(),
            manifest_length: 100,
            partition_spec_id: 0,
            content: ManifestContentType::Data,
            sequence_number: UNASSIGNED_SEQUENCE_NUMBER,
            min_sequence_number: UNASSIGNED_SEQUENCE_NUMBER,
            added_snapshot_id: UNASSIGNED_SNAPSHOT_ID,
            added_files_count: Some(added_files),
            existing_files_count: Some(0),
            deleted_files_count: Some(deleted_files),
            added_rows_count: Some(0),
            existing_rows_count: Some(0),
            deleted_rows_count: Some(0),
            partitions: None,
            key_metadata: None,
            first_row_id: None,
        }
    }

    /// Write a real parquet data file (rows are `(x, y)` with a constant z) under the table location and
    /// return its [`DataFile`] routed to partition `x = part_value`.
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

    /// Write a real position-delete parquet file dropping the given `(data_path, pos)` pairs.
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

    /// Scan the table and collect the `y` column values.
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
}
