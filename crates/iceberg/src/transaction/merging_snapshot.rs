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

//! `MergingSnapshotProducer` — the stateful, cross-retry object that holds only the
//! delta between a delete-class operation and the reused append-capable
//! `SnapshotProducer`.
//!
//! MSP is **owned by the action as a field**, so it lives across all commit retries.
//! It holds only the delta (staging inputs + `data_sequence_number` + the two filter
//! managers + a single `Mutex<MergingCache>` + the plain `commit_uuid`) and delegates
//! all shared write mechanics to a per-attempt `SnapshotProducer` it composes.
//!
//! Two state categories live on MSP:
//! 1. plain staging fields, mutated with `&mut self` during the **build** phase;
//! 2. the `Mutex<MergingCache>`, mutated with `&self` during the **commit** phase.

use std::collections::HashMap;
use std::sync::Mutex;

use uuid::Uuid;

use crate::Result;
use crate::spec::{DataContentType, DataFile, ManifestContentType, ManifestFile};
use crate::table::Table;
use crate::transaction::ActionCommit;
use crate::transaction::manifest_filter::ManifestFilterManager;
use crate::transaction::snapshot::{ManifestProcess, SnapshotProduceOperation, SnapshotProducer};

/// All mutable cross-retry state lives behind a single `Mutex<MergingCache>` — not
/// scattered per-field `OnceCell`/`Mutex`.
///
/// In v1 this holds only the lazily-generated `snapshot_id`; the single-`Mutex` shape
/// is kept deliberately so the deferred filter cache (future work) has a home.
struct MergingCache {
    /// Generated lazily against the base on the first commit attempt
    /// (collision-checked), then reused every attempt. Mirrors Java's lazy
    /// `snapshotId()`. This is the ONLY interior-mutable cross-retry state in v1.
    snapshot_id: Option<i64>,
    // TODO(future): filter_cache: HashMap<String, CachedRewrite>
    //   Deferred cross-retry cache of rewritten manifests keyed by input manifest
    //   path. Kept out of v1; the one-mutex shape is retained so this cache has a
    //   home when filter caching lands.
}

/// The stateful, cross-retry producer that holds **only the delta** between a
/// delete-class operation and the reused `SnapshotProducer`.
///
/// Owned by the action as a field; therefore lives across all retries. It delegates
/// all shared write mechanics to a per-attempt `SnapshotProducer` (see task 6.2 for
/// `commit`). It owns file staging — actions forward `add_file` /
/// `delete_file` into it (mirroring Java `MergingSnapshotProducer.add(DataFile)`
/// / `delete(...)`).
pub(crate) struct MergingSnapshotProducer {
    // ---- plain, immutable identity (set once at construction) ----
    /// Like Java's `final commitUUID`. NOT in the cache; needs no locking.
    commit_uuid: Uuid,

    // ---- plain staging inputs (mutated with &mut during the BUILD phase) ----
    snapshot_properties: HashMap<String, String>,
    added_data_files: Vec<DataFile>,
    added_delete_files: Vec<DataFile>,
    deleted_data_files: Vec<DataFile>,
    deleted_delete_files: Vec<DataFile>,
    /// MSP-owned config; the action's setter threads through here. Mirrors Java
    /// `newDataFilesDataSequenceNumber`.
    data_sequence_number: Option<i64>,

    // ---- filter managers (own their per-manifest rewrite behavior) ----
    data_filter: ManifestFilterManager,
    delete_filter: ManifestFilterManager,

    // ---- ALL mutable cross-retry state behind ONE mutex ----
    /// In v1 this holds only the lazily-generated `snapshot_id`; the single-mutex
    /// shape is kept so the deferred filter cache has a home (see `MergingCache`).
    cache: Mutex<MergingCache>,
}

impl MergingSnapshotProducer {
    /// Construct a fresh MSP with the given stable `commit_uuid`.
    ///
    /// All staging state starts empty and is populated during the build phase via the
    /// `&mut self` mutators. The `snapshot_id` is left unresolved (generated lazily on
    /// the first commit attempt).
    pub(crate) fn new(commit_uuid: Uuid, snapshot_properties: HashMap<String, String>) -> Self {
        Self {
            commit_uuid,
            snapshot_properties,
            added_data_files: Vec::new(),
            added_delete_files: Vec::new(),
            deleted_data_files: Vec::new(),
            deleted_delete_files: Vec::new(),
            data_sequence_number: None,
            data_filter: ManifestFilterManager::default(),
            delete_filter: ManifestFilterManager::default(),
            cache: Mutex::new(MergingCache { snapshot_id: None }),
        }
    }

    /// Stage an added file (routed to data vs delete bucket by content type, as in
    /// iceberg-rust #1606). Mutates plain staging state with `&mut self`; only
    /// reachable in the BUILD phase, before `.apply(tx)` wraps the action in an `Arc`.
    /// Mirrors Java `MergingSnapshotProducer.add(...)`.
    pub(crate) fn add_file(&mut self, file: DataFile) {
        match file.content_type() {
            DataContentType::Data => self.added_data_files.push(file),
            _ => self.added_delete_files.push(file),
        }
    }

    /// Stage a removed file (routed by content type). Mirrors Java
    /// `MergingSnapshotProducer.delete(...)`.
    ///
    /// In addition to recording the removal in the appropriate staging vector, the file is
    /// recorded into the matching [`ManifestFilterManager`] so the filters are fully populated
    /// by the time the operation drives filtering from `existing_manifest`. Data files are
    /// dropped from **data** manifests (`data_filter`); delete files are dropped from **delete**
    /// manifests (`delete_filter`). Recording here (during the `&mut self` build phase) is what
    /// lets `filter_existing_manifests` run against a shared `&self` borrow during commit,
    /// since [`ManifestFilterManager::delete_file`] needs `&mut self`.
    pub(crate) fn delete_file(&mut self, file: DataFile) {
        match file.content_type() {
            DataContentType::Data => {
                self.data_filter.delete_file(file.clone());
                self.deleted_data_files.push(file);
            }
            _ => {
                self.delete_filter.delete_file(file.clone());
                self.deleted_delete_files.push(file);
            }
        }
    }

    /// MSP-owned config; the action's `set_data_sequence_number` threads through here.
    /// Mirrors Java `setNewDataFilesDataSequenceNumber`.
    pub(crate) fn set_data_sequence_number(&mut self, seq: i64) {
        self.data_sequence_number = Some(seq);
    }

    /// Override the stable `commit_uuid`. The action's `set_commit_uuid` threads
    /// through here so the caller-supplied UUID replaces the one generated at
    /// construction. Runs in the BUILD phase (`&mut self`), before commit.
    pub(crate) fn set_commit_uuid(&mut self, commit_uuid: Uuid) {
        self.commit_uuid = commit_uuid;
    }

    /// MSP-owned config; the action's `set_snapshot_properties` threads through here.
    pub(crate) fn set_snapshot_properties(
        &mut self,
        snapshot_properties: HashMap<String, String>,
    ) {
        self.snapshot_properties = snapshot_properties;
    }

    pub(crate) fn data_sequence_number(&self) -> Option<i64> {
        self.data_sequence_number
    }

    pub(crate) fn deleted_data_files(&self) -> &[DataFile] {
        &self.deleted_data_files
    }

    pub(crate) fn deleted_delete_files(&self) -> &[DataFile] {
        &self.deleted_delete_files
    }

    /// Rewrite the carried-forward `manifests` through the MSP's two filter managers.
    ///
    /// Called by a delete-class operation from inside its `existing_manifest`. Carried-forward
    /// manifests are partitioned by content type: **data** manifests go through `data_filter`
    /// (which drops removed data files) and **delete** manifests go through `delete_filter`
    /// (which drops removed delete files). The filter managers were already populated with the
    /// removed files during the build phase (see [`delete_file`](Self::delete_file)),
    /// so this only needs a shared `&self` borrow — [`ManifestFilterManager::filter_manifests`]
    /// itself takes `&self`.
    ///
    /// `base` is the refreshed base table (`sp.table`); it is threaded through to the filter
    /// managers for manifest IO.
    pub(crate) async fn filter_existing_manifests(
        &self,
        sp: &mut SnapshotProducer<'_>,
        base: &Table,
        manifests: Vec<ManifestFile>,
    ) -> Result<Vec<ManifestFile>> {
        let (data_manifests, delete_manifests): (Vec<ManifestFile>, Vec<ManifestFile>) = manifests
            .into_iter()
            .partition(|manifest| manifest.content == ManifestContentType::Data);

        let mut filtered = self
            .data_filter
            .filter_manifests(sp, base, data_manifests)
            .await?;
        let filtered_deletes = self
            .delete_filter
            .filter_manifests(sp, base, delete_manifests)
            .await?;
        filtered.extend(filtered_deletes);
        Ok(filtered)
    }

    /// The DRY validate-then-write entry point. Called once per commit attempt by
    /// the action, against the freshly refreshed `base`. Takes `&self` — the cache
    /// mutates through the interior `Mutex`.
    ///
    /// This is the MIDDLE link in the three-layer delegation chain (each on a
    /// different receiver):
    /// ```text
    ///   TransactionAction::commit      (self: Arc<Self>)
    ///     -> MergingSnapshotProducer::commit   (&self)
    ///       -> SnapshotProducer::commit        (mut self)
    /// ```
    ///
    /// Discipline: the `Mutex` guard is NEVER held across an `.await`. The pattern
    /// is always lock → read/decide → clone out → unlock → async IO.
    ///
    /// The `process` seam (manifest merge / compaction) is supplied by the caller
    /// rather than hardcoded, so the action decides how the manifest set is
    /// post-processed. Append and today's delete-class actions pass
    /// `DefaultManifestProcess` (a no-op), but the seam stays open for merging.
    pub(crate) async fn commit<OP, MP>(
        &self,
        base: &Table,
        operation: OP,
        process: MP,
    ) -> Result<ActionCommit>
    where
        OP: SnapshotProduceOperation,
        MP: ManifestProcess,
    {
        let parent_snapshot_id = base.metadata().current_snapshot_id();

        // 1. Validation ALWAYS runs against the current base. Never cached.
        //    `validate` is the trait method (default no-op; overridden by
        //    delete-class ops to compose the stateless `validation::*` helpers).
        operation.validate(base, parent_snapshot_id).await?;

        // 2. Resolve `snapshot_id`: generated once against the base on the first
        //    attempt (collision-checked), then reused every attempt thereafter.
        //    Lock briefly; NO IO under the guard. `commit_uuid` is a plain field
        //    set at construction, so it needs no locking.
        let snapshot_id = {
            let mut cache = self.cache.lock().unwrap();
            *cache
                .snapshot_id
                .get_or_insert_with(|| SnapshotProducer::generate_unique_snapshot_id(base))
        };

        // 3. Compose + delegate: a fresh per-attempt writer, built via the derived
        //    `TypedBuilder` with the stable `commit_uuid` + `snapshot_id` injected.
        //    ALL shared write mechanics live in `SnapshotProducer`, reused as-is.
        let producer = SnapshotProducer::builder()
            .table(base)
            .snapshot_id(Some(snapshot_id))
            .commit_uuid(self.commit_uuid)
            .snapshot_properties(self.snapshot_properties.clone())
            .added_data_files(self.added_data_files.clone())
            .build();

        // 4. Delegate the write, forwarding the caller-supplied manifest process.
        //    The operation's `existing_manifest` is where the filter managers run.
        producer.commit(operation, process).await
    }
}
