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

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex};

use futures::stream::{self, StreamExt};
use uuid::Uuid;

use super::snapshot::{DefaultManifestProcess, SnapshotProduceOperation, SnapshotProducer};
use crate::error::Result;
use crate::spec::{
    DataFile, MIN_FORMAT_VERSION_ROW_LINEAGE, ManifestContentType, ManifestEntry, ManifestFile,
    ManifestWriter, Operation,
};
use crate::table::Table;
use crate::transaction::{
    ActionCommit, MANIFEST_TARGET_SIZE_BYTES, MANIFEST_TARGET_SIZE_BYTES_DEFAULT, TransactionAction,
};
use crate::utils::DEFAULT_LOAD_CONCURRENCY_LIMIT;
use crate::{Error, ErrorKind};

const KEPT_MANIFESTS_COUNT: &str = "manifests-kept";
const CREATED_MANIFESTS_COUNT: &str = "manifests-created";
const REPLACED_MANIFESTS_COUNT: &str = "manifests-replaced";
/// Tracks entries processed during clustering. Always 0 for manual add/delete operations.
const PROCESSED_ENTRY_COUNT: &str = "entries-processed";

/// Function that maps a DataFile to a cluster key for grouping entries into manifests.
type ClusterByFunc = Box<dyn Fn(&DataFile) -> String + Send + Sync>;

/// Predicate function to select which manifests to rewrite.
type ManifestPredicate = Box<dyn Fn(&ManifestFile) -> bool + Send + Sync>;

/// State preserved across retry attempts of a single `RewriteManifestsAction` commit.
///
/// The transaction retry loop in `Transaction::commit` reuses the same
/// `Arc<dyn TransactionAction>` across retries, so any state stored inside the
/// action persists across attempts. We exploit that to skip re-reading and
/// re-writing manifests when a concurrent commit only added new manifests
/// (e.g. from a concurrent FastAppend commit).
#[derive(Default)]
struct RewriteManifestsState {
    /// Input manifests consumed by the last full-rewrite pass. Empty on the
    /// first attempt. Used only for the `requires_rewrite` set-membership check
    /// against the fresh manifest list; we do not need to reload them.
    rewritten_manifests: Vec<ManifestFile>,

    /// Output manifest files written by the last full-rewrite pass. Reused
    /// verbatim on the retry path when every path in `rewritten_manifests` is
    /// still present in the fresh snapshot (i.e. concurrent commits only added
    /// new manifests; none of ours were removed by a concurrent rewrite).
    new_manifests: Vec<ManifestFile>,

    /// Entry count from the last full-rewrite pass, recorded in the snapshot
    /// summary. Preserved across retries because the reuse path does not
    /// re-count entries.
    entry_count: usize,

    /// We store the commit_uuid and proposed_snapshot_id as the stable identify
    /// of the the new snapshot this action is attempting to create.
    ///
    /// This is not the the parent snapshot id. The parent snapshot changes
    /// on every retry. We want *our* proposed snapshot to keep the same
    /// identity across attempts so that manifests written by an earlier
    /// attempt can be reused verbatim.
    ///
    /// Concretely: `SnapshotProducer::new_manifest_writer` stamps every new
    /// manifest with `added_snapshot_id = SnapshotProducer::snapshot_id`.
    /// The `ManifestListWriter` for the new snapshot then checks that its
    /// own `snapshot_id` matches every carried manifest's `added_snapshot_id`
    /// (see `ManifestListWriter::assign_sequence_numbers`). If we let each
    /// attempt generate a fresh proposed snapshot ID, the list writer on
    /// attempt 2 would reject the manifests attempt 1 already wrote.
    ///
    /// Preserved even on the full-rewrite retry path (where cached output is
    /// discarded and re-created) so the identity stays stable for whichever
    /// attempt ultimately succeeds.
    commit_uuid: Option<Uuid>,
    proposed_snapshot_id: Option<i64>,
}

/// Transaction action for rewriting manifest files.
///
/// This action reorganizes manifest files without changing the underlying data files.
/// It can consolidate small manifests or re-cluster entries by partition values or
/// custom keys.
///
/// Manifests with delete content type are never rewritten.
pub struct RewriteManifestsAction {
    commit_uuid: Option<Uuid>,
    key_metadata: Option<Vec<u8>>,
    snapshot_properties: HashMap<String, String>,
    snapshot_id: Option<i64>,
    target_branch: Option<String>,

    cluster_by_func: Option<ClusterByFunc>,
    manifest_predicate: Option<ManifestPredicate>,
    added_manifests: Vec<ManifestFile>,
    deleted_manifests: Vec<ManifestFile>,

    /// Retry state carried across attempts of this action's commit. The
    /// `Transaction::commit` retry loop reuses the same `Arc<Self>` across
    /// attempts, so interior mutability is required.
    ///
    /// `std::sync::Mutex` is used deliberately: state accesses are cheap
    /// in-memory operations and no `.await` is issued while holding the guard.
    state: Mutex<RewriteManifestsState>,
}

impl RewriteManifestsAction {
    /// Creates a new rewrite manifests action with default settings.
    pub fn new() -> Self {
        Self {
            commit_uuid: None,
            key_metadata: None,
            snapshot_properties: HashMap::new(),
            snapshot_id: None,
            target_branch: None,

            cluster_by_func: None,
            manifest_predicate: None,
            added_manifests: Vec::new(),
            deleted_manifests: Vec::new(),

            state: Mutex::new(RewriteManifestsState::default()),
        }
    }

    /// Set a clustering function that determines how data file entries are grouped
    /// into new manifests. Files with the same cluster key will be written to the
    /// same manifest.
    pub fn cluster_by(mut self, func: ClusterByFunc) -> Self {
        self.cluster_by_func = Some(func);
        self
    }

    /// Set a predicate to filter which manifests should be rewritten.
    /// Manifests that don't match the predicate will be kept as-is.
    pub fn rewrite_if(mut self, predicate: ManifestPredicate) -> Self {
        self.manifest_predicate = Some(predicate);
        self
    }

    /// Manually add a manifest to the snapshot. The manifest must not contain
    /// any added or deleted file entries, and its `partition_spec_id` must
    /// reference a partition spec that exists in the table metadata.
    ///
    /// Manifests with unknown (None) file counts — such as V1 manifests — are
    /// rejected because the Iceberg spec treats None as "assumed non-zero",
    /// which conflicts with the requirement that added and deleted counts be
    /// zero.
    pub fn add_manifest(mut self, manifest: ManifestFile) -> Self {
        self.added_manifests.push(manifest);
        self
    }

    /// Manually remove a manifest from the snapshot. The manifest must exist
    /// in the current snapshot.
    pub fn delete_manifest(mut self, manifest: ManifestFile) -> Self {
        self.deleted_manifests.push(manifest);
        self
    }

    /// Set snapshot properties.
    pub fn set_snapshot_properties(mut self, properties: HashMap<String, String>) -> Self {
        self.snapshot_properties = properties;
        self
    }

    /// Set the target branch for this action.
    pub fn set_target_branch(mut self, target_branch: String) -> Self {
        self.target_branch = Some(target_branch);
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

    /// Set snapshot id.
    pub fn set_snapshot_id(mut self, snapshot_id: i64) -> Self {
        self.snapshot_id = Some(snapshot_id);
        self
    }

    /// Full clustering pass over the current snapshot's manifests.
    ///
    /// Returns `(new_manifests, rewritten_manifests, entry_count)`.
    ///
    /// This is the expensive path: it reads every manifest matching the
    /// predicate from object storage, re-clusters live entries into new
    /// writers, and finalizes those writers (each producing a new manifest
    /// file written to object storage). Invoke this on the first attempt
    /// or when a concurrent rewrite has invalidated the output of a prior
    /// attempt (see `RewriteManifestsState`).
    async fn run_clustering_pass(
        &self,
        table: &Table,
        snapshot_producer: &mut SnapshotProducer<'_>,
        current_manifests: &[ManifestFile],
        deleted_paths: &HashSet<&str>,
        cluster_func: &ClusterByFunc,
    ) -> Result<(Vec<ManifestFile>, Vec<ManifestFile>, usize)> {
        // Target size for a single output manifest. Once an in-progress
        // writer reaches this, it is sealed and a fresh writer is started
        // for the same cluster key. Mirrors Spark/Iceberg
        // `commit.manifest.target-size-bytes` (default 8 MiB) so a hot
        // partition no longer produces one unbounded manifest.
        //
        // Resolution order: the per-commit snapshot property set on this
        // action via `set_snapshot_properties` wins, so a caller can size
        // the manifests for a single rewrite without persisting a table
        // property (the snapshot property is also recorded in the snapshot
        // summary); otherwise fall back to the table property, then the
        // Iceberg default.
        let target_manifest_size_bytes: u64 = self
            .snapshot_properties
            .get(MANIFEST_TARGET_SIZE_BYTES)
            .or_else(|| {
                table
                    .metadata()
                    .properties()
                    .get(MANIFEST_TARGET_SIZE_BYTES)
            })
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(MANIFEST_TARGET_SIZE_BYTES_DEFAULT as u64);

        // Currently-open writer per (cluster_key, partition_spec_id).
        // BTreeMap gives deterministic ordering for the writers that are
        // still open when streaming finishes.
        let mut writers: BTreeMap<(String, i32), ManifestWriter> = BTreeMap::new();

        // Separate manifests into to-be-rewritten and everything else. We
        // don't need to track the "kept" set here — the outer commit()
        // recomputes it from the fresh manifest list. Delete-type manifests
        // and predicate-excluded manifests are simply not added to
        // `manifests_to_rewrite`.
        let mut rewritten_manifests: Vec<ManifestFile> = Vec::new();
        let mut manifests_to_rewrite: Vec<ManifestFile> = Vec::new();
        for manifest_file in current_manifests {
            if deleted_paths.contains(manifest_file.manifest_path.as_str()) {
                continue;
            }
            if manifest_file.content == ManifestContentType::Deletes {
                continue;
            }
            if let Some(ref predicate) = self.manifest_predicate
                && !predicate(manifest_file)
            {
                continue;
            }
            rewritten_manifests.push(manifest_file.clone());
            manifests_to_rewrite.push(manifest_file.clone());
        }

        let mut entry_count: usize = 0;
        let mut new_manifests: Vec<ManifestFile> = Vec::new();

        // Stream the manifests to rewrite with bounded load concurrency,
        // routing their entries into per-key writers and dropping each loaded
        // input manifest immediately (never holding all inputs at once).
        //
        // Crucially, a writer that reaches the target size is serialized and
        // dropped *here* — inside the async loop — rather than parked in a
        // buffer until the stream finishes. The entries it holds are freed as
        // soon as its manifest is written, so peak memory is bounded by the
        // in-flight input manifests plus the still-open (below-target) writers
        // (at most one per active cluster key) instead of by the total entry
        // count. A large single-key rewrite previously buffered every output
        // entry in memory at once and could OOM the process.
        //
        // The load stream can run concurrently, but entry routing is
        // sequential (writers are stateful), so we consume the buffered
        // manifests one at a time.
        let mut load_stream = stream::iter(manifests_to_rewrite)
            .map(|manifest_file| {
                let file_io = table.file_io().clone();
                async move {
                    let manifest = manifest_file.load_manifest(&file_io).await?;
                    Ok::<_, Error>((manifest_file, manifest))
                }
            })
            .buffer_unordered(DEFAULT_LOAD_CONCURRENCY_LIMIT);

        while let Some(loaded) = load_stream.next().await {
            let (manifest_file, manifest) = loaded?;
            let spec_id = manifest_file.partition_spec_id;
            for entry in manifest.entries() {
                if !entry.is_alive() {
                    continue;
                }

                let key = cluster_func(entry.data_file());
                let writer_key = (key, spec_id);

                if !writers.contains_key(&writer_key) {
                    let w = snapshot_producer
                        .new_manifest_writer(ManifestContentType::Data, spec_id)?;
                    writers.insert(writer_key.clone(), w);
                }
                let writer = writers
                    .get_mut(&writer_key)
                    .expect("writer was just inserted for this key");
                writer.add_existing_entry(entry.as_ref().clone())?;
                entry_count += 1;

                // Roll over once this writer reaches the target size: seal and
                // finalize it now, freeing its entries, and let the next entry
                // for this key open a fresh writer.
                if writer.estimated_manifest_size() >= target_manifest_size_bytes {
                    let sealed = writers
                        .remove(&writer_key)
                        .expect("writer present for this key");
                    new_manifests.push(sealed.write_manifest_file().await?);
                }
            }
        }

        // Finalize the writers still open at the end (the partially-filled tail
        // per cluster key). Manifest ordering is not required for correctness:
        // existing entries keep their sequence numbers, and kept manifests are
        // placed before new ones in the outer assembly for first_row_id
        // assignment.
        for (_key, writer) in std::mem::take(&mut writers) {
            new_manifests.push(writer.write_manifest_file().await?);
        }

        Ok((new_manifests, rewritten_manifests, entry_count))
    }
}

impl Default for RewriteManifestsAction {
    fn default() -> Self {
        Self::new()
    }
}

/// Count of active (added + existing) files in a list of manifests.
///
/// Returns `None` if any manifest has unknown counts (`None`), since the
/// Iceberg spec says `None` means "assumed to be non-zero" and we cannot
/// compute a reliable total.
fn active_files_count(manifests: &[ManifestFile]) -> Option<u64> {
    let mut total: u64 = 0;
    for m in manifests {
        let added = m.added_files_count? as u64;
        let existing = m.existing_files_count? as u64;
        total += added + existing;
    }
    Some(total)
}

/// The operation implementation for rewrite manifests.
///
/// This holds the computed manifest lists after the rewrite logic has been applied,
/// so that `existing_manifest()` can return them to the `SnapshotProducer`.
struct RewriteManifestsOperation {
    /// Manifests that carry forward to the new snapshot (kept + newly written + manually added).
    result_manifests: Vec<ManifestFile>,
}

impl SnapshotProduceOperation for RewriteManifestsOperation {
    fn operation(&self) -> Operation {
        Operation::Replace
    }

    async fn delete_entries(
        &self,
        _snapshot_produce: &SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestEntry>> {
        // Rewrite manifests doesn't change data files, so no delete entries.
        Ok(vec![])
    }

    async fn existing_manifest(
        &self,
        _snapshot_produce: &mut SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestFile>> {
        // Return the pre-computed manifest list.
        // Existing manifests come first (kept), then new manifests — the
        // SnapshotProducer will append any added-data-file manifests after these,
        // but for rewrite_manifests there are none.
        Ok(self.result_manifests.clone())
    }
}

#[async_trait::async_trait]
impl TransactionAction for RewriteManifestsAction {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        // Reject rewrite_manifests for tables with row lineage (V3+).
        // Rewriting manifests creates new ManifestFiles with first_row_id unset,
        // causing ManifestListWriter to assign fresh row IDs and advance
        // next_row_id even though no new rows were added. This breaks row lineage
        // semantics. Until a strategy to preserve row IDs through manifest rewrites
        // is implemented, this operation is unsupported for V3 tables.
        if table.metadata().format_version() >= MIN_FORMAT_VERSION_ROW_LINEAGE {
            return Err(Error::new(
                ErrorKind::FeatureUnsupported,
                format!(
                    "rewrite_manifests is not supported for tables with row lineage \
                     (format version >= {MIN_FORMAT_VERSION_ROW_LINEAGE}). Rewriting \
                     manifests would incorrectly advance next_row_id without adding \
                     new rows.",
                ),
            ));
        }

        // Resolve the identity of the *new* snapshot we are proposing to
        // commit. See the doc-comment on `RewriteManifestsState::commit_uuid`
        // / `proposed_snapshot_id` for the motivation — in short, this
        // identity must be stable across retry attempts so that manifests
        // written by an earlier attempt can be reused verbatim by a later
        // one (their `added_snapshot_id` was stamped with this ID).
        //
        // Resolution order (both fields follow the same order):
        //   1. Caller-provided value (`set_commit_uuid` / `set_snapshot_id`).
        //   2. Value cached in state from a prior attempt.
        //   3. Fresh value; cached for future retries.
        //      For `proposed_snapshot_id` the "fresh" branch is delegated to
        //      `SnapshotProducer::new` (which generates a random ID) and the
        //      resulting ID is captured back into state below.
        let (commit_uuid, cached_proposed_snapshot_id) = {
            let mut state = self.state.lock().expect("rewrite-manifests state poisoned");
            let commit_uuid = self
                .commit_uuid
                .or(state.commit_uuid)
                .unwrap_or_else(Uuid::now_v7);
            // Persist even when the caller provided a value, so later
            // retries share the same identity.
            if state.commit_uuid.is_none() {
                state.commit_uuid = Some(commit_uuid);
            }
            (commit_uuid, self.snapshot_id.or(state.proposed_snapshot_id))
        };

        // Build a SnapshotProducer. Since rewrite_manifests doesn't add or remove
        // data files, all file vectors are empty. Snapshot properties are set later
        // (after computing rewrite metrics) via `set_snapshot_properties()`.
        //
        // `cached_proposed_snapshot_id` is Some on a retry (or when the caller
        // pinned it via `set_snapshot_id`); passing it here keeps the proposed
        // snapshot's identity stable across attempts. On the first attempt with
        // no caller-provided ID it is None, and `SnapshotProducer::new` will
        // generate a fresh ID — we capture that ID back into state below.
        let mut snapshot_producer = SnapshotProducer::new(
            table,
            commit_uuid,
            self.key_metadata.clone(),
            cached_proposed_snapshot_id,
            HashMap::new(),
            vec![], // no added data files
            vec![], // no added delete files
            vec![], // no removed data files
            vec![], // no removed delete files
        );

        // Persist the resolved proposed snapshot ID so subsequent retries
        // reuse it. `SnapshotProducer` exposes it via `snapshot_id()`.
        {
            let mut state = self.state.lock().expect("rewrite-manifests state poisoned");
            if state.proposed_snapshot_id.is_none() {
                state.proposed_snapshot_id = Some(snapshot_producer.snapshot_id());
            }
        }

        if let Some(branch) = &self.target_branch {
            snapshot_producer.set_target_branch(branch.clone());
        }

        let target_branch = snapshot_producer.target_branch();
        let metadata_ref = table.metadata_ref();
        let parent_snapshot = metadata_ref.snapshot_for_ref(target_branch);

        // Load current manifests from the parent snapshot
        let current_manifests = if let Some(snapshot) = parent_snapshot {
            let manifest_list = snapshot
                .load_manifest_list(table.file_io(), metadata_ref.as_ref())
                .await?;
            manifest_list
                .consume_entries()
                .into_iter()
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };

        // Map paths to the actual ManifestFile in the snapshot for content-type checks
        // and existence lookups.
        let current_manifests_by_path: HashMap<&str, &ManifestFile> = current_manifests
            .iter()
            .map(|m| (m.manifest_path.as_str(), m))
            .collect();

        let deleted_paths: HashSet<&str> = self
            .deleted_manifests
            .iter()
            .map(|m| m.manifest_path.as_str())
            .collect();

        // Check for duplicate paths in deleted_manifests
        if deleted_paths.len() != self.deleted_manifests.len() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "deleted_manifests contains duplicate manifest paths",
            ));
        }

        // Check for duplicate paths in added_manifests
        let added_paths: HashSet<&str> = self
            .added_manifests
            .iter()
            .map(|m| m.manifest_path.as_str())
            .collect();
        if added_paths.len() != self.added_manifests.len() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "added_manifests contains duplicate manifest paths",
            ));
        }

        // Validate deleted manifests exist in current snapshot and are not
        // delete-type manifests (which must never be removed by rewrite_manifests).
        for manifest in &self.deleted_manifests {
            let path = manifest.manifest_path.as_str();
            match current_manifests_by_path.get(path) {
                None => {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!("Deleted manifest does not exist in the current snapshot: {path}",),
                    ));
                }
                Some(current) if current.content == ManifestContentType::Deletes => {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!(
                            "Cannot delete a delete-type manifest via rewrite_manifests: {path}",
                        ),
                    ));
                }
                _ => {}
            }
        }

        // Validate added manifests don't already exist in the current snapshot
        // (unless they are also being deleted — i.e. swapped) and don't have
        // added/deleted files.
        // `None` counts (e.g. V1 manifests) are treated as non-zero per the
        // Iceberg spec, so manifests with unknown counts are rejected.
        for manifest in &self.added_manifests {
            if manifest.content == ManifestContentType::Deletes {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Cannot add a delete-type manifest via rewrite_manifests: {}",
                        manifest.manifest_path
                    ),
                ));
            }
            if metadata_ref
                .partition_spec_by_id(manifest.partition_spec_id)
                .is_none()
            {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Cannot add manifest with unknown partition spec id {}: {}",
                        manifest.partition_spec_id, manifest.manifest_path
                    ),
                ));
            }
            if current_manifests_by_path.contains_key(manifest.manifest_path.as_str())
                && !deleted_paths.contains(manifest.manifest_path.as_str())
            {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Cannot add manifest that already exists in the current snapshot: {}",
                        manifest.manifest_path
                    ),
                ));
            }
            if manifest.has_added_files() {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Cannot add manifest with added files: {}",
                        manifest.manifest_path
                    ),
                ));
            }
            if manifest.has_deleted_files() {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Cannot add manifest with deleted files: {}",
                        manifest.manifest_path
                    ),
                ));
            }
        }

        // ---- Decide whether we can reuse output from a prior attempt. ----
        //
        // The transaction retry loop reuses the same `Arc<Self>` across
        // attempts, so `self.state` persists. If a previous attempt already
        // ran the (expensive) clustering pass and every input manifest we
        // consumed is still present in the current snapshot, we can skip the
        // clustering pass entirely and reuse its output.
        //
        // The reuse machinery only applies to the clustering path. The
        // non-clustering path (manual add/delete only) has no expensive work
        // to skip, so we leave state empty and treat every attempt as a
        // first attempt for that path.
        let new_manifests: Vec<ManifestFile>;
        let rewritten_manifests: Vec<ManifestFile>;
        let entry_count: usize;

        if let Some(cluster_func) = &self.cluster_by_func {
            // Snapshot the reuse decision without holding the lock while we
            // do I/O. `requires_rewrite` is true iff we need to re-run the
            // full clustering pass. Holding the guard here is safe: only
            // in-memory reads.
            let requires_rewrite = {
                let state = self.state.lock().expect("rewrite-manifests state poisoned");
                if state.rewritten_manifests.is_empty() {
                    // First attempt (or a prior attempt cleared state).
                    true
                } else {
                    // If any input we previously consumed is no longer in the
                    // fresh snapshot, a concurrent rewrite invalidated our
                    // work and we must redo the clustering pass.
                    state
                        .rewritten_manifests
                        .iter()
                        .any(|m| !current_manifests_by_path.contains_key(m.manifest_path.as_str()))
                }
            };

            if requires_rewrite {
                // Full clustering pass.
                let (fresh_new, fresh_rewritten, fresh_entry_count) = self
                    .run_clustering_pass(
                        table,
                        &mut snapshot_producer,
                        &current_manifests,
                        &deleted_paths,
                        cluster_func,
                    )
                    .await?;

                // Persist so a future retry can reuse this output.
                let mut state = self.state.lock().expect("rewrite-manifests state poisoned");
                state.rewritten_manifests = fresh_rewritten.clone();
                state.new_manifests = fresh_new.clone();
                state.entry_count = fresh_entry_count;

                new_manifests = fresh_new;
                rewritten_manifests = fresh_rewritten;
                entry_count = fresh_entry_count;
            } else {
                // Reuse path: skip clustering entirely. The output manifests
                // we already wrote in a prior attempt are still valid because
                // every input we consumed is still referenced by the current
                // snapshot.
                let state = self.state.lock().expect("rewrite-manifests state poisoned");
                new_manifests = state.new_manifests.clone();
                rewritten_manifests = state.rewritten_manifests.clone();
                entry_count = state.entry_count;
            }
        } else {
            // Non-clustering path: no clustering pass to skip. State stays
            // empty across attempts.
            new_manifests = Vec::new();
            rewritten_manifests = Vec::new();
            entry_count = 0;
        }

        // ---- Recompute kept manifests from the FRESH manifest list. ----
        //
        // This must happen every attempt (even on the reuse path) because
        // concurrent commits may have added new manifests that we need to
        // carry forward into our new snapshot. Excluded from `kept`:
        //   * paths marked for deletion via `delete_manifest`,
        //   * paths we already rewrote (present in `rewritten_manifests`).
        //
        // Delete-type manifests and manifests filtered out by
        // `manifest_predicate` on the clustering path are never added to
        // `rewritten_manifests`, so they flow through to `kept` naturally.
        // The same holds on the non-clustering path, where
        // `rewritten_manifests` is always empty.
        //
        // Concurrent-append manifests (present in the fresh list but not in
        // `rewritten_manifests`) flow into `kept_manifests` automatically —
        // this is what makes the reuse path correct on FastAppend retries.
        let rewritten_paths: HashSet<&str> = rewritten_manifests
            .iter()
            .map(|m| m.manifest_path.as_str())
            .collect();
        let kept_manifests: Vec<ManifestFile> = current_manifests
            .iter()
            .filter(|m| {
                !deleted_paths.contains(m.manifest_path.as_str())
                    && !rewritten_paths.contains(m.manifest_path.as_str())
            })
            .cloned()
            .collect();

        // Nothing was actually rewritten, added, or deleted — bail out instead
        // of creating a redundant snapshot identical to the parent.
        if new_manifests.is_empty()
            && self.added_manifests.is_empty()
            && rewritten_manifests.is_empty()
            && self.deleted_manifests.is_empty()
        {
            return Ok(ActionCommit::new(vec![], vec![]));
        }

        // Validate file counts when all manifests have known counts.
        // If any manifest has None counts (e.g. V1 format), we skip validation
        // because the Iceberg spec says None means "assumed non-zero" and we
        // cannot compute a reliable total.
        let created_count = active_files_count(&new_manifests)
            .and_then(|a| active_files_count(&self.added_manifests).map(|b| a + b));
        let replaced_count = active_files_count(&rewritten_manifests)
            .and_then(|a| active_files_count(&self.deleted_manifests).map(|b| a + b));

        if let (Some(created), Some(replaced)) = (created_count, replaced_count)
            && created != replaced
        {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Rewrite manifests file count mismatch: created {created} files but replaced {replaced} files",
                ),
            ));
        }

        // Inject rewrite-specific summary properties so they appear in the snapshot.
        // Internal metrics are inserted after user properties, so they take
        // precedence if a user sets a key like "manifests-created".
        let mut rewrite_properties = self.snapshot_properties.clone();
        rewrite_properties.insert(
            CREATED_MANIFESTS_COUNT.to_string(),
            (new_manifests.len() + self.added_manifests.len()).to_string(),
        );
        rewrite_properties.insert(
            KEPT_MANIFESTS_COUNT.to_string(),
            kept_manifests.len().to_string(),
        );
        rewrite_properties.insert(
            REPLACED_MANIFESTS_COUNT.to_string(),
            (rewritten_manifests.len() + self.deleted_manifests.len()).to_string(),
        );
        rewrite_properties.insert(PROCESSED_ENTRY_COUNT.to_string(), entry_count.to_string());
        snapshot_producer.set_snapshot_properties(rewrite_properties);

        // Assemble final manifest list: kept manifests first (existing), then new
        // manifests and manually added manifests. Existing manifests must come
        // before new ones to ensure correct first_row_id assignment by
        // ManifestListWriter.
        let mut result_manifests: Vec<ManifestFile> = Vec::new();
        result_manifests.extend(kept_manifests);
        result_manifests.extend(new_manifests);
        result_manifests.extend(self.added_manifests.clone());

        let operation = RewriteManifestsOperation { result_manifests };

        snapshot_producer
            .commit(operation, DefaultManifestProcess)
            .await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use crate::spec::{
        DataContentType, DataFile, DataFileBuilder, DataFileFormat, Literal, MAIN_BRANCH,
        ManifestContentType, ManifestFile, ManifestListWriter, ManifestWriterBuilder, Operation,
        Snapshot, SnapshotReference, SnapshotRetention, Struct, Summary,
    };
    use crate::table::Table;
    use crate::transaction::TransactionAction;
    use crate::transaction::rewrite_manifests::{
        CREATED_MANIFESTS_COUNT, KEPT_MANIFESTS_COUNT, PROCESSED_ENTRY_COUNT,
        RewriteManifestsAction,
    };
    use crate::transaction::tests::{make_v2_minimal_table, make_v3_minimal_table};
    use crate::{TableRequirement, TableUpdate};

    fn test_manifest(
        path: &str,
        added: Option<u32>,
        existing: Option<u32>,
        deleted: Option<u32>,
    ) -> ManifestFile {
        ManifestFile {
            manifest_path: path.to_string(),
            manifest_length: 1000,
            partition_spec_id: 0,
            content: ManifestContentType::Data,
            sequence_number: 0,
            min_sequence_number: 0,
            added_snapshot_id: 0,
            added_files_count: added,
            existing_files_count: existing,
            deleted_files_count: deleted,
            added_rows_count: Some(0),
            existing_rows_count: Some(0),
            deleted_rows_count: Some(0),
            partitions: None,
            key_metadata: None,
            first_row_id: None,
        }
    }

    /// Helper to commit an action and assert it returns an error containing the expected message.
    async fn assert_commit_err(action: RewriteManifestsAction, table: &Table, expected_msg: &str) {
        let action = Arc::new(action);
        let result = action.commit(table).await;
        match result {
            Ok(_) => panic!("expected error containing '{expected_msg}', but commit succeeded"),
            Err(e) => assert!(
                e.to_string().contains(expected_msg),
                "expected error containing '{expected_msg}', got: {e}"
            ),
        }
    }

    #[tokio::test]
    async fn test_rewrite_manifests_rejects_v3_table() {
        let table = make_v3_minimal_table();
        // Even a no-op rewrite should be rejected for V3 tables.
        let action = RewriteManifestsAction::new();
        assert_commit_err(action, &table, "rewrite_manifests is not supported").await;
    }

    #[tokio::test]
    async fn test_rewrite_manifests_rejects_v3_table_with_cluster_by() {
        let table = make_v3_minimal_table();
        let action = RewriteManifestsAction::new().cluster_by(Box::new(|_| "default".to_string()));
        assert_commit_err(action, &table, "rewrite_manifests is not supported").await;
    }

    #[tokio::test]
    async fn test_rewrite_manifests_rejects_v3_table_with_add_manifest() {
        let table = make_v3_minimal_table();
        let manifest = test_manifest("s3://bucket/manifest-ok.avro", Some(0), Some(5), Some(0));
        let action = RewriteManifestsAction::new().add_manifest(manifest);
        assert_commit_err(action, &table, "rewrite_manifests is not supported").await;
    }

    #[tokio::test]
    async fn test_add_manifest_rejects_delete_type_manifest() {
        let table = make_v2_minimal_table();
        let mut manifest =
            test_manifest("s3://bucket/manifest-del.avro", Some(0), Some(5), Some(0));
        manifest.content = ManifestContentType::Deletes;
        let action = RewriteManifestsAction::new().add_manifest(manifest);
        assert_commit_err(
            action,
            &table,
            "Cannot add a delete-type manifest via rewrite_manifests",
        )
        .await;
    }

    #[tokio::test]
    async fn test_add_manifest_rejects_unknown_partition_spec_id() {
        let table = make_v2_minimal_table();
        // The minimal table only has partition spec id 0.
        let mut manifest = test_manifest(
            "s3://bucket/manifest-bad-spec.avro",
            Some(0),
            Some(5),
            Some(0),
        );
        manifest.partition_spec_id = 9999;
        let action = RewriteManifestsAction::new().add_manifest(manifest);
        assert_commit_err(action, &table, "unknown partition spec id 9999").await;
    }

    #[tokio::test]
    async fn test_add_manifest_rejects_added_files_count_some_positive() {
        let table = make_v2_minimal_table();
        let manifest = test_manifest("s3://bucket/manifest-1.avro", Some(5), Some(0), Some(0));
        let action = RewriteManifestsAction::new().add_manifest(manifest);
        assert_commit_err(action, &table, "Cannot add manifest with added files").await;
    }

    #[tokio::test]
    async fn test_add_manifest_rejects_deleted_files_count_some_positive() {
        let table = make_v2_minimal_table();
        let manifest = test_manifest("s3://bucket/manifest-1.avro", Some(0), Some(0), Some(3));
        let action = RewriteManifestsAction::new().add_manifest(manifest);
        assert_commit_err(action, &table, "Cannot add manifest with deleted files").await;
    }

    #[tokio::test]
    async fn test_add_manifest_rejects_none_added_files_count() {
        let table = make_v2_minimal_table();
        // None means "assumed non-zero" per Iceberg spec — should be rejected.
        let manifest = test_manifest("s3://bucket/manifest-v1.avro", None, Some(10), None);
        let action = RewriteManifestsAction::new().add_manifest(manifest);
        assert_commit_err(action, &table, "Cannot add manifest with added files").await;
    }

    #[tokio::test]
    async fn test_add_manifest_rejects_none_deleted_files_count() {
        let table = make_v2_minimal_table();
        // added_files_count is known-zero, but deleted_files_count is None → rejected.
        let manifest = test_manifest("s3://bucket/manifest-v1.avro", Some(0), Some(10), None);
        let action = RewriteManifestsAction::new().add_manifest(manifest);
        assert_commit_err(action, &table, "Cannot add manifest with deleted files").await;
    }

    #[tokio::test]
    async fn test_add_manifest_accepts_zero_counts() {
        let table = make_v2_minimal_table();
        // Both added and deleted are known-zero — should pass the validation.
        // (It will fail later during snapshot commit because there is no matching
        // deleted manifest, but the add_manifest validation itself should succeed.)
        let manifest = test_manifest("s3://bucket/manifest-ok.avro", Some(0), Some(5), Some(0));
        let action = Arc::new(RewriteManifestsAction::new().add_manifest(manifest));
        let result = action.commit(&table).await;
        // The error, if any, should NOT be about added/deleted files.
        if let Err(e) = &result {
            assert!(
                !e.to_string()
                    .contains("Cannot add manifest with added files")
                    && !e
                        .to_string()
                        .contains("Cannot add manifest with deleted files"),
                "unexpected rejection for zero-count manifest: {e}"
            );
        }
    }

    // ------------------------------------------------------------------
    // Retry / reuse tests
    //
    // These tests exercise the state-reuse machinery on
    // `RewriteManifestsAction` by driving the action's `commit()` directly
    // against successive `Table` states, each simulating a snapshot the
    // catalog would return after a concurrent commit rejected our prior
    // attempt with a 409. The transaction retry loop in
    // `Transaction::commit` reuses the same `Arc<dyn TransactionAction>`
    // across attempts, so state stored on the action persists — which is
    // exactly what these tests observe.
    //
    // We call `TransactionAction::commit(Arc<Self>, &Table)` directly (not
    // through `Transaction::commit(&catalog)`) so we retain a handle to the
    // action after each attempt and can inspect `self.state` without going
    // through a catalog mock.
    // ------------------------------------------------------------------

    /// Rewrite the minimal V2 table's location to `memory://` so any manifest
    /// lists / manifest files we write in the tests are addressable by the
    /// same in-memory `FileIO` the table exposes.
    fn make_v2_memory_table() -> Table {
        let base = make_v2_minimal_table();
        let metadata = base
            .metadata()
            .clone()
            .into_builder(Some("s3://bucket/test/location/metadata/v1.json".into()))
            .set_location("memory:///test/location".to_string())
            .build()
            .unwrap()
            .metadata;
        base.with_metadata(Arc::new(metadata))
    }

    /// Build a `DataFile` for the minimal V2 table partitioned by identity(x).
    fn data_file(path: &str, partition_x: i64, record_count: u64) -> DataFile {
        DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(path.to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(record_count)
            .partition(Struct::from_iter([Some(Literal::long(partition_x))]))
            .partition_spec_id(0)
            .build()
            .unwrap()
    }

    /// Write a real data manifest (V2, data content) to the given path in the
    /// table's `FileIO`. The manifest contains one live existing entry per
    /// supplied `DataFile`, so a rewrite pass can cluster them.
    ///
    /// The returned `ManifestFile` has its `sequence_number` and
    /// `min_sequence_number` pre-assigned to `sequence_number` so it can be
    /// carried forward into subsequent snapshots' manifest lists without
    /// tripping `ManifestListWriter::assign_sequence_numbers` (which only
    /// assigns sequence numbers when the manifest belongs to the snapshot
    /// being written).
    async fn write_data_manifest(
        table: &Table,
        path: &str,
        added_snapshot_id: i64,
        sequence_number: i64,
        files: Vec<DataFile>,
    ) -> ManifestFile {
        let file_io = table.file_io().clone();
        let mut writer = ManifestWriterBuilder::new(
            file_io.new_output(path).unwrap(),
            Some(added_snapshot_id),
            None,
            table.metadata().current_schema().clone(),
            table.metadata().default_partition_spec().as_ref().clone(),
        )
        .build_v2_data();

        for file in files {
            writer
                .add_existing_file(
                    file,
                    added_snapshot_id,
                    sequence_number,
                    Some(sequence_number),
                )
                .unwrap();
        }
        let mut manifest = writer.write_manifest_file().await.unwrap();
        // Pre-assign sequence numbers so carrying this manifest into later
        // snapshots' manifest lists is a no-op for `assign_sequence_numbers`.
        manifest.sequence_number = sequence_number;
        manifest.min_sequence_number = sequence_number;
        manifest
    }

    /// Write a manifest list at `path` containing exactly `manifests` and
    /// return a snapshot referencing it. `snapshot_id` and `sequence_number`
    /// are recorded in both the manifest list and the snapshot.
    async fn write_manifest_list_snapshot(
        table: &Table,
        path: &str,
        snapshot_id: i64,
        sequence_number: i64,
        manifests: Vec<ManifestFile>,
    ) -> Snapshot {
        let file_io = table.file_io().clone();
        let mut writer = ManifestListWriter::v2(
            file_io.new_output(path).unwrap(),
            snapshot_id,
            None,
            sequence_number,
        );
        writer.add_manifests(manifests.into_iter()).unwrap();
        writer.close().await.unwrap();

        Snapshot::builder()
            .with_snapshot_id(snapshot_id)
            .with_timestamp_ms(table.metadata().last_updated_ms() + snapshot_id)
            .with_sequence_number(sequence_number)
            .with_schema_id(0)
            .with_manifest_list(path)
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: HashMap::new(),
            })
            .build()
    }

    /// Return a fresh `Table` value with `snapshot` set as the current main
    /// snapshot. The original `base` table is left untouched so the caller
    /// can produce further variants without disturbing state.
    fn table_at_snapshot(base: &Table, snapshot: Snapshot) -> Table {
        let snapshot_id = snapshot.snapshot_id();
        let metadata = base
            .metadata()
            .clone()
            .into_builder(Some("s3://bucket/test/location/metadata/v1.json".into()))
            .add_snapshot(snapshot)
            .unwrap()
            .set_ref(MAIN_BRANCH, SnapshotReference {
                snapshot_id,
                retention: SnapshotRetention::Branch {
                    min_snapshots_to_keep: None,
                    max_snapshot_age_ms: None,
                    max_ref_age_ms: None,
                },
            })
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        base.clone().with_metadata(Arc::new(metadata))
    }

    /// Extract the newly-created snapshot summary from an `ActionCommit`.
    /// Rewrite-manifest commits always emit exactly one `AddSnapshot` update
    /// on success.
    fn snapshot_summary(
        updates: &[TableUpdate],
        _requirements: &[TableRequirement],
    ) -> HashMap<String, String> {
        for update in updates {
            if let TableUpdate::AddSnapshot { snapshot } = update {
                return snapshot.summary().additional_properties.clone();
            }
        }
        panic!("no AddSnapshot in ActionCommit updates");
    }

    /// Simple deterministic cluster function used by the retry tests: bucket
    /// entries by the file path's first character so tests can assert on
    /// stable output ordering without depending on partition values.
    fn cluster_by_first_char() -> Box<dyn Fn(&DataFile) -> String + Send + Sync> {
        Box::new(|df: &DataFile| {
            df.file_path()
                .chars()
                .next()
                .map(|c| c.to_string())
                .unwrap_or_default()
        })
    }

    /// A single hot cluster key with many entries and a small target size must
    /// roll over into multiple output manifests, and no entry may be lost.
    ///
    /// This guards the incremental-flush rollover in `run_clustering_pass`,
    /// which seals and writes each full writer mid-stream (freeing its entries)
    /// rather than buffering every output entry until the end — the behavior
    /// that bounds rewrite memory for large single-key (e.g. unpartitioned)
    /// tables.
    #[tokio::test]
    async fn test_rewrite_manifests_rolls_over_and_preserves_count() {
        let base = make_v2_memory_table();

        // Eight files that all cluster to the same key ('a'), in one input
        // manifest.
        let files: Vec<DataFile> = (0..8)
            .map(|i| data_file(&format!("a-{i}.parquet"), 1, 10))
            .collect();
        let m1 = write_data_manifest(
            &base,
            "memory:///test/location/metadata/m1.avro",
            1,
            1,
            files,
        )
        .await;
        let snapshot = write_manifest_list_snapshot(
            &base,
            "memory:///test/location/metadata/mlist-v1.avro",
            1,
            1,
            vec![m1.clone()],
        )
        .await;
        let table = table_at_snapshot(&base, snapshot);

        // Tiny target so each writer seals after a couple of entries, forcing
        // the single hot key to span multiple manifests.
        let action = Arc::new(
            RewriteManifestsAction::new()
                .cluster_by(cluster_by_first_char())
                .set_snapshot_properties(HashMap::from([(
                    crate::transaction::MANIFEST_TARGET_SIZE_BYTES.to_string(),
                    "200".to_string(),
                )])),
        );
        Arc::clone(&action).commit(&table).await.unwrap();

        let state = action.state.lock().unwrap();
        assert!(
            state.new_manifests.len() >= 2,
            "a hot key over the target must roll over into multiple manifests, got {}",
            state.new_manifests.len()
        );
        let total_files: u32 = state
            .new_manifests
            .iter()
            .map(|m| m.added_files_count.unwrap_or(0) + m.existing_files_count.unwrap_or(0))
            .sum();
        assert_eq!(
            total_files, 8,
            "all 8 input files must be preserved across the split manifests"
        );
        assert_eq!(
            state.entry_count, 8,
            "every live entry must be processed exactly once"
        );
    }

    /// Reuse path: after the first attempt writes new manifests, a concurrent
    /// FastAppend advances the parent snapshot with an additional manifest
    /// (M3). The retry must reuse the first attempt's output verbatim and
    /// carry the new manifest into `kept`.
    #[tokio::test]
    async fn test_rewrite_manifests_reuses_output_on_concurrent_append() {
        let base = make_v2_memory_table();

        // Seed two input data manifests (M1, M2) for the initial snapshot.
        let m1 = write_data_manifest(
            &base,
            "memory:///test/location/metadata/m1.avro",
            1,
            1,
            vec![data_file("a-1.parquet", 1, 10)],
        )
        .await;
        let m2 = write_data_manifest(
            &base,
            "memory:///test/location/metadata/m2.avro",
            1,
            1,
            vec![data_file("a-2.parquet", 1, 10)],
        )
        .await;

        // Snapshot S1: parent references [M1, M2].
        let snapshot_v1 = write_manifest_list_snapshot(
            &base,
            "memory:///test/location/metadata/mlist-v1.avro",
            1,
            1,
            vec![m1.clone(), m2.clone()],
        )
        .await;
        let table_v1 = table_at_snapshot(&base, snapshot_v1);

        // Attempt 1: full rewrite against S1.
        let action = Arc::new(RewriteManifestsAction::new().cluster_by(cluster_by_first_char()));
        let mut commit_v1 = Arc::clone(&action).commit(&table_v1).await.unwrap();
        let updates_v1 = commit_v1.take_updates();
        let requirements_v1 = commit_v1.take_requirements();
        let summary_v1 = snapshot_summary(&updates_v1, &requirements_v1);

        // Snapshot the state written by attempt 1 for later comparison.
        let (first_new_paths, first_rewritten_paths, first_entry_count) = {
            let state = action.state.lock().unwrap();
            assert!(
                !state.new_manifests.is_empty(),
                "attempt 1 should have produced new manifests"
            );
            (
                state
                    .new_manifests
                    .iter()
                    .map(|m| m.manifest_path.clone())
                    .collect::<Vec<_>>(),
                state
                    .rewritten_manifests
                    .iter()
                    .map(|m| m.manifest_path.clone())
                    .collect::<Vec<_>>(),
                state.entry_count,
            )
        };

        // Sanity: attempt 1 recorded M1+M2 as rewritten and captured 2 live
        // entries.
        assert_eq!(
            first_rewritten_paths
                .iter()
                .cloned()
                .collect::<std::collections::HashSet<_>>(),
            [m1.manifest_path.clone(), m2.manifest_path.clone()]
                .into_iter()
                .collect::<std::collections::HashSet<_>>(),
        );
        assert_eq!(first_entry_count, 2);
        assert_eq!(
            summary_v1.get(PROCESSED_ENTRY_COUNT),
            Some(&"2".to_string())
        );

        // Simulate a concurrent FastAppend: a new manifest M3 has been added
        // to the current snapshot between attempts.
        let m3 = write_data_manifest(
            &base,
            "memory:///test/location/metadata/m3.avro",
            2,
            2,
            vec![data_file("a-3.parquet", 1, 10)],
        )
        .await;
        let snapshot_v2 = write_manifest_list_snapshot(
            &base,
            "memory:///test/location/metadata/mlist-v2.avro",
            2,
            2,
            vec![m1.clone(), m2.clone(), m3.clone()],
        )
        .await;
        let table_v2 = table_at_snapshot(&base, snapshot_v2);

        // Attempt 2: retry against the advanced snapshot. Same action → same
        // state.
        let mut commit_v2 = Arc::clone(&action).commit(&table_v2).await.unwrap();
        let updates_v2 = commit_v2.take_updates();
        let requirements_v2 = commit_v2.take_requirements();
        let summary_v2 = snapshot_summary(&updates_v2, &requirements_v2);

        // The action's `new_manifests` output must not have changed — reuse
        // means we did NOT re-cluster and did NOT write new manifest files
        // on the retry.
        let second_new_paths: Vec<String> = {
            let state = action.state.lock().unwrap();
            state
                .new_manifests
                .iter()
                .map(|m| m.manifest_path.clone())
                .collect()
        };
        assert_eq!(
            second_new_paths, first_new_paths,
            "reuse path must return the same new_manifests across attempts"
        );

        // The processed entry count is preserved from attempt 1 (we did not
        // walk any entries on the retry).
        assert_eq!(
            summary_v2.get(PROCESSED_ENTRY_COUNT),
            Some(&"2".to_string()),
            "entry_count must be preserved from attempt 1 on the reuse path"
        );

        // Kept-manifest count on attempt 1 = 0 (M1 and M2 were both
        // rewritten). On attempt 2, the concurrent-append manifest M3 flows
        // into kept, so the count grows by 1.
        assert_eq!(summary_v1.get(KEPT_MANIFESTS_COUNT), Some(&"0".to_string()));
        assert_eq!(summary_v2.get(KEPT_MANIFESTS_COUNT), Some(&"1".to_string()));

        // The created-manifests count must be identical: reuse means we did
        // not write additional new manifests on the retry.
        assert_eq!(
            summary_v1.get(CREATED_MANIFESTS_COUNT),
            summary_v2.get(CREATED_MANIFESTS_COUNT),
            "reuse path must not create additional new manifests"
        );
    }

    /// Fallback path: a concurrent commit removed one of the manifests we
    /// consumed on attempt 1 (simulating a concurrent RewriteFiles or
    /// RewriteManifests). The retry must clear the previous output and
    /// re-execute the full clustering pass.
    #[tokio::test]
    async fn test_rewrite_manifests_full_reexecution_when_concurrent_rewrite_removes_input() {
        let base = make_v2_memory_table();

        let m1 = write_data_manifest(
            &base,
            "memory:///test/location/metadata/m1.avro",
            1,
            1,
            vec![data_file("a-1.parquet", 1, 10)],
        )
        .await;
        let m2 = write_data_manifest(
            &base,
            "memory:///test/location/metadata/m2.avro",
            1,
            1,
            vec![data_file("a-2.parquet", 1, 10)],
        )
        .await;
        let snapshot_v1 = write_manifest_list_snapshot(
            &base,
            "memory:///test/location/metadata/mlist-v1.avro",
            1,
            1,
            vec![m1.clone(), m2.clone()],
        )
        .await;
        let table_v1 = table_at_snapshot(&base, snapshot_v1);

        let action = Arc::new(RewriteManifestsAction::new().cluster_by(cluster_by_first_char()));
        let _ = Arc::clone(&action).commit(&table_v1).await.unwrap();
        let first_new_paths: Vec<String> = {
            let state = action.state.lock().unwrap();
            state
                .new_manifests
                .iter()
                .map(|m| m.manifest_path.clone())
                .collect()
        };
        assert!(
            !first_new_paths.is_empty(),
            "attempt 1 should have produced new manifests"
        );

        // Simulate a concurrent rewrite that dropped M2 and introduced a
        // replacement M2' holding M2's entry. M1 is still present. Since M2
        // is one of the manifests we previously consumed, `requires_rewrite`
        // must be true and the retry must produce a fresh clustering.
        let m2_prime = write_data_manifest(
            &base,
            "memory:///test/location/metadata/m2-prime.avro",
            2,
            2,
            vec![data_file("a-2.parquet", 1, 10)],
        )
        .await;
        let snapshot_v2 = write_manifest_list_snapshot(
            &base,
            "memory:///test/location/metadata/mlist-v2.avro",
            2,
            2,
            vec![m1.clone(), m2_prime.clone()],
        )
        .await;
        let table_v2 = table_at_snapshot(&base, snapshot_v2);

        let _ = Arc::clone(&action).commit(&table_v2).await.unwrap();
        let second_rewritten_paths = {
            let state = action.state.lock().unwrap();
            state
                .rewritten_manifests
                .iter()
                .map(|m| m.manifest_path.clone())
                .collect::<Vec<_>>()
        };

        // Attempt 2's rewritten set must reflect the fresh snapshot: M1 and
        // M2' (NOT M2, which is no longer in the current manifest list).
        // This is the core correctness check for the full re-execution path
        // — the retry re-ran clustering against the fresh input set.
        let second_rewritten_set: std::collections::HashSet<String> =
            second_rewritten_paths.into_iter().collect();
        assert_eq!(
            second_rewritten_set,
            [m1.manifest_path.clone(), m2_prime.manifest_path.clone()]
                .into_iter()
                .collect::<std::collections::HashSet<_>>(),
            "retry must re-run clustering against the fresh manifest list"
        );

        // On the full re-execution path we deliberately keep the same
        // `commit_uuid` and proposed snapshot ID across attempts (to keep
        // the identity of the proposed snapshot stable if any attempt
        // eventually commits). That means the fresh manifest counter and
        // shared UUID cause attempt 2's new manifest files to be written to
        // the same object-store paths as attempt 1's — attempt 1's files are
        // simply overwritten. So we cannot assert path disjointness here;
        // the observable proof that the re-execution happened is the
        // updated `rewritten_manifests` set above, and the fact that
        // `first_new_paths` is not empty (otherwise no rewrite happened).
        assert!(
            !first_new_paths.is_empty(),
            "attempt 1 must have produced at least one new manifest"
        );
    }

    /// Repeated reuse: three consecutive attempts, each preceded by a
    /// distinct concurrent FastAppend. The action must reuse the first
    /// attempt's output across all retries, and each new concurrent-append
    /// manifest must flow into `kept`.
    #[tokio::test]
    async fn test_rewrite_manifests_reuse_after_multiple_concurrent_appends() {
        let base = make_v2_memory_table();

        let m1 = write_data_manifest(
            &base,
            "memory:///test/location/metadata/m1.avro",
            1,
            1,
            vec![data_file("a-1.parquet", 1, 10)],
        )
        .await;
        let m2 = write_data_manifest(
            &base,
            "memory:///test/location/metadata/m2.avro",
            1,
            1,
            vec![data_file("a-2.parquet", 1, 10)],
        )
        .await;
        let snapshot_v1 = write_manifest_list_snapshot(
            &base,
            "memory:///test/location/metadata/mlist-v1.avro",
            1,
            1,
            vec![m1.clone(), m2.clone()],
        )
        .await;
        let table_v1 = table_at_snapshot(&base, snapshot_v1);

        let action = Arc::new(RewriteManifestsAction::new().cluster_by(cluster_by_first_char()));
        let mut commit_v1 = Arc::clone(&action).commit(&table_v1).await.unwrap();
        let updates_v1 = commit_v1.take_updates();
        let requirements_v1 = commit_v1.take_requirements();
        let summary_v1 = snapshot_summary(&updates_v1, &requirements_v1);
        let (baseline_new_paths, baseline_rewritten_paths) = {
            let state = action.state.lock().unwrap();
            (
                state
                    .new_manifests
                    .iter()
                    .map(|m| m.manifest_path.clone())
                    .collect::<Vec<_>>(),
                state
                    .rewritten_manifests
                    .iter()
                    .map(|m| m.manifest_path.clone())
                    .collect::<Vec<_>>(),
            )
        };

        // Simulate three concurrent FastAppends, each producing a snapshot
        // that adds one more manifest to the parent's manifest list.
        let mut carried: Vec<ManifestFile> = vec![m1.clone(), m2.clone()];
        for i in 0..3 {
            let mi = write_data_manifest(
                &base,
                &format!("memory:///test/location/metadata/concurrent-{i}.avro"),
                (10 + i) as i64,
                (10 + i) as i64,
                vec![data_file(&format!("a-c{i}.parquet"), 1, 10)],
            )
            .await;
            carried.push(mi);

            let snapshot_next = write_manifest_list_snapshot(
                &base,
                &format!("memory:///test/location/metadata/mlist-c{i}.avro"),
                (10 + i) as i64,
                (10 + i) as i64,
                carried.clone(),
            )
            .await;
            let table_next = table_at_snapshot(&base, snapshot_next);

            let mut commit_next = Arc::clone(&action).commit(&table_next).await.unwrap();
            let updates_next = commit_next.take_updates();
            let requirements_next = commit_next.take_requirements();
            let summary_next = snapshot_summary(&updates_next, &requirements_next);

            // State stays anchored to the initial full-rewrite pass.
            let state = action.state.lock().unwrap();
            let cur_new: Vec<String> = state
                .new_manifests
                .iter()
                .map(|m| m.manifest_path.clone())
                .collect();
            let cur_rewritten: Vec<String> = state
                .rewritten_manifests
                .iter()
                .map(|m| m.manifest_path.clone())
                .collect();
            assert_eq!(
                cur_new,
                baseline_new_paths,
                "attempt {} (retry): new_manifests must not change on the reuse path",
                i + 2,
            );
            assert_eq!(
                cur_rewritten,
                baseline_rewritten_paths,
                "attempt {} (retry): rewritten_manifests must not change on the reuse path",
                i + 2,
            );

            // The number of kept manifests grows by one per concurrent
            // append (from 0 in the initial commit, so kept == i + 1 here).
            let expected_kept = (i + 1).to_string();
            assert_eq!(
                summary_next.get(KEPT_MANIFESTS_COUNT),
                Some(&expected_kept),
                "attempt {} (retry): kept-manifest count must include all prior concurrent appends",
                i + 2,
            );

            // Created / entry counts remain the same as attempt 1.
            assert_eq!(
                summary_next.get(CREATED_MANIFESTS_COUNT),
                summary_v1.get(CREATED_MANIFESTS_COUNT),
                "attempt {} (retry): reuse path must not create additional new manifests",
                i + 2,
            );
            assert_eq!(
                summary_next.get(PROCESSED_ENTRY_COUNT),
                summary_v1.get(PROCESSED_ENTRY_COUNT),
                "attempt {} (retry): reuse path must preserve entry_count from attempt 1",
                i + 2,
            );
        }
    }
}
