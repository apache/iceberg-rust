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

//! Merge pass for the merging snapshot producer.
//!
//! After the filter pass produces a manifest list of (residual + carried-forward)
//! manifests, this stage groups siblings of the same partition spec, bin-packs
//! them with `ListPacker`, and rewrites each multi-element bin into a single
//! merged manifest. The merge step is what stops manifest count from growing
//! linearly with commit count, and it's where prior-snapshot tombstones are
//! finally suppressed.
//!
//! Java analog: `org.apache.iceberg.ManifestMergeManager`.
//!
//! Concurrency note: the merge cache is an `Arc<Mutex<HashMap<...>>>`. Locks are
//! held only for `insert`/`get` — never across an `.await`. Reaching for
//! `DashMap` here is a footgun: its `RefMut` guard is not async-aware and can
//! deadlock if held across an await point. See brainstorm §16.3.

#![allow(dead_code)] // Wired up in the rewrite_files.rs commit.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use futures::stream::{StreamExt, TryStreamExt};
use tracing::warn;

use crate::Result;
use crate::spec::{
    DataFileFormat, FormatVersion, ManifestContentType, ManifestFile, ManifestStatus,
    ManifestWriterBuilder,
};
use crate::transaction::snapshot::{META_ROOT_PATH, SnapshotProducer};
use crate::utils::bin_packing::ListPacker;

/// Maximum number of merge tasks running concurrently. Caps both the storage
/// client's outstanding write requests and the per-task working-set memory
/// (each task buffers a manifest's worth of entries while writing).
const MANIFEST_WRITE_CONCURRENCY: usize = 8;

pub(crate) struct ManifestMergeManager {
    target_size_bytes: u64,
    min_count_to_merge: u32,
    merge_enabled: bool,
    /// Cache: bin (as a vector of source manifest paths) → merged manifest.
    /// Persisted across commit retries so the same input bin returns the same
    /// `ManifestFile` (idempotent retry contract).
    cache: Arc<Mutex<HashMap<Vec<String>, ManifestFile>>>,
    /// Paths written this attempt — consulted by `clean_uncommitted` on commit
    /// failure to delete orphan merged manifests.
    cleanup_paths: Arc<Mutex<Vec<String>>>,
    replaced_count: Arc<AtomicU64>,
}

impl ManifestMergeManager {
    pub(crate) fn new(target_size_bytes: u64, min_count_to_merge: u32, merge_enabled: bool) -> Self {
        Self {
            target_size_bytes,
            min_count_to_merge,
            merge_enabled,
            cache: Arc::new(Mutex::new(HashMap::new())),
            cleanup_paths: Arc::new(Mutex::new(Vec::new())),
            replaced_count: Arc::new(AtomicU64::new(0)),
        }
    }

    pub(crate) fn replaced_manifests_count(&self) -> u64 {
        self.replaced_count.load(Ordering::Relaxed)
    }

    pub(crate) fn cleanup_paths(&self) -> Vec<String> {
        self.cleanup_paths.lock().expect("merge cleanup mutex").clone()
    }

    /// Bin-pack `unmerged` and rewrite each multi-element bin into a single
    /// merged manifest. The first element of `unmerged` must be the manifest
    /// the action just produced (the "first" manifest); the bin containing it
    /// is exempted from merging when it has fewer than `min_count_to_merge`
    /// siblings, so a cluster of small commits doesn't trigger a giant
    /// historical rewrite.
    pub(crate) async fn merge_manifests(
        &self,
        producer: &SnapshotProducer<'_>,
        unmerged: Vec<ManifestFile>,
    ) -> Result<Vec<ManifestFile>> {
        if !self.merge_enabled || unmerged.len() < 2 {
            return Ok(unmerged);
        }

        let first_path = unmerged[0].manifest_path.clone();

        // Group by partition_spec_id, preserving input order within each group.
        let mut groups: HashMap<i32, Vec<ManifestFile>> = HashMap::new();
        for m in unmerged {
            groups
                .entry(m.partition_spec_id)
                .or_default()
                .push(m);
        }

        // Process spec ids in deterministic order so the result is stable.
        let mut spec_ids: Vec<i32> = groups.keys().copied().collect();
        spec_ids.sort_unstable_by(|a, b| b.cmp(a)); // reverse, matching Java

        // Build bins. Each bin is paired with its spec id and a sequence index
        // so we can preserve global ordering after the parallel merge.
        let packer = ListPacker::<ManifestFile>::new(self.target_size_bytes, 1, false);
        let mut sequenced_bins: Vec<(usize, i32, Vec<ManifestFile>)> = Vec::new();
        let mut seq = 0usize;
        for spec_id in spec_ids {
            let group = groups.remove(&spec_id).expect("spec id was in keys");
            for bin in packer.pack_end(group, |m| m.manifest_length as u64) {
                sequenced_bins.push((seq, spec_id, bin));
                seq += 1;
            }
        }

        // Run bins concurrently with a bounded fan-out.
        let merge_futures = sequenced_bins
            .into_iter()
            .map(|(idx, spec_id, bin)| {
                let first_path = first_path.clone();
                async move {
                    let out = self.process_bin(producer, spec_id, bin, &first_path).await?;
                    Ok::<_, crate::Error>((idx, out))
                }
            });

        let mut results: Vec<(usize, Vec<ManifestFile>)> =
            futures::stream::iter(merge_futures)
                .buffer_unordered(MANIFEST_WRITE_CONCURRENCY)
                .try_collect()
                .await?;
        results.sort_by_key(|(idx, _)| *idx);

        Ok(results.into_iter().flat_map(|(_, mfs)| mfs).collect())
    }

    async fn process_bin(
        &self,
        producer: &SnapshotProducer<'_>,
        spec_id: i32,
        bin: Vec<ManifestFile>,
        first_path: &str,
    ) -> Result<Vec<ManifestFile>> {
        // Singleton bins pass through unchanged.
        if bin.len() == 1 {
            return Ok(bin);
        }
        // First-guard: if the bin contains the new first manifest and isn't yet
        // big enough to warrant a merge, leave the whole bin alone. Mirrors Java
        // ManifestMergeManager.mergeGroup ("if bin.contains(first) && bin.size()
        // < minCountToMerge").
        let contains_first = bin.iter().any(|m| m.manifest_path == first_path);
        if contains_first && (bin.len() as u32) < self.min_count_to_merge {
            return Ok(bin);
        }
        // Otherwise merge into a single manifest.
        let merged = self.create_manifest(producer, spec_id, bin).await?;
        Ok(vec![merged])
    }

    async fn create_manifest(
        &self,
        producer: &SnapshotProducer<'_>,
        spec_id: i32,
        bin: Vec<ManifestFile>,
    ) -> Result<ManifestFile> {
        let cache_key: Vec<String> = bin.iter().map(|m| m.manifest_path.clone()).collect();

        // Lock-clone-release: hold the mutex only long enough to read the cached
        // value. Never hold the guard across an `.await`.
        if let Some(cached) = {
            let guard = self.cache.lock().expect("merge cache mutex");
            guard.get(&cache_key).cloned()
        } {
            return Ok(cached);
        }

        let snap_id = producer.snapshot_id();
        let metadata = producer.table.metadata();
        let file_io = producer.table.file_io();

        // Derive the partition spec for this bin. The first manifest's spec is
        // authoritative because the merge rule never crosses spec boundaries.
        let spec = metadata
            .partition_spec_by_id(spec_id)
            .ok_or_else(|| {
                crate::Error::new(
                    crate::ErrorKind::DataInvalid,
                    format!("merge: partition spec id {spec_id} not in table metadata"),
                )
            })?
            .as_ref()
            .clone();
        let schema = metadata.current_schema().clone();
        let fmt = metadata.format_version();

        let path = format!(
            "{}/{}/{}-merge-{}.{}",
            metadata.location(),
            META_ROOT_PATH,
            producer.commit_uuid(),
            merge_suffix(&cache_key),
            DataFileFormat::Avro,
        );

        let output_file = file_io.new_output(&path)?;
        let builder = ManifestWriterBuilder::new(
            output_file,
            Some(snap_id),
            producer.key_metadata().map(<[u8]>::to_vec),
            schema,
            spec,
        );
        // Use the content type of the first source manifest. The merge step never
        // crosses content boundaries because data and delete manifests are
        // managed by separate `ManifestMergeManager`s in the Java spec; we only
        // build the data path here.
        let content = bin[0].content;
        let mut writer = match (fmt, content) {
            (FormatVersion::V1, _) => builder.build_v1(),
            (FormatVersion::V2, ManifestContentType::Data) => builder.build_v2_data(),
            (FormatVersion::V2, ManifestContentType::Deletes) => builder.build_v2_deletes(),
            (FormatVersion::V3, ManifestContentType::Data) => builder.build_v3_data(),
            (FormatVersion::V3, ManifestContentType::Deletes) => builder.build_v3_deletes(),
        };

        for source in &bin {
            let manifest = source.load_manifest(file_io).await?;
            for entry in manifest.entries() {
                let mut e = entry.as_ref().clone();
                e.inherit_data(source);
                match e.status() {
                    ManifestStatus::Deleted => {
                        // Suppress prior-snapshot tombstones. Only this commit's
                        // tombstones are written into the merged manifest;
                        // historical tombstones are dropped, which is what stops
                        // tombstone bloat across many compacts.
                        if e.snapshot_id == Some(snap_id) {
                            writer.add_delete_entry(e)?;
                        }
                    }
                    ManifestStatus::Added => {
                        if e.snapshot_id == Some(snap_id) {
                            writer.add_entry(e)?;
                        } else {
                            // ADDED from a prior snapshot → demote to EXISTING.
                            // Sequence numbers must come from the inherited entry,
                            // unmodified, so equality deletes covering the demoted
                            // file continue to apply.
                            writer.add_existing_file(
                                e.data_file.clone(),
                                e.snapshot_id.unwrap_or(0),
                                e.sequence_number.unwrap_or(0),
                                e.file_sequence_number,
                            )?;
                        }
                    }
                    ManifestStatus::Existing => {
                        writer.add_existing_file(
                            e.data_file.clone(),
                            e.snapshot_id.unwrap_or(0),
                            e.sequence_number.unwrap_or(0),
                            e.file_sequence_number,
                        )?;
                    }
                }
            }
        }

        let merged = writer.write_manifest_file().await?;

        // Cache + bookkeeping. The mutex is held only briefly across two simple
        // inserts; no `.await` in scope.
        {
            let mut guard = self.cache.lock().expect("merge cache mutex");
            guard.insert(cache_key, merged.clone());
        }
        {
            let mut guard = self.cleanup_paths.lock().expect("merge cleanup mutex");
            guard.push(merged.manifest_path.clone());
        }
        // Count source manifests as replaced — but only those carried over from
        // prior snapshots. The "first" manifest from the current commit isn't
        // counted as replaced by the merge (it was created by *this* commit).
        for source in &bin {
            if source.added_snapshot_id != snap_id {
                self.replaced_count.fetch_add(1, Ordering::Relaxed);
            }
        }

        Ok(merged)
    }

    /// Best-effort delete of any merged manifests not present in `committed_paths`.
    /// IO errors are logged but never fail the caller — orphans get caught by the
    /// orphan-file sweep later.
    pub(crate) async fn clean_uncommitted(
        &self,
        file_io: &crate::io::FileIO,
        committed_paths: &std::collections::HashSet<String>,
    ) {
        let entries: Vec<(Vec<String>, ManifestFile)> = {
            let guard = self.cache.lock().expect("merge cache mutex");
            guard.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
        };

        for (key, merged) in entries {
            if committed_paths.contains(&merged.manifest_path) {
                continue;
            }
            if let Err(e) = file_io.delete(&merged.manifest_path).await {
                warn!(
                    path = %merged.manifest_path,
                    error = %e,
                    "manifest_merge: best-effort delete of orphan merge failed; \
                     will be reclaimed by the orphan-file sweeper"
                );
            }
            // Drop the entry from the cache and roll back the replaced-count
            // bump that this merge contributed. Mirrors Java
            // ManifestMergeManager.cleanUncommitted, but uses bin size as the
            // upper bound on the rollback (Java decrements per source manifest
            // whose snapshotId differs from the producer's; we don't keep that
            // fidelity here because the cache key only stores paths). The next
            // merge call recomputes from scratch.
            let mut guard = self.cache.lock().expect("merge cache mutex");
            if guard.remove(&key).is_some() {
                self.replaced_count
                    .fetch_sub(key.len().saturating_sub(1) as u64, Ordering::Relaxed);
            }
        }
    }
}

/// Stable suffix derived from a bin's source paths so that retries on the same
/// bin produce the same merged-manifest path. Hashes the concatenated paths.
fn merge_suffix(source_paths: &[String]) -> String {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let mut hasher = DefaultHasher::new();
    for p in source_paths {
        p.hash(&mut hasher);
        // Use a separator unlikely to appear inside a manifest path, so two
        // different splits produce different hashes.
        '\0'.hash(&mut hasher);
    }
    format!("{:016x}", hasher.finish())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merge_suffix_is_stable_and_distinguishing() {
        let a = merge_suffix(&["m1.avro".to_string(), "m2.avro".to_string()]);
        let b = merge_suffix(&["m1.avro".to_string(), "m2.avro".to_string()]);
        assert_eq!(a, b, "stable across calls");

        let c = merge_suffix(&["m2.avro".to_string(), "m1.avro".to_string()]);
        assert_ne!(a, c, "ordering changes the suffix");
    }

    #[test]
    fn replaced_count_starts_at_zero() {
        let mgr = ManifestMergeManager::new(1024, 100, true);
        assert_eq!(mgr.replaced_manifests_count(), 0);
        assert!(mgr.cleanup_paths().is_empty());
    }
}
