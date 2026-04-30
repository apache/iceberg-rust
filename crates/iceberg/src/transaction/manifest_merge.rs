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

//! Merge pass: groups siblings by partition spec, bin-packs them with `ListPacker`,
//! and rewrites multi-element bins into a single merged manifest. This is what
//! stops manifest count from growing linearly with commit count, and where
//! prior-snapshot tombstones are suppressed.
//!
//! Java analog: `org.apache.iceberg.ManifestMergeManager`.
//!
//! Concurrency footgun: locks on `cache` and `cleanup_paths` must NEVER be held
//! across an `.await`. `DashMap` looks tempting but its `RefMut` guard is not
//! async-aware and can deadlock if held across an await point.

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
use crate::prptl_utils::bin_packing::ListPacker;

/// Bounds storage-client write concurrency and per-task working-set memory
/// (each task buffers a full bin's worth of entries while writing).
const MANIFEST_WRITE_CONCURRENCY: usize = 8;

pub(crate) struct ManifestMergeManager {
    target_size_bytes: u64,
    min_count_to_merge: u32,
    merge_enabled: bool,
    /// `bin (source paths) → merged manifest`. Persists across retries so the
    /// same input bin returns the same `ManifestFile` — Java's idempotent retry
    /// contract.
    cache: Arc<Mutex<HashMap<Vec<String>, ManifestFile>>>,
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

    /// `unmerged[0]` must be the new "first" manifest this commit produced; the
    /// bin containing it is exempted when it has fewer than `min_count_to_merge`
    /// siblings, so a cluster of small commits doesn't trigger a historical rewrite.
    pub(crate) async fn merge_manifests(
        &self,
        producer: &SnapshotProducer<'_>,
        unmerged: Vec<ManifestFile>,
    ) -> Result<Vec<ManifestFile>> {
        if !self.merge_enabled || unmerged.len() < 2 {
            return Ok(unmerged);
        }

        let first_path = unmerged[0].manifest_path.clone();

        let mut groups: HashMap<i32, Vec<ManifestFile>> = HashMap::new();
        for m in unmerged {
            groups
                .entry(m.partition_spec_id)
                .or_default()
                .push(m);
        }

        // Reverse-sorted spec ids match Java's iteration order, keeping output
        // diffs comparable across engines.
        let mut spec_ids: Vec<i32> = groups.keys().copied().collect();
        spec_ids.sort_unstable_by(|a, b| b.cmp(a));

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
        if bin.len() == 1 {
            return Ok(bin);
        }
        // First-guard (Java parity): a small bin holding the new first manifest
        // passes through unmerged so a cluster of small commits doesn't trigger
        // a historical rewrite.
        let contains_first = bin.iter().any(|m| m.manifest_path == first_path);
        if contains_first && (bin.len() as u32) < self.min_count_to_merge {
            return Ok(bin);
        }
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

        // Lock-clone-release: never hold a Mutex guard across an `.await`.
        if let Some(cached) = {
            let guard = self.cache.lock().expect("merge cache mutex");
            guard.get(&cache_key).cloned()
        } {
            return Ok(cached);
        }

        let snap_id = producer.snapshot_id();
        let metadata = producer.table.metadata();
        let file_io = producer.table.file_io();

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
        // The merge never crosses content boundaries: data and delete manifests
        // get separate managers in Java, and only the data path is built here.
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
                        // Prior-snapshot tombstones are dropped here — the load-bearing
                        // rule that stops tombstone bloat across many compacts.
                        if e.snapshot_id == Some(snap_id) {
                            writer.add_delete_entry(e)?;
                        }
                    }
                    ManifestStatus::Added => {
                        if e.snapshot_id == Some(snap_id) {
                            writer.add_entry(e)?;
                        } else {
                            // Demote ADDED→EXISTING with the inherited sequence numbers
                            // unmodified. Re-stamping breaks equality-delete coverage.
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

        {
            let mut guard = self.cache.lock().expect("merge cache mutex");
            guard.insert(cache_key, merged.clone());
        }
        {
            let mut guard = self.cleanup_paths.lock().expect("merge cleanup mutex");
            guard.push(merged.manifest_path.clone());
        }
        // Only count manifests carried in from prior snapshots — the "first"
        // manifest from this commit wasn't replaced by the merge, it was created.
        for source in &bin {
            if source.added_snapshot_id != snap_id {
                self.replaced_count.fetch_add(1, Ordering::Relaxed);
            }
        }

        Ok(merged)
    }

    /// Best-effort delete of cached merges not in `committed_paths`. IO errors
    /// are logged, never propagated.
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
                    "manifest_merge: orphan merge delete failed; orphan-file sweeper will reclaim"
                );
            }
            // Roll back the cache + count for this bin. The Java analog decrements
            // per prior-snapshot source; the cache key only stores paths so we use
            // bin size as the upper bound. The next merge recomputes from scratch.
            let mut guard = self.cache.lock().expect("merge cache mutex");
            if guard.remove(&key).is_some() {
                self.replaced_count
                    .fetch_sub(key.len().saturating_sub(1) as u64, Ordering::Relaxed);
            }
        }
    }
}

/// Stable hash of a bin's source paths, so retries on the same bin produce the
/// same merged-manifest path and the cache short-circuits.
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
        assert_eq!(a, b);
        // Bin order is part of the cache key, so it must change the suffix too.
        let c = merge_suffix(&["m2.avro".to_string(), "m1.avro".to_string()]);
        assert_ne!(a, c);
    }

    #[test]
    fn replaced_count_starts_at_zero() {
        let mgr = ManifestMergeManager::new(1024, 100, true);
        assert_eq!(mgr.replaced_manifests_count(), 0);
    }
}
