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

//! Residual-write pass for the merging snapshot producer.
//!
//! Manifests with no replaced entries are carried forward unchanged. Manifests
//! whose alive entries are all being replaced are dropped. Mixed manifests are
//! rewritten as residuals containing only the survivors, with original sequence
//! numbers preserved.
//!
//! Java analog: `org.apache.iceberg.ManifestFilterManager`.

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use futures::stream::{StreamExt, TryStreamExt};
use tracing::warn;

use crate::Result;
use crate::spec::{
    DataFileFormat, FormatVersion, Manifest, ManifestContentType, ManifestEntry,
    ManifestFile, ManifestWriterBuilder,
};
use crate::transaction::snapshot::{META_ROOT_PATH, SnapshotProducer};

/// Cap concurrent manifest reads so a table with thousands of manifests doesn't
/// blow out the storage client's connection pool.
const MANIFEST_READ_CONCURRENCY: usize = 32;

pub(crate) struct ManifestFilterManager {
    deleted_paths: Mutex<HashSet<String>>,
    /// `source_manifest_path → rewritten_residual (None = fully dropped)`. Persists
    /// across commit retries — Java's idempotent retry contract.
    cache: Mutex<HashMap<String, Option<ManifestFile>>>,
    /// Drives the `manifests-replaced` snapshot summary key.
    replaced_count: AtomicU64,
}

impl ManifestFilterManager {
    pub(crate) fn new() -> Self {
        Self {
            deleted_paths: Mutex::new(HashSet::new()),
            cache: Mutex::new(HashMap::new()),
            replaced_count: AtomicU64::new(0),
        }
    }

    /// Mark a path for removal in the next filter pass.
    pub(crate) fn delete(&self, path: String) {
        self.deleted_paths
            .lock()
            .expect("filter deleted-paths mutex")
            .insert(path);
    }

    pub(crate) fn replaced_manifests_count(&self) -> u64 {
        self.replaced_count.load(Ordering::Relaxed)
    }

    /// Drop manifests whose alive entries are all being replaced; rewrite mixed
    /// manifests as residuals; pass everything else through. Cache hits short-circuit
    /// retries.
    pub(crate) async fn filter_manifests(
        &self,
        producer: &SnapshotProducer<'_>,
        current_manifests: Vec<ManifestFile>,
    ) -> Result<Vec<ManifestFile>> {
        // Lock-clone-release: never hold a Mutex guard across an `.await`.
        let deleted_paths: HashSet<String> = {
            let g = self.deleted_paths.lock().expect("filter deleted-paths mutex");
            g.clone()
        };
        if deleted_paths.is_empty() || current_manifests.is_empty() {
            return Ok(current_manifests);
        }

        let object_cache = producer.table.object_cache();

        let read_futures = current_manifests.into_iter().map(|m| {
            let object_cache = object_cache.clone();
            async move {
                let loaded = object_cache.get_manifest(&m).await?;
                Ok::<_, crate::Error>((m, loaded))
            }
        });
        let mut loaded: Vec<(ManifestFile, std::sync::Arc<Manifest>)> = futures::stream::iter(read_futures)
            .buffer_unordered(MANIFEST_READ_CONCURRENCY)
            .try_collect()
            .await?;
        // Restore deterministic order by source manifest path.
        loaded.sort_by(|(a, _), (b, _)| a.manifest_path.cmp(&b.manifest_path));

        let mut result = Vec::with_capacity(loaded.len());
        for (ml_entry, manifest) in loaded.into_iter() {
            // Cache hit: Some(mf) = prior residual, None = was fully dropped.
            if let Some(cached_opt) = {
                let g = self.cache.lock().expect("filter cache mutex");
                g.get(&ml_entry.manifest_path).cloned()
            } {
                if let Some(cached) = cached_opt {
                    result.push(cached);
                }
                continue;
            }

            // Single pass over entries: classify and bucket survivors at once,
            // tracking whether anything was deleted from this manifest.
            let mut had_deletion = false;
            let mut survivors: Vec<&ManifestEntry> = Vec::new();
            for entry in manifest.entries() {
                if !entry.is_alive() {
                    continue;
                }
                if deleted_paths.contains(entry.file_path()) {
                    had_deletion = true;
                } else {
                    survivors.push(entry.as_ref());
                }
            }

            if !had_deletion {
                result.push(ml_entry);
                continue;
            }

            if survivors.is_empty() {
                self.replaced_count.fetch_add(1, Ordering::Relaxed);
                self.cache
                    .lock()
                    .expect("filter cache mutex")
                    .insert(ml_entry.manifest_path.clone(), None);
                continue;
            }

            let residual = write_residual(producer, &ml_entry, &survivors).await?;
            {
                let mut g = self.cache.lock().expect("filter cache mutex");
                g.insert(ml_entry.manifest_path.clone(), Some(residual.clone()));
            }
            self.replaced_count.fetch_add(1, Ordering::Relaxed);
            result.push(residual);
        }

        Ok(result)
    }

    /// Best-effort delete of uncommitted residuals not in `committed_paths`. IO errors
    /// are logged, never propagated. `None` cache entries (fully-dropped manifests)
    /// are kept for retry idempotency.
    pub(crate) async fn clean_uncommitted(
        &self,
        file_io: &crate::io::FileIO,
        committed_paths: &HashSet<String>,
    ) {
        let entries: Vec<(String, Option<ManifestFile>)> = {
            let g = self.cache.lock().expect("filter cache mutex");
            g.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
        };
        for (source_path, cached) in entries {
            let Some(ref mf) = cached else {
                continue;
            };
            if committed_paths.contains(&mf.manifest_path) {
                continue;
            }
            if let Err(e) = file_io.delete(&mf.manifest_path).await {
                warn!(
                    path = %mf.manifest_path,
                    error = %e,
                    "manifest_filter: orphan residual delete failed; orphan-file sweeper will reclaim"
                );
            }
            self.cache
                .lock()
                .expect("filter cache mutex")
                .remove(&source_path);
        }
    }
}

/// Write a residual manifest containing only `source`'s survivors. Sequence
/// numbers are inherited from the source list entry and re-emitted unmodified —
/// equality-delete coverage breaks if they're re-stamped to the current snapshot.
async fn write_residual(
    producer: &SnapshotProducer<'_>,
    source: &ManifestFile,
    survivors: &[&ManifestEntry],
) -> Result<ManifestFile> {
    let file_io = producer.table.file_io();
    let metadata = producer.table.metadata();
    let schema = metadata.current_schema().clone();
    let spec = metadata.default_partition_spec().as_ref().clone();
    let fmt = metadata.format_version();

    // Stable per-source suffix so retries reproduce the same path and the cache
    // can short-circuit. `commit_uuid` keeps paths from colliding across actions.
    let path = format!(
        "{location}/{root}/{uuid}-residual-{suffix}.{ext}",
        location = metadata.location(),
        root = META_ROOT_PATH,
        uuid = producer.commit_uuid(),
        suffix = residual_suffix(&source.manifest_path),
        ext = DataFileFormat::Avro,
    );

    let output_file = file_io.new_output(path)?;
    let builder = ManifestWriterBuilder::new(
        output_file,
        Some(producer.snapshot_id()),
        producer.key_metadata().map(<[u8]>::to_vec),
        schema,
        spec,
    );
    let mut writer = match (fmt, source.content) {
        (FormatVersion::V1, _) => builder.build_v1(),
        (FormatVersion::V2, ManifestContentType::Data) => builder.build_v2_data(),
        (FormatVersion::V2, ManifestContentType::Deletes) => builder.build_v2_deletes(),
        (FormatVersion::V3, ManifestContentType::Data) => builder.build_v3_data(),
        (FormatVersion::V3, ManifestContentType::Deletes) => builder.build_v3_deletes(),
    };
    for me in survivors {
        writer.add_existing_file(
            me.data_file.clone(),
            me.snapshot_id.unwrap_or(0),
            me.sequence_number.unwrap_or(0),
            me.file_sequence_number,
        )?;
    }
    writer.write_manifest_file().await
}

/// Hash a source manifest's basename to a stable 16-char hex suffix. Same input
/// produces the same suffix across retries, letting the cache short-circuit
/// rewrites that would otherwise write the same residual twice.
fn residual_suffix(source_path: &str) -> String {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let basename = source_path
        .rsplit('/')
        .next()
        .unwrap_or(source_path)
        .trim_end_matches(".avro");
    let mut hasher = DefaultHasher::new();
    basename.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::FileIO;

    fn fake_manifest(path: &str) -> ManifestFile {
        ManifestFile {
            manifest_path: path.to_string(),
            manifest_length: 0,
            partition_spec_id: 0,
            content: ManifestContentType::Data,
            sequence_number: 0,
            min_sequence_number: 0,
            added_snapshot_id: 0,
            added_files_count: None,
            existing_files_count: None,
            deleted_files_count: None,
            added_rows_count: None,
            existing_rows_count: None,
            deleted_rows_count: None,
            partitions: None,
            key_metadata: None,
            first_row_id: None,
        }
    }

    #[test]
    fn residual_suffix_is_stable() {
        let a = residual_suffix("s3://bucket/table/metadata/abc-m0.avro");
        let b = residual_suffix("s3://bucket/table/metadata/abc-m0.avro");
        assert_eq!(a, b);
        let c = residual_suffix("s3://bucket/table/metadata/different-m0.avro");
        assert_ne!(a, c);
    }

    #[test]
    fn delete_dedupes_paths() {
        let filter = ManifestFilterManager::new();
        filter.delete("data/x.parquet".to_string());
        filter.delete("data/x.parquet".to_string());
        assert_eq!(
            filter.deleted_paths.lock().expect("test mutex").len(),
            1
        );
    }

    /// clean_uncommitted removes cache entries for residuals not in committed_paths.
    #[tokio::test]
    async fn clean_uncommitted_removes_uncommitted_residual_from_cache() {
        let mgr = ManifestFilterManager::new();
        let residual = fake_manifest("mem:///meta/residual.avro");
        mgr.cache
            .lock()
            .unwrap()
            .insert("mem:///meta/source.avro".to_string(), Some(residual));

        mgr.clean_uncommitted(&FileIO::new_with_memory(), &HashSet::new())
            .await;

        assert!(
            mgr.cache.lock().unwrap().is_empty(),
            "uncommitted residual should be removed from cache"
        );
    }

    /// clean_uncommitted preserves cache entries for residuals in committed_paths.
    #[tokio::test]
    async fn clean_uncommitted_preserves_committed_residual_in_cache() {
        let mgr = ManifestFilterManager::new();
        let residual = fake_manifest("mem:///meta/residual.avro");
        mgr.cache
            .lock()
            .unwrap()
            .insert("mem:///meta/source.avro".to_string(), Some(residual.clone()));

        let mut committed = HashSet::new();
        committed.insert("mem:///meta/residual.avro".to_string());
        mgr.clean_uncommitted(&FileIO::new_with_memory(), &committed)
            .await;

        assert_eq!(
            mgr.cache.lock().unwrap().get("mem:///meta/source.avro"),
            Some(&Some(residual)),
            "committed residual must stay in cache"
        );
    }

    /// clean_uncommitted keeps None (fully-dropped) entries for retry idempotency.
    #[tokio::test]
    async fn clean_uncommitted_keeps_dropped_sentinel_in_cache() {
        let mgr = ManifestFilterManager::new();
        mgr.cache
            .lock()
            .unwrap()
            .insert("mem:///meta/source.avro".to_string(), None);

        mgr.clean_uncommitted(&FileIO::new_with_memory(), &HashSet::new())
            .await;

        assert!(
            mgr.cache
                .lock()
                .unwrap()
                .contains_key("mem:///meta/source.avro"),
            "None sentinel must be kept for retry idempotency"
        );
    }
}
