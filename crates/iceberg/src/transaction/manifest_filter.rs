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

//! Filter pass for the merging snapshot producer.
//!
//! For every manifest in the current snapshot's manifest list, decide whether the
//! manifest must be rewritten because some of its alive entries are being replaced
//! by this commit. A manifest with no overlap is carried forward as-is; a manifest
//! whose alive entries are all being replaced is dropped from the new manifest list
//! entirely; a manifest with both replaced and surviving entries is rewritten as a
//! "residual" manifest containing only the survivors, with their original sequence
//! numbers preserved.
//!
//! Java analog: `org.apache.iceberg.ManifestFilterManager`.

#![allow(dead_code)] // Wired up in the rewrite_files.rs commit.

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use futures::stream::{StreamExt, TryStreamExt};
use tracing::warn;

use crate::Result;
use crate::spec::{
    DataFile, DataFileFormat, FormatVersion, Manifest, ManifestContentType, ManifestEntry,
    ManifestFile, ManifestWriterBuilder,
};
use crate::transaction::snapshot::{META_ROOT_PATH, SnapshotProducer};

/// Maximum number of manifests read concurrently during the filter pass.
/// Bounded to avoid exhausting the storage client's connection pool on tables
/// with very large manifest lists.
const MANIFEST_READ_CONCURRENCY: usize = 32;

pub(crate) struct ManifestFilterManager {
    /// Paths of files this commit replaces. Manifests containing any of these as
    /// alive entries will be rewritten.
    deleted_paths: Mutex<HashSet<String>>,
    /// Cache of `original_manifest_path → rewritten_residual`. Persists across
    /// commit retries so identical inputs produce identical residuals
    /// (idempotent retry contract — see brainstorm §5.5, §6.5).
    cache: Mutex<HashMap<String, ManifestFile>>,
    /// Paths written to the cache so far this attempt; consulted by
    /// `clean_uncommitted` on commit failure to delete orphan residuals.
    cleanup_paths: Mutex<Vec<String>>,
    /// Count of manifests this filter pass rewrote (residuals produced + dropped
    /// manifests). Surfaces as `manifests-replaced` in the snapshot summary.
    replaced_count: AtomicU64,
}

impl ManifestFilterManager {
    pub(crate) fn new() -> Self {
        Self {
            deleted_paths: Mutex::new(HashSet::new()),
            cache: Mutex::new(HashMap::new()),
            cleanup_paths: Mutex::new(Vec::new()),
            replaced_count: AtomicU64::new(0),
        }
    }

    /// Mark a path for removal during the next filter pass.
    pub(crate) fn delete(&self, file: &DataFile) {
        self.deleted_paths
            .lock()
            .expect("filter deleted-paths mutex")
            .insert(file.file_path.clone());
    }

    /// Number of manifests rewritten or dropped by the filter pass. Drives the
    /// `manifests-replaced` summary key.
    pub(crate) fn replaced_manifests_count(&self) -> u64 {
        self.replaced_count.load(Ordering::Relaxed)
    }

    /// Snapshot of the residual paths written this attempt. The
    /// `MergingState::clean_uncommitted` orchestrator consults the union with the
    /// merge manager's paths to decide which Avro files to delete on `Err`.
    pub(crate) fn cleanup_paths(&self) -> Vec<String> {
        self.cleanup_paths
            .lock()
            .expect("filter cleanup-paths mutex")
            .clone()
    }

    /// Rewrite the input manifest list so any manifest with replaced entries is
    /// either dropped (all alive entries replaced) or rewritten as a residual
    /// containing only the survivors (mixed). Manifests with no overlap pass
    /// through unchanged.
    ///
    /// Caches every rewritten manifest by source path, so a retry with the same
    /// inputs returns the same `ManifestFile` instances and no new files are
    /// written to object storage.
    pub(crate) async fn filter_manifests(
        &self,
        producer: &SnapshotProducer<'_>,
        current_manifests: Vec<ManifestFile>,
    ) -> Result<Vec<ManifestFile>> {
        // Lock-clone-release: take a snapshot of the current delete set, then
        // drop the guard before any `.await`.
        let deleted_paths: HashSet<String> = {
            let g = self.deleted_paths.lock().expect("filter deleted-paths mutex");
            g.clone()
        };
        if deleted_paths.is_empty() || current_manifests.is_empty() {
            return Ok(current_manifests);
        }

        let file_io = producer.table.file_io();

        // Bounded concurrent reads. Each future returns (original, loaded) so the
        // post-processing loop can iterate in input order — the cache key is the
        // source manifest's path, so result order doesn't actually matter for
        // correctness, but stable order makes diffs and tests deterministic.
        let read_futures = current_manifests.into_iter().map(|m| {
            let file_io = file_io.clone();
            async move {
                let loaded = m.load_manifest(&file_io).await?;
                Ok::<_, crate::Error>((m, loaded))
            }
        });
        let mut loaded: Vec<(ManifestFile, Manifest)> = futures::stream::iter(read_futures)
            .buffer_unordered(MANIFEST_READ_CONCURRENCY)
            .try_collect()
            .await?;
        // Restore deterministic order by source manifest path.
        loaded.sort_by(|(a, _), (b, _)| a.manifest_path.cmp(&b.manifest_path));

        let mut result = Vec::with_capacity(loaded.len());
        for (ml_entry, manifest) in loaded.into_iter() {
            // Cache check (lock-clone-release).
            if let Some(cached) = {
                let g = self.cache.lock().expect("filter cache mutex");
                g.get(&ml_entry.manifest_path).cloned()
            } {
                result.push(cached);
                continue;
            }

            let has_deletions = manifest
                .entries()
                .iter()
                .any(|e| e.is_alive() && deleted_paths.contains(e.file_path()));

            if !has_deletions {
                result.push(ml_entry);
                continue;
            }

            let survivors: Vec<&ManifestEntry> = manifest
                .entries()
                .iter()
                .filter(|e| e.is_alive() && !deleted_paths.contains(e.file_path()))
                .map(|e| e.as_ref())
                .collect();

            if survivors.is_empty() {
                // All alive entries are being replaced; drop the manifest. The
                // delete pass marks the entries as `Deleted` in the new snapshot.
                self.replaced_count.fetch_add(1, Ordering::Relaxed);
                continue;
            }

            let residual = write_residual(producer, &ml_entry, &survivors).await?;
            {
                let mut g = self.cleanup_paths.lock().expect("filter cleanup-paths mutex");
                g.push(residual.manifest_path.clone());
            }
            {
                let mut g = self.cache.lock().expect("filter cache mutex");
                g.insert(ml_entry.manifest_path.clone(), residual.clone());
            }
            self.replaced_count.fetch_add(1, Ordering::Relaxed);
            result.push(residual);
        }

        Ok(result)
    }

    /// Best-effort delete of any residual paths that didn't end up in the
    /// committed manifest set (e.g., because the commit retried and wrote a
    /// different residual on the second attempt). Storage IO errors are logged
    /// but never fail the caller — orphans get caught by the standard
    /// orphan-file sweep later.
    pub(crate) async fn clean_uncommitted(
        &self,
        file_io: &crate::io::FileIO,
        committed_paths: &HashSet<String>,
    ) {
        let paths_snapshot = {
            let g = self.cleanup_paths.lock().expect("filter cleanup-paths mutex");
            g.clone()
        };
        for path in paths_snapshot {
            if committed_paths.contains(&path) {
                continue;
            }
            if let Err(e) = file_io.delete(&path).await {
                warn!(
                    path = %path,
                    error = %e,
                    "manifest_filter: best-effort delete of orphan residual failed; \
                     will be reclaimed by the orphan-file sweeper"
                );
            }
        }
    }
}

/// Write a residual manifest containing only the survivors of `source`. The
/// surviving entries' sequence numbers are inherited from the source manifest
/// list entry and re-emitted unmodified — required for equality-delete
/// correctness (brainstorm §5.7 #5 / §16.5).
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

    // Use the source manifest's path stem (after stripping directory) plus a
    // stable suffix so retries hit identical paths and the cache prevents
    // re-writes. The `commit_uuid` keeps paths from colliding across actions.
    let path = format!(
        "{}/{}/{}-residual-{}.{}",
        metadata.location(),
        META_ROOT_PATH,
        producer.commit_uuid(),
        residual_suffix(&source.manifest_path),
        DataFileFormat::Avro,
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
        let mut me = (*me).clone();
        // Resolve any inherited fields (snapshot_id, sequence numbers) from the
        // source manifest list entry. `load_manifest` already does this internally,
        // but be explicit here so a future refactor that changes how manifests are
        // loaded doesn't silently lose the inheritance. Whatever the source entry's
        // status was (Added or Existing — survivors are alive entries that are not
        // being replaced), `add_existing_file` writes it as EXISTING in the residual,
        // matching the post-snapshot semantics of "files we already wrote and are
        // keeping."
        me.inherit_data(source);
        writer.add_existing_file(
            me.data_file.clone(),
            me.snapshot_id.unwrap_or(0),
            me.sequence_number.unwrap_or(0),
            me.file_sequence_number,
        )?;
    }
    writer.write_manifest_file().await
}

/// Stable filename component derived from a source manifest's path, so that retries
/// with the same source produce the same residual filename and the cache short-circuits
/// the second write. Uses the basename's leading 16 hex characters (post-hash) to keep
/// paths short.
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

    #[test]
    fn residual_suffix_is_stable() {
        let a = residual_suffix("s3://bucket/table/metadata/abc-m0.avro");
        let b = residual_suffix("s3://bucket/table/metadata/abc-m0.avro");
        assert_eq!(a, b, "same source path → same suffix");

        let c = residual_suffix("s3://bucket/table/metadata/different-m0.avro");
        assert_ne!(a, c, "different sources → different suffixes (with overwhelming probability)");
    }

    #[test]
    fn delete_dedupes_paths() {
        use crate::spec::{DataContentType, DataFileBuilder, DataFileFormat, Literal, Struct};
        let filter = ManifestFilterManager::new();
        let f = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("data/x.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(10)
            .record_count(1)
            .partition_spec_id(0)
            .partition(Struct::from_iter([Some(Literal::long(1))]))
            .build()
            .unwrap();
        filter.delete(&f);
        filter.delete(&f);
        assert_eq!(
            filter.deleted_paths.lock().expect("test mutex").len(),
            1
        );
    }
}
