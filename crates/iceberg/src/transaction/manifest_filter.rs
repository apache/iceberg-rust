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

use std::collections::HashSet;

use crate::error::Result;
use crate::spec::{DataFile, ManifestFile};
use crate::table::Table;
use crate::transaction::snapshot::SnapshotProducer;

/// Accumulates the set of files an operation removes from a table and rewrites the
/// affected manifests during manifest production.
///
/// A delete-class operation owns one `ManifestFilterManager` for data manifests and one for
/// delete manifests. Removed files are recorded via [`ManifestFilterManager::delete_file`]
/// keyed by their file path; the later filtering pass drops the matching manifest entries
/// and re-emits the survivors.
///
/// **In v1 this always rewrites**: there is no cross-retry rewrite cache (that is deferred
/// to future work). The rewrite body is a placeholder / pass-through for now — the filter
/// seam is the normative part. The single `Mutex<MergingCache>` on `MergingSnapshotProducer`
/// is retained precisely so the deferred cache has a home.
#[derive(Default)]
pub(crate) struct ManifestFilterManager {
    /// Files to drop, keyed by file path. `DataFile` covers both data and delete files.
    #[allow(dead_code)]
    deleted_files: HashSet<String>,
}

impl ManifestFilterManager {
    /// Record a file for removal.
    ///
    /// `DataFile` covers both data and delete files, so the same entry point serves
    /// data-manifest and delete-manifest filtering. Removals are keyed by file path;
    /// recording the same path more than once keeps a single removal entry.
    #[allow(unused)]
    pub(crate) fn delete_file(&mut self, file: DataFile) {
        self.deleted_files.insert(file.file_path().to_string());
    }

    /// Rewrite the given `manifests`, dropping any entries recorded for removal and
    /// re-emitting the survivors.
    ///
    /// **v1 always rewrites**: every input manifest goes through the rewrite path
    /// unconditionally on every attempt — there is no cache lookup/store. The rewrite body
    /// is a placeholder (pass-through) for now; the filter seam is the normative part.
    #[allow(unused)]
    pub(crate) async fn filter_manifests(
        &self,
        sp: &mut SnapshotProducer<'_>,
        base: &Table,
        manifests: Vec<ManifestFile>,
    ) -> Result<Vec<ManifestFile>> {
        // TODO(future): cache rewritten manifests per input manifest path to avoid
        // re-writing (and orphaning) on retry. Deferred from v1; would read/write
        // MergingCache.filter_cache under a brief lock (never held across IO).
        let mut out = Vec::with_capacity(manifests.len());
        for manifest in manifests {
            // PLACEHOLDER: the real rewrite drops entries in `deleted_files` and re-emits
            // survivors via `sp`. For now this is effectively pass-through. v1 rewrites
            // unconditionally on every attempt (no cache short-circuit).
            let rewritten = self.rewrite_placeholder(sp, base, &manifest).await?;
            out.extend(rewritten);
        }
        Ok(out)
    }

    /// Placeholder rewrite body: passes the manifest through unchanged.
    ///
    /// The real implementation will load the manifest, drop entries whose file path is in
    /// `deleted_files`, and re-emit the survivors through a producer-provided manifest
    /// writer. It is intentionally left as a pass-through in v1.
    #[allow(unused)]
    async fn rewrite_placeholder(
        &self,
        _sp: &mut SnapshotProducer<'_>,
        _base: &Table,
        manifest: &ManifestFile,
    ) -> Result<Vec<ManifestFile>> {
        Ok(vec![manifest.clone()])
    }
}
