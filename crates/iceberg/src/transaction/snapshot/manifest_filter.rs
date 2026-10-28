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

//! Rewrites existing manifests to mark files as deleted.

use std::collections::{HashMap, HashSet};

use uuid::Uuid;

use super::{manifest_path, new_manifest_writer};
use crate::error::{Result, invalid_data};
use crate::io::object_cache::ManifestKey;
use crate::spec::{DataFile, FormatVersion, ManifestContentType, ManifestFile};
use crate::table::Table;

/// Removes a fixed set of files from the manifests of one content type.
///
/// One manager lives for one logical execution of an action and is driven
/// once per attempt via [`ManifestFilterManager::filter`]. A manifest that
/// references none of the files passes through untouched. One that does is
/// rewritten with those entries marked `Deleted` and the rest `Existing`.
///
/// A rewrite is reused across attempts while its inputs are unchanged. Its
/// entries carry the sequence numbers and row ids the source's entries
/// inherited from the manifest list when they were read, so the reuse key is
/// the source's [`ManifestKey`], not its path alone: a transaction-local
/// source manifest keeps its path across attempts but inherits new values
/// after a rebase.
pub(crate) struct ManifestFilterManager {
    content: ManifestContentType,
    /// Paths of the files to remove. Fixed for the execution.
    files_to_remove: HashSet<String>,
    /// Source manifest → rewritten result.
    filtered: HashMap<ManifestKey, FilteredManifest>,
    /// Format version the rewrites were written under. Per spec, v1 entries
    /// cannot inherit sequence numbers, so an upgrade invalidates them.
    format_version: Option<FormatVersion>,
}

/// One rewritten manifest and the files it marked as deleted.
#[derive(Clone)]
struct FilteredManifest {
    manifest: ManifestFile,
    removed_files: Vec<DataFile>,
}

/// The result of one filter pass.
pub(crate) struct Filtered {
    /// The resulting manifests, in input order.
    pub manifests: Vec<ManifestFile>,
    /// The files that were live and are now marked deleted, including their
    /// metrics as read from the source manifests.
    pub removed_files: Vec<DataFile>,
}

impl ManifestFilterManager {
    pub(crate) fn new(content: ManifestContentType, files_to_remove: &[DataFile]) -> Self {
        Self {
            content,
            files_to_remove: files_to_remove
                .iter()
                .map(|f| f.file_path().to_string())
                .collect(),
            filtered: HashMap::new(),
            format_version: None,
        }
    }

    /// Applies the removals to `manifests`, rewriting those of this manager's
    /// content type that reference a file to remove.
    ///
    /// Fails if a file to remove is not live in any manifest: it was never in
    /// the table, or a concurrent commit already removed it.
    ///
    /// Rewritten manifests take their paths from `next_manifest_number` and are
    /// recorded in `owned_artifacts` before being written, so a failed write
    /// still leaves its path tracked for cleanup.
    pub(crate) async fn filter(
        &mut self,
        table: &Table,
        snapshot_id: i64,
        commit_uuid: Uuid,
        manifests: Vec<ManifestFile>,
        next_manifest_number: &mut u64,
        owned_artifacts: &mut HashSet<String>,
    ) -> Result<Filtered> {
        if self.files_to_remove.is_empty() {
            return Ok(Filtered {
                manifests,
                removed_files: Vec::new(),
            });
        }

        let format_version = table.metadata().format_version();
        if self.format_version != Some(format_version) {
            self.filtered.clear();
            self.format_version = Some(format_version);
        }

        let mut out = Vec::with_capacity(manifests.len());
        let mut removed_files: Vec<DataFile> = Vec::new();

        for source in manifests {
            if source.content != self.content {
                out.push(source);
                continue;
            }

            let key = ManifestKey::new(&source);
            let result = match self.filtered.get(&key) {
                Some(cached) => cached.clone(),
                None => {
                    let result = self
                        .rewrite(
                            table,
                            snapshot_id,
                            commit_uuid,
                            &source,
                            next_manifest_number,
                            owned_artifacts,
                        )
                        .await?;
                    self.filtered.insert(key, result.clone());
                    result
                }
            };

            removed_files.extend(result.removed_files);
            out.push(result.manifest);
        }

        let found: HashSet<&str> = removed_files.iter().map(|f| f.file_path()).collect();
        let mut missing: Vec<&str> = self
            .files_to_remove
            .iter()
            .map(String::as_str)
            .filter(|p| !found.contains(p))
            .collect();
        if !missing.is_empty() {
            missing.sort_unstable();
            return Err(invalid_data!(
                "Cannot remove files that are not live in the table: {}",
                missing.join(", ")
            ));
        }

        Ok(Filtered {
            manifests: out,
            removed_files,
        })
    }

    /// Reads `source` and, if it references any file to remove, writes a copy
    /// with those entries marked `Deleted` and the rest `Existing`.
    async fn rewrite(
        &self,
        table: &Table,
        snapshot_id: i64,
        commit_uuid: Uuid,
        source: &ManifestFile,
        next_manifest_number: &mut u64,
        owned_artifacts: &mut HashSet<String>,
    ) -> Result<FilteredManifest> {
        let manifest = table.object_cache().get_manifest(source).await?;

        let removed_files: Vec<DataFile> = manifest
            .entries()
            .iter()
            .filter(|e| e.is_alive() && self.files_to_remove.contains(e.file_path()))
            .map(|e| e.data_file().clone())
            .collect();
        if removed_files.is_empty() {
            return Ok(FilteredManifest {
                manifest: source.clone(),
                removed_files,
            });
        }

        let path = manifest_path(table, commit_uuid, *next_manifest_number)?;
        *next_manifest_number += 1;
        owned_artifacts.insert(path.clone());

        // Keep the source manifest's spec: its entries' partition values are
        // laid out for that spec, not the table's current default.
        let spec = manifest.metadata().partition_spec().clone();
        let mut writer = new_manifest_writer(
            table,
            path,
            snapshot_id,
            self.content,
            table.metadata().current_schema().clone(),
            spec.into(),
        )?;
        for entry in manifest.entries() {
            if !entry.is_alive() {
                continue;
            }
            let entry = (**entry).clone();
            if self.files_to_remove.contains(entry.file_path()) {
                writer.add_delete_entry(entry)?;
            } else {
                writer.add_existing_entry(entry)?;
            }
        }
        let manifest = writer.write_manifest_file().await?;

        Ok(FilteredManifest {
            manifest,
            removed_files,
        })
    }
}
