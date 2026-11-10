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
use crate::spec::{DataFile, DataFileFormat, FormatVersion, ManifestContentType, ManifestFile};
use crate::table::Table;

/// Identifies one entry to mark as `Deleted`, a data file or a delete file.
///
/// A path alone is not enough: a Puffin file may hold several deletion
/// vectors, each its own entry at a distinct blob offset. Data files and
/// non-DV delete files have no offset, so for them this is the path.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct RemovedFileIdentity {
    /// The absolute path, as stored in the manifest entry. Comparison is
    /// textual, so a relative path would not match the entry it names.
    path: String,
    content_offset: Option<i64>,
    content_size_in_bytes: Option<i64>,
}

impl RemovedFileIdentity {
    fn of(file: &DataFile) -> Self {
        Self {
            path: file.file_path().to_string(),
            content_offset: file.content_offset(),
            content_size_in_bytes: file.content_size_in_bytes(),
        }
    }
}

impl std::fmt::Display for RemovedFileIdentity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match (self.content_offset, self.content_size_in_bytes) {
            (Some(offset), Some(size)) => write!(f, "{}@{offset}+{size}", self.path),
            _ => f.write_str(&self.path),
        }
    }
}

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
    /// The files to remove, by absolute path (and blob range for deletion
    /// vectors). Fixed for the execution.
    files_to_remove: HashSet<RemovedFileIdentity>,
    /// Whether a file to remove that is not live in any manifest fails the
    /// attempt. Off lets a retry succeed after a concurrent commit removed the
    /// same file; the action decides which it needs.
    fail_missing_delete_paths: bool,
    /// Absolute paths of the data files the commit removes. A deletion vector
    /// referencing one of them is dangling and is marked `Deleted` too. Not
    /// required to be found, unlike `files_to_remove`. Only set on the delete
    /// filter; see [`Self::with_dangling_dvs_for`].
    removed_data_file_paths: HashSet<String>,
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
    pub(crate) fn new(
        content: ManifestContentType,
        files_to_remove: &[DataFile],
        fail_missing_delete_paths: bool,
    ) -> Self {
        Self {
            content,
            files_to_remove: files_to_remove
                .iter()
                .map(RemovedFileIdentity::of)
                .collect(),
            fail_missing_delete_paths,
            removed_data_file_paths: HashSet::new(),
            filtered: HashMap::new(),
            format_version: None,
        }
    }

    /// Also marks `Deleted` any deletion vector whose referenced data file is
    /// among `removed_data_files`, as Java's `removeDanglingDeletesFor` does.
    /// Only deletion vectors qualify: a position delete file's
    /// `referenced_data_file` is a hint, not a guarantee of its contents.
    pub(crate) fn with_dangling_dvs_for(mut self, removed_data_files: &[DataFile]) -> Self {
        debug_assert_eq!(self.content, ManifestContentType::Deletes);
        self.removed_data_file_paths = removed_data_files
            .iter()
            .map(|f| f.file_path().to_string())
            .collect();
        self
    }

    /// Applies the removals to `manifests`, rewriting those of this manager's
    /// content type that reference a file to remove.
    ///
    /// With `fail_missing_delete_paths`, fails if a file to remove is not live
    /// in any manifest: it was never in the table, or a concurrent commit
    /// already removed it. Otherwise such files are skipped.
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
        let format_version = table.metadata().format_version();
        // The delete filter must still scan when no delete file is named but a
        // removed data file may leave a DV dangling. DVs exist from v3, so on
        // older tables nothing can dangle.
        let may_have_dangling_dvs =
            !matches!(format_version, FormatVersion::V1 | FormatVersion::V2)
                && !self.removed_data_file_paths.is_empty();
        if self.files_to_remove.is_empty() && !may_have_dangling_dvs {
            return Ok(Filtered {
                manifests,
                removed_files: Vec::new(),
            });
        }

        if self.format_version != Some(format_version) {
            self.filtered.clear();
            self.format_version = Some(format_version);
        }

        let mut out = Vec::with_capacity(manifests.len());
        let mut removed_files: Vec<DataFile> = Vec::new();

        // TODO: every manifest of this content type is read, one at a time.
        // Skip manifests with no live files and prune by partition summary
        // before reading, as Java's ManifestFilterManager does, and read the
        // rest concurrently as `validate_files_not_live` does.
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

        if self.fail_missing_delete_paths {
            let found: HashSet<RemovedFileIdentity> =
                removed_files.iter().map(RemovedFileIdentity::of).collect();
            let mut missing: Vec<String> = self
                .files_to_remove
                .iter()
                .filter(|f| !found.contains(f))
                .map(ToString::to_string)
                .collect();
            if !missing.is_empty() {
                missing.sort_unstable();
                return Err(invalid_data!(
                    "Cannot remove files that are not live in the table: {}",
                    missing.join(", ")
                ));
            }
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
            .filter(|e| e.is_alive() && self.removes(e.data_file()))
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

        // Write under the source manifest's own schema and spec: its entries'
        // partition values and column bounds were encoded for that schema's
        // types, which the table's current schema may since have promoted.
        let source_metadata = manifest.metadata();
        let mut writer = new_manifest_writer(
            table,
            path,
            snapshot_id,
            self.content,
            source_metadata.schema().clone(),
            source_metadata.partition_spec().clone().into(),
        )?;
        for entry in manifest.entries() {
            if !entry.is_alive() {
                continue;
            }
            let entry = (**entry).clone();
            if self.removes(entry.data_file()) {
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

    fn removes(&self, file: &DataFile) -> bool {
        self.files_to_remove
            .contains(&RemovedFileIdentity::of(file))
            || self.is_dangling_dv(file)
    }

    fn is_dangling_dv(&self, file: &DataFile) -> bool {
        file.file_format() == DataFileFormat::Puffin
            && file
                .referenced_data_file()
                .is_some_and(|data| self.removed_data_file_paths.contains(&data))
    }
}
