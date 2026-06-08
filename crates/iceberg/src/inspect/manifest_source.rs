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

//! The shared manifest source for the file/entry inspection tables.
//!
//! Both the `files` family ([`crate::inspect::FilesTable`]) and the `entries` table
//! ([`crate::inspect::EntriesTable`]) read a set of manifests and project each manifest's entries to
//! rows. They differ only in WHICH manifests that set is:
//!
//! - [`MetadataScope::CurrentSnapshot`] — the current snapshot's manifest list (the `files` /
//!   `data_files` / `delete_files` / `entries` tables). Empty when the table has no current snapshot.
//! - [`MetadataScope::AllSnapshots`] — the **deduplicated union of manifests reachable from ALL
//!   snapshots** (the `all_files` / `all_data_files` / `all_delete_files` / `all_entries` tables),
//!   mirroring Java `BaseAllMetadataTableScan.reachableManifests` (a `HashSet<ManifestFile>`, where
//!   manifest equality is by path).
//!
//! This is the single source of truth for the manifest set so the two tables cannot drift on the
//! cross-snapshot semantics. The per-manifest read, the content filter
//! ([`crate::inspect::FilesTable`]'s `FilesTableKind`), and the live-entry filter (`is_alive` for the
//! `files` family / none for `entries`) all stay in the tables themselves — only the source list
//! changes between scopes.
//!
//! Reference:
//! - <https://github.com/apache/iceberg/blob/main/core/src/main/java/org/apache/iceberg/BaseAllMetadataTableScan.java>

use std::collections::HashSet;

use crate::Result;
use crate::spec::ManifestFile;
use crate::table::Table;

/// The snapshot SCOPE of a file/entry inspection table — orthogonal to the content/kind axis.
///
/// `CurrentSnapshot` is the current-snapshot family (`files`, `data_files`, `delete_files`,
/// `entries`); `AllSnapshots` is the cross-snapshot family (`all_files`, `all_data_files`,
/// `all_delete_files`, `all_entries`). See [`collect_manifest_files`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum MetadataScope {
    /// Only the current snapshot's manifests (Java `snapshot.allManifests`/`dataManifests`/…).
    CurrentSnapshot,
    /// The deduplicated union of manifests reachable from ALL snapshots (Java
    /// `BaseAllMetadataTableScan.reachableManifests`).
    AllSnapshots,
}

/// Collects the manifest files an inspection table of the given [`MetadataScope`] must read.
///
/// - [`MetadataScope::CurrentSnapshot`]: the current snapshot's manifest-list entries, in list order.
///   Returns an empty `Vec` when the table has no current snapshot.
/// - [`MetadataScope::AllSnapshots`]: the union of every snapshot's manifest-list entries,
///   **deduplicated by [`ManifestFile::manifest_path`]** (Java's `HashSet<ManifestFile>`), preserving
///   first-seen order so the row output is deterministic (Java's `HashSet` is unordered, but a stable
///   order is a strict superset of its contract and makes tests reproducible). The FILES inside the
///   manifests are NOT deduplicated — only the manifests are (Java javadoc: "may return duplicate
///   rows").
///
/// The returned manifests are NOT content-filtered; the caller applies its own content filter
/// (`data_files` → DATA manifests, `delete_files` → DELETE manifests, `files`/`entries` → all). Because
/// dedup is by `manifest_path` and a manifest's content is intrinsic, deduplicating-then-filtering and
/// filtering-then-deduplicating produce the identical set — so this single all-content source is
/// equivalent to Java's per-content `reachableManifests(dataManifests | deleteManifests | allManifests)`.
pub(super) async fn collect_manifest_files(
    table: &Table,
    scope: MetadataScope,
) -> Result<Vec<ManifestFile>> {
    match scope {
        MetadataScope::CurrentSnapshot => collect_current_snapshot_manifests(table).await,
        MetadataScope::AllSnapshots => collect_reachable_manifests(table).await,
    }
}

/// The current snapshot's manifest-list entries (empty when there is no current snapshot).
async fn collect_current_snapshot_manifests(table: &Table) -> Result<Vec<ManifestFile>> {
    let Some(snapshot) = table.metadata().current_snapshot() else {
        return Ok(Vec::new());
    };
    let manifest_list = snapshot
        .load_manifest_list(table.file_io(), table.metadata())
        .await?;
    Ok(manifest_list.entries().to_vec())
}

/// The deduplicated union of every snapshot's manifest-list entries (Java `reachableManifests`).
///
/// Dedup key is [`ManifestFile::manifest_path`]; first-seen order is preserved for deterministic
/// output. A manifest referenced by two snapshots is therefore read exactly once.
async fn collect_reachable_manifests(table: &Table) -> Result<Vec<ManifestFile>> {
    let mut seen_paths: HashSet<String> = HashSet::new();
    let mut manifests: Vec<ManifestFile> = Vec::new();
    for snapshot in table.metadata().snapshots() {
        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await?;
        for manifest_file in manifest_list.entries() {
            if seen_paths.insert(manifest_file.manifest_path.clone()) {
                manifests.push(manifest_file.clone());
            }
        }
    }
    Ok(manifests)
}
