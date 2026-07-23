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

//! Table maintenance operations that are NOT snapshot commits.
//!
//! Currently: [`RemoveOrphanFilesAction`] — delete files under the table
//! location that no retained snapshot or table metadata references. Modeled on
//! iceberg-go's `Table.DeleteOrphanFiles` (itself modeled on Java's
//! `DeleteOrphanFiles` action): dry-run support, an `older_than` age guard
//! (protects in-flight commits), and prefix-mismatch handling with optional
//! scheme/authority equivalence.

use std::collections::{HashMap, HashSet};

use futures::{StreamExt, stream};

use crate::io::ListEntry;
use crate::table::Table;
use crate::{Error, ErrorKind, Result};

/// Default age guard: only files older than 3 days are deletion candidates
/// (iceberg-go/Java default). Protects files written by in-flight commits.
pub const DEFAULT_OLDER_THAN_MS: i64 = 3 * 24 * 60 * 60 * 1000;

const DEFAULT_DELETE_CONCURRENCY: usize = 8;

/// How to handle a listed file that matches a referenced file except for its
/// URI scheme/authority (after applying the equivalence maps). Mirrors
/// iceberg-go's `PrefixMismatchMode` / Java's `prefix_mismatch_mode`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PrefixMismatchMode {
    /// Fail the action (default — safest; forces the operator to declare
    /// scheme/authority equivalence explicitly).
    #[default]
    Error,
    /// Treat mismatched files as referenced (never delete them).
    Ignore,
    /// Treat mismatched files as orphans (delete them).
    Delete,
}

/// Result of a [`RemoveOrphanFilesAction`] execution.
#[derive(Debug, Default)]
pub struct RemoveOrphanFilesResult {
    /// Orphan files identified (deletion candidates). In dry-run mode nothing
    /// is deleted — this IS the report.
    pub orphan_files: Vec<String>,
    /// Files actually deleted (empty in dry-run mode).
    pub deleted_files: Vec<String>,
    /// Per-file delete failures as `(path, error)` — deletion is best-effort
    /// (iceberg-go parity: continue past individual failures and report them).
    pub failed_deletes: Vec<(String, String)>,
    /// Total files listed under the location.
    pub listed_count: usize,
    /// Size of the referenced-file set.
    pub referenced_count: usize,
    /// Candidates skipped because they are newer than `older_than_ms`.
    pub skipped_recent: usize,
    /// Candidates skipped because the storage reported no modification time
    /// (treated as possibly-in-flight; never deleted).
    pub skipped_missing_mtime: usize,
}

/// Deletes files under the table location that are not referenced by any
/// retained snapshot or table metadata.
///
/// ```ignore
/// let result = RemoveOrphanFilesAction::new(table)
///     .older_than_ms(cutoff_ms)
///     .dry_run(true)
///     .execute()
///     .await?;
/// ```
///
/// Safety properties:
/// - **Dry-run first**: `dry_run(true)` produces the full orphan report with
///   zero writes; callers should verify it (e.g. against an independent
///   engine's `all_files` metadata) before ever running the delete mode.
/// - **Age guard**: files newer than `older_than_ms` are never touched —
///   an in-flight commit's freshly-written files are unreferenced until the
///   commit lands. Files without a modification time are never touched.
/// - **Location guard**: only files under the listed location are considered.
/// - **Conservative referenced set**: ALL manifest entries of ALL retained
///   snapshots count as referenced, regardless of entry status. This
///   deliberately diverges from iceberg-go (which drops DELETED-status
///   entries): a superseded file costs storage until `expire_snapshots`
///   drops its last referencing manifest, but can never be deleted while any
///   retained manifest still mentions it — and it keeps the orphan report
///   verifiable against Java's `all_files` metadata table, which also
///   includes logically-deleted files.
pub struct RemoveOrphanFilesAction {
    table: Table,
    location: Option<String>,
    older_than_ms: Option<i64>,
    dry_run: bool,
    delete_concurrency: usize,
    prefix_mismatch_mode: PrefixMismatchMode,
    equal_schemes: HashMap<String, String>,
    equal_authorities: HashMap<String, String>,
}

impl RemoveOrphanFilesAction {
    /// Creates the action for `table` with iceberg-go's defaults: table
    /// location, 3-day age guard, no dry-run, `PrefixMismatchMode::Error`,
    /// no scheme/authority equivalence.
    pub fn new(table: Table) -> Self {
        Self {
            table,
            location: None,
            older_than_ms: None,
            dry_run: false,
            delete_concurrency: DEFAULT_DELETE_CONCURRENCY,
            prefix_mismatch_mode: PrefixMismatchMode::default(),
            equal_schemes: HashMap::new(),
            equal_authorities: HashMap::new(),
        }
    }

    /// Location to scan (defaults to the table location).
    pub fn location(mut self, location: impl Into<String>) -> Self {
        self.location = Some(location.into());
        self
    }

    /// Absolute cutoff: only files last modified BEFORE this timestamp
    /// (milliseconds since epoch) are deletion candidates. Defaults to
    /// now − 3 days.
    pub fn older_than_ms(mut self, timestamp_ms: i64) -> Self {
        self.older_than_ms = Some(timestamp_ms);
        self
    }

    /// Identify orphans without deleting anything.
    pub fn dry_run(mut self, dry_run: bool) -> Self {
        self.dry_run = dry_run;
        self
    }

    /// Concurrency for the delete phase.
    pub fn delete_concurrency(mut self, concurrency: usize) -> Self {
        self.delete_concurrency = concurrency.max(1);
        self
    }

    /// How to handle candidates that match a referenced file except for
    /// scheme/authority. See [`PrefixMismatchMode`].
    pub fn prefix_mismatch_mode(mut self, mode: PrefixMismatchMode) -> Self {
        self.prefix_mismatch_mode = mode;
        self
    }

    /// Declare URI schemes as equivalent, e.g. `{"s3a" => "s3", "s3n" => "s3"}`.
    pub fn equal_schemes(mut self, schemes: HashMap<String, String>) -> Self {
        self.equal_schemes = schemes;
        self
    }

    /// Declare URI authorities as equivalent.
    pub fn equal_authorities(mut self, authorities: HashMap<String, String>) -> Self {
        self.equal_authorities = authorities;
        self
    }

    /// Executes the scan (and, unless dry-run, the delete phase).
    pub async fn execute(self) -> Result<RemoveOrphanFilesResult> {
        let location = self
            .location
            .clone()
            .unwrap_or_else(|| self.table.metadata().location().to_string());
        let cutoff_ms = self.older_than_ms.unwrap_or_else(|| {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0)
                - DEFAULT_OLDER_THAN_MS
        });

        let referenced = self.collect_referenced_files().await?;
        // normalized form -> one original referenced path (for mismatch reporting).
        let mut normalized_referenced: HashMap<String, String> = HashMap::new();
        for path in &referenced {
            normalized_referenced
                .entry(self.normalize(path))
                .or_insert_with(|| path.clone());
        }

        let listed = self.table.file_io().list_prefix(&location).await?;

        let mut result = RemoveOrphanFilesResult {
            listed_count: listed.len(),
            referenced_count: referenced.len(),
            ..Default::default()
        };

        for entry in listed {
            match self.classify(&entry, &referenced, &normalized_referenced, cutoff_ms)? {
                Classification::Referenced => {}
                Classification::Recent => result.skipped_recent += 1,
                Classification::MissingMtime => result.skipped_missing_mtime += 1,
                Classification::Orphan => result.orphan_files.push(entry.path),
            }
        }

        if self.dry_run || result.orphan_files.is_empty() {
            return Ok(result);
        }

        // Delete phase: bounded concurrency, best-effort per file.
        let file_io = self.table.file_io().clone();
        let outcomes: Vec<(String, std::result::Result<(), String>)> =
            stream::iter(result.orphan_files.clone())
                .map(|path| {
                    let file_io = file_io.clone();
                    async move {
                        let outcome = file_io.delete(&path).await.map_err(|e| e.to_string());
                        (path, outcome)
                    }
                })
                .buffer_unordered(self.delete_concurrency)
                .collect()
                .await;
        for (path, outcome) in outcomes {
            match outcome {
                Ok(()) => result.deleted_files.push(path),
                Err(e) => result.failed_deletes.push((path, e)),
            }
        }

        Ok(result)
    }

    fn classify(
        &self,
        entry: &ListEntry,
        referenced: &HashSet<String>,
        normalized_referenced: &HashMap<String, String>,
        cutoff_ms: i64,
    ) -> Result<Classification> {
        if referenced.contains(&entry.path) {
            return Ok(Classification::Referenced);
        }
        let normalized = self.normalize(&entry.path);
        if let Some(original) = normalized_referenced.get(&normalized) {
            // Same file modulo scheme/authority — iceberg-go's prefix-mismatch
            // handling. (An exact match was already caught above; reaching here
            // with `original == entry.path` is impossible.)
            return match self.prefix_mismatch_mode {
                PrefixMismatchMode::Error => Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Found file with mismatched scheme/authority: listed `{}` vs referenced `{}`. \
                         Declare equivalence via equal_schemes/equal_authorities, or set \
                         PrefixMismatchMode::Ignore / ::Delete.",
                        entry.path, original
                    ),
                )),
                PrefixMismatchMode::Ignore => Ok(Classification::Referenced),
                PrefixMismatchMode::Delete => self.age_gate(entry, cutoff_ms),
            };
        }
        self.age_gate(entry, cutoff_ms)
    }

    fn age_gate(&self, entry: &ListEntry, cutoff_ms: i64) -> Result<Classification> {
        Ok(match entry.last_modified_ms {
            None => Classification::MissingMtime,
            Some(ts) if ts >= cutoff_ms => Classification::Recent,
            Some(_) => Classification::Orphan,
        })
    }

    fn normalize(&self, path: &str) -> String {
        normalize_uri(path, &self.equal_schemes, &self.equal_authorities)
    }

    /// The referenced-file set: everything reachable from table metadata.
    async fn collect_referenced_files(&self) -> Result<HashSet<String>> {
        let metadata = self.table.metadata_ref();
        let file_io = self.table.file_io();
        let mut referenced: HashSet<String> = HashSet::new();

        // Metadata files: current + historical + version hint (harmless if absent).
        if let Some(loc) = self.table.metadata_location() {
            referenced.insert(loc.to_string());
        }
        referenced.extend(
            metadata
                .metadata_log()
                .iter()
                .map(|e| e.metadata_file.clone()),
        );
        referenced.insert(format!(
            "{}/metadata/version-hint.text",
            metadata.location().trim_end_matches('/')
        ));

        // Statistics + partition statistics (Puffin).
        referenced.extend(
            metadata
                .statistics_iter()
                .map(|s| s.statistics_path.clone()),
        );
        referenced.extend(
            metadata
                .partition_statistics_iter()
                .map(|s| s.statistics_path.clone()),
        );

        // Every retained snapshot: manifest list, manifests, and ALL entries'
        // file paths (any status — see the struct doc for why deleted-status
        // entries stay in the referenced set).
        let mut manifests_to_load = Vec::new();
        for snapshot in metadata.snapshots() {
            if !snapshot.manifest_list().is_empty() {
                referenced.insert(snapshot.manifest_list().to_string());
            }
            let manifest_list = self.table.manifest_list_reader(snapshot).load().await?;
            for manifest_file in manifest_list.entries() {
                // insert() returning true = first sighting -> load once.
                if referenced.insert(manifest_file.manifest_path.clone()) {
                    manifests_to_load.push(manifest_file.clone());
                }
            }
        }
        for manifest_file in manifests_to_load {
            let manifest = manifest_file.load_manifest(file_io).await?;
            for entry in manifest.entries() {
                referenced.insert(entry.data_file().file_path().to_string());
            }
        }

        Ok(referenced)
    }
}

enum Classification {
    Referenced,
    Recent,
    MissingMtime,
    Orphan,
}

/// Normalize a URI for equality-modulo-scheme/authority comparison: apply the
/// equivalence maps to `scheme` and `authority`, leave the path untouched.
/// Scheme-less paths are returned as-is.
fn normalize_uri(
    path: &str,
    equal_schemes: &HashMap<String, String>,
    equal_authorities: &HashMap<String, String>,
) -> String {
    let Some((scheme, rest)) = path.split_once("://") else {
        return path.to_string();
    };
    let scheme = equal_schemes
        .get(scheme)
        .map(String::as_str)
        .unwrap_or(scheme);
    let (authority, tail) = match rest.split_once('/') {
        Some((a, t)) => (a, Some(t)),
        None => (rest, None),
    };
    let authority = equal_authorities
        .get(authority)
        .map(String::as_str)
        .unwrap_or(authority);
    match tail {
        Some(t) => format!("{scheme}://{authority}/{t}"),
        None => format!("{scheme}://{authority}"),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    fn maps(
        schemes: &[(&str, &str)],
        authorities: &[(&str, &str)],
    ) -> (HashMap<String, String>, HashMap<String, String>) {
        (
            schemes
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            authorities
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        )
    }

    #[test]
    fn test_normalize_identity_without_maps() {
        let (s, a) = maps(&[], &[]);
        assert_eq!(
            normalize_uri("s3://bucket/a/b.parquet", &s, &a),
            "s3://bucket/a/b.parquet"
        );
        assert_eq!(normalize_uri("/plain/path", &s, &a), "/plain/path");
    }

    #[test]
    fn test_normalize_equal_schemes() {
        let (s, a) = maps(&[("s3a", "s3"), ("s3n", "s3")], &[]);
        assert_eq!(
            normalize_uri("s3a://bucket/a/b.parquet", &s, &a),
            "s3://bucket/a/b.parquet"
        );
        assert_eq!(
            normalize_uri("s3n://bucket/a/b.parquet", &s, &a),
            "s3://bucket/a/b.parquet"
        );
        assert_eq!(
            normalize_uri("s3://bucket/a/b.parquet", &s, &a),
            "s3://bucket/a/b.parquet"
        );
    }

    #[test]
    fn test_normalize_equal_authorities() {
        let (s, a) = maps(&[], &[("bucket-alias", "bucket")]);
        assert_eq!(
            normalize_uri("s3://bucket-alias/a/b.parquet", &s, &a),
            "s3://bucket/a/b.parquet"
        );
    }
}
