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

//! `DeleteOrphanFiles` — list a table location and delete files that no valid snapshot
//! references. The Rust port of Java's `DeleteOrphanFilesSparkAction`
//! (`spark/.../actions/DeleteOrphanFilesSparkAction.java`), minus the Spark distribution layer
//! (driver/executor RDD listing, broadcast joins). The algorithm — the valid-file universe, the
//! hidden-path filter, the `olderThan` cut, the URI-normalized orphan join, and
//! `PrefixMismatchMode` — is ported 1:1.
//!
//! **THIS ACTION DELETES FILES.** A file it classifies as orphan is deleted with no rollback. A
//! reachability omission, a URI-normalization mistake, or an over-broad listing deletes LIVE
//! table data. Every eligibility decision below is a corruption-class judgment, so the design
//! biases toward UNDER-deletion at every fork.
//!
//! # Java provenance and the 1.10.0 pin
//!
//! The action class itself lives in the Spark module and there is **no 1.10.0 Spark bytecode**
//! available locally to pin it against — the algorithm here is ported from tagless `MAIN` source
//! (`spark/v4.0/.../DeleteOrphanFilesSparkAction.java`). Every load-bearing helper it *delegates*
//! to, however, lives in `iceberg-core` / `iceberg-api` 1.10.0 and was bytecode-verified:
//!
//! - [`PrefixMismatchMode`] = `{ERROR, IGNORE, DELETE}` + `fromString` —
//!   `DeleteOrphanFiles$PrefixMismatchMode` (api 1.10.0, javap-verified).
//! - The scheme/authority match rule `uriComponentMatch(valid, actual)` = "valid is null/empty OR
//!   `valid.equalsIgnoreCase(actual)`" — `FileURI.uriComponentMatch` (core 1.10.0, javap-verified:
//!   `Strings.isNullOrEmpty(valid) || valid.equalsIgnoreCase(actual)`).
//! - The hidden-path rule (`_`/`.` prefix) + the partition-aware exception (`_<field>=` for a
//!   partition field whose name starts `_`/`.`) — `HiddenPathFilter.accept` /
//!   `FileSystemWalker$PartitionAwareHiddenPathFilter.forSpecs` (core 1.10.0, javap-verified).
//! - The valid-file universe (all content files of all snapshots, all manifests, all manifest
//!   lists, current + previous metadata.json, version-hint, statistics + partition statistics) —
//!   `ReachableFileUtil` (core 1.10.0) + `BaseSparkAction.{contentFileDS,manifestDS,
//!   manifestListDS,otherMetadataFileDS}` (MAIN — Spark).
//!
//! Everything NOT in that bytecode-verified list is **MAIN-only** — pinned to tagless
//! `DeleteOrphanFilesSparkAction.java`, not 1.10.0 bytecode. Concretely the MAIN-only facts are:
//! the default `olderThan` = `now − 3 days`; the `EQUAL_SCHEMES_DEFAULT = {"s3n,s3a": "s3"}`
//! constant and its `putAll(defaults); putAll(user)` merge order; the GC-gate
//! `ValidationException` message; the ERROR-mode conflict message; the path-only join key; and the
//! overall valid-file-universe composition (`validFileIdentDS`'s union). The `BaseSparkAction`
//! dataset builders these compose are `iceberg-spark` (MAIN) too. The orphan classification logic
//! (`FindOrphanFiles.toOrphanFile`) is MAIN, but its only non-trivial primitive
//! (`uriComponentMatch`) is the bytecode-verified one above.
//!
//! # The algorithm (Java `doExecute`)
//!
//! 1. **Valid-file universe** (`validFileIdentDS`): the union of —
//!    - every **content file** of every snapshot — Java `contentFileDS` reads `ALL_MANIFESTS` and
//!      flat-maps each manifest through `ManifestFiles.read` / `readDeleteManifest`, which yields
//!      **every entry, including `DELETED`-status tombstones** (it does NOT call `liveEntries()`).
//!      So a file referenced by *any* manifest entry of *any* snapshot is valid — even a delete
//!      tombstone keeps a file out of the orphan set. (This is the load-bearing difference from
//!      `expire_cleanup`, which subtracts on `is_alive()`; here we must NOT filter on liveness.)
//!    - every **manifest** of every snapshot (Java `manifestDS` over `ALL_MANIFESTS`);
//!    - every **manifest list** of every snapshot (Java `manifestListDS` =
//!      `ReachableFileUtil.manifestListLocations(table, null)`);
//!    - the **current metadata.json** + the **metadata-log previous-files** entries
//!      (NON-recursive — Java `otherMetadataFileDS()` defaults `recursive=false`, so it adds the
//!      `previousFiles()` locations but does NOT walk into them), the **version-hint** location,
//!      and **all statistics + partition-statistics** files
//!      (`ReachableFileUtil.{metadataFileLocations(table,false),versionHintLocation,
//!      statisticsFilesLocations}`).
//!
//!    If the fork tracked **partition-statistics** files they would be included here; the fork's
//!    `TableMetadata` does expose
//!    [`partition_statistics_iter`](crate::spec::TableMetadata::partition_statistics_iter), so they
//!    ARE included.
//!
//! 2. **Actual-file listing** (`listedFileDS`): [`FileIO::list`](crate::io::FileIO::list) over the
//!    location (default the table's [`location`](crate::spec::TableMetadata::location)), filtered
//!    by the hidden-path rule (`PartitionAwareHiddenPathFilter`) applied to path segments UNDER the
//!    listed location.
//!
//! 3. **`olderThan` cut**: keep only listed files whose `created_at_millis < older_than` (Java
//!    `fileInfo.createdAtMillis() < olderThanTimestamp` — the in-flight-write safety grace).
//!
//! 4. **Orphan join with URI normalization** (`find_orphan_files`): normalize each side to a
//!    `FileUri` (scheme via `equal_schemes`, authority via `equal_authorities`, plus the raw
//!    path), join on **path** equality, and for each listed file classify it via
//!    [`PrefixMismatchMode`] (`classify_against_valid` — Java `FindOrphanFiles.toOrphanFile`).
//!
//! 5. **Deletion** ([`Self::execute`]): delete each orphan via the delete function, collecting
//!    per-file failures rather than aborting (see the failure posture below), and return every
//!    orphan location in the [`DeleteOrphanFilesResult`] regardless of individual delete success.
//!
//! # Defaults (Java parity)
//!
//! - `older_than` = `now − 3 days` (Java `System.currentTimeMillis() - TimeUnit.DAYS.toMillis(3)`).
//! - `prefix_mismatch_mode` = [`PrefixMismatchMode::Error`].
//! - `equal_schemes` = `{s3n → s3, s3a → s3}` (Java `EQUAL_SCHEMES_DEFAULT = {"s3n,s3a": "s3"}`,
//!   comma-flattened) MERGED with user additions (Java `putAll(defaults); putAll(user)` — user
//!   wins on a key collision).
//! - `equal_authorities` = empty by default (Java `Collections.emptyMap()`; no built-in default),
//!   replaced wholesale by user additions.
//! - `location` = the table's location.
//! - delete = [`FileIO::delete`](crate::io::FileIO::delete).
//! - **GC gate:** Java's constructor throws `ValidationException` if `gc.enabled=false`; this port
//!   refuses at [`Self::execute`] with the verbatim message.
//!
//! # Failure posture
//!
//! Java's `deleteNonBulk` runs `Tasks.foreach(...).suppressFailureWhenFinished()` — a per-file
//! delete failure is logged and the sweep continues; the orphan list (the result) is collected
//! from the join BEFORE deletion, so it is returned in full regardless of delete success. This
//! port mirrors that: per-file delete failures are **collected** in
//! [`DeleteOrphanFilesResult::delete_failures`] (the `iceberg` crate has no logging-facade
//! dependency, and silent swallowing is unacceptable for a deletion sweep) and the sweep
//! continues; `orphan_file_locations` always holds the full orphan set.
//!
//! A planning-stage failure (an unreadable manifest list / manifest, or an `ERROR`-mode prefix
//! conflict) returns `Err` BEFORE any deletion — A1's loud-empty-listing default and the eager
//! planning make the deletion path unreachable on an errored/empty listing.
//!
//! # Deferred (loudly)
//!
//! - **Executor parallelism** (Java `executeDeleteWith(ExecutorService)`): the sweep is
//!   SEQUENTIAL. A parallel sweep is a throughput optimization, not a correctness requirement, and
//!   is deferred (the action carries no `execute_delete_with`).
//! - **Bulk deletes** (`SupportsBulkOperations.deleteFiles`): the fork's [`FileIO`] has no bulk
//!   surface; deletes go one-by-one through the delete function.
//! - **`compareToFileList` / streaming results** (Java's driver-memory escape hatches): deferred.
//! - **Java interop evidence** (the GAP_MATRIX row stays 🟡).

use std::collections::{HashMap, HashSet};

use futures::future::BoxFuture;

use crate::error::Result;
use crate::io::FileIO;
use crate::spec::{PartitionSpecRef, TableProperties};
use crate::table::Table;
use crate::{Error, ErrorKind};

/// `gc.enabled` default — mirrors [`TableProperties::PROPERTY_GC_ENABLED_DEFAULT`]. Parsed
/// locally (the `transaction` module's `parse_property` is `pub(super)` and not reachable here),
/// matching Java `PropertyUtil.propertyAsBoolean(properties, GC_ENABLED, GC_ENABLED_DEFAULT)`.
const DEFAULT_OLDER_THAN_AGE_MILLIS: i64 = 3 * 24 * 60 * 60 * 1000;

/// The injected delete function: receives a file location, resolves to a deletion outcome. The
/// default deletes through [`FileIO::delete`].
pub type OrphanDeleteFunction = dyn Fn(String) -> BoxFuture<'static, Result<()>> + Send + Sync;

/// How [`DeleteOrphanFiles`] treats a listed file whose PATH matches a valid file but whose
/// scheme or authority differs after normalization (Java
/// `DeleteOrphanFiles.PrefixMismatchMode`, api 1.10.0 — javap-verified `{ERROR, IGNORE, DELETE}`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PrefixMismatchMode {
    /// Fail the whole action when any prefix (scheme/authority) conflict remains — the default
    /// and recommended mode (Java `ERROR`). The conflicting scheme/authority pairs are reported
    /// in the error message so the operator can resolve them via
    /// [`DeleteOrphanFiles::equal_schemes`] / [`DeleteOrphanFiles::equal_authorities`].
    Error,
    /// Treat a prefix-conflicting file as NOT orphan and skip it (Java `IGNORE`).
    Ignore,
    /// Treat a prefix-conflicting file as orphan and delete it (Java `DELETE`) — use with extreme
    /// caution; the deletion is unrecoverable.
    Delete,
}

impl PrefixMismatchMode {
    /// Parse a [`PrefixMismatchMode`] from a string, case-insensitively (Java
    /// `PrefixMismatchMode.fromString` — `valueOf(s.toUpperCase(Locale.ENGLISH))`). Returns a
    /// `DataInvalid` error with Java's "Invalid mode: %s" message on an unknown value.
    pub fn from_string(mode: &str) -> Result<Self> {
        match mode.to_uppercase().as_str() {
            "ERROR" => Ok(PrefixMismatchMode::Error),
            "IGNORE" => Ok(PrefixMismatchMode::Ignore),
            "DELETE" => Ok(PrefixMismatchMode::Delete),
            _ => Err(Error::new(
                ErrorKind::DataInvalid,
                format!("Invalid mode: {mode}"),
            )),
        }
    }
}

/// The outcome of a [`DeleteOrphanFiles::execute`] sweep: every orphan file location (Java
/// `Result.orphanFileLocations()`), plus every collected per-file delete failure.
///
/// `orphan_file_locations` holds the FULL orphan set the join produced — Java returns the orphan
/// list regardless of individual delete success (the list is collected before deletion), so a
/// path appears here even if its deletion failed and was recorded in `delete_failures`.
#[derive(Debug, Default)]
pub struct DeleteOrphanFilesResult {
    /// Locations of all orphan files (the listed-but-unreferenced set), sorted deterministically.
    pub orphan_file_locations: Vec<String>,
    /// Per-file delete failures collected during the sweep (Java logs-and-continues; this port
    /// collects). Empty means every orphan deleted cleanly.
    pub delete_failures: Vec<OrphanDeleteFailure>,
}

/// One collected, non-aborting delete failure (the Rust replacement for Java's log-and-continue).
#[derive(Debug)]
pub struct OrphanDeleteFailure {
    /// The orphan file whose deletion failed.
    pub path: String,
    /// The underlying error.
    pub error: Error,
}

/// A normalized file identity for the orphan join (Java `org.apache.iceberg.actions.FileURI`).
///
/// Built from a raw location string by parsing it into (scheme, authority, path) the same way
/// Hadoop's `new Path(s).toUri()` does (see [`Self::parse`]), then canonicalizing the scheme via
/// `equal_schemes` and the authority via `equal_authorities`. The orphan join matches on `path`
/// alone; scheme/authority feed [`PrefixMismatchMode`].
#[derive(Debug, Clone, PartialEq, Eq)]
struct FileUri {
    /// Normalized scheme (`None` for a scheme-less local path), after `equal_schemes` mapping.
    scheme: Option<String>,
    /// Normalized authority (`None` when absent), after `equal_authorities` mapping.
    authority: Option<String>,
    /// The URI path component — the orphan join key.
    path: String,
    /// The original location string (what gets deleted / returned).
    uri_as_string: String,
}

impl FileUri {
    /// Parse `location` into a [`FileUri`], mirroring Java's
    /// `new FileURI(new Path(uriAsString).toUri(), equalSchemes, equalAuthorities)` (the
    /// `ToFileURI.toFileURI` path in `DeleteOrphanFilesSparkAction`).
    ///
    /// # Parsing (Hadoop `Path(s).toUri()` semantics, the subset table writers ever produce)
    ///
    /// * A `scheme://authority/path` form (e.g. `s3://bucket/key`, `file://host/p`) →
    ///   `scheme=Some(scheme)`, `authority=Some(authority)` (possibly empty), `path=/path`.
    /// * A `scheme:/path` or `scheme:path` form with no `//` (e.g. `file:/tmp/a`) →
    ///   `scheme=Some(scheme)`, `authority=None`, `path` = the remainder.
    /// * A scheme-less absolute local path (`/tmp/a`) → `scheme=None`, `authority=None`,
    ///   `path=/tmp/a`. This is the **local-path** case: a scheme-less valid path matches an actual
    ///   path of ANY scheme on the scheme axis (Java `uriComponentMatch`: a null/empty valid
    ///   matches anything), so a metadata-stored bare path and a `file://`-listed path are NOT a
    ///   prefix conflict in that direction.
    ///
    /// Normalization then maps `scheme` through `equal_schemes` and `authority` through
    /// `equal_authorities` (Java `getOrDefault`), so e.g. an `s3a://` actual file normalizes to
    /// scheme `s3` and matches an `s3://` valid file.
    fn parse(
        location: &str,
        equal_schemes: &HashMap<String, String>,
        equal_authorities: &HashMap<String, String>,
    ) -> Self {
        let (raw_scheme, authority, path) = split_uri(location);
        let scheme = raw_scheme.map(|scheme| equal_schemes.get(&scheme).cloned().unwrap_or(scheme));
        let authority = authority.map(|authority| {
            equal_authorities
                .get(&authority)
                .cloned()
                .unwrap_or(authority)
        });
        FileUri {
            scheme,
            authority,
            path,
            uri_as_string: location.to_string(),
        }
    }

    /// Whether this (valid) URI's scheme matches `actual`'s (Java `FileURI.schemeMatch` →
    /// `uriComponentMatch`). A `None`/empty valid scheme matches any actual scheme.
    fn scheme_matches(&self, actual: &FileUri) -> bool {
        uri_component_match(self.scheme.as_deref(), actual.scheme.as_deref())
    }

    /// Whether this (valid) URI's authority matches `actual`'s (Java `FileURI.authorityMatch`).
    fn authority_matches(&self, actual: &FileUri) -> bool {
        uri_component_match(self.authority.as_deref(), actual.authority.as_deref())
    }
}

/// Java `FileURI.uriComponentMatch(valid, actual)` (core 1.10.0, javap-verified):
/// `Strings.isNullOrEmpty(valid) || valid.equalsIgnoreCase(actual)`. A null OR empty VALID
/// component matches any actual component; otherwise it is a case-insensitive equality.
fn uri_component_match(valid: Option<&str>, actual: Option<&str>) -> bool {
    // Java `Strings.isNullOrEmpty(valid)`: a `None` or empty-string valid component matches any
    // actual; otherwise a case-insensitive equality against the actual component.
    match valid {
        None | Some("") => true,
        Some(valid) => actual.is_some_and(|actual| valid.eq_ignore_ascii_case(actual)),
    }
}

/// Split a location into `(scheme, authority, path)` mirroring Hadoop `new Path(s).toUri()` for
/// the location shapes Iceberg writers produce. See [`FileUri::parse`] for the cases.
fn split_uri(location: &str) -> (Option<String>, Option<String>, String) {
    // Find a scheme: a leading run of [A-Za-z][A-Za-z0-9+.-]* immediately followed by ':'.
    // (Hadoop's Path treats a Windows drive letter "c:" specially, but Iceberg locations are
    // POSIX/object-store URIs, so the RFC-3986 scheme rule is faithful for the writer-produced
    // set — flagged: Windows drive letters are not a parity target.)
    let scheme_end = scheme_delimiter(location);
    let (scheme, rest) = match scheme_end {
        Some(index) => (Some(location[..index].to_string()), &location[index + 1..]),
        None => (None, location),
    };

    // After the scheme (if any), an authority is present iff the remainder starts with "//".
    if let Some(after_slashes) = rest.strip_prefix("//") {
        // authority runs until the next '/', '?' or '#'; the rest (including the leading '/') is
        // the path. An empty authority (e.g. "file:///tmp/a") yields Some("").
        let authority_end = after_slashes
            .find(['/', '?', '#'])
            .unwrap_or(after_slashes.len());
        let authority = after_slashes[..authority_end].to_string();
        let path = after_slashes[authority_end..].to_string();
        (scheme, Some(authority), path)
    } else {
        // No authority. The path is the remainder as-is (e.g. "file:/tmp/a" → "/tmp/a",
        // "/tmp/a" → "/tmp/a"). Strip a query/fragment defensively (never present for file
        // locations, but mirrors URI.getPath()).
        let path_end = rest.find(['?', '#']).unwrap_or(rest.len());
        (scheme, None, rest[..path_end].to_string())
    }
}

/// Index of the ':' that ends a URI scheme, or `None` if `location` has no scheme. A scheme is a
/// leading ASCII letter followed by letters/digits/`+`/`-`/`.`, then ':'. A leading '/' (absolute
/// local path) has no scheme.
fn scheme_delimiter(location: &str) -> Option<usize> {
    let bytes = location.as_bytes();
    if bytes.is_empty() || !bytes[0].is_ascii_alphabetic() {
        return None;
    }
    for (index, &byte) in bytes.iter().enumerate() {
        match byte {
            b':' => return if index == 0 { None } else { Some(index) },
            b if b.is_ascii_alphanumeric() || matches!(b, b'+' | b'-' | b'.') => continue,
            _ => return None,
        }
    }
    None
}

/// The hidden-path filter (Java `FileSystemWalker$PartitionAwareHiddenPathFilter`, core 1.10.0):
/// a path segment is hidden when its name starts with `_` or `.`, EXCEPT a partition directory
/// `<field>=...` whose `<field>` is a partition field (in any of the table's specs) whose name
/// itself starts with `_` or `.`.
///
/// Construction (`forSpecs`, javap-verified): collect every partition field name across every
/// spec that starts with `_` or `.`, append `=` to each, and treat a segment as visible if it
/// starts with any of those `<name>=` strings (`isHiddenPartitionPath`) OR passes the plain
/// [`HiddenPathFilter`](crate::maintenance) rule (does NOT start with `_`/`.`).
struct PartitionAwareHiddenPathFilter {
    /// `<field>=` prefixes for partition fields whose name starts with `_`/`.` — the directories
    /// the plain hidden rule would wrongly hide. Empty ⇒ the plain hidden rule applies.
    hidden_partition_prefixes: Vec<String>,
}

impl PartitionAwareHiddenPathFilter {
    /// Build the filter from the table's partition specs (Java `forSpecs`).
    fn for_specs<'a>(specs: impl Iterator<Item = &'a PartitionSpecRef>) -> Self {
        let mut hidden_partition_prefixes: Vec<String> = specs
            .flat_map(|spec| spec.fields().iter())
            .filter(|field| field.name.starts_with('_') || field.name.starts_with('.'))
            .map(|field| format!("{}=", field.name))
            .collect();
        hidden_partition_prefixes.sort();
        hidden_partition_prefixes.dedup();
        PartitionAwareHiddenPathFilter {
            hidden_partition_prefixes,
        }
    }

    /// Whether a single path SEGMENT name is accepted (visible). Java `accept`:
    /// `isHiddenPartitionPath(path) || HiddenPathFilter.get().accept(path)`.
    fn accepts_segment(&self, segment: &str) -> bool {
        self.is_partition_segment(segment) || !is_plain_hidden(segment)
    }

    /// Whether `segment` is a partition directory exempt from the hidden rule (Java
    /// `isHiddenPartitionPath`): it starts with one of the `<field>=` prefixes.
    fn is_partition_segment(&self, segment: &str) -> bool {
        self.hidden_partition_prefixes
            .iter()
            .any(|prefix| segment.starts_with(prefix.as_str()))
    }

    /// Whether `location`'s path UNDER `base` contains a hidden segment (Java
    /// `FileSystemWalker.isHiddenPath`): walk the segments of the path relative to `base` and
    /// reject if any is hidden. Only segments strictly under `base` are checked — a hidden
    /// component of `base` itself (the table root) never disqualifies the listing.
    fn is_hidden_under(&self, base: &str, location: &str) -> bool {
        let Some(relative) = relative_under(base, location) else {
            // location is not under base (shouldn't happen for a listing of `base`); be
            // conservative and treat it as hidden so it is NOT deleted.
            return true;
        };
        // The final segment is the file name; intermediate segments are directories. Java's
        // PathFilter applies to every segment from the file up to (excluding) baseDir, so the
        // file name is checked too (a file literally named "_x" or ".x" under the root is hidden).
        relative
            .split('/')
            .filter(|segment| !segment.is_empty())
            .any(|segment| !self.accepts_segment(segment))
    }
}

/// Plain [`HiddenPathFilter`](crate::maintenance) rule (Java `HiddenPathFilter.accept`,
/// core 1.10.0, javap-verified): a name is hidden iff it starts with `_` or `.`.
fn is_plain_hidden(segment: &str) -> bool {
    segment.starts_with('_') || segment.starts_with('.')
}

/// The path portion of `location` strictly under `base`, or `None` if `location` is not under
/// `base`. Both are compared as `/`-delimited paths; a trailing `/` on `base` is tolerated.
fn relative_under<'a>(base: &str, location: &'a str) -> Option<&'a str> {
    let base = base.strip_suffix('/').unwrap_or(base);
    let remainder = location.strip_prefix(base)?;
    // Require a directory boundary: the remainder must start with '/' (so base="ab" does not
    // match location "ab2/x"). An exact equality (location == base) yields "" → not under.
    remainder.strip_prefix('/')
}

/// An action that deletes orphan metadata, data, and delete files in a table by listing a
/// location and comparing the listed files against the valid-file universe of all snapshots. See
/// the module docs for the full algorithm, the defaults, and the Java provenance.
///
/// **This action deletes files.** Build it with [`DeleteOrphanFiles::new`], configure it with the
/// builder methods, and run it with [`Self::execute`].
pub struct DeleteOrphanFiles {
    table: Table,
    location: String,
    older_than_millis: i64,
    prefix_mismatch_mode: PrefixMismatchMode,
    equal_schemes: HashMap<String, String>,
    equal_authorities: HashMap<String, String>,
    delete_function: Option<Box<OrphanDeleteFunction>>,
}

impl DeleteOrphanFiles {
    /// Create a `DeleteOrphanFiles` action for `table` with Java's defaults: location = the
    /// table's location, `older_than` = `now − 3 days`, `prefix_mismatch_mode` =
    /// [`PrefixMismatchMode::Error`], `equal_schemes` = `{s3n→s3, s3a→s3}`, `equal_authorities` =
    /// empty, delete = [`FileIO::delete`].
    pub fn new(table: Table) -> Self {
        let location = table.metadata().location().to_string();
        DeleteOrphanFiles {
            table,
            location,
            older_than_millis: now_millis().saturating_sub(DEFAULT_OLDER_THAN_AGE_MILLIS),
            prefix_mismatch_mode: PrefixMismatchMode::Error,
            equal_schemes: default_equal_schemes(),
            equal_authorities: HashMap::new(),
            delete_function: None,
        }
    }

    /// The location to scan for orphan files (Java `location(String)`). Defaults to the table's
    /// location; point it at a subdirectory (e.g. the data folder) to sweep only that subtree.
    pub fn location(mut self, location: impl Into<String>) -> Self {
        self.location = location.into();
        self
    }

    /// Only files older than this timestamp (epoch millis) are eligible for deletion (Java
    /// `olderThan(long)`). The grace protects files an in-flight commit is adding but has not yet
    /// referenced. Defaults to `now − 3 days`.
    pub fn older_than(mut self, older_than_millis: i64) -> Self {
        self.older_than_millis = older_than_millis;
        self
    }

    /// Set how prefix (scheme/authority) conflicts are handled (Java `prefixMismatchMode`).
    /// Defaults to [`PrefixMismatchMode::Error`].
    pub fn prefix_mismatch_mode(mut self, mode: PrefixMismatchMode) -> Self {
        self.prefix_mismatch_mode = mode;
        self
    }

    /// Add schemes that should be considered equal during normalization (Java `equalSchemes`).
    /// The supplied map is MERGED on top of the defaults `{s3n→s3, s3a→s3}` (Java
    /// `putAll(defaults); putAll(user)` — a user mapping for a default key wins). Comma-separated
    /// keys (e.g. `"s3a,s3n" → "s3"`) are flattened, matching Java `flattenMap`.
    pub fn equal_schemes(mut self, equal_schemes: HashMap<String, String>) -> Self {
        // Java: start from the defaults, then overlay the user map (user wins on collision).
        let mut merged = default_equal_schemes();
        merged.extend(flatten_map(equal_schemes));
        self.equal_schemes = merged;
        self
    }

    /// Set authorities that should be considered equal during normalization (Java
    /// `equalAuthorities`). There is no built-in default; the supplied map (comma-flattened)
    /// REPLACES the current one, matching Java (`equalAuthorities = newHashMap(); putAll(user)`).
    pub fn equal_authorities(mut self, equal_authorities: HashMap<String, String>) -> Self {
        self.equal_authorities = flatten_map(equal_authorities);
        self
    }

    /// Replace the delete function (Java `deleteWith(Consumer<String>)`). The default deletes
    /// through [`FileIO::delete`]. A custom function receives exactly the orphan set (e.g. to
    /// collect orphans without deleting, or route deletions through an external queue).
    pub fn delete_with(
        mut self,
        delete_function: impl Fn(String) -> BoxFuture<'static, Result<()>> + Send + Sync + 'static,
    ) -> Self {
        self.delete_function = Some(Box::new(delete_function));
        self
    }

    /// Run the action: plan the orphan set and delete it. See the module docs for the
    /// algorithm and failure posture.
    ///
    /// Returns `Err` WITHOUT deleting anything when the `gc.enabled` gate refuses, when a manifest
    /// list / manifest cannot be read during planning, or when an `ERROR`-mode prefix conflict
    /// remains. Per-file delete failures are collected in the returned
    /// [`DeleteOrphanFilesResult`].
    pub async fn execute(self) -> Result<DeleteOrphanFilesResult> {
        // GC gate (Java's constructor `ValidationException`). Refuse before any listing/deletion.
        self.check_gc_enabled()?;

        let file_io = self.table.file_io().clone();

        // 1. Valid-file universe — the full reachable set of all snapshots + metadata.
        let valid_locations = self.collect_valid_files().await?;

        // 2 + 3. Listed files under the location, hidden-path-filtered + olderThan-cut.
        let listed = self.list_candidate_files(&file_io).await?;

        // 4. Orphan join with URI normalization + PrefixMismatchMode.
        let orphan_locations = self.find_orphan_files(listed, &valid_locations)?;

        // 5. Delete the orphans (collecting per-file failures), return the full orphan set.
        let mut result = DeleteOrphanFilesResult {
            orphan_file_locations: orphan_locations.clone(),
            delete_failures: Vec::new(),
        };
        for path in orphan_locations {
            let outcome = match &self.delete_function {
                Some(delete) => delete(path.clone()).await,
                None => file_io.delete(&path).await,
            };
            if let Err(error) = outcome {
                result
                    .delete_failures
                    .push(OrphanDeleteFailure { path, error });
            }
        }
        Ok(result)
    }

    /// Java's `gc.enabled` gate (constructor `ValidationException.check(... GC_ENABLED ...)`).
    fn check_gc_enabled(&self) -> Result<()> {
        let gc_enabled = parse_bool_property(
            self.table.metadata().properties(),
            TableProperties::PROPERTY_GC_ENABLED,
            TableProperties::PROPERTY_GC_ENABLED_DEFAULT,
        )?;
        if !gc_enabled {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Cannot delete orphan files: GC is disabled (deleting files may corrupt other \
                 tables)"
                    .to_string(),
            ));
        }
        Ok(())
    }

    /// Build the valid-file universe: all content files of all snapshots (every entry, incl.
    /// DELETED tombstones), all manifests, all manifest lists, current + previous metadata.json,
    /// version-hint, and statistics + partition-statistics (see the module docs step 1).
    ///
    /// A manifest-list or manifest read failure aborts with `Err` BEFORE any deletion can run
    /// (this runs inside [`Self::execute`] strictly before the sweep).
    async fn collect_valid_files(&self) -> Result<HashSet<String>> {
        let metadata = self.table.metadata();
        let file_io = self.table.file_io();
        let mut valid: HashSet<String> = HashSet::new();

        for snapshot in metadata.snapshots() {
            // Manifest list of this snapshot.
            valid.insert(snapshot.manifest_list().to_string());

            let manifest_list = snapshot
                .load_manifest_list(file_io, metadata)
                .await
                .map_err(|error| {
                    error.with_context(
                        "snapshot_id",
                        format!(
                            "failed to read manifest list of snapshot {} while planning \
                             delete-orphan-files (no files were deleted)",
                            snapshot.snapshot_id()
                        ),
                    )
                })?;

            for manifest_file in manifest_list.entries() {
                // The manifest file itself.
                valid.insert(manifest_file.manifest_path.clone());

                // Every content file referenced by every entry — INCLUDING DELETED tombstones
                // (Java reads via ManifestFiles.read, not liveEntries; a tombstoned file is still
                // referenced and must not be treated as orphan).
                let manifest = manifest_file
                    .load_manifest(file_io)
                    .await
                    .map_err(|error| {
                        error.with_context(
                            "manifest_path",
                            format!(
                                "failed to read manifest {} while planning delete-orphan-files \
                                 (no files were deleted)",
                                manifest_file.manifest_path
                            ),
                        )
                    })?;
                for entry in manifest.entries() {
                    valid.insert(entry.file_path().to_string());
                }
            }
        }

        // Metadata-log previous-files (NON-recursive — Java otherMetadataFileDS default), plus the
        // current metadata.json location if known. Plus version-hint and statistics.
        for log_entry in metadata.metadata_log() {
            valid.insert(log_entry.metadata_file.clone());
        }
        if let Some(current_metadata_location) = self.table.metadata_location() {
            valid.insert(current_metadata_location.to_string());
        }
        valid.insert(version_hint_location(metadata.location()));
        for statistics in metadata.statistics_iter() {
            valid.insert(statistics.statistics_path.clone());
        }
        for statistics in metadata.partition_statistics_iter() {
            valid.insert(statistics.statistics_path.clone());
        }

        Ok(valid)
    }

    /// List the candidate (potentially-orphan) files under the location: every listed file that
    /// passes the hidden-path filter AND the `olderThan` cut (the module docs steps 2+3).
    async fn list_candidate_files(&self, file_io: &FileIO) -> Result<Vec<crate::io::FileInfo>> {
        let hidden_filter =
            PartitionAwareHiddenPathFilter::for_specs(self.table.metadata().partition_specs_iter());

        // A1's FileIO::list errors loudly on a backend that cannot enumerate (never an empty Ok),
        // so the orphan decision can never be made against a silently-empty listing.
        let listed = file_io.list(&self.location).await?;

        Ok(listed
            .into_iter()
            .filter(|file| !hidden_filter.is_hidden_under(&self.location, &file.location))
            .filter(|file| file.created_at_millis < self.older_than_millis)
            .collect())
    }

    /// The orphan join with URI normalization + [`PrefixMismatchMode`] (Java `findOrphanFiles` +
    /// `FindOrphanFiles.toOrphanFile`). Returns the orphan locations (sorted deterministically),
    /// or `Err` when `ERROR`-mode prefix conflicts remain.
    fn find_orphan_files(
        &self,
        listed: Vec<crate::io::FileInfo>,
        valid_locations: &HashSet<String>,
    ) -> Result<Vec<String>> {
        // Build the valid index keyed by normalized PATH (Java joins on `path`). A path can be
        // produced by multiple valid locations with different schemes/authorities; collect all so
        // a listed file matches if ANY valid file on its path is prefix-compatible.
        let mut valid_by_path: HashMap<String, Vec<FileUri>> = HashMap::new();
        for location in valid_locations {
            let valid_uri = FileUri::parse(location, &self.equal_schemes, &self.equal_authorities);
            valid_by_path
                .entry(valid_uri.path.clone())
                .or_default()
                .push(valid_uri);
        }

        let mut orphans: Vec<String> = Vec::new();
        // Conflicting (valid, actual) scheme/authority pairs, for the ERROR-mode message.
        let mut scheme_conflicts: HashSet<(String, String)> = HashSet::new();
        let mut authority_conflicts: HashSet<(String, String)> = HashSet::new();

        for file in listed {
            let actual =
                FileUri::parse(&file.location, &self.equal_schemes, &self.equal_authorities);
            match valid_by_path.get(&actual.path) {
                // No valid file shares this path ⇒ orphan (Java `valid == null`).
                None => orphans.push(actual.uri_as_string.clone()),
                Some(valid_candidates) => {
                    let classification = classify_against_valid(
                        &actual,
                        valid_candidates,
                        self.prefix_mismatch_mode,
                        &mut scheme_conflicts,
                        &mut authority_conflicts,
                    );
                    if let OrphanClassification::Orphan = classification {
                        orphans.push(actual.uri_as_string.clone());
                    }
                }
            }
        }

        // ERROR mode: any remaining conflict fails the whole action (Java's ValidationException).
        if self.prefix_mismatch_mode == PrefixMismatchMode::Error
            && (!scheme_conflicts.is_empty() || !authority_conflicts.is_empty())
        {
            return Err(prefix_conflict_error(
                &scheme_conflicts,
                &authority_conflicts,
            ));
        }

        orphans.sort();
        orphans.dedup();
        Ok(orphans)
    }
}

/// Whether a listed file is classified as orphan after a PATH match against the valid set.
enum OrphanClassification {
    Orphan,
    NotOrphan,
}

/// Classify a path-matched `actual` against its `valid_candidates` (Java
/// `FindOrphanFiles.toOrphanFile`, applied per candidate with an "any match ⇒ not orphan"
/// reduction).
///
/// Java's left-outer join pairs an actual file with at most one valid file per path; the Rust
/// universe is a set, so a path may carry several valid locations (e.g. the same path via `s3://`
/// and `s3a://`). A listed file is NOT orphan if it is prefix-compatible with ANY of them
/// (under-deletion bias). A conflict is recorded only when NO candidate is compatible.
fn classify_against_valid(
    actual: &FileUri,
    valid_candidates: &[FileUri],
    mode: PrefixMismatchMode,
    scheme_conflicts: &mut HashSet<(String, String)>,
    authority_conflicts: &mut HashSet<(String, String)>,
) -> OrphanClassification {
    // If any valid candidate matches both scheme and authority, the file is definitively reachable.
    let mut any_scheme_conflict: Option<(String, String)> = None;
    let mut any_authority_conflict: Option<(String, String)> = None;

    for valid in valid_candidates {
        let scheme_match = valid.scheme_matches(actual);
        let authority_match = valid.authority_matches(actual);
        if scheme_match && authority_match {
            // Fully compatible with a valid file ⇒ not orphan, no conflict.
            return OrphanClassification::NotOrphan;
        }
        // Remember a representative conflict in case no candidate fully matches.
        if !scheme_match && any_scheme_conflict.is_none() {
            any_scheme_conflict = Some((
                valid.scheme.clone().unwrap_or_default(),
                actual.scheme.clone().unwrap_or_default(),
            ));
        }
        if !authority_match && any_authority_conflict.is_none() {
            any_authority_conflict = Some((
                valid.authority.clone().unwrap_or_default(),
                actual.authority.clone().unwrap_or_default(),
            ));
        }
    }

    // No candidate fully matched: a prefix conflict on this path (Java's `!schemeMatch ||
    // !authorityMatch` branch).
    match mode {
        // DELETE: treat the conflicting file as orphan (Java `mode == DELETE`).
        PrefixMismatchMode::Delete => OrphanClassification::Orphan,
        // ERROR / IGNORE: record the conflict and do NOT delete now (Java registers the conflict
        // and returns null). ERROR later turns a non-empty conflict set into a thrown error;
        // IGNORE simply leaves the file untouched.
        PrefixMismatchMode::Error | PrefixMismatchMode::Ignore => {
            if let Some(conflict) = any_scheme_conflict {
                scheme_conflicts.insert(conflict);
            }
            if let Some(conflict) = any_authority_conflict {
                authority_conflicts.insert(conflict);
            }
            OrphanClassification::NotOrphan
        }
    }
}

/// The ERROR-mode prefix-conflict error (Java `findOrphanFiles`'s `ValidationException`). The
/// message is Java's verbatim text plus the conflicting pairs.
fn prefix_conflict_error(
    scheme_conflicts: &HashSet<(String, String)>,
    authority_conflicts: &HashSet<(String, String)>,
) -> Error {
    let mut conflicts: Vec<String> = scheme_conflicts
        .iter()
        .chain(authority_conflicts.iter())
        .map(|(valid, actual)| format!("({valid}, {actual})"))
        .collect();
    conflicts.sort();
    Error::new(
        ErrorKind::DataInvalid,
        format!(
            "Unable to determine whether certain files are orphan. Metadata references files that \
             match listed/provided files except for authority/scheme. Please, inspect the \
             conflicting authorities/schemes and provide which of them are equal by further \
             configuring the action via equalSchemes() and equalAuthorities() methods. Set the \
             prefix mismatch mode to 'IGNORE' to skip remaining locations with conflicting \
             authorities/schemes or to 'DELETE' iff you are ABSOLUTELY confident that remaining \
             conflicting authorities/schemes are different. It will be impossible to recover \
             deleted files. Conflicting authorities/schemes: [{}].",
            conflicts.join(", ")
        ),
    )
}

/// Java `EQUAL_SCHEMES_DEFAULT = ImmutableMap.of("s3n,s3a", "s3")`, comma-flattened to
/// `{s3n → s3, s3a → s3}`.
fn default_equal_schemes() -> HashMap<String, String> {
    HashMap::from([
        ("s3n".to_string(), "s3".to_string()),
        ("s3a".to_string(), "s3".to_string()),
    ])
}

/// Flatten comma-separated keys (Java `flattenMap`): `{"s3a,s3n": "s3"}` → `{s3a→s3, s3n→s3}`,
/// trimming whitespace around each split key and the value.
fn flatten_map(map: HashMap<String, String>) -> HashMap<String, String> {
    let mut flattened = HashMap::new();
    for (key, value) in map {
        let value = value.trim().to_string();
        for split_key in key.split(',') {
            flattened.insert(split_key.trim().to_string(), value.clone());
        }
    }
    flattened
}

/// The version-hint file location (Java `ReachableFileUtil.versionHintLocation`:
/// `<location>/metadata/version-hint.text`). Only Hadoop tables have one, but Java always adds it
/// to the valid set, so a stray hint file under a non-Hadoop table is never deleted.
fn version_hint_location(table_location: &str) -> String {
    let trimmed = table_location.strip_suffix('/').unwrap_or(table_location);
    format!("{trimmed}/metadata/version-hint.text")
}

/// Parse a boolean table property (Java `PropertyUtil.propertyAsBoolean`). A present-but-
/// unparsable value is a loud error rather than a silent default (the GC gate must never be
/// silently bypassed by a typo).
fn parse_bool_property(
    properties: &HashMap<String, String>,
    key: &str,
    default: bool,
) -> Result<bool> {
    match properties.get(key) {
        None => Ok(default),
        Some(value) => value.parse::<bool>().map_err(|error| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("Invalid boolean value '{value}' for table property '{key}'"),
            )
            .with_source(error)
        }),
    }
}

/// Current wall-clock time in epoch millis, saturating on the (impossible) pre-epoch / overflow
/// cases. Used only to derive the default `olderThan` cutoff.
fn now_millis() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => i64::try_from(duration.as_millis()).unwrap_or(i64::MAX),
        Err(_) => 0,
    }
}

/// Test-only accessors for the private normalization / join / hidden-path helpers, so the unit
/// tests can pin the corruption-class logic directly without going through a full table fixture.
#[cfg(test)]
pub(super) mod test_hooks {
    use std::collections::HashMap;

    use super::{
        FileUri, OrphanClassification, PartitionAwareHiddenPathFilter, PrefixMismatchMode,
        classify_against_valid, default_equal_schemes, flatten_map, split_uri,
        version_hint_location,
    };

    /// A thin test handle over the private [`FileUri`].
    #[derive(Debug, Clone)]
    pub struct FileUriProbe(FileUri);

    impl FileUriProbe {
        /// Parse a location into a normalized URI (see [`FileUri::parse`]).
        pub fn parse(
            location: &str,
            equal_schemes: &HashMap<String, String>,
            equal_authorities: &HashMap<String, String>,
        ) -> Self {
            FileUriProbe(FileUri::parse(location, equal_schemes, equal_authorities))
        }

        /// Whether this (valid) URI's scheme matches `actual`'s.
        pub fn scheme_matches(&self, actual: &FileUriProbe) -> bool {
            self.0.scheme_matches(&actual.0)
        }

        /// Whether this (valid) URI's authority matches `actual`'s.
        pub fn authority_matches(&self, actual: &FileUriProbe) -> bool {
            self.0.authority_matches(&actual.0)
        }

        /// The normalized path component (the join key).
        pub fn path_probe(&self) -> &str {
            &self.0.path
        }
    }

    /// Split a location into `(scheme, authority, path)`.
    pub fn split_uri_probe(location: &str) -> (Option<String>, Option<String>, String) {
        split_uri(location)
    }

    /// The default equal-schemes map.
    pub fn default_equal_schemes_probe() -> HashMap<String, String> {
        default_equal_schemes()
    }

    /// Comma-flatten a map.
    pub fn flatten_map_probe(map: HashMap<String, String>) -> HashMap<String, String> {
        flatten_map(map)
    }

    /// The version-hint location for a table location.
    pub fn version_hint_probe(table_location: &str) -> String {
        version_hint_location(table_location)
    }

    /// Whether `location`'s path under `base` is hidden, given the named-partition exception
    /// prefixes (e.g. `["_part="]`).
    pub fn is_hidden_under_probe(
        base: &str,
        location: &str,
        partition_prefixes: &[String],
    ) -> bool {
        let filter = PartitionAwareHiddenPathFilter {
            hidden_partition_prefixes: partition_prefixes.to_vec(),
        };
        filter.is_hidden_under(base, location)
    }

    /// `(valid, actual)` conflict pairs for a single component (scheme or authority).
    type ConflictPairs = Vec<(String, String)>;

    /// The result of [`classify_one`]: `(is_orphan, scheme_conflicts, authority_conflicts)`.
    type ClassifyResult = (bool, ConflictPairs, ConflictPairs);

    /// Classify `actual` against `valid_candidates` under `mode`, returning
    /// `(is_orphan, scheme_conflicts, authority_conflicts)` (each conflict is a `(valid, actual)`
    /// pair).
    pub fn classify_one(
        actual: &FileUriProbe,
        valid_candidates: &[FileUriProbe],
        mode: PrefixMismatchMode,
    ) -> ClassifyResult {
        let valids: Vec<FileUri> = valid_candidates
            .iter()
            .map(|probe| probe.0.clone())
            .collect();
        let mut scheme_conflicts = std::collections::HashSet::new();
        let mut authority_conflicts = std::collections::HashSet::new();
        let classification = classify_against_valid(
            &actual.0,
            &valids,
            mode,
            &mut scheme_conflicts,
            &mut authority_conflicts,
        );
        let is_orphan = matches!(classification, OrphanClassification::Orphan);
        let mut schemes: Vec<(String, String)> = scheme_conflicts.into_iter().collect();
        let mut authorities: Vec<(String, String)> = authority_conflicts.into_iter().collect();
        schemes.sort();
        authorities.sort();
        (is_orphan, schemes, authorities)
    }
}
