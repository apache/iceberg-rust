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

//! ExpireSnapshots — the METADATA retention semantics of Java's `ExpireSnapshots` /
//! `RemoveSnapshots` (the `cleanExpiredFiles(false)` posture).
//!
//! **THIS ACTION NEVER DELETES FILES.** It computes exactly which snapshots and snapshot
//! references fall out of the table's retention policy and emits the corresponding
//! [`TableUpdate::RemoveSnapshots`] / [`TableUpdate::RemoveSnapshotRef`] metadata updates —
//! nothing else. Physical cleanup of the manifests, manifest lists, data files, delete files,
//! and statistics files that become unreachable (Java's `cleanExpiredFiles(true)` default,
//! `ReachableFileCleanup`, `deleteWith`) lives in the sibling module
//! [`expire_cleanup`](super::expire_cleanup) (Increment B2) as an EXPLICIT post-commit step —
//! [`ExpireSnapshotsCleanup`](crate::transaction::ExpireSnapshotsCleanup) — never run by this
//! action: committing this action only shrinks the metadata; the files stay on storage until
//! the caller invokes the cleanup (preferably via `ExpireSnapshotsCleanup::commit_and_clean`).
//!
//! The retention computation is a 1:1 port of Java 1.10.0 `RemoveSnapshots.internalApply`
//! (every branch bytecode-verified against `iceberg-core-1.10.0.jar`):
//!
//! 1. **Retained refs** (`computeRetainedRefs`): `main` is always retained (never ref-expired);
//!    any other branch or tag is retained iff `now − snapshot.timestamp_ms <= maxRefAgeMs`
//!    (the ref's own `max_ref_age_ms`, falling back to `history.expire.max-ref-age-ms`, default
//!    forever); a ref whose snapshot no longer exists is dropped with a warning.
//! 2. **Explicit-id guard:** an id passed to [`ExpireSnapshotsAction::expire_snapshot_id`] that a
//!    RETAINED ref still points at fails with Java's exact message
//!    (`Cannot expire %s. Still referenced by refs: %s`). Ids pointed at only by refs that expire
//!    in the same pass are allowed.
//! 3. **Per-branch retention** (`computeBranchSnapshotsToRetain`): for every retained branch, walk
//!    its ancestry from the head and keep each ancestor while
//!    `kept < minSnapshotsToKeep || ancestor.timestamp_ms >= expireOlderThan`, STOPPING at the
//!    first ancestor that fails both (the retained set is a contiguous prefix of the chain). The
//!    cutoff is `now − ref.max_snapshot_age_ms` when the branch sets one, else the default below;
//!    the floor is `ref.min_snapshots_to_keep`, else `history.expire.min-snapshots-to-keep`
//!    (default 1).
//! 4. **Unreferenced retention** (`unreferencedSnapshotsToRetain`): snapshots reachable from no
//!    retained ref (branch ancestries + tag targets) are retained iff
//!    `timestamp_ms >= defaultExpireOlderThan`.
//! 5. Everything not retained — plus the explicit ids, which override age/ancestry retention for
//!    non-ref-head snapshots exactly as in Java (the `idsToRemove` seed set) — is removed.
//!
//! The default cutoff (`defaultExpireOlderThan`) is `expire_older_than(ts)` when called, else
//! `now − history.expire.max-snapshot-age-ms` (default 5 days).
//!
//! **Documented divergences from Java (timing only, same outcomes):**
//! - Java validates `retainLast` and the `gc.enabled` gate eagerly (builder call / constructor);
//!   this action is built without a table (the fork's stateless-retry pattern), so both checks run
//!   at commit time against the refreshed table, with Java's exact messages.
//! - Java pins the `history.expire.*` defaults from the properties at action construction and the
//!   wall clock at construction; this action reads the properties at commit time (strictly
//!   fresher; observable only under a concurrent property change) and pins `now` at construction.
//! - A malformed `history.expire.*` / `gc.enabled` property value fails the commit loudly
//!   (`DataInvalid`), mirroring Java's `NumberFormatException` from `PropertyUtil` in
//!   fail-loud effect.
//! - Optimistic-concurrency posture: Java's primary path commits through a full-metadata CAS
//!   (`ops.commit(base, updated)`), while its REST `UpdateRequirements` derives no requirement at
//!   all for snapshot removal. This action emits a [`TableRequirement::RefSnapshotIdMatch`] for
//!   EVERY ref consulted by the computation — narrower than Java's full CAS, strictly safer than
//!   the REST shape: a concurrent rollback (the case where applying a stale removal would destroy
//!   the new head) is rejected and recomputed on retry.
//!
//! **Deferred (loudly):** the `IncrementalFileCleanup` strategy (see the deferral note in
//! [`expire_cleanup`](super::expire_cleanup) — B2 ports the general-correct
//! `ReachableFileCleanup` only); `cleanExpiredMetadata(true)` (unreachable spec/schema removal —
//! needs manifest IO to compute reachable spec ids); Java interop evidence.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;

use crate::error::Result;
use crate::spec::{
    MAIN_BRANCH, Snapshot, SnapshotReference, SnapshotRetention, TableMetadata, TableProperties,
};
use crate::table::Table;
use crate::transaction::{ActionCommit, TransactionAction};
use crate::{Error, ErrorKind, TableRequirement, TableUpdate};

/// Transaction action expiring snapshots per the table's retention policy — the metadata half of
/// Java `ExpireSnapshots` (see the [module docs](self) for the exact semantics and the loud
/// **no-file-deletion** boundary: file cleanup is Increment B2, not this action).
pub struct ExpireSnapshotsAction {
    /// Expire snapshots strictly older than this timestamp (ms). `None` defers to
    /// `now − history.expire.max-snapshot-age-ms`.
    expire_older_than_ms: Option<i64>,
    /// Minimum number of snapshots to retain on each branch. `None` defers to the branch's
    /// `min_snapshots_to_keep`, then `history.expire.min-snapshots-to-keep`.
    retain_last: Option<i32>,
    /// Snapshot ids explicitly requested for removal (Java's `idsToRemove` seed set).
    ids_to_expire: Vec<i64>,
    /// Wall-clock millis captured when the action was created (Java pins `now` in the
    /// `RemoveSnapshots` constructor); every age computation derives from this single instant.
    now_ms: i64,
}

impl ExpireSnapshotsAction {
    /// Creates an expire-snapshots action with no explicit cutoff (the table's
    /// `history.expire.*` defaults apply at commit time).
    pub fn new() -> Self {
        ExpireSnapshotsAction {
            expire_older_than_ms: None,
            retain_last: None,
            ids_to_expire: vec![],
            now_ms: chrono::Utc::now().timestamp_millis(),
        }
    }

    /// Expire snapshots with a timestamp STRICTLY older than `timestamp_ms` (a snapshot whose
    /// timestamp equals `timestamp_ms` is kept — Java retains on
    /// `timestampMillis() >= expireOlderThan`). Overrides the table's
    /// `history.expire.max-snapshot-age-ms` default; the last call wins (Java
    /// `expireOlderThan` overwrites `defaultExpireOlderThan`).
    pub fn expire_older_than(mut self, timestamp_ms: i64) -> Self {
        self.expire_older_than_ms = Some(timestamp_ms);
        self
    }

    /// Retain at least `num_snapshots` snapshots on each branch regardless of age, unless the
    /// branch carries its own `min_snapshots_to_keep`. Must be `>= 1`; rejected at commit with
    /// Java's exact message (Java validates eagerly in `retainLast` — same message, deferred
    /// timing per this fork's stateless-action pattern). The last call wins.
    pub fn retain_last(mut self, num_snapshots: i32) -> Self {
        self.retain_last = Some(num_snapshots);
        self
    }

    /// Explicitly expire the snapshot with the given id; repeatable. Mirrors Java
    /// `expireSnapshotId`: the id is removed even when age/ancestry retention would keep it, an id
    /// still pointed at by a RETAINED ref fails the commit, and an id not present in the table
    /// metadata is silently ignored.
    pub fn expire_snapshot_id(mut self, snapshot_id: i64) -> Self {
        self.ids_to_expire.push(snapshot_id);
        self
    }

    /// Test-only override of the pinned wall clock so age boundaries can be exercised against
    /// fixed fixture timestamps.
    #[cfg(test)]
    fn with_now_ms(mut self, now_ms: i64) -> Self {
        self.now_ms = now_ms;
        self
    }
}

impl Default for ExpireSnapshotsAction {
    fn default() -> Self {
        Self::new()
    }
}

fn data_invalid(message: String) -> Error {
    Error::new(ErrorKind::DataInvalid, message)
}

/// Parse a table property, failing loudly on a malformed value (a silently-defaulted retention
/// setting could over-expire; Java's `PropertyUtil` throws `NumberFormatException` likewise).
/// `pub(super)` so the B2 cleanup sibling ([`super::expire_cleanup`]) re-honors the
/// `gc.enabled` gate with identical parsing.
pub(super) fn parse_property<T: std::str::FromStr>(
    properties: &HashMap<String, String>,
    key: &str,
    default: T,
) -> Result<T>
where
    <T as std::str::FromStr>::Err: std::fmt::Display,
{
    match properties.get(key) {
        None => Ok(default),
        Some(value) => value
            .parse::<T>()
            .map_err(|error| data_invalid(format!("Invalid value for {key}: {error}"))),
    }
}

/// A ref's `max_ref_age_ms`, regardless of branch/tag shape (Java `SnapshotRef.maxRefAgeMs()`).
fn max_ref_age_ms(reference: &SnapshotReference) -> Option<i64> {
    match &reference.retention {
        SnapshotRetention::Branch { max_ref_age_ms, .. } => *max_ref_age_ms,
        SnapshotRetention::Tag { max_ref_age_ms } => *max_ref_age_ms,
    }
}

/// The ancestor chain of `head_id`, head first and inclusive, following `parent_snapshot_id`
/// until the root or the first missing parent (Java `SnapshotUtil.ancestorsOf`: the lookup
/// returning null ends the walk silently, but a missing HEAD is an error — Java's
/// `Preconditions.checkArgument(start != null, "Cannot find snapshot: %s", snapshotId)`).
fn ancestors_of(metadata: &TableMetadata, head_id: i64) -> Result<Vec<&Arc<Snapshot>>> {
    let head = metadata
        .snapshot_by_id(head_id)
        .ok_or_else(|| data_invalid(format!("Cannot find snapshot: {head_id}")))?;
    let mut chain = vec![head];
    let mut parent_id = head.parent_snapshot_id();
    while let Some(id) = parent_id {
        match metadata.snapshot_by_id(id) {
            Some(snapshot) => {
                chain.push(snapshot);
                parent_id = snapshot.parent_snapshot_id();
            }
            None => break,
        }
    }
    Ok(chain)
}

/// The contiguous prefix of a branch's ancestry to retain (Java
/// `computeBranchSnapshotsToRetain`): keep each ancestor (head first) while
/// `kept < min_snapshots_to_keep || timestamp_ms >= expire_older_than_ms`, and STOP at the first
/// ancestor failing both — an out-of-order older-but-recent ancestor behind the stop point is NOT
/// retained, exactly as Java's early `return`.
fn branch_snapshots_to_retain(
    metadata: &TableMetadata,
    branch_head_id: i64,
    expire_older_than_ms: i64,
    min_snapshots_to_keep: i32,
) -> Result<HashSet<i64>> {
    let min_snapshots_to_keep = min_snapshots_to_keep.max(0) as usize;
    let mut retained = HashSet::new();
    for ancestor in ancestors_of(metadata, branch_head_id)? {
        if retained.len() < min_snapshots_to_keep || ancestor.timestamp_ms() >= expire_older_than_ms
        {
            retained.insert(ancestor.snapshot_id());
        } else {
            break;
        }
    }
    Ok(retained)
}

/// Snapshots referenced by NO retained ref (branch ancestries + tag targets) that are still young
/// enough to keep: `timestamp_ms >= default_expire_older_than_ms` retains (Java
/// `unreferencedSnapshotsToRetain` — note the default cutoff applies here even when a branch
/// carries its own `max_snapshot_age_ms`).
fn unreferenced_snapshots_to_retain(
    metadata: &TableMetadata,
    retained_refs: &HashMap<&String, &SnapshotReference>,
    default_expire_older_than_ms: i64,
) -> Result<HashSet<i64>> {
    let mut referenced: HashSet<i64> = HashSet::new();
    for reference in retained_refs.values() {
        if reference.is_branch() {
            for ancestor in ancestors_of(metadata, reference.snapshot_id)? {
                referenced.insert(ancestor.snapshot_id());
            }
        } else {
            referenced.insert(reference.snapshot_id);
        }
    }

    Ok(metadata
        .snapshots()
        .filter(|snapshot| {
            !referenced.contains(&snapshot.snapshot_id())
                && snapshot.timestamp_ms() >= default_expire_older_than_ms
        })
        .map(|snapshot| snapshot.snapshot_id())
        .collect())
}

#[async_trait]
impl TransactionAction for ExpireSnapshotsAction {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        let metadata = table.metadata();
        let properties = metadata.properties();

        // Java `RemoveSnapshots` constructor: refuse to expire when GC is disabled. The message is
        // verbatim; the check runs at commit (against the refreshed table) instead of at builder
        // construction — see the module docs.
        let gc_enabled = parse_property(
            properties,
            TableProperties::PROPERTY_GC_ENABLED,
            TableProperties::PROPERTY_GC_ENABLED_DEFAULT,
        )?;
        if !gc_enabled {
            return Err(data_invalid(
                "Cannot expire snapshots: GC is disabled (deleting files may corrupt other tables)"
                    .to_string(),
            ));
        }

        // Java `retainLast`: `Preconditions.checkArgument(1 <= numSnapshots, ...)`, verbatim
        // message (deferred to commit per the stateless-action pattern).
        if let Some(num_snapshots) = self.retain_last
            && num_snapshots < 1
        {
            return Err(data_invalid(format!(
                "Number of snapshots to retain must be at least 1, cannot be: {num_snapshots}"
            )));
        }

        // Java `internalApply`: a table with no snapshots is a no-op.
        if metadata.snapshots().len() == 0 {
            return Ok(ActionCommit::new(vec![], vec![]));
        }

        let now_ms = self.now_ms;
        let default_max_snapshot_age_ms = parse_property(
            properties,
            TableProperties::PROPERTY_MAX_SNAPSHOT_AGE_MS,
            TableProperties::PROPERTY_MAX_SNAPSHOT_AGE_MS_DEFAULT,
        )?;
        // The default cutoff: an explicit `expire_older_than` wins over the table property
        // (Java: `expireOlderThan` overwrites the constructor-derived `defaultExpireOlderThan`).
        let default_expire_older_than_ms = self
            .expire_older_than_ms
            .unwrap_or_else(|| now_ms.saturating_sub(default_max_snapshot_age_ms));
        // `retain_last` wins over the table property (Java: `retainLast` overwrites
        // `defaultMinNumSnapshots`).
        let default_min_snapshots_to_keep = match self.retain_last {
            Some(num_snapshots) => num_snapshots,
            None => parse_property(
                properties,
                TableProperties::PROPERTY_MIN_SNAPSHOTS_TO_KEEP,
                TableProperties::PROPERTY_MIN_SNAPSHOTS_TO_KEEP_DEFAULT,
            )?,
        };
        let default_max_ref_age_ms = parse_property(
            properties,
            TableProperties::PROPERTY_MAX_REF_AGE_MS,
            TableProperties::PROPERTY_MAX_REF_AGE_MS_DEFAULT,
        )?;

        // 1. Retained refs (Java `computeRetainedRefs`): `main` always survives; any other ref
        //    survives while `now − snapshot.timestamp <= maxRefAgeMs`; a ref whose snapshot is
        //    missing from metadata is dropped with a warning.
        let mut retained_refs: HashMap<&String, &SnapshotReference> = HashMap::new();
        for (name, reference) in &metadata.refs {
            if name == MAIN_BRANCH {
                retained_refs.insert(name, reference);
                continue;
            }
            // A ref whose snapshot no longer exists falls through and is dropped. Java logs
            // `Removing invalid ref {}: snapshot {} does not exist` at WARN; the `iceberg` crate
            // has no logging-facade dependency (adding one needs approval — see the metrics
            // module), so the drop is silent here; the emitted `RemoveSnapshotRef` update still
            // records the removal.
            if let Some(snapshot) = metadata.snapshot_by_id(reference.snapshot_id) {
                let max_age_ms = max_ref_age_ms(reference).unwrap_or(default_max_ref_age_ms);
                // Saturating: `timestamp_ms` comes from on-disk metadata and must not be able
                // to panic/wrap an age computation (hostile-value rule).
                let ref_age_ms = now_ms.saturating_sub(snapshot.timestamp_ms());
                if ref_age_ms <= max_age_ms {
                    retained_refs.insert(name, reference);
                }
            }
        }

        // 2. Explicit-id guard (Java's `Preconditions.checkArgument(refsForId == null, ...)`):
        //    an explicitly-expired id that a RETAINED ref still points at is an error. Names are
        //    sorted so the message is deterministic (Java renders an unordered list).
        let mut retained_id_to_refs: HashMap<i64, Vec<&str>> = HashMap::new();
        for (name, reference) in &retained_refs {
            retained_id_to_refs
                .entry(reference.snapshot_id)
                .or_default()
                .push(name.as_str());
        }
        let explicit_ids: BTreeSet<i64> = self.ids_to_expire.iter().copied().collect();
        for id in &explicit_ids {
            if let Some(ref_names) = retained_id_to_refs.get_mut(id) {
                ref_names.sort_unstable();
                return Err(data_invalid(format!(
                    "Cannot expire {id}. Still referenced by refs: [{}]",
                    ref_names.join(", ")
                )));
            }
        }

        // 3 + 4. The retained set: every retained ref's head, each retained branch's contiguous
        //    retention prefix, and unreferenced-but-recent snapshots.
        let mut ids_to_retain: HashSet<i64> = retained_refs
            .values()
            .map(|reference| reference.snapshot_id)
            .collect();
        for reference in retained_refs.values() {
            if let SnapshotRetention::Branch {
                min_snapshots_to_keep,
                max_snapshot_age_ms,
                ..
            } = &reference.retention
            {
                // The branch's own age/count settings beat the defaults (Java
                // `computeAllBranchSnapshotsToRetain`); a branch-specific age is anchored at `now`.
                let expire_older_than_ms = max_snapshot_age_ms
                    .map(|age_ms| now_ms.saturating_sub(age_ms))
                    .unwrap_or(default_expire_older_than_ms);
                let min_snapshots_to_keep =
                    min_snapshots_to_keep.unwrap_or(default_min_snapshots_to_keep);
                ids_to_retain.extend(branch_snapshots_to_retain(
                    metadata,
                    reference.snapshot_id,
                    expire_older_than_ms,
                    min_snapshots_to_keep,
                )?);
            }
        }
        ids_to_retain.extend(unreferenced_snapshots_to_retain(
            metadata,
            &retained_refs,
            default_expire_older_than_ms,
        )?);

        // 5. Everything not retained is removed; the explicit ids are the seed set and override
        //    age/ancestry retention (Java applies `idsToRemove` unconditionally after the ref-head
        //    guard above). Ids absent from metadata are dropped: Java's builder only records
        //    actually-removed snapshots. BTreeSet ⇒ deterministic (sorted) update output.
        let mut ids_to_remove: BTreeSet<i64> = explicit_ids
            .into_iter()
            .filter(|id| metadata.snapshot_by_id(*id).is_some())
            .collect();
        ids_to_remove.extend(
            metadata
                .snapshots()
                .map(|snapshot| snapshot.snapshot_id())
                .filter(|id| !ids_to_retain.contains(id)),
        );

        let mut refs_to_remove: Vec<&String> = metadata
            .refs
            .keys()
            .filter(|name| !retained_refs.contains_key(name))
            .collect();
        refs_to_remove.sort_unstable();

        // No-op suppression: nothing expired ⇒ emit nothing (the manage_snapshots convention;
        // Java's apply would return the base metadata unchanged).
        if ids_to_remove.is_empty() && refs_to_remove.is_empty() {
            return Ok(ActionCommit::new(vec![], vec![]));
        }

        // Ref removals first, then the snapshot removal — the order Java's `internalApply` builds
        // its changes in (`removeRef` per expired ref, then `removeSnapshots`).
        let mut updates: Vec<TableUpdate> = Vec::with_capacity(refs_to_remove.len() + 1);
        for ref_name in &refs_to_remove {
            updates.push(TableUpdate::RemoveSnapshotRef {
                ref_name: (*ref_name).clone(),
            });
        }
        if !ids_to_remove.is_empty() {
            updates.push(TableUpdate::RemoveSnapshots {
                snapshot_ids: ids_to_remove.into_iter().collect(),
            });
        }

        // Optimistic-concurrency guards: every ref consulted by the computation must still point
        // where we observed it, or the commit conflicts and the retry loop recomputes against the
        // refreshed base. This is the action-level analogue of Java's full-metadata CAS
        // (`ops.commit(base, updated)`) narrowed to the ref surface; it is what rejects the
        // concurrent-rollback case where applying a stale removal would destroy the new head.
        let mut guarded_ref_names: Vec<&String> = metadata.refs.keys().collect();
        guarded_ref_names.sort_unstable();
        let requirements: Vec<TableRequirement> = guarded_ref_names
            .into_iter()
            .map(|name| TableRequirement::RefSnapshotIdMatch {
                r#ref: name.clone(),
                snapshot_id: Some(metadata.refs[name].snapshot_id),
            })
            .collect();

        Ok(ActionCommit::new(updates, requirements))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use crate::spec::{
        MAIN_BRANCH, Operation, Snapshot, SnapshotReference, SnapshotRetention, Summary,
        TableProperties,
    };
    use crate::table::Table;
    use crate::transaction::{ApplyTransactionAction, Transaction, TransactionAction};
    use crate::{ErrorKind, TableRequirement, TableUpdate};

    // From TableMetadataV2Valid.json: main -> CURRENT, whose parent is ROOT.
    const ROOT: i64 = 3051729675574597004;
    const CURRENT: i64 = 3055729675574597004;
    // Grafted main-chain snapshots (CURRENT ← S1 ← S2 ← S3, main -> S3).
    const S1: i64 = 101;
    const S2: i64 = 102;
    const S3: i64 = 103;
    const T0: i64 = 1_700_000_000_000;
    const S1_TS: i64 = T0 + 1000;
    const S2_TS: i64 = T0 + 2000;
    const S3_TS: i64 = T0 + 3000;
    /// The pinned "wall clock" for every test (see `with_now_ms`): far enough after every fixture
    /// timestamp that the 5-day default age cutoff (now − 432000000) postdates them all.
    const NOW: i64 = 1_800_000_000_000;

    fn table() -> Table {
        crate::transaction::tests::make_v2_table()
    }

    /// Append a parent-linked chain of `(snapshot_id, timestamp_ms)` snapshots to `branch`,
    /// starting from `parent`. One metadata build per snapshot, so each main-branch commit gets
    /// its own snapshot-log entry (a single-pass chain would be collapsed by the
    /// intermediate-snapshot suppression in `build()`).
    fn append_chain(table: Table, branch: &str, parent: i64, chain: &[(i64, i64)]) -> Table {
        let mut table = table;
        let mut parent_id = parent;
        for (snapshot_id, timestamp_ms) in chain {
            let sequence_number = table.metadata().last_sequence_number() + 1;
            let snapshot = Snapshot::builder()
                .with_snapshot_id(*snapshot_id)
                .with_parent_snapshot_id(Some(parent_id))
                .with_sequence_number(sequence_number)
                .with_timestamp_ms(*timestamp_ms)
                .with_manifest_list(format!("/tmp/manifest-list-{snapshot_id}.avro"))
                .with_summary(Summary {
                    operation: Operation::Append,
                    additional_properties: HashMap::new(),
                })
                .with_schema_id(1)
                .build();
            let metadata = table
                .metadata()
                .clone()
                .into_builder(None)
                .set_branch_snapshot(snapshot, branch)
                .expect("append chain snapshot")
                .build()
                .expect("build chain metadata")
                .metadata;
            table = table.with_metadata(Arc::new(metadata));
            parent_id = *snapshot_id;
        }
        table
    }

    /// Graft a single DANGLING snapshot (present in metadata, referenced by no ref).
    fn add_dangling_snapshot(
        table: Table,
        snapshot_id: i64,
        parent: i64,
        timestamp_ms: i64,
    ) -> Table {
        let sequence_number = table.metadata().last_sequence_number() + 1;
        let snapshot = Snapshot::builder()
            .with_snapshot_id(snapshot_id)
            .with_parent_snapshot_id(Some(parent))
            .with_sequence_number(sequence_number)
            .with_timestamp_ms(timestamp_ms)
            .with_manifest_list(format!("/tmp/manifest-list-{snapshot_id}.avro"))
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: HashMap::new(),
            })
            .with_schema_id(1)
            .build();
        let metadata = table
            .metadata()
            .clone()
            .into_builder(None)
            .add_snapshot(snapshot)
            .expect("add dangling snapshot")
            .build()
            .expect("build dangling metadata")
            .metadata;
        table.with_metadata(Arc::new(metadata))
    }

    /// Set (or override) a non-`main` ref. Never use for `main` — `set_ref(main)` appends a
    /// snapshot-log entry.
    fn with_ref(table: Table, name: &str, snapshot_id: i64, retention: SnapshotRetention) -> Table {
        assert_ne!(name, MAIN_BRANCH, "with_ref must not touch main");
        let metadata = table
            .metadata()
            .clone()
            .into_builder(None)
            .set_ref(name, SnapshotReference::new(snapshot_id, retention))
            .expect("set ref")
            .build()
            .expect("build ref metadata")
            .metadata;
        table.with_metadata(Arc::new(metadata))
    }

    fn with_properties(table: Table, properties: &[(&str, &str)]) -> Table {
        let properties: HashMap<String, String> = properties
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect();
        let metadata = table
            .metadata()
            .clone()
            .into_builder(None)
            .set_properties(properties)
            .expect("set properties")
            .build()
            .expect("build properties metadata")
            .metadata;
        table.with_metadata(Arc::new(metadata))
    }

    /// `table()` + the S1..S3 main chain.
    fn chain_table() -> Table {
        append_chain(table(), MAIN_BRANCH, CURRENT, &[
            (S1, S1_TS),
            (S2, S2_TS),
            (S3, S3_TS),
        ])
    }

    async fn run(
        action: super::ExpireSnapshotsAction,
        table: &Table,
    ) -> (Vec<TableUpdate>, Vec<TableRequirement>) {
        let mut commit = Arc::new(action)
            .commit(table)
            .await
            .expect("commit expire action");
        (commit.take_updates(), commit.take_requirements())
    }

    /// The (sorted, by construction) id list of the single `RemoveSnapshots` update, or empty.
    fn removed_snapshot_ids(updates: &[TableUpdate]) -> Vec<i64> {
        updates
            .iter()
            .find_map(|update| match update {
                TableUpdate::RemoveSnapshots { snapshot_ids } => Some(snapshot_ids.clone()),
                _ => None,
            })
            .unwrap_or_default()
    }

    fn removed_ref_names(updates: &[TableUpdate]) -> Vec<String> {
        updates
            .iter()
            .filter_map(|update| match update {
                TableUpdate::RemoveSnapshotRef { ref_name } => Some(ref_name.clone()),
                _ => None,
            })
            .collect()
    }

    /// Apply the action's emitted updates through `TableMetadataBuilder` — the real catalog apply
    /// path (`TableUpdate::apply`), exercising ref-sweep / statistics / snapshot-log handling.
    fn apply_updates(table: &Table, updates: Vec<TableUpdate>) -> Table {
        let mut builder = table.metadata().clone().into_builder(None);
        for update in updates {
            builder = update.apply(builder).expect("apply update");
        }
        let metadata = builder.build().expect("build applied metadata").metadata;
        table.clone().with_metadata(Arc::new(metadata))
    }

    fn expire(table: &Table) -> super::ExpireSnapshotsAction {
        Transaction::new(table).expire_snapshots().with_now_ms(NOW)
    }

    // =======================================================================
    // retain_last boundaries
    // =======================================================================

    /// Risk: OVER-expiry past the count floor (head history destroyed) or UNDER-expiry (retention
    /// never shrinks the chain). `retain_last(2)` with everything age-expired must keep EXACTLY
    /// the 2 newest ancestors of main and remove exactly the other 3.
    #[tokio::test]
    async fn test_retain_last_keeps_exactly_n_newest_ancestors() {
        let table = chain_table();
        let action = expire(&table).expire_older_than(NOW).retain_last(2);
        let (updates, _) = run(action, &table).await;
        assert_eq!(removed_snapshot_ids(&updates), vec![S1, ROOT, CURRENT]);
        assert!(removed_ref_names(&updates).is_empty(), "main must survive");
    }

    /// Risk: the floor under-protecting at its minimum — `retain_last(1)` must keep the branch
    /// head itself and nothing else.
    #[tokio::test]
    async fn test_retain_last_one_keeps_only_branch_head() {
        let table = chain_table();
        let action = expire(&table).expire_older_than(NOW).retain_last(1);
        let (updates, _) = run(action, &table).await;
        assert_eq!(removed_snapshot_ids(&updates), vec![S1, S2, ROOT, CURRENT]);
    }

    /// Risk: expiring when the floor exceeds the history length (there is nothing beyond the
    /// floor to expire) — must be a complete no-op.
    #[tokio::test]
    async fn test_retain_last_exceeding_history_length_expires_nothing() {
        let table = chain_table();
        let action = expire(&table).expire_older_than(NOW).retain_last(10);
        let (updates, requirements) = run(action, &table).await;
        assert!(updates.is_empty(), "nothing must be expired: {updates:?}");
        assert!(requirements.is_empty());
    }

    /// Risk: a zero/negative floor silently expiring every snapshot. Java's exact
    /// `retainLast` message, non-retryable.
    #[tokio::test]
    async fn test_retain_last_zero_fails_with_java_message() {
        let table = chain_table();
        let action = expire(&table).retain_last(0);
        let error = Arc::new(action)
            .commit(&table)
            .await
            .map(drop)
            .expect_err("retain_last(0) must fail");
        assert_eq!(error.kind(), ErrorKind::DataInvalid);
        assert_eq!(
            error.message(),
            "Number of snapshots to retain must be at least 1, cannot be: 0"
        );
        assert!(!error.retryable());
    }

    #[tokio::test]
    async fn test_retain_last_negative_fails_with_java_message() {
        let table = chain_table();
        let action = expire(&table).retain_last(-3);
        let error = Arc::new(action)
            .commit(&table)
            .await
            .map(drop)
            .expect_err("negative retain_last must fail");
        assert_eq!(
            error.message(),
            "Number of snapshots to retain must be at least 1, cannot be: -3"
        );
    }

    // =======================================================================
    // expire_older_than age boundary — pinned ON the boundary
    // =======================================================================

    /// Risk: the age comparison flipping `>=` to `>` (one extra snapshot silently expired — the
    /// over-expiry direction) or to always-true (nothing ever expires). Pinned ON the boundary:
    /// a snapshot whose timestamp EQUALS the cutoff is KEPT (Java retains on
    /// `timestampMillis() >= expireSnapshotsOlderThan`), while its strictly-older parent goes.
    #[tokio::test]
    async fn test_expire_older_than_keeps_snapshot_with_timestamp_equal_to_cutoff() {
        let table = chain_table();
        let action = expire(&table).expire_older_than(S2_TS);
        let (updates, _) = run(action, &table).await;
        let removed = removed_snapshot_ids(&updates);
        assert!(!removed.contains(&S2), "ts == cutoff must be KEPT");
        assert!(!removed.contains(&S3));
        assert_eq!(removed, vec![S1, ROOT, CURRENT]);
    }

    /// The other direction of the same boundary: one millisecond past the snapshot's timestamp
    /// expires it.
    #[tokio::test]
    async fn test_expire_older_than_expires_snapshot_one_ms_below_cutoff() {
        let table = chain_table();
        let action = expire(&table).expire_older_than(S2_TS + 1);
        let (updates, _) = run(action, &table).await;
        let removed = removed_snapshot_ids(&updates);
        assert!(removed.contains(&S2), "ts == cutoff - 1 must be expired");
        assert_eq!(removed, vec![S1, S2, ROOT, CURRENT]);
    }

    // =======================================================================
    // per-branch retention overrides
    // =======================================================================

    /// Risk: dropping the per-branch min-snapshots floor — an old-but-protected snapshot must
    /// SURVIVE age expiry because a branch's own `min_snapshots_to_keep` (3) outranks the default
    /// (1). The unprotected older ancestors must still go (the floor must not over-protect).
    #[tokio::test]
    async fn test_branch_min_snapshots_to_keep_protects_old_snapshots_from_age_expiry() {
        let table = with_ref(
            chain_table(),
            "dev",
            S3,
            SnapshotRetention::branch(Some(3), None, None),
        );
        let action = expire(&table).expire_older_than(NOW);
        let (updates, _) = run(action, &table).await;
        let removed = removed_snapshot_ids(&updates);
        assert!(
            !removed.contains(&S1) && !removed.contains(&S2),
            "dev's min_snapshots_to_keep=3 must protect S1/S2 from age expiry: {removed:?}"
        );
        assert_eq!(removed, vec![ROOT, CURRENT]);
        assert!(removed_ref_names(&updates).is_empty());
    }

    /// Risk: ignoring a branch's own `max_snapshot_age_ms` (keep direction): the table default
    /// expires everything old, but the dev branch's large own age (anchored at `now`) must keep
    /// its side-chain ancestors alive.
    #[tokio::test]
    async fn test_branch_max_snapshot_age_keeps_what_table_default_would_expire() {
        const D1: i64 = 201;
        const D2: i64 = 202;
        let d1_ts = T0 + 10_000;
        let d2_ts = T0 + 11_000;
        let table = append_chain(chain_table(), "dev", CURRENT, &[(D1, d1_ts), (D2, d2_ts)]);
        // cutoff_dev = NOW - (NOW - T0) = T0 <= d1_ts => age keeps D1 and D2.
        let table = with_ref(
            table,
            "dev",
            D2,
            SnapshotRetention::branch(None, Some(NOW - T0), None),
        );
        // No explicit expire_older_than: the table default (5 days before NOW) postdates every
        // fixture timestamp, so main keeps only its head.
        let action = expire(&table);
        let (updates, _) = run(action, &table).await;
        let removed = removed_snapshot_ids(&updates);
        assert!(
            !removed.contains(&D1) && !removed.contains(&D2),
            "dev's own max_snapshot_age_ms must keep its ancestors: {removed:?}"
        );
        assert_eq!(removed, vec![S1, S2, ROOT, CURRENT]);
    }

    /// Risk: ignoring a branch's own `max_snapshot_age_ms` (expire direction): the table property
    /// keeps everything, but the dev branch's tiny own age must expire its non-head ancestor.
    #[tokio::test]
    async fn test_branch_max_snapshot_age_expires_what_table_default_would_keep() {
        const D1: i64 = 201;
        const D2: i64 = 202;
        let d1_ts = T0 + 10_000;
        let d2_ts = T0 + 11_000;
        let table = append_chain(chain_table(), "dev", CURRENT, &[(D1, d1_ts), (D2, d2_ts)]);
        // cutoff_dev = NOW - 1000 > d2_ts => age expires both; the default floor (1) keeps D2.
        let table = with_ref(
            table,
            "dev",
            D2,
            SnapshotRetention::branch(None, Some(1000), None),
        );
        // Table-wide default age keeps EVERYTHING (cutoff predates ROOT).
        let huge_age = NOW - 1_500_000_000_000;
        let table = with_properties(table, &[(
            TableProperties::PROPERTY_MAX_SNAPSHOT_AGE_MS,
            &huge_age.to_string(),
        )]);
        let action = expire(&table);
        let (updates, _) = run(action, &table).await;
        assert_eq!(
            removed_snapshot_ids(&updates),
            vec![D1],
            "only dev's over-age ancestor must go; main's chain is kept by the table default"
        );
    }

    /// Risk pinned: a branch's own `max_snapshot_age_ms` beats even an EXPLICIT
    /// `expire_older_than` call. Java's `expireOlderThan` merely overwrites
    /// `defaultExpireOlderThan`; `computeAllBranchSnapshotsToRetain` still prefers the ref's own
    /// age (bytecode-verified). A refactor letting the explicit cutoff trump per-branch settings
    /// would over-expire protected branch history and must fail here.
    #[tokio::test]
    async fn test_branch_max_snapshot_age_beats_explicit_expire_older_than() {
        const D1: i64 = 211;
        const D2: i64 = 212;
        let d1_ts = T0 + 10_000;
        let d2_ts = T0 + 11_000;
        let table = append_chain(chain_table(), "dev", CURRENT, &[(D1, d1_ts), (D2, d2_ts)]);
        // cutoff_dev = NOW - (NOW - T0) = T0 <= d1_ts => dev's own age keeps D1 and D2, even
        // though the EXPLICIT cutoff below (NOW) postdates every fixture timestamp.
        let table = with_ref(
            table,
            "dev",
            D2,
            SnapshotRetention::branch(None, Some(NOW - T0), None),
        );
        let action = expire(&table).expire_older_than(NOW);
        let (updates, _) = run(action, &table).await;
        let removed = removed_snapshot_ids(&updates);
        assert!(
            !removed.contains(&D1) && !removed.contains(&D2),
            "dev's own max_snapshot_age_ms must beat the explicit cutoff: {removed:?}"
        );
        assert_eq!(removed, vec![S1, S2, ROOT, CURRENT]);
    }

    /// Risk pinned: CLOCK SKEW vs the EARLY STOP. The branch walk keeps ancestors while
    /// `kept < min || ts >= cutoff` and STOPS at the first ancestor failing both — Java 1.10.0
    /// `computeBranchSnapshotsToRetain`'s early `return` (bytecode-verified `areturn` inside the
    /// loop). With out-of-order timestamps (clock skew / backfilled commits within the 60s
    /// `add_snapshot` tolerance), an ancestor NEWER than the cutoff sitting BEHIND an
    /// older-than-cutoff ancestor is still EXPIRED: the retained set is a contiguous prefix, never
    /// a timestamp filter. Counterintuitive but the Java semantic — a future "fix" dropping the
    /// early stop (walking the full ancestry and keeping every recent ancestor) would silently
    /// diverge from Java and must fail here.
    #[tokio::test]
    async fn test_clock_skew_recent_ancestor_behind_early_stop_is_expired() {
        const A1: i64 = 401; // ts AFTER the cutoff, but behind the stop point
        const A2: i64 = 402; // ts before the cutoff — the early stop fires here
        const A3: i64 = 403; // head, ts after the cutoff
        let a1_ts = T0 + 50_000;
        let a2_ts = T0 + 1_000; // 49s behind its child — inside the clock-skew tolerance
        let a3_ts = T0 + 50_500;
        let cutoff = T0 + 10_000;
        let table = append_chain(table(), MAIN_BRANCH, CURRENT, &[
            (A1, a1_ts),
            (A2, a2_ts),
            (A3, a3_ts),
        ]);

        let action = expire(&table).expire_older_than(cutoff);
        let (updates, _) = run(action, &table).await;
        let removed = removed_snapshot_ids(&updates);
        assert!(
            removed.contains(&A1),
            "A1 is newer than the cutoff but BEHIND the early stop at A2 — Java expires it: {removed:?}"
        );
        assert_eq!(
            removed,
            vec![A1, A2, ROOT, CURRENT],
            "only the head survives"
        );

        // The early-stop result must survive the real apply: the log clears at the removed
        // entries, leaving the head.
        let expired_table = apply_updates(&table, updates);
        let log: Vec<i64> = expired_table
            .metadata()
            .snapshot_log
            .iter()
            .map(|entry| entry.snapshot_id)
            .collect();
        assert_eq!(log, vec![A3]);
    }

    // =======================================================================
    // ref (tag/branch) age expiry
    // =======================================================================

    /// Risk pinned: the ref-age boundary EXACTLY ON the limit. Java retains on
    /// `now − ts <= maxRefAgeMs` (bytecode `lcmp ifgt` — strictly-greater expires), so a ref whose
    /// age EQUALS its `max_ref_age_ms` is KEPT; one millisecond older goes. A `<=`→`<` flip
    /// (expiring at the boundary — the over-expiry direction) must fail here.
    #[tokio::test]
    async fn test_ref_age_equal_to_max_ref_age_is_retained() {
        const X: i64 = 321;
        let table = add_dangling_snapshot(chain_table(), X, CURRENT, T0);
        let age = NOW - T0;
        let table = with_ref(table, "at-limit", X, SnapshotRetention::Tag {
            max_ref_age_ms: Some(age), // ref age == max age => retained
        });
        let table = with_ref(table, "past-limit", X, SnapshotRetention::Tag {
            max_ref_age_ms: Some(age - 1), // ref age == max age + 1 => expired
        });
        let action = expire(&table).expire_older_than(0);
        let (updates, _) = run(action, &table).await;
        assert_eq!(
            removed_ref_names(&updates),
            vec!["past-limit".to_string()],
            "age == max_ref_age_ms must be KEPT; age == max_ref_age_ms + 1 must be expired"
        );
        assert!(
            removed_snapshot_ids(&updates).is_empty(),
            "the at-limit tag still protects X"
        );
    }

    /// Risk: inverting the ref-age comparison, or keeping the expired tag's snapshot alive
    /// forever. An over-age tag is removed AND its target — old and referenced by nothing else —
    /// is expired with it.
    #[tokio::test]
    async fn test_tag_past_max_ref_age_is_removed_and_unreferenced_snapshot_expires() {
        const X: i64 = 301;
        let table = add_dangling_snapshot(chain_table(), X, CURRENT, T0);
        let table = with_ref(table, "archive", X, SnapshotRetention::Tag {
            max_ref_age_ms: Some(1000), // ref age = NOW - T0 >> 1000 => expired
        });
        // Cutoff above X's timestamp but below the main chain's, so the focus stays on X.
        let action = expire(&table).expire_older_than(T0 + 500);
        let (updates, _) = run(action, &table).await;
        assert_eq!(removed_ref_names(&updates), vec!["archive".to_string()]);
        let removed = removed_snapshot_ids(&updates);
        assert!(
            removed.contains(&X),
            "the expired tag's old, otherwise-unreferenced snapshot must be expired: {removed:?}"
        );
    }

    /// Risk: expiring a snapshot another ref still needs. The tag expires, but its target is also
    /// the head of a live branch — the ref goes, the SNAPSHOT stays.
    #[tokio::test]
    async fn test_expired_tag_snapshot_is_kept_when_another_ref_still_needs_it() {
        const X: i64 = 301;
        let table = add_dangling_snapshot(chain_table(), X, CURRENT, T0);
        let table = with_ref(table, "archive", X, SnapshotRetention::Tag {
            max_ref_age_ms: Some(1000),
        });
        let table = with_ref(
            table,
            "keeper",
            X,
            SnapshotRetention::branch(None, None, None),
        );
        let action = expire(&table).expire_older_than(T0 + 500);
        let (updates, _) = run(action, &table).await;
        assert_eq!(removed_ref_names(&updates), vec!["archive".to_string()]);
        assert!(
            !removed_snapshot_ids(&updates).contains(&X),
            "keeper's head must not be expired"
        );
    }

    /// Risk: ref-age expiry reaching `main` (time-travel root destroyed). With the table-wide
    /// max-ref-age at 1ms every non-main ref is over-age, but `main` — just as old — must never
    /// be ref-expired. The same-age tag in the same fixture proves the expiry path actually ran.
    #[tokio::test]
    async fn test_main_branch_is_never_ref_expired() {
        let table = with_ref(chain_table(), "old-tag", S2, SnapshotRetention::Tag {
            max_ref_age_ms: None, // falls back to the table-wide default below
        });
        let table = with_properties(table, &[(TableProperties::PROPERTY_MAX_REF_AGE_MS, "1")]);
        // Cutoff 0: nothing is age-expired; only ref expiry can act.
        let action = expire(&table).expire_older_than(0);
        let (updates, _) = run(action, &table).await;
        assert_eq!(
            removed_ref_names(&updates),
            vec!["old-tag".to_string()],
            "the same-age tag must expire (proves the ref-age path ran) but main must not"
        );
        assert!(
            removed_snapshot_ids(&updates).is_empty(),
            "S2 is still an ancestor of main and must survive"
        );
    }

    // =======================================================================
    // expire_snapshot_id
    // =======================================================================

    /// Risk: explicitly expiring a snapshot a retained ref points at (the ref would dangle).
    /// Java's exact message, non-retryable.
    #[tokio::test]
    async fn test_expire_snapshot_id_still_referenced_by_retained_ref_fails() {
        let table = with_ref(chain_table(), "pin", S2, SnapshotRetention::Tag {
            max_ref_age_ms: None,
        });
        let action = expire(&table).expire_older_than(0).expire_snapshot_id(S2);
        let error = Arc::new(action)
            .commit(&table)
            .await
            .map(drop)
            .expect_err("expiring a retained ref's snapshot must fail");
        assert_eq!(error.kind(), ErrorKind::DataInvalid);
        assert_eq!(
            error.message(),
            format!("Cannot expire {S2}. Still referenced by refs: [pin]")
        );
        assert!(!error.retryable());
    }

    /// Risk: the explicit seed set being filtered by retention. Java applies `idsToRemove`
    /// unconditionally (only ref HEADS are guarded), so an explicitly-expired mid-chain ancestor
    /// is removed even though age retention (cutoff 0) would keep everything — punching a hole in
    /// main's ancestry. Applying the hole must then trigger the snapshot log's
    /// clear-history-before-invalid-entry rule (Java `updateSnapshotLog`): everything at/before
    /// the hole is cleared, only the still-contiguous tail survives.
    #[tokio::test]
    async fn test_expire_snapshot_id_removes_ancestor_even_when_age_would_retain_it() {
        let table = chain_table();
        let action = expire(&table).expire_older_than(0).expire_snapshot_id(S2);
        let (updates, _) = run(action, &table).await;
        assert_eq!(removed_snapshot_ids(&updates), vec![S2]);

        // Apply the mid-ancestry hole through the real update path: log was
        // [ROOT, CURRENT, S1, S2, S3]; the invalid S2 entry clears all history before it.
        let expired_table = apply_updates(&table, updates);
        let log: Vec<i64> = expired_table
            .metadata()
            .snapshot_log
            .iter()
            .map(|entry| entry.snapshot_id)
            .collect();
        assert_eq!(log, vec![S3], "history at/before the hole must be cleared");
        assert_eq!(expired_table.metadata().current_snapshot_id(), Some(S3));
        assert!(expired_table.metadata().snapshot_by_id(S1).is_some());
        assert!(expired_table.metadata().snapshot_by_id(S2).is_none());
    }

    /// Risk: the still-referenced guard missing `main` itself. Java's `retainedIdToRefs` map is
    /// built from ALL retained refs including `main`, so explicitly expiring main's CURRENT head
    /// fails with the same message naming `main`.
    #[tokio::test]
    async fn test_expire_snapshot_id_of_main_head_fails() {
        let table = chain_table();
        let action = expire(&table).expire_older_than(0).expire_snapshot_id(S3);
        let error = Arc::new(action)
            .commit(&table)
            .await
            .map(drop)
            .expect_err("expiring main's current head must fail");
        assert_eq!(error.kind(), ErrorKind::DataInvalid);
        assert_eq!(
            error.message(),
            format!("Cannot expire {S3}. Still referenced by refs: [main]")
        );
    }

    /// Risk: a typo'd explicit id removing something else or erroring. An unknown id is silently
    /// ignored (Java's builder only records actually-present snapshots) — full no-op.
    #[tokio::test]
    async fn test_expire_snapshot_id_unknown_id_is_noop() {
        let table = chain_table();
        let action = expire(&table)
            .expire_older_than(0)
            .expire_snapshot_id(424242);
        let (updates, requirements) = run(action, &table).await;
        assert!(updates.is_empty());
        assert!(requirements.is_empty());
    }

    /// Risk: the still-referenced guard consulting EXPIRED refs. A tag that ref-age-expires in the
    /// same pass no longer protects its target: the explicit expiry of that target is legal.
    #[tokio::test]
    async fn test_expire_snapshot_id_of_simultaneously_expired_refs_target_is_allowed() {
        const X: i64 = 301;
        let table = add_dangling_snapshot(chain_table(), X, CURRENT, T0);
        let table = with_ref(table, "archive", X, SnapshotRetention::Tag {
            max_ref_age_ms: Some(1000),
        });
        let action = expire(&table)
            .expire_older_than(T0 + 500)
            .expire_snapshot_id(X);
        let (updates, _) = run(action, &table).await;
        assert_eq!(removed_ref_names(&updates), vec!["archive".to_string()]);
        assert!(removed_snapshot_ids(&updates).contains(&X));
    }

    // =======================================================================
    // unreferenced-snapshot retention
    // =======================================================================

    /// Risk: the unreferenced-retention boundary flipping (recent dangling snapshots expired /
    /// old ones kept forever). Pinned ON the cutoff: a dangling snapshot AT the cutoff is kept, a
    /// dangling snapshot 1ms older goes.
    #[tokio::test]
    async fn test_unreferenced_snapshot_boundary_on_cutoff() {
        const X_AT_CUTOFF: i64 = 311;
        const X_BELOW_CUTOFF: i64 = 312;
        let cutoff = T0 + 5000;
        let table = add_dangling_snapshot(chain_table(), X_AT_CUTOFF, CURRENT, cutoff);
        let table = add_dangling_snapshot(table, X_BELOW_CUTOFF, CURRENT, cutoff - 1);
        let action = expire(&table).expire_older_than(cutoff);
        let (updates, _) = run(action, &table).await;
        let removed = removed_snapshot_ids(&updates);
        assert!(
            !removed.contains(&X_AT_CUTOFF),
            "dangling snapshot AT the cutoff must be kept: {removed:?}"
        );
        assert!(
            removed.contains(&X_BELOW_CUTOFF),
            "dangling snapshot below the cutoff must be expired: {removed:?}"
        );
    }

    // =======================================================================
    // no-op / idempotence / emitted shape
    // =======================================================================

    /// Risk: a no-op expiry emitting updates/requirements anyway (a spurious commit on every
    /// maintenance run). Nothing expires at cutoff 0 ⇒ nothing is emitted.
    #[tokio::test]
    async fn test_noop_expiry_emits_no_updates_and_no_requirements() {
        let table = chain_table();
        let action = expire(&table).expire_older_than(0);
        let (updates, requirements) = run(action, &table).await;
        assert!(updates.is_empty());
        assert!(requirements.is_empty());
    }

    /// Risk: re-running the same expiry after applying it finding NEW victims (the computation
    /// must be a fixpoint, or scheduled maintenance erodes history run by run).
    #[tokio::test]
    async fn test_rerun_after_apply_is_idempotent() {
        let table = chain_table();
        let first = expire(&table).expire_older_than(S2_TS);
        let (updates, _) = run(first, &table).await;
        assert!(!updates.is_empty());
        let expired_table = apply_updates(&table, updates);

        let second = expire(&expired_table).expire_older_than(S2_TS);
        let (updates, requirements) = run(second, &expired_table).await;
        assert!(updates.is_empty(), "re-run must be a no-op: {updates:?}");
        assert!(requirements.is_empty());
    }

    /// Risk: time-travel history gaps — after applying the expiry, snapshot-log entries that
    /// reference removed snapshots (and everything before them) must be pruned, leaving exactly
    /// the still-valid tail (Java `updateSnapshotLog`'s clear-on-invalid-entry rule).
    #[tokio::test]
    async fn test_snapshot_log_pruned_consistently_after_apply() {
        let table = chain_table();
        let base_log: Vec<i64> = table
            .metadata()
            .snapshot_log
            .iter()
            .map(|entry| entry.snapshot_id)
            .collect();
        // Pre-flight (fixture reaches the path under test): the log starts with the snapshots
        // the expiry below removes.
        assert_eq!(base_log, vec![ROOT, CURRENT, S1, S2, S3]);

        let action = expire(&table).expire_older_than(S2_TS);
        let (updates, _) = run(action, &table).await;
        assert_eq!(removed_snapshot_ids(&updates), vec![S1, ROOT, CURRENT]);
        let expired_table = apply_updates(&table, updates);

        let log_after: Vec<i64> = expired_table
            .metadata()
            .snapshot_log
            .iter()
            .map(|entry| entry.snapshot_id)
            .collect();
        assert_eq!(
            log_after,
            vec![S2, S3],
            "entries at/before the removed snapshots must be cleared, valid tail kept"
        );
        assert_eq!(expired_table.metadata().current_snapshot_id(), Some(S3));
    }

    /// Risk pinned: update emission ORDER. Java's `internalApply` removes the expired refs first
    /// and calls `removeSnapshots` last, so its builder change log (which REST serializes) is
    /// `RemoveSnapshotRef`* then `RemoveSnapshots` — the emitted updates must mirror that order
    /// (refs sorted by name; the snapshot removal exactly once, at the end). Nothing else pins
    /// this: a reorder survives every behavioral test because the apply-side dangling-ref sweep
    /// self-heals, but the REST payload order would silently diverge from Java.
    #[tokio::test]
    async fn test_updates_emit_refs_first_then_remove_snapshots_last() {
        const X: i64 = 331;
        let table = add_dangling_snapshot(chain_table(), X, CURRENT, T0);
        let table = with_ref(table, "b-tag", X, SnapshotRetention::Tag {
            max_ref_age_ms: Some(1000),
        });
        let table = with_ref(table, "a-tag", X, SnapshotRetention::Tag {
            max_ref_age_ms: Some(1000),
        });
        let action = expire(&table).expire_older_than(T0 + 500);
        let (updates, _) = run(action, &table).await;
        assert_eq!(updates.len(), 3);
        assert_eq!(updates[0], TableUpdate::RemoveSnapshotRef {
            ref_name: "a-tag".to_string(),
        });
        assert_eq!(updates[1], TableUpdate::RemoveSnapshotRef {
            ref_name: "b-tag".to_string(),
        });
        assert!(
            matches!(updates[2], TableUpdate::RemoveSnapshots { .. }),
            "the snapshot removal must come last: {updates:?}"
        );
    }

    /// Risk: losing the optimistic-concurrency guards — every ref consulted by the computation
    /// must be pinned at its observed snapshot id (sorted by name for determinism).
    #[tokio::test]
    async fn test_requirements_guard_every_ref_at_observed_snapshot() {
        let table = with_ref(
            chain_table(),
            "dev",
            S3,
            SnapshotRetention::branch(None, None, None),
        );
        let table = with_ref(table, "pin", S2, SnapshotRetention::Tag {
            max_ref_age_ms: None,
        });
        let action = expire(&table).expire_older_than(NOW);
        let (updates, requirements) = run(action, &table).await;
        assert!(!updates.is_empty());
        assert_eq!(requirements, vec![
            TableRequirement::RefSnapshotIdMatch {
                r#ref: "dev".to_string(),
                snapshot_id: Some(S3),
            },
            TableRequirement::RefSnapshotIdMatch {
                r#ref: MAIN_BRANCH.to_string(),
                snapshot_id: Some(S3),
            },
            TableRequirement::RefSnapshotIdMatch {
                r#ref: "pin".to_string(),
                snapshot_id: Some(S2),
            },
        ]);
    }

    /// Risk: THE over-expiry hazard window — a concurrent rollback moves main to an older
    /// snapshot after the expiry was computed; applying the stale removal would destroy the new
    /// head. The emitted ref guard must reject the stale commit (retryable conflict), forcing a
    /// recompute against the rolled-back base.
    #[tokio::test]
    async fn test_concurrent_rollback_is_rejected_by_ref_guard() {
        let table = chain_table();
        let action = expire(&table).expire_older_than(NOW);
        let (updates, requirements) = run(action, &table).await;
        assert!(removed_snapshot_ids(&updates).contains(&S1));

        // Concurrent rollback: main moves back to S1 (one of the would-be-expired snapshots).
        let rolled_back = {
            let metadata = table
                .metadata()
                .clone()
                .into_builder(None)
                .set_ref(
                    MAIN_BRANCH,
                    SnapshotReference::new(S1, SnapshotRetention::branch(None, None, None)),
                )
                .expect("roll back main")
                .build()
                .expect("build rolled-back metadata")
                .metadata;
            table.clone().with_metadata(Arc::new(metadata))
        };

        let error = requirements
            .iter()
            .find_map(|requirement| requirement.check(Some(rolled_back.metadata())).err())
            .expect("the stale main guard must reject the rolled-back base");
        assert_eq!(error.kind(), ErrorKind::CatalogCommitConflicts);
        assert!(error.retryable());
    }

    // =======================================================================
    // gates
    // =======================================================================

    /// Risk: expiring on a GC-disabled table (its files may be shared; B2's cleanup would corrupt
    /// other tables). Java's constructor check, verbatim message.
    #[tokio::test]
    async fn test_gc_disabled_fails_with_java_message() {
        let table = with_properties(chain_table(), &[(
            TableProperties::PROPERTY_GC_ENABLED,
            "false",
        )]);
        let action = expire(&table).expire_older_than(NOW);
        let error = Arc::new(action)
            .commit(&table)
            .await
            .map(drop)
            .expect_err("gc.enabled=false must fail");
        assert_eq!(error.kind(), ErrorKind::DataInvalid);
        assert_eq!(
            error.message(),
            "Cannot expire snapshots: GC is disabled (deleting files may corrupt other tables)"
        );
    }

    /// Risk: a malformed retention property silently falling back to the default and expiring
    /// with the wrong cutoff. It must fail loudly instead.
    #[tokio::test]
    async fn test_malformed_retention_property_fails_loudly() {
        let table = with_properties(chain_table(), &[(
            TableProperties::PROPERTY_MAX_SNAPSHOT_AGE_MS,
            "five days",
        )]);
        let action = expire(&table);
        let error = Arc::new(action)
            .commit(&table)
            .await
            .map(drop)
            .expect_err("malformed property must fail");
        assert_eq!(error.kind(), ErrorKind::DataInvalid);
        assert!(
            error
                .message()
                .contains("Invalid value for history.expire.max-snapshot-age-ms"),
            "unexpected message: {}",
            error.message()
        );
    }

    /// Risk: an empty table erroring or emitting updates (Java's `internalApply` early-returns).
    #[tokio::test]
    async fn test_table_without_snapshots_is_noop() {
        let table = crate::transaction::tests::make_v2_minimal_table();
        assert_eq!(table.metadata().snapshots().len(), 0);
        let action = expire(&table).retain_last(1);
        let (updates, requirements) = run(action, &table).await;
        assert!(updates.is_empty());
        assert!(requirements.is_empty());
    }

    // =======================================================================
    // end-to-end through a real catalog commit
    // =======================================================================

    /// Risk: the action working in isolation but not through the real commit path (requirement
    /// checks, metadata rebuild, snapshot-log pruning at the catalog layer). Three appends, then
    /// expire to the head: exactly one snapshot and one log entry survive.
    #[tokio::test]
    async fn test_expire_through_catalog_commit_end_to_end() {
        use crate::memory::tests::new_memory_catalog;
        use crate::spec::{DataContentType, DataFileBuilder, DataFileFormat, Literal, Struct};
        use crate::transaction::tests::make_v2_minimal_table_in_catalog;

        let catalog = new_memory_catalog().await;
        let mut table = make_v2_minimal_table_in_catalog(&catalog).await;
        for ordinal in 0..3 {
            let data_file = DataFileBuilder::default()
                .content(DataContentType::Data)
                .file_path(format!("test/expire-{ordinal}.parquet"))
                .file_format(DataFileFormat::Parquet)
                .file_size_in_bytes(100)
                .record_count(1)
                .partition(Struct::from_iter([Some(Literal::long(0))]))
                .partition_spec_id(0)
                .build()
                .expect("build data file");
            let tx = Transaction::new(&table);
            let tx = tx
                .fast_append()
                .add_data_files(vec![data_file])
                .apply(tx)
                .expect("apply fast append");
            table = tx.commit(&catalog).await.expect("commit fast append");
        }
        assert_eq!(table.metadata().snapshots().len(), 3);
        let head = table
            .metadata()
            .current_snapshot_id()
            .expect("current snapshot");

        // Age-expire everything (cutoff in the far future); the default floor keeps the head.
        let tx = Transaction::new(&table);
        let tx = tx
            .expire_snapshots()
            .expire_older_than(i64::MAX)
            .retain_last(1)
            .apply(tx)
            .expect("apply expire");
        let expired_table = tx.commit(&catalog).await.expect("commit expire");

        let metadata = expired_table.metadata();
        assert_eq!(metadata.snapshots().len(), 1);
        assert_eq!(metadata.current_snapshot_id(), Some(head));
        let log: Vec<i64> = metadata
            .snapshot_log
            .iter()
            .map(|entry| entry.snapshot_id)
            .collect();
        assert_eq!(log, vec![head]);

        // Idempotence through the real path: an identical re-run commits as a no-op.
        let tx = Transaction::new(&expired_table);
        let tx = tx
            .expire_snapshots()
            .expire_older_than(i64::MAX)
            .retain_last(1)
            .apply(tx)
            .expect("apply expire rerun");
        let rerun_table = tx.commit(&catalog).await.expect("commit expire rerun");
        assert_eq!(rerun_table.metadata().snapshots().len(), 1);
    }
}
