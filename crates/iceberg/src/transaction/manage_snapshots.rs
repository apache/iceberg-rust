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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;

use crate::error::Result;
use crate::spec::{MAIN_BRANCH, SnapshotReference, SnapshotRetention, TableMetadata};
use crate::table::Table;
use crate::transaction::{ActionCommit, TransactionAction};
use crate::{Error, ErrorKind, TableRequirement, TableUpdate};

/// Which retention field a `set_*` retention op targets.
#[derive(Debug, Clone)]
enum RetentionField {
    MinSnapshotsToKeep(i32),
    MaxSnapshotAgeMs(i64),
    MaxRefAgeMs(i64),
}

/// A pending snapshot-reference operation. Operations are recorded by the builder methods and
/// resolved against the table metadata at commit time (mirroring `transaction/sort_order.rs`,
/// which defers validation until a `Table` is available).
#[derive(Debug, Clone)]
enum SnapshotOp {
    /// Create a new branch/tag (the ref must not already exist).
    Create {
        name: String,
        snapshot_id: i64,
        branch: bool,
    },
    /// Point an existing branch/tag at a snapshot (the ref must exist and match the kind).
    Replace {
        name: String,
        snapshot_id: i64,
        branch: bool,
    },
    /// Remove an existing branch/tag (the ref must exist and match the kind).
    Remove { name: String, branch: bool },
    /// Rename an existing branch.
    RenameBranch { from: String, to: String },
    /// Set the `main` branch to a snapshot (no ancestry check — this is `setCurrentSnapshot`).
    SetCurrent { snapshot_id: i64 },
    /// Roll the `main` branch back to an ancestor of its current snapshot.
    RollbackTo { snapshot_id: i64 },
    /// Roll the `main` branch back to the newest ancestor of its current snapshot whose timestamp is
    /// strictly older than `timestamp_ms`.
    RollbackToTime { timestamp_ms: i64 },
    /// Fast-forward branch `from` to the snapshot of branch `to` (requires `from` ⊑ `to`).
    FastForward { from: String, to: String },
    /// Update a retention field on an existing ref.
    SetRetention { name: String, field: RetentionField },
}

/// Transaction action for managing snapshot references: branch/tag lifecycle, rollback,
/// rollback-to-time, fast-forward, and ref retention — the engine-agnostic subset of Java's
/// `ManageSnapshots`.
///
/// `cherrypick` is intentionally NOT a method here: Java's `cherrypick` extends `MergingSnapshotProducer`
/// and replays data files, which this metadata-only ref-management surface cannot produce. It lives in its
/// own [`crate::transaction::cherry_pick::CherryPickAction`] (reachable via
/// [`crate::transaction::Transaction::cherry_pick`]) — the honest shape, since the two do not compose
/// cleanly (this action only EMITS ref updates; cherry-pick needs the full snapshot producer).
pub struct ManageSnapshotsAction {
    ops: Vec<SnapshotOp>,
}

impl ManageSnapshotsAction {
    pub fn new() -> Self {
        ManageSnapshotsAction { ops: vec![] }
    }

    /// Create a new branch pointing at `snapshot_id`. Fails at commit if the ref already exists.
    pub fn create_branch(mut self, name: &str, snapshot_id: i64) -> Self {
        self.ops.push(SnapshotOp::Create {
            name: name.to_string(),
            snapshot_id,
            branch: true,
        });
        self
    }

    /// Create a new tag pointing at `snapshot_id`. Fails at commit if the ref already exists.
    pub fn create_tag(mut self, name: &str, snapshot_id: i64) -> Self {
        self.ops.push(SnapshotOp::Create {
            name: name.to_string(),
            snapshot_id,
            branch: false,
        });
        self
    }

    /// Point an existing branch at `snapshot_id`. Fails at commit if the branch does not exist.
    pub fn replace_branch(mut self, name: &str, snapshot_id: i64) -> Self {
        self.ops.push(SnapshotOp::Replace {
            name: name.to_string(),
            snapshot_id,
            branch: true,
        });
        self
    }

    /// Point an existing tag at `snapshot_id`. Fails at commit if the tag does not exist.
    pub fn replace_tag(mut self, name: &str, snapshot_id: i64) -> Self {
        self.ops.push(SnapshotOp::Replace {
            name: name.to_string(),
            snapshot_id,
            branch: false,
        });
        self
    }

    /// Remove an existing branch.
    pub fn remove_branch(mut self, name: &str) -> Self {
        self.ops.push(SnapshotOp::Remove {
            name: name.to_string(),
            branch: true,
        });
        self
    }

    /// Remove an existing tag.
    pub fn remove_tag(mut self, name: &str) -> Self {
        self.ops.push(SnapshotOp::Remove {
            name: name.to_string(),
            branch: false,
        });
        self
    }

    /// Rename an existing branch.
    pub fn rename_branch(mut self, from: &str, to: &str) -> Self {
        self.ops.push(SnapshotOp::RenameBranch {
            from: from.to_string(),
            to: to.to_string(),
        });
        self
    }

    /// Set the `main` branch to `snapshot_id` (Java `setCurrentSnapshot`).
    pub fn set_current_snapshot(mut self, snapshot_id: i64) -> Self {
        self.ops.push(SnapshotOp::SetCurrent { snapshot_id });
        self
    }

    /// Roll the `main` branch back to `snapshot_id`, which must be an ancestor of the current
    /// `main` snapshot.
    pub fn rollback_to(mut self, snapshot_id: i64) -> Self {
        self.ops.push(SnapshotOp::RollbackTo { snapshot_id });
        self
    }

    /// Roll the `main` branch back to the latest snapshot in its current ancestry whose timestamp is
    /// strictly older than `timestamp_ms` (Java `SnapshotManager.rollbackToTime` ⇒
    /// `SetSnapshotOperation.rollbackToTime`). Fails at commit if no ancestor qualifies.
    pub fn rollback_to_time(mut self, timestamp_ms: i64) -> Self {
        self.ops.push(SnapshotOp::RollbackToTime { timestamp_ms });
        self
    }

    /// Fast-forward branch `from` to the snapshot referenced by `to`. `to` may be any ref (branch
    /// or tag). If `from` already exists it must be a branch and its snapshot must be an ancestor of
    /// `to`'s snapshot; if `from` does not exist it is created as a branch at `to`'s snapshot
    /// (matching Java `ManageSnapshots.fastForwardBranch`).
    pub fn fast_forward(mut self, from: &str, to: &str) -> Self {
        self.ops.push(SnapshotOp::FastForward {
            from: from.to_string(),
            to: to.to_string(),
        });
        self
    }

    /// Set the minimum number of snapshots to keep for a branch.
    pub fn set_min_snapshots_to_keep(mut self, name: &str, value: i32) -> Self {
        self.ops.push(SnapshotOp::SetRetention {
            name: name.to_string(),
            field: RetentionField::MinSnapshotsToKeep(value),
        });
        self
    }

    /// Set the maximum snapshot age (ms) to keep for a branch.
    pub fn set_max_snapshot_age_ms(mut self, name: &str, value: i64) -> Self {
        self.ops.push(SnapshotOp::SetRetention {
            name: name.to_string(),
            field: RetentionField::MaxSnapshotAgeMs(value),
        });
        self
    }

    /// Set the maximum reference age (ms) to keep for a branch or tag.
    pub fn set_max_ref_age_ms(mut self, name: &str, value: i64) -> Self {
        self.ops.push(SnapshotOp::SetRetention {
            name: name.to_string(),
            field: RetentionField::MaxRefAgeMs(value),
        });
        self
    }
}

impl Default for ManageSnapshotsAction {
    fn default() -> Self {
        Self::new()
    }
}

/// Returns true if `ancestor_id` is `descendant_id` or any snapshot reachable from it by following
/// `parent_snapshot_id`.
fn is_ancestor_of(metadata: &TableMetadata, ancestor_id: i64, descendant_id: i64) -> bool {
    let mut current = Some(descendant_id);
    while let Some(id) = current {
        if id == ancestor_id {
            return true;
        }
        current = metadata
            .snapshot_by_id(id)
            .and_then(|s| s.parent_snapshot_id());
    }
    false
}

/// Return the snapshot id of the newest ancestor of `from_snapshot_id` (inclusive) whose
/// `timestamp_ms` is strictly older than `timestamp_ms`, or `None` if no ancestor qualifies.
///
/// Mirrors Java `SetSnapshotOperation.findLatestAncestorOlderThan`: it walks the parent chain of the
/// current snapshot (Java's `SnapshotUtil.ancestorIds(currentSnapshot)`) and keeps the qualifying
/// snapshot with the maximum timestamp. The comparison is strict (`<`), matching Java — so a timestamp
/// equal to a snapshot's own timestamp does not select that snapshot.
fn find_latest_ancestor_older_than(
    metadata: &TableMetadata,
    from_snapshot_id: i64,
    timestamp_ms: i64,
) -> Option<i64> {
    let mut best: Option<(i64, i64)> = None; // (snapshot_id, timestamp_ms)
    let mut current = Some(from_snapshot_id);
    while let Some(id) = current {
        let snapshot = metadata.snapshot_by_id(id);
        if let Some(snapshot) = snapshot {
            let snapshot_timestamp = snapshot.timestamp_ms();
            if snapshot_timestamp < timestamp_ms
                && best.is_none_or(|(_, best_timestamp)| snapshot_timestamp > best_timestamp)
            {
                best = Some((id, snapshot_timestamp));
            }
        }
        current = snapshot.and_then(|s| s.parent_snapshot_id());
    }
    best.map(|(snapshot_id, _)| snapshot_id)
}

fn data_invalid(msg: String) -> Error {
    Error::new(ErrorKind::DataInvalid, msg)
}

#[async_trait]
impl TransactionAction for ManageSnapshotsAction {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        let metadata = table.metadata();

        // Validate a snapshot id exists, returning a descriptive error otherwise.
        let require_snapshot = |snapshot_id: i64| -> Result<()> {
            if metadata.snapshot_by_id(snapshot_id).is_none() {
                return Err(data_invalid(format!(
                    "Cannot use snapshot {snapshot_id}: not found in table metadata"
                )));
            }
            Ok(())
        };

        // Working copy of refs, mutated as ops are applied so later ops observe earlier ones.
        let mut refs: HashMap<String, SnapshotReference> = metadata.refs.clone();
        // Names touched by this action, in first-touch order (drives deterministic update output).
        let mut touched_order: Vec<String> = vec![];
        let mut touched_set: HashSet<String> = HashSet::new();
        let mark_touched = |name: &str, order: &mut Vec<String>, set: &mut HashSet<String>| {
            if set.insert(name.to_string()) {
                order.push(name.to_string());
            }
        };

        for op in &self.ops {
            match op {
                SnapshotOp::Create {
                    name,
                    snapshot_id,
                    branch,
                } => {
                    require_snapshot(*snapshot_id)?;
                    if refs.contains_key(name) {
                        return Err(data_invalid(format!("Ref {name} already exists")));
                    }
                    let retention = if *branch {
                        SnapshotRetention::branch(None, None, None)
                    } else {
                        SnapshotRetention::Tag {
                            max_ref_age_ms: None,
                        }
                    };
                    refs.insert(
                        name.clone(),
                        SnapshotReference::new(*snapshot_id, retention),
                    );
                    mark_touched(name, &mut touched_order, &mut touched_set);
                }
                SnapshotOp::Replace {
                    name,
                    snapshot_id,
                    branch,
                } => {
                    require_snapshot(*snapshot_id)?;
                    let existing = refs
                        .get(name)
                        .ok_or_else(|| data_invalid(format!("Ref {name} does not exist")))?;
                    check_kind(name, existing, *branch)?;
                    let retention = existing.retention.clone();
                    refs.insert(
                        name.clone(),
                        SnapshotReference::new(*snapshot_id, retention),
                    );
                    mark_touched(name, &mut touched_order, &mut touched_set);
                }
                SnapshotOp::Remove { name, branch } => {
                    if name == MAIN_BRANCH {
                        return Err(data_invalid("Cannot remove the main branch".to_string()));
                    }
                    let existing = refs
                        .get(name)
                        .ok_or_else(|| data_invalid(format!("Ref {name} does not exist")))?;
                    check_kind(name, existing, *branch)?;
                    refs.remove(name);
                    mark_touched(name, &mut touched_order, &mut touched_set);
                }
                SnapshotOp::RenameBranch { from, to } => {
                    if from == MAIN_BRANCH || to == MAIN_BRANCH {
                        return Err(data_invalid("Cannot rename the main branch".to_string()));
                    }
                    let existing = refs
                        .get(from)
                        .ok_or_else(|| data_invalid(format!("Branch {from} does not exist")))?;
                    check_kind(from, existing, true)?;
                    if refs.contains_key(to) {
                        return Err(data_invalid(format!("Ref {to} already exists")));
                    }
                    let reference = existing.clone();
                    refs.remove(from);
                    refs.insert(to.clone(), reference);
                    mark_touched(from, &mut touched_order, &mut touched_set);
                    mark_touched(to, &mut touched_order, &mut touched_set);
                }
                SnapshotOp::SetCurrent { snapshot_id } => {
                    require_snapshot(*snapshot_id)?;
                    set_main(&mut refs, *snapshot_id);
                    mark_touched(MAIN_BRANCH, &mut touched_order, &mut touched_set);
                }
                SnapshotOp::RollbackTo { snapshot_id } => {
                    require_snapshot(*snapshot_id)?;
                    let current =
                        refs.get(MAIN_BRANCH)
                            .map(|r| r.snapshot_id)
                            .ok_or_else(|| {
                                data_invalid(
                                    "Cannot roll back: table has no current snapshot".to_string(),
                                )
                            })?;
                    if !is_ancestor_of(metadata, *snapshot_id, current) {
                        return Err(data_invalid(format!(
                            "Cannot roll back to snapshot {snapshot_id}: not an ancestor of the \
                             current snapshot {current}"
                        )));
                    }
                    set_main(&mut refs, *snapshot_id);
                    mark_touched(MAIN_BRANCH, &mut touched_order, &mut touched_set);
                }
                SnapshotOp::RollbackToTime { timestamp_ms } => {
                    let current =
                        refs.get(MAIN_BRANCH)
                            .map(|r| r.snapshot_id)
                            .ok_or_else(|| {
                                data_invalid(
                                    "Cannot roll back: table has no current snapshot".to_string(),
                                )
                            })?;
                    let target = find_latest_ancestor_older_than(metadata, current, *timestamp_ms)
                        .ok_or_else(|| {
                            data_invalid(format!(
                                "Cannot roll back, no valid snapshot older than: {timestamp_ms}"
                            ))
                        })?;
                    set_main(&mut refs, target);
                    mark_touched(MAIN_BRANCH, &mut touched_order, &mut touched_set);
                }
                SnapshotOp::FastForward { from, to } => {
                    // Matches Java `UpdateSnapshotReferencesOperation.replaceBranch(from, to, true)`:
                    // `to` may be ANY ref (branch or tag) — only its snapshot id matters; and an
                    // absent `from` is auto-created as a branch at `to`'s snapshot (no ancestry check).
                    let to_snapshot = refs
                        .get(to)
                        .ok_or_else(|| data_invalid(format!("Ref {to} does not exist")))?
                        .snapshot_id;
                    // Copy `from`'s state out so the borrow ends before we mutate `refs`.
                    let from_state = refs
                        .get(from)
                        .map(|r| (r.is_branch(), r.snapshot_id, r.retention.clone()));
                    match from_state {
                        None => {
                            refs.insert(
                                from.clone(),
                                SnapshotReference::new(
                                    to_snapshot,
                                    SnapshotRetention::branch(None, None, None),
                                ),
                            );
                        }
                        Some((is_branch, from_snapshot, retention)) => {
                            if !is_branch {
                                return Err(data_invalid(format!(
                                    "Ref {from} is a tag, but a branch was expected"
                                )));
                            }
                            // No-op when already at target (Java returns early; the emit-time
                            // suppression below would also drop it, but skip the ancestry check too).
                            if from_snapshot != to_snapshot {
                                if !is_ancestor_of(metadata, from_snapshot, to_snapshot) {
                                    return Err(data_invalid(format!(
                                        "Cannot fast-forward {from} to {to}: {from}'s snapshot \
                                         {from_snapshot} is not an ancestor of {to}'s snapshot \
                                         {to_snapshot}"
                                    )));
                                }
                                refs.insert(
                                    from.clone(),
                                    SnapshotReference::new(to_snapshot, retention),
                                );
                            }
                        }
                    }
                    mark_touched(from, &mut touched_order, &mut touched_set);
                }
                SnapshotOp::SetRetention { name, field } => {
                    let existing = refs
                        .get(name)
                        .ok_or_else(|| data_invalid(format!("Ref {name} does not exist")))?;
                    let retention = apply_retention(name, &existing.retention, field)?;
                    let snapshot_id = existing.snapshot_id;
                    refs.insert(name.clone(), SnapshotReference::new(snapshot_id, retention));
                    mark_touched(name, &mut touched_order, &mut touched_set);
                }
            }
        }

        let mut updates: Vec<TableUpdate> = Vec::with_capacity(touched_order.len());
        let mut requirements: Vec<TableRequirement> = Vec::with_capacity(touched_order.len());
        for name in &touched_order {
            let original = metadata.refs.get(name);
            let resolved = refs.get(name);
            // Net no-op: a ref that ends in the same state it started (unchanged, or created and
            // then removed within this action). Emit nothing — matching Java, which only records
            // refs that actually changed, and keeping the commit idempotent at the catalog layer.
            if original == resolved {
                continue;
            }
            // Optimistic-concurrency guard: the ref must still be at the snapshot we observed
            // (or still absent, when snapshot_id is None), or the commit is rejected and retried.
            requirements.push(TableRequirement::RefSnapshotIdMatch {
                r#ref: name.clone(),
                snapshot_id: original.map(|r| r.snapshot_id),
            });
            match resolved {
                Some(reference) => updates.push(TableUpdate::SetSnapshotRef {
                    ref_name: name.clone(),
                    reference: reference.clone(),
                }),
                None => updates.push(TableUpdate::RemoveSnapshotRef {
                    ref_name: name.clone(),
                }),
            }
        }

        Ok(ActionCommit::new(updates, requirements))
    }
}

/// Set the `main` branch to a snapshot, preserving its retention if it already exists.
fn set_main(refs: &mut HashMap<String, SnapshotReference>, snapshot_id: i64) {
    let retention = refs
        .get(MAIN_BRANCH)
        .map(|r| r.retention.clone())
        .unwrap_or_else(|| SnapshotRetention::branch(None, None, None));
    refs.insert(
        MAIN_BRANCH.to_string(),
        SnapshotReference::new(snapshot_id, retention),
    );
}

/// Validate that an existing ref matches the expected kind (branch vs tag).
fn check_kind(name: &str, reference: &SnapshotReference, expect_branch: bool) -> Result<()> {
    if reference.is_branch() != expect_branch {
        let (have, want) = if expect_branch {
            ("a tag", "a branch")
        } else {
            ("a branch", "a tag")
        };
        return Err(data_invalid(format!(
            "Ref {name} is {have}, but {want} was expected"
        )));
    }
    Ok(())
}

/// Reject a non-positive retention value, matching the `value > 0` `Preconditions.checkArgument`s in
/// Java's `SnapshotRef.Builder` (`minSnapshotsToKeep` / `maxSnapshotAgeMs` / `maxRefAgeMs`). The Java
/// messages are reproduced verbatim so error-shape parity holds. (Java also permits `null` to clear a
/// field, but this builder API always sets a concrete value, so only the `<= 0` case can occur here.)
fn validate_retention_positive(field: &RetentionField) -> Result<()> {
    match field {
        RetentionField::MinSnapshotsToKeep(value) if *value <= 0 => Err(data_invalid(
            "Min snapshots to keep must be greater than 0".to_string(),
        )),
        RetentionField::MaxSnapshotAgeMs(value) if *value <= 0 => Err(data_invalid(
            "Max snapshot age must be greater than 0 ms".to_string(),
        )),
        RetentionField::MaxRefAgeMs(value) if *value <= 0 => Err(data_invalid(
            "Max reference age must be greater than 0".to_string(),
        )),
        _ => Ok(()),
    }
}

/// Produce a new retention policy with one field updated. Branch fields are rejected on tags, and a
/// non-positive value is rejected (Java `SnapshotRef.Builder`).
fn apply_retention(
    name: &str,
    current: &SnapshotRetention,
    field: &RetentionField,
) -> Result<SnapshotRetention> {
    validate_retention_positive(field)?;
    match current {
        SnapshotRetention::Branch {
            min_snapshots_to_keep,
            max_snapshot_age_ms,
            max_ref_age_ms,
        } => {
            let mut min = *min_snapshots_to_keep;
            let mut max_age = *max_snapshot_age_ms;
            let mut max_ref = *max_ref_age_ms;
            match field {
                RetentionField::MinSnapshotsToKeep(v) => min = Some(*v),
                RetentionField::MaxSnapshotAgeMs(v) => max_age = Some(*v),
                RetentionField::MaxRefAgeMs(v) => max_ref = Some(*v),
            }
            Ok(SnapshotRetention::branch(min, max_age, max_ref))
        }
        SnapshotRetention::Tag { .. } => match field {
            RetentionField::MaxRefAgeMs(v) => Ok(SnapshotRetention::Tag {
                max_ref_age_ms: Some(*v),
            }),
            _ => Err(data_invalid(format!(
                "Ref {name} is a tag; only max_ref_age_ms can be set on a tag"
            ))),
        },
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use crate::spec::{
        MAIN_BRANCH, Operation, Snapshot, SnapshotReference, SnapshotRetention, Summary,
    };
    use crate::table::Table;
    use crate::transaction::{Transaction, TransactionAction};
    use crate::{ErrorKind, TableRequirement, TableUpdate};

    // From TableMetadataV2Valid.json: main -> 3055..., whose parent is the root 3051....
    const CURRENT: i64 = 3055729675574597004;
    const ROOT: i64 = 3051729675574597004;
    // A snapshot we graft on as a SIBLING of CURRENT (also a child of ROOT) — it exists but is NOT
    // in main's ancestry, which is exactly what the rollback/fast-forward negative tests need.
    const SIBLING: i64 = 3060729675574597004;

    fn table() -> Table {
        crate::transaction::tests::make_v2_table()
    }

    /// `make_v2_table()` augmented with a forked history and pre-existing refs:
    /// snapshots {ROOT, CURRENT(child of ROOT), SIBLING(child of ROOT)}; refs
    /// {main->CURRENT, `dev` branch->CURRENT, `stable` tag->ROOT}. This lets us exercise paths the
    /// straight-line base fixture cannot: a valid-but-non-ancestor snapshot, and remove/replace of a
    /// ref that already existed at commit time (so the optimistic-concurrency guard carries the
    /// original snapshot id rather than `None`).
    fn forked_table() -> Table {
        let base = table();
        let sibling = Snapshot::builder()
            .with_snapshot_id(SIBLING)
            .with_parent_snapshot_id(Some(ROOT))
            .with_sequence_number(35) // > fixture last-sequence-number (34)
            .with_timestamp_ms(1700000000000) // after the fixture's last-updated-ms (1602638573590)
            .with_manifest_list("/tmp/sibling-manifest-list.avro")
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: HashMap::new(),
            })
            .with_schema_id(1)
            .build();
        let metadata = base
            .metadata()
            .clone()
            .into_builder(None)
            .add_snapshot(sibling)
            .expect("add sibling snapshot")
            .set_ref(
                "dev",
                SnapshotReference::new(CURRENT, SnapshotRetention::branch(None, None, None)),
            )
            .expect("add dev branch")
            .set_ref(
                "stable",
                SnapshotReference::new(ROOT, SnapshotRetention::Tag {
                    max_ref_age_ms: None,
                }),
            )
            .expect("add stable tag")
            .build()
            .expect("build forked metadata")
            .metadata;
        base.with_metadata(Arc::new(metadata))
    }

    /// The `snapshot_id` carried by the `RefSnapshotIdMatch` requirement for `name`, if present.
    /// Outer `Option`: was a requirement emitted? Inner `Option<i64>`: the guarded snapshot id
    /// (`None` ⇒ "ref must not already exist").
    fn requirement_for(reqs: &[TableRequirement], name: &str) -> Option<Option<i64>> {
        reqs.iter().find_map(|r| match r {
            TableRequirement::RefSnapshotIdMatch { r#ref, snapshot_id } if r#ref == name => {
                Some(*snapshot_id)
            }
            _ => None,
        })
    }

    fn find_set<'a>(updates: &'a [TableUpdate], name: &str) -> Option<&'a SnapshotReference> {
        updates.iter().find_map(|u| match u {
            TableUpdate::SetSnapshotRef {
                ref_name,
                reference,
            } if ref_name == name => Some(reference),
            _ => None,
        })
    }

    fn is_removed(updates: &[TableUpdate], name: &str) -> bool {
        updates
            .iter()
            .any(|u| matches!(u, TableUpdate::RemoveSnapshotRef { ref_name } if ref_name == name))
    }

    #[tokio::test]
    async fn test_create_branch_and_tag() {
        let table = table();
        let action = Transaction::new(&table)
            .manage_snapshots()
            .create_branch("b1", ROOT)
            .create_tag("t1", ROOT);
        let mut commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = commit.take_updates();
        let requirements = commit.take_requirements();

        let b1 = find_set(&updates, "b1").expect("b1 set");
        assert_eq!(b1.snapshot_id, ROOT);
        assert!(b1.is_branch());
        let t1 = find_set(&updates, "t1").expect("t1 set");
        assert_eq!(t1.snapshot_id, ROOT);
        assert!(!t1.is_branch());

        // New refs must be guarded as "must not exist" (snapshot_id == None).
        for name in ["b1", "t1"] {
            assert!(requirements.iter().any(|r| matches!(
                r,
                TableRequirement::RefSnapshotIdMatch { r#ref, snapshot_id: None } if r#ref == name
            )));
        }
    }

    #[tokio::test]
    async fn test_create_existing_ref_fails() {
        let table = table();
        let action = Transaction::new(&table)
            .manage_snapshots()
            .create_branch(MAIN_BRANCH, ROOT);
        assert!(Arc::new(action).commit(&table).await.is_err());
    }

    #[tokio::test]
    async fn test_create_unknown_snapshot_fails() {
        let table = table();
        let action = Transaction::new(&table)
            .manage_snapshots()
            .create_tag("t1", 42);
        assert!(Arc::new(action).commit(&table).await.is_err());
    }

    #[tokio::test]
    async fn test_set_current_snapshot() {
        let table = table();
        let action = Transaction::new(&table)
            .manage_snapshots()
            .set_current_snapshot(ROOT);
        let mut commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = commit.take_updates();
        let requirements = commit.take_requirements();

        assert_eq!(find_set(&updates, MAIN_BRANCH).unwrap().snapshot_id, ROOT);
        // main currently points at CURRENT, so the guard must require that.
        assert!(requirements.iter().any(|r| matches!(
            r,
            TableRequirement::RefSnapshotIdMatch { r#ref, snapshot_id: Some(id) }
                if r#ref == MAIN_BRANCH && *id == CURRENT
        )));
    }

    #[tokio::test]
    async fn test_rollback_to_ancestor_ok() {
        let table = table();
        let action = Transaction::new(&table)
            .manage_snapshots()
            .rollback_to(ROOT);
        let mut commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = commit.take_updates();
        assert_eq!(find_set(&updates, MAIN_BRANCH).unwrap().snapshot_id, ROOT);
    }

    #[tokio::test]
    async fn test_rollback_to_unknown_snapshot_fails() {
        let table = table();
        let action = Transaction::new(&table).manage_snapshots().rollback_to(42);
        assert!(Arc::new(action).commit(&table).await.is_err());
    }

    #[tokio::test]
    async fn test_replace_then_inspect() {
        let table = table();
        // Create a branch at the root, then move it to the current snapshot in the same action.
        let action = Transaction::new(&table)
            .manage_snapshots()
            .create_branch("b1", ROOT)
            .replace_branch("b1", CURRENT);
        let mut commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = commit.take_updates();
        assert_eq!(find_set(&updates, "b1").unwrap().snapshot_id, CURRENT);
    }

    #[tokio::test]
    async fn test_rename_preexisting_branch() {
        // Rename a branch that already exists at commit time: the old name is removed (with its
        // original-snapshot guard) and the new name is set to the same snapshot.
        let table = forked_table();
        let action = Transaction::new(&table)
            .manage_snapshots()
            .rename_branch("dev", "dev_renamed");
        let mut commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = commit.take_updates();
        let requirements = commit.take_requirements();

        assert!(is_removed(&updates, "dev"));
        assert_eq!(requirement_for(&requirements, "dev"), Some(Some(CURRENT)));
        assert_eq!(
            find_set(&updates, "dev_renamed").unwrap().snapshot_id,
            CURRENT
        );
        assert_eq!(requirement_for(&requirements, "dev_renamed"), Some(None));
    }

    #[tokio::test]
    async fn test_kind_mismatch_fails() {
        let table = table();
        let action = Transaction::new(&table)
            .manage_snapshots()
            .create_tag("t1", ROOT)
            .remove_branch("t1"); // t1 is a tag, not a branch
        assert!(Arc::new(action).commit(&table).await.is_err());
    }

    #[tokio::test]
    async fn test_fast_forward_ok() {
        let table = table();
        // b1 at the root is an ancestor of main (CURRENT) -> fast-forward moves b1 to CURRENT.
        let action = Transaction::new(&table)
            .manage_snapshots()
            .create_branch("b1", ROOT)
            .fast_forward("b1", MAIN_BRANCH);
        let mut commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = commit.take_updates();
        assert_eq!(find_set(&updates, "b1").unwrap().snapshot_id, CURRENT);
    }

    #[tokio::test]
    async fn test_set_retention_on_branch_and_tag() {
        let table = table();
        let action = Transaction::new(&table)
            .manage_snapshots()
            .create_branch("b1", ROOT)
            .set_min_snapshots_to_keep("b1", 3)
            .set_max_snapshot_age_ms("b1", 1000)
            .create_tag("t1", ROOT)
            .set_max_ref_age_ms("t1", 500);
        let mut commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = commit.take_updates();

        match &find_set(&updates, "b1").unwrap().retention {
            SnapshotRetention::Branch {
                min_snapshots_to_keep,
                max_snapshot_age_ms,
                ..
            } => {
                assert_eq!(*min_snapshots_to_keep, Some(3));
                assert_eq!(*max_snapshot_age_ms, Some(1000));
            }
            _ => panic!("b1 should be a branch"),
        }
        match &find_set(&updates, "t1").unwrap().retention {
            SnapshotRetention::Tag { max_ref_age_ms } => assert_eq!(*max_ref_age_ms, Some(500)),
            _ => panic!("t1 should be a tag"),
        }
    }

    #[tokio::test]
    async fn test_set_branch_retention_on_tag_fails() {
        let table = table();
        let action = Transaction::new(&table)
            .manage_snapshots()
            .create_tag("t1", ROOT)
            .set_min_snapshots_to_keep("t1", 3); // invalid on a tag
        assert!(Arc::new(action).commit(&table).await.is_err());
    }

    #[tokio::test]
    async fn test_set_max_ref_age_on_branch() {
        let table = table();
        let action = Transaction::new(&table)
            .manage_snapshots()
            .create_branch("b1", ROOT)
            .set_max_ref_age_ms("b1", 777);
        let mut commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = commit.take_updates();
        match &find_set(&updates, "b1").unwrap().retention {
            SnapshotRetention::Branch { max_ref_age_ms, .. } => {
                assert_eq!(*max_ref_age_ms, Some(777))
            }
            _ => panic!("b1 should be a branch"),
        }
    }

    #[tokio::test]
    async fn test_remove_main_branch_fails() {
        let table = table();
        let action = Transaction::new(&table)
            .manage_snapshots()
            .remove_branch(MAIN_BRANCH);
        assert!(Arc::new(action).commit(&table).await.is_err());
    }

    #[tokio::test]
    async fn test_rename_main_branch_fails() {
        let table = table();
        let from_main = Transaction::new(&table)
            .manage_snapshots()
            .rename_branch(MAIN_BRANCH, "x");
        assert!(Arc::new(from_main).commit(&table).await.is_err());

        let to_main = Transaction::new(&table)
            .manage_snapshots()
            .create_branch("b1", ROOT)
            .rename_branch("b1", MAIN_BRANCH);
        assert!(Arc::new(to_main).commit(&table).await.is_err());
    }

    #[tokio::test]
    async fn test_rename_branch_to_existing_name_fails() {
        let table = table();
        let action = Transaction::new(&table)
            .manage_snapshots()
            .create_branch("b1", ROOT)
            .create_branch("b2", ROOT)
            .rename_branch("b1", "b2"); // b2 already exists
        assert!(Arc::new(action).commit(&table).await.is_err());
    }

    #[tokio::test]
    async fn test_create_then_remove_new_ref_is_suppressed() {
        let table = table();
        // A ref that is created and removed within the same action nets to no change: emit nothing.
        let action = Transaction::new(&table)
            .manage_snapshots()
            .create_branch("b1", ROOT)
            .remove_branch("b1");
        let mut commit = Arc::new(action).commit(&table).await.unwrap();
        assert!(
            commit.take_updates().is_empty(),
            "net no-op must emit no updates"
        );
        assert!(
            commit.take_requirements().is_empty(),
            "net no-op must emit no requirements"
        );
    }

    #[tokio::test]
    async fn test_fast_forward_to_a_tag_is_allowed() {
        // `to` may be a tag (Java only requires `from` to be a branch).
        let table = table();
        let action = Transaction::new(&table)
            .manage_snapshots()
            .create_branch("b1", ROOT) // b1 at the root, an ancestor of CURRENT
            .create_tag("release", CURRENT) // tag at CURRENT
            .fast_forward("b1", "release");
        let mut commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = commit.take_updates();
        assert_eq!(find_set(&updates, "b1").unwrap().snapshot_id, CURRENT);
    }

    #[tokio::test]
    async fn test_fast_forward_creates_absent_from() {
        // An absent `from` is auto-created as a branch at `to`'s snapshot.
        let table = table();
        let action = Transaction::new(&table)
            .manage_snapshots()
            .fast_forward("newb", MAIN_BRANCH);
        let mut commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = commit.take_updates();
        let requirements = commit.take_requirements();
        let newb = find_set(&updates, "newb").expect("newb created");
        assert_eq!(newb.snapshot_id, CURRENT);
        assert!(newb.is_branch());
        assert_eq!(requirement_for(&requirements, "newb"), Some(None)); // must-not-exist guard
    }

    #[tokio::test]
    async fn test_fast_forward_non_ancestor_fails() {
        // b1 at CURRENT is NOT an ancestor of b2 at ROOT, so fast-forwarding b1->b2 must fail.
        let table = table();
        let action = Transaction::new(&table)
            .manage_snapshots()
            .create_branch("b1", CURRENT)
            .create_branch("b2", ROOT)
            .fast_forward("b1", "b2");
        assert!(Arc::new(action).commit(&table).await.is_err());
    }

    #[tokio::test]
    async fn test_fast_forward_noop_is_suppressed() {
        // main is already at CURRENT; fast-forwarding it to a tag also at CURRENT is a no-op.
        let table = table();
        let action = Transaction::new(&table)
            .manage_snapshots()
            .create_tag("here", CURRENT)
            .fast_forward(MAIN_BRANCH, "here");
        let mut commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = commit.take_updates();
        let requirements = commit.take_requirements();
        // The tag is created, but main is unchanged and must not be emitted.
        assert!(find_set(&updates, "here").is_some());
        assert!(
            find_set(&updates, MAIN_BRANCH).is_none(),
            "no-op main must be suppressed"
        );
        assert_eq!(requirement_for(&requirements, MAIN_BRANCH), None);
    }

    #[tokio::test]
    async fn test_rollback_to_non_ancestor_fails() {
        // SIBLING exists but is not in main's ancestry (it is a sibling of CURRENT) -> reject.
        let table = forked_table();
        let action = Transaction::new(&table)
            .manage_snapshots()
            .rollback_to(SIBLING);
        assert!(Arc::new(action).commit(&table).await.is_err());
    }

    #[tokio::test]
    async fn test_remove_preexisting_tag_emits_guard() {
        let table = forked_table();
        let action = Transaction::new(&table)
            .manage_snapshots()
            .remove_tag("stable");
        let mut commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = commit.take_updates();
        let requirements = commit.take_requirements();
        assert!(is_removed(&updates, "stable"));
        // Guard must carry the ORIGINAL snapshot id (ROOT), not None.
        assert_eq!(requirement_for(&requirements, "stable"), Some(Some(ROOT)));
    }

    #[tokio::test]
    async fn test_replace_preexisting_tag() {
        let table = forked_table();
        let action = Transaction::new(&table)
            .manage_snapshots()
            .replace_tag("stable", CURRENT);
        let mut commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = commit.take_updates();
        let requirements = commit.take_requirements();
        let stable = find_set(&updates, "stable").expect("stable replaced");
        assert_eq!(stable.snapshot_id, CURRENT);
        assert!(!stable.is_branch(), "stable must stay a tag");
        assert_eq!(requirement_for(&requirements, "stable"), Some(Some(ROOT)));
    }

    #[tokio::test]
    async fn test_remove_preexisting_branch_emits_guard() {
        let table = forked_table();
        let action = Transaction::new(&table)
            .manage_snapshots()
            .remove_branch("dev");
        let mut commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = commit.take_updates();
        let requirements = commit.take_requirements();
        assert!(is_removed(&updates, "dev"));
        assert_eq!(requirement_for(&requirements, "dev"), Some(Some(CURRENT)));
    }

    // Base-fixture snapshot timestamps (from TableMetadataV2Valid.json), used by the rollback-to-time
    // tests. main points at CURRENT, whose ancestry is {CURRENT, ROOT}.
    const ROOT_TIMESTAMP_MS: i64 = 1515100955770; // ROOT
    const CURRENT_TIMESTAMP_MS: i64 = 1555100955770; // CURRENT (child of ROOT)

    #[tokio::test]
    async fn test_rollback_to_time_picks_newest_older_ancestor() {
        // A timestamp strictly between ROOT and CURRENT selects ROOT (the newest ancestor older than
        // it); CURRENT is too new. Pins: the resolver picks the right ancestor by timestamp.
        let table = table();
        let between = ROOT_TIMESTAMP_MS + 1;
        let action = Transaction::new(&table)
            .manage_snapshots()
            .rollback_to_time(between);
        let mut commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = commit.take_updates();
        assert_eq!(find_set(&updates, MAIN_BRANCH).unwrap().snapshot_id, ROOT);
    }

    #[tokio::test]
    async fn test_rollback_to_time_strict_less_than_skips_equal_timestamp() {
        // The comparison is STRICT (`<`): a timestamp exactly equal to CURRENT's own timestamp does
        // not select CURRENT, so the rollback lands on the next-older ancestor, ROOT. Pins: the strict
        // `<` boundary (Java `snapshot.timestampMillis() < timestampMillis`).
        let table = table();
        let action = Transaction::new(&table)
            .manage_snapshots()
            .rollback_to_time(CURRENT_TIMESTAMP_MS);
        let mut commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = commit.take_updates();
        assert_eq!(find_set(&updates, MAIN_BRANCH).unwrap().snapshot_id, ROOT);
    }

    #[tokio::test]
    async fn test_rollback_to_time_before_first_snapshot_fails() {
        // A timestamp older than every ancestor leaves no valid snapshot to roll back to -> error with
        // the exact Java message. Pins: the empty-result error path.
        let table = table();
        let before_root = ROOT_TIMESTAMP_MS - 1;
        let action = Transaction::new(&table)
            .manage_snapshots()
            .rollback_to_time(before_root);
        let error = Arc::new(action)
            .commit(&table)
            .await
            .map(drop)
            .expect_err("rollback before the first snapshot must fail");
        assert_eq!(error.kind(), ErrorKind::DataInvalid);
        assert!(
            error
                .message()
                .contains("Cannot roll back, no valid snapshot older than"),
            "unexpected message: {}",
            error.message()
        );
    }

    #[tokio::test]
    async fn test_rollback_to_time_after_current_is_noop() {
        // A timestamp newer than CURRENT selects CURRENT (the newest ancestor older than it), which is
        // already main's snapshot -> the change is a net no-op and is suppressed at emit. Pins:
        // "at/after current keeps current".
        let table = table();
        let after_current = CURRENT_TIMESTAMP_MS + 1;
        let action = Transaction::new(&table)
            .manage_snapshots()
            .rollback_to_time(after_current);
        let mut commit = Arc::new(action).commit(&table).await.unwrap();
        assert!(
            commit.take_updates().is_empty(),
            "rollback-to-time that keeps the current snapshot must emit no updates"
        );
        assert!(commit.take_requirements().is_empty());
    }

    #[tokio::test]
    async fn test_rollback_to_time_never_selects_a_sibling() {
        // SIBLING (timestamp 1700000000000, child of ROOT) is NOT in main's ancestry. Even with a
        // timestamp newer than SIBLING, the ancestry walk from CURRENT only sees {CURRENT, ROOT}, so
        // the target is CURRENT (a no-op), never SIBLING. Pins: non-ancestor snapshots are never chosen.
        let table = forked_table();
        let after_sibling = 1800000000000; // > SIBLING's 1700000000000 and > CURRENT
        let action = Transaction::new(&table)
            .manage_snapshots()
            .rollback_to_time(after_sibling);
        let mut commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = commit.take_updates();
        // No SetSnapshotRef pointing main at SIBLING; in fact main is unchanged (CURRENT), so nothing
        // is emitted at all.
        assert!(
            find_set(&updates, MAIN_BRANCH).is_none(),
            "main must not be moved to a non-ancestor sibling"
        );
    }

    #[tokio::test]
    async fn test_set_min_snapshots_to_keep_zero_fails() {
        // Java `SnapshotRef.Builder.minSnapshotsToKeep` rejects `<= 0`. Pins: positivity guard + message.
        let table = table();
        let action = Transaction::new(&table)
            .manage_snapshots()
            .create_branch("b1", ROOT)
            .set_min_snapshots_to_keep("b1", 0);
        let error = Arc::new(action)
            .commit(&table)
            .await
            .map(drop)
            .expect_err("zero min-snapshots-to-keep must fail");
        assert_eq!(error.kind(), ErrorKind::DataInvalid);
        assert_eq!(
            error.message(),
            "Min snapshots to keep must be greater than 0"
        );
    }

    #[tokio::test]
    async fn test_set_min_snapshots_to_keep_negative_fails() {
        let table = table();
        let action = Transaction::new(&table)
            .manage_snapshots()
            .create_branch("b1", ROOT)
            .set_min_snapshots_to_keep("b1", -5);
        let error = Arc::new(action)
            .commit(&table)
            .await
            .map(drop)
            .expect_err("negative min-snapshots-to-keep must fail");
        assert_eq!(error.kind(), ErrorKind::DataInvalid);
        assert_eq!(
            error.message(),
            "Min snapshots to keep must be greater than 0"
        );
    }

    #[tokio::test]
    async fn test_set_max_snapshot_age_ms_zero_fails() {
        let table = table();
        let action = Transaction::new(&table)
            .manage_snapshots()
            .create_branch("b1", ROOT)
            .set_max_snapshot_age_ms("b1", 0);
        let error = Arc::new(action)
            .commit(&table)
            .await
            .map(drop)
            .expect_err("zero max-snapshot-age-ms must fail");
        assert_eq!(error.kind(), ErrorKind::DataInvalid);
        assert_eq!(
            error.message(),
            "Max snapshot age must be greater than 0 ms"
        );
    }

    #[tokio::test]
    async fn test_set_max_snapshot_age_ms_negative_fails() {
        let table = table();
        let action = Transaction::new(&table)
            .manage_snapshots()
            .create_branch("b1", ROOT)
            .set_max_snapshot_age_ms("b1", -1);
        let error = Arc::new(action)
            .commit(&table)
            .await
            .map(drop)
            .expect_err("negative max-snapshot-age-ms must fail");
        assert_eq!(error.kind(), ErrorKind::DataInvalid);
        assert_eq!(
            error.message(),
            "Max snapshot age must be greater than 0 ms"
        );
    }

    #[tokio::test]
    async fn test_set_max_ref_age_ms_zero_fails() {
        let table = table();
        let action = Transaction::new(&table)
            .manage_snapshots()
            .create_tag("t1", ROOT)
            .set_max_ref_age_ms("t1", 0);
        let error = Arc::new(action)
            .commit(&table)
            .await
            .map(drop)
            .expect_err("zero max-ref-age-ms must fail");
        assert_eq!(error.kind(), ErrorKind::DataInvalid);
        assert_eq!(error.message(), "Max reference age must be greater than 0");
    }

    #[tokio::test]
    async fn test_set_max_ref_age_ms_negative_fails() {
        let table = table();
        let action = Transaction::new(&table)
            .manage_snapshots()
            .create_tag("t1", ROOT)
            .set_max_ref_age_ms("t1", -100);
        let error = Arc::new(action)
            .commit(&table)
            .await
            .map(drop)
            .expect_err("negative max-ref-age-ms must fail");
        assert_eq!(error.kind(), ErrorKind::DataInvalid);
        assert_eq!(error.message(), "Max reference age must be greater than 0");
    }

    #[tokio::test]
    async fn test_fast_forward_preexisting_branch_to_tag() {
        // dev (branch at CURRENT) cannot fast-forward to stable (tag at ROOT): CURRENT is not an
        // ancestor of ROOT. But a branch AT root CAN fast-forward to a ref at CURRENT.
        let table = forked_table();
        let ok = Transaction::new(&table)
            .manage_snapshots()
            .create_branch("at_root", ROOT)
            .fast_forward("at_root", "dev"); // dev is a branch at CURRENT
        let mut commit = Arc::new(ok).commit(&table).await.unwrap();
        assert_eq!(
            find_set(&commit.take_updates(), "at_root")
                .unwrap()
                .snapshot_id,
            CURRENT
        );
    }
}
