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

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;

use crate::spec::{MAIN_BRANCH, SnapshotReference, SnapshotRetention, TableMetadataRef};
use crate::table::Table;
use crate::transaction::action::{ActionCommit, TransactionAction};
use crate::util::snapshot::is_ancestor_of;
use crate::{Error, ErrorKind, Result, TableRequirement, TableUpdate};

/// Every validation failure in this action is a `DataInvalid`.
fn invalid(message: impl Into<String>) -> Error {
    Error::new(ErrorKind::DataInvalid, message)
}

/// A branch reference with no retention overrides.
fn default_branch(snapshot_id: i64) -> SnapshotReference {
    SnapshotReference {
        snapshot_id,
        retention: SnapshotRetention::Branch {
            min_snapshots_to_keep: None,
            max_snapshot_age_ms: None,
            max_ref_age_ms: None,
        },
    }
}

/// A transaction action that manages snapshot references (branches and tags), mirroring Java
/// `ManageSnapshots` / `UpdateSnapshotReferencesOperation`. It creates, removes, replaces, and
/// renames branches and tags, sets per-ref retention, and fast-forwards branches; no snapshots are
/// produced.
///
/// Builder methods only record operations; validation runs in [`commit`](TransactionAction::commit),
/// which replays them in order against a working copy of the refs map (so `create_branch("a", id)`
/// followed by `rename_branch("a", "b")` sees the intermediate `a`). The resulting updates and
/// requirements come from diffing the base refs against the final ones.
pub struct ManageSnapshotsAction {
    ops: Vec<RefOp>,
}

/// A single snapshot-reference operation. `replace_branch_with_ref` and `fast_forward_branch` share
/// [`RefOp::ReplaceBranchWithRef`], differing only by the `fast_forward` ancestry guard.
enum RefOp {
    CreateBranch {
        name: String,
        snapshot_id: i64,
    },
    CreateTag {
        name: String,
        snapshot_id: i64,
    },
    RemoveBranch {
        name: String,
    },
    RemoveTag {
        name: String,
    },
    RenameBranch {
        from: String,
        to: String,
    },
    ReplaceBranch {
        name: String,
        snapshot_id: i64,
    },
    ReplaceBranchWithRef {
        from: String,
        to: String,
        fast_forward: bool,
    },
    ReplaceTag {
        name: String,
        snapshot_id: i64,
    },
    SetMinSnapshotsToKeep {
        name: String,
        value: i32,
    },
    SetMaxSnapshotAgeMs {
        name: String,
        value: i64,
    },
    SetMaxRefAgeMs {
        name: String,
        value: i64,
    },
}

impl ManageSnapshotsAction {
    pub(crate) fn new() -> Self {
        Self { ops: vec![] }
    }

    /// Create a branch named `name` pointing at `snapshot_id`.
    pub fn create_branch(mut self, name: impl Into<String>, snapshot_id: i64) -> Self {
        self.ops.push(RefOp::CreateBranch {
            name: name.into(),
            snapshot_id,
        });
        self
    }

    /// Create a tag named `name` pointing at `snapshot_id`.
    pub fn create_tag(mut self, name: impl Into<String>, snapshot_id: i64) -> Self {
        self.ops.push(RefOp::CreateTag {
            name: name.into(),
            snapshot_id,
        });
        self
    }

    /// Remove the branch named `name`. The `main` branch cannot be removed.
    pub fn remove_branch(mut self, name: impl Into<String>) -> Self {
        self.ops.push(RefOp::RemoveBranch { name: name.into() });
        self
    }

    /// Remove the tag named `name`.
    pub fn remove_tag(mut self, name: impl Into<String>) -> Self {
        self.ops.push(RefOp::RemoveTag { name: name.into() });
        self
    }

    /// Rename the branch `from` to `to`. The `main` branch cannot be renamed.
    pub fn rename_branch(mut self, from: impl Into<String>, to: impl Into<String>) -> Self {
        self.ops.push(RefOp::RenameBranch {
            from: from.into(),
            to: to.into(),
        });
        self
    }

    /// Point the existing branch `name` at `snapshot_id`, preserving its retention.
    pub fn replace_branch(mut self, name: impl Into<String>, snapshot_id: i64) -> Self {
        self.ops.push(RefOp::ReplaceBranch {
            name: name.into(),
            snapshot_id,
        });
        self
    }

    /// Point the branch `from` at the snapshot referenced by `to`. If `from` does not exist it is
    /// created as a branch; `to` must exist.
    pub fn replace_branch_with_ref(
        mut self,
        from: impl Into<String>,
        to: impl Into<String>,
    ) -> Self {
        self.ops.push(RefOp::ReplaceBranchWithRef {
            from: from.into(),
            to: to.into(),
            fast_forward: false,
        });
        self
    }

    /// Fast-forward the branch `from` to the snapshot referenced by `to`. Like
    /// [`replace_branch_with_ref`](Self::replace_branch_with_ref), but additionally requires
    /// `from`'s current snapshot to be an ancestor of `to`'s snapshot.
    pub fn fast_forward_branch(mut self, from: impl Into<String>, to: impl Into<String>) -> Self {
        self.ops.push(RefOp::ReplaceBranchWithRef {
            from: from.into(),
            to: to.into(),
            fast_forward: true,
        });
        self
    }

    /// Point the existing tag `name` at `snapshot_id`, preserving its retention.
    pub fn replace_tag(mut self, name: impl Into<String>, snapshot_id: i64) -> Self {
        self.ops.push(RefOp::ReplaceTag {
            name: name.into(),
            snapshot_id,
        });
        self
    }

    /// Set the `min_snapshots_to_keep` retention on a branch (branch-only).
    pub fn set_min_snapshots_to_keep(mut self, branch: impl Into<String>, value: i32) -> Self {
        self.ops.push(RefOp::SetMinSnapshotsToKeep {
            name: branch.into(),
            value,
        });
        self
    }

    /// Set the `max_snapshot_age_ms` retention on a branch (branch-only).
    pub fn set_max_snapshot_age_ms(mut self, branch: impl Into<String>, value: i64) -> Self {
        self.ops.push(RefOp::SetMaxSnapshotAgeMs {
            name: branch.into(),
            value,
        });
        self
    }

    /// Set the `max_ref_age_ms` retention on a branch or tag.
    pub fn set_max_ref_age_ms(mut self, name: impl Into<String>, value: i64) -> Self {
        self.ops.push(RefOp::SetMaxRefAgeMs {
            name: name.into(),
            value,
        });
        self
    }

    /// Validates `op` against the running `working_refs` and `metadata`, then applies it to
    /// `working_refs`.
    fn apply_op(
        op: &RefOp,
        working_refs: &mut HashMap<String, SnapshotReference>,
        metadata: &TableMetadataRef,
    ) -> Result<()> {
        match op {
            RefOp::CreateBranch { name, snapshot_id } => {
                Self::ensure_absent(working_refs, name)?;
                Self::ensure_snapshot_exists(metadata, *snapshot_id)?;
                working_refs.insert(name.clone(), default_branch(*snapshot_id));
            }
            RefOp::CreateTag { name, snapshot_id } => {
                // `main` must be a branch; reject it as a tag name even if the ref isn't set yet.
                if name == MAIN_BRANCH {
                    return Err(invalid("Cannot create a tag with the reserved name main"));
                }
                Self::ensure_absent(working_refs, name)?;
                Self::ensure_snapshot_exists(metadata, *snapshot_id)?;
                working_refs.insert(name.clone(), SnapshotReference {
                    snapshot_id: *snapshot_id,
                    retention: SnapshotRetention::Tag {
                        max_ref_age_ms: None,
                    },
                });
            }
            RefOp::RemoveBranch { name } => {
                if name == MAIN_BRANCH {
                    return Err(invalid("Cannot remove the main branch"));
                }
                Self::ensure_branch(working_refs, name)?;
                working_refs.remove(name);
            }
            RefOp::RemoveTag { name } => {
                Self::ensure_tag(working_refs, name)?;
                working_refs.remove(name);
            }
            RefOp::RenameBranch { from, to } => {
                if from == MAIN_BRANCH {
                    return Err(invalid("Cannot rename the main branch"));
                }
                Self::ensure_branch(working_refs, from)?;
                Self::ensure_absent(working_refs, to)?;
                let reference = working_refs.remove(from).expect("branch checked above");
                working_refs.insert(to.clone(), reference);
            }
            RefOp::ReplaceBranch { name, snapshot_id } => {
                let reference = Self::ensure_branch(working_refs, name)?.clone();
                Self::ensure_snapshot_exists(metadata, *snapshot_id)?;
                working_refs.insert(name.clone(), SnapshotReference {
                    snapshot_id: *snapshot_id,
                    retention: reference.retention,
                });
            }
            RefOp::ReplaceBranchWithRef {
                from,
                to,
                fast_forward,
            } => {
                let to_snapshot_id = working_refs
                    .get(to)
                    .ok_or_else(|| {
                        invalid(format!(
                            "Cannot replace branch {from}: source ref does not exist: {to}"
                        ))
                    })?
                    .snapshot_id;

                // An existing destination keeps its retention; a missing one is created as a branch.
                let new_ref = match working_refs.get(from) {
                    Some(existing) => {
                        if !existing.is_branch() {
                            return Err(invalid(format!("Ref {from} is a tag, not a branch")));
                        }
                        // Already there: nothing to replace, and a no-op must not produce a
                        // requirement. Return before the fast-forward ancestor check.
                        if existing.snapshot_id == to_snapshot_id {
                            return Ok(());
                        }
                        if *fast_forward
                            && !is_ancestor_of(metadata, to_snapshot_id, existing.snapshot_id)
                        {
                            return Err(invalid(format!(
                                "Cannot fast-forward branch {from}: its snapshot is not an ancestor of {to}"
                            )));
                        }
                        SnapshotReference {
                            snapshot_id: to_snapshot_id,
                            retention: existing.retention.clone(),
                        }
                    }
                    None => default_branch(to_snapshot_id),
                };
                working_refs.insert(from.clone(), new_ref);
            }
            RefOp::ReplaceTag { name, snapshot_id } => {
                let reference = Self::ensure_tag(working_refs, name)?.clone();
                Self::ensure_snapshot_exists(metadata, *snapshot_id)?;
                working_refs.insert(name.clone(), SnapshotReference {
                    snapshot_id: *snapshot_id,
                    retention: reference.retention,
                });
            }
            RefOp::SetMinSnapshotsToKeep { name, value } => {
                Self::ensure_positive(*value as i64, "min_snapshots_to_keep")?;
                let reference = Self::ensure_branch(working_refs, name)?;
                if let SnapshotRetention::Branch {
                    min_snapshots_to_keep,
                    ..
                } = &mut reference.retention
                {
                    *min_snapshots_to_keep = Some(*value);
                }
            }
            RefOp::SetMaxSnapshotAgeMs { name, value } => {
                Self::ensure_positive(*value, "max_snapshot_age_ms")?;
                let reference = Self::ensure_branch(working_refs, name)?;
                if let SnapshotRetention::Branch {
                    max_snapshot_age_ms,
                    ..
                } = &mut reference.retention
                {
                    *max_snapshot_age_ms = Some(*value);
                }
            }
            RefOp::SetMaxRefAgeMs { name, value } => {
                Self::ensure_positive(*value, "max_ref_age_ms")?;
                let reference = working_refs
                    .get_mut(name)
                    .ok_or_else(|| invalid(format!("Ref does not exist: {name}")))?;
                match &mut reference.retention {
                    SnapshotRetention::Branch { max_ref_age_ms, .. }
                    | SnapshotRetention::Tag { max_ref_age_ms } => *max_ref_age_ms = Some(*value),
                }
            }
        }
        Ok(())
    }

    fn ensure_absent(refs: &HashMap<String, SnapshotReference>, name: &str) -> Result<()> {
        if refs.contains_key(name) {
            return Err(invalid(format!("Ref already exists: {name}")));
        }
        Ok(())
    }

    fn ensure_snapshot_exists(metadata: &TableMetadataRef, snapshot_id: i64) -> Result<()> {
        if metadata.snapshot_by_id(snapshot_id).is_none() {
            return Err(invalid(format!(
                "Cannot set snapshot ref: unknown snapshot id {snapshot_id}"
            )));
        }
        Ok(())
    }

    /// Returns a mutable handle to an existing branch, erroring if it is missing or a tag.
    fn ensure_branch<'a>(
        refs: &'a mut HashMap<String, SnapshotReference>,
        name: &str,
    ) -> Result<&'a mut SnapshotReference> {
        let reference = refs
            .get_mut(name)
            .ok_or_else(|| invalid(format!("Branch does not exist: {name}")))?;
        if !reference.is_branch() {
            return Err(invalid(format!("Ref {name} is a tag, not a branch")));
        }
        Ok(reference)
    }

    /// Returns a mutable handle to an existing tag, erroring if it is missing or a branch.
    fn ensure_tag<'a>(
        refs: &'a mut HashMap<String, SnapshotReference>,
        name: &str,
    ) -> Result<&'a mut SnapshotReference> {
        let reference = refs
            .get_mut(name)
            .ok_or_else(|| invalid(format!("Tag does not exist: {name}")))?;
        if reference.is_branch() {
            return Err(invalid(format!("Ref {name} is a branch, not a tag")));
        }
        Ok(reference)
    }

    fn ensure_positive(value: i64, field: &str) -> Result<()> {
        if value <= 0 {
            return Err(invalid(format!(
                "{field} must be a positive number, was {value}"
            )));
        }
        Ok(())
    }
}

#[async_trait]
impl TransactionAction for ManageSnapshotsAction {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        let metadata = table.metadata_ref();
        let base_refs = metadata.refs().clone();
        let mut working_refs = base_refs.clone();

        for op in &self.ops {
            Self::apply_op(op, &mut working_refs, &metadata)?;
        }

        // Remove refs that disappeared, set ones that were added or changed; unchanged refs (and
        // net no-ops) emit nothing. Diffing yields at most one update per ref name.
        let mut updates: Vec<TableUpdate> = vec![];
        let mut refs_with_updates: Vec<String> = vec![];

        for ref_name in base_refs.keys() {
            if !working_refs.contains_key(ref_name) {
                updates.push(TableUpdate::RemoveSnapshotRef {
                    ref_name: ref_name.clone(),
                });
                refs_with_updates.push(ref_name.clone());
            }
        }
        for (ref_name, reference) in &working_refs {
            if base_refs.get(ref_name) != Some(reference) {
                updates.push(TableUpdate::SetSnapshotRef {
                    ref_name: ref_name.clone(),
                    reference: reference.clone(),
                });
                refs_with_updates.push(ref_name.clone());
            }
        }

        if updates.is_empty() {
            return Ok(ActionCommit::new(vec![], vec![]));
        }

        // One requirement per changed ref, asserting its base snapshot id (`None` means the ref
        // must not exist yet).
        let mut requirements: Vec<TableRequirement> = vec![TableRequirement::UuidMatch {
            uuid: metadata.uuid(),
        }];
        for ref_name in &refs_with_updates {
            requirements.push(TableRequirement::RefSnapshotIdMatch {
                r#ref: ref_name.clone(),
                snapshot_id: base_refs.get(ref_name).map(|r| r.snapshot_id),
            });
        }

        Ok(ActionCommit::new(updates, requirements))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use crate::spec::{
        MAIN_BRANCH, Operation, Snapshot, SnapshotReference, SnapshotRetention, Summary,
        TableMetadata,
    };
    use crate::table::Table;
    use crate::transaction::Transaction;
    use crate::transaction::action::{ApplyTransactionAction, TransactionAction};
    use crate::transaction::manage_snapshots::ManageSnapshotsAction;
    use crate::transaction::tests::make_v2_minimal_table;
    use crate::{TableRequirement, TableUpdate};

    const TS: i64 = 1_700_000_000_000;

    fn action() -> ManageSnapshotsAction {
        ManageSnapshotsAction::new()
    }

    fn snapshot(id: i64, parent: Option<i64>, sequence_number: i64, timestamp_ms: i64) -> Snapshot {
        Snapshot::builder()
            .with_snapshot_id(id)
            .with_parent_snapshot_id(parent)
            .with_sequence_number(sequence_number)
            .with_timestamp_ms(timestamp_ms)
            .with_schema_id(0)
            .with_manifest_list(format!("/snap-{id}.avro"))
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: HashMap::new(),
            })
            .build()
    }

    fn branch(snapshot_id: i64) -> SnapshotReference {
        SnapshotReference {
            snapshot_id,
            retention: SnapshotRetention::Branch {
                min_snapshots_to_keep: None,
                max_snapshot_age_ms: None,
                max_ref_age_ms: None,
            },
        }
    }

    fn tag(snapshot_id: i64) -> SnapshotReference {
        SnapshotReference {
            snapshot_id,
            retention: SnapshotRetention::Tag {
                max_ref_age_ms: None,
            },
        }
    }

    /// Builds a table from synthetic snapshots and refs on top of an empty base.
    fn table_with(snapshots: Vec<Snapshot>, refs: Vec<(&str, SnapshotReference)>) -> Table {
        let base = make_v2_minimal_table();
        let mut builder = base.metadata().clone().into_builder(None);
        for snapshot in snapshots {
            builder = builder.add_snapshot(snapshot).unwrap();
        }
        for (name, reference) in refs {
            builder = builder.set_ref(name, reference).unwrap();
        }
        base.with_metadata(Arc::new(builder.build().unwrap().metadata))
    }

    /// A three-snapshot chain 1 -> 2 -> 3 with `main` at the head (3).
    fn table_main_chain() -> Table {
        table_with(
            vec![
                snapshot(1, None, 35, TS + 1),
                snapshot(2, Some(1), 36, TS + 2),
                snapshot(3, Some(2), 37, TS + 3),
            ],
            vec![(MAIN_BRANCH, branch(3))],
        )
    }

    async fn commit(
        table: &Table,
        action: ManageSnapshotsAction,
    ) -> (Vec<TableUpdate>, Vec<TableRequirement>) {
        let mut commit = Arc::new(action).commit(table).await.unwrap();
        (commit.take_updates(), commit.take_requirements())
    }

    fn set_ref<'a>(updates: &'a [TableUpdate], name: &str) -> Option<&'a SnapshotReference> {
        updates.iter().find_map(|update| match update {
            TableUpdate::SetSnapshotRef {
                ref_name,
                reference,
            } if ref_name == name => Some(reference),
            _ => None,
        })
    }

    fn removed_ref(updates: &[TableUpdate], name: &str) -> bool {
        updates.iter().any(|update| {
            matches!(update, TableUpdate::RemoveSnapshotRef { ref_name } if ref_name == name)
        })
    }

    fn requirement_for<'a>(
        requirements: &'a [TableRequirement],
        name: &str,
    ) -> Option<&'a Option<i64>> {
        requirements
            .iter()
            .find_map(|requirement| match requirement {
                TableRequirement::RefSnapshotIdMatch { r#ref, snapshot_id } if r#ref == name => {
                    Some(snapshot_id)
                }
                _ => None,
            })
    }

    /// Applies `updates` through a `TableMetadataBuilder` and returns the resulting refs map.
    fn apply_updates(
        table: &Table,
        updates: Vec<TableUpdate>,
    ) -> HashMap<String, SnapshotReference> {
        let mut builder = table.metadata().clone().into_builder(None);
        for update in updates {
            builder = update.apply(builder).unwrap();
        }
        let metadata: TableMetadata = builder.build().unwrap().metadata;
        metadata.refs().clone()
    }

    #[tokio::test]
    async fn test_create_branch() {
        let table = table_main_chain();
        let (updates, requirements) = commit(&table, action().create_branch("b", 2)).await;
        assert_eq!(set_ref(&updates, "b"), Some(&branch(2)));
        // create asserts the ref must not yet exist.
        assert_eq!(requirement_for(&requirements, "b"), Some(&None));
    }

    #[tokio::test]
    async fn test_create_tag() {
        let table = table_main_chain();
        let (updates, requirements) = commit(&table, action().create_tag("t", 1)).await;
        assert_eq!(set_ref(&updates, "t"), Some(&tag(1)));
        assert_eq!(requirement_for(&requirements, "t"), Some(&None));
    }

    #[tokio::test]
    async fn test_create_duplicate_ref_fails() {
        let table = table_main_chain();
        assert!(
            Arc::new(action().create_branch(MAIN_BRANCH, 2))
                .commit(&table)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn test_create_on_missing_snapshot_fails() {
        let table = table_main_chain();
        assert!(
            Arc::new(action().create_tag("t", 999))
                .commit(&table)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn test_create_tag_named_main_fails() {
        // No current snapshot, so `main` is absent: the reserved-name guard, not the duplicate
        // check, rejects the tag.
        let table = table_with(vec![snapshot(1, None, 35, TS + 1)], vec![("b", branch(1))]);
        assert!(table.metadata().refs().get(MAIN_BRANCH).is_none());
        assert!(
            Arc::new(action().create_tag(MAIN_BRANCH, 1))
                .commit(&table)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn test_remove_branch() {
        let table = table_with(
            vec![snapshot(1, None, 35, TS + 1), snapshot(2, None, 36, TS + 2)],
            vec![(MAIN_BRANCH, branch(1)), ("b", branch(2))],
        );
        let (updates, requirements) = commit(&table, action().remove_branch("b")).await;
        assert!(removed_ref(&updates, "b"));
        assert_eq!(requirement_for(&requirements, "b"), Some(&Some(2)));
    }

    #[tokio::test]
    async fn test_remove_tag() {
        let table = table_with(
            vec![snapshot(1, None, 35, TS + 1), snapshot(2, None, 36, TS + 2)],
            vec![(MAIN_BRANCH, branch(1)), ("t", tag(2))],
        );
        let (updates, _) = commit(&table, action().remove_tag("t")).await;
        assert!(removed_ref(&updates, "t"));
    }

    #[tokio::test]
    async fn test_remove_main_fails() {
        let table = table_main_chain();
        assert!(
            Arc::new(action().remove_branch(MAIN_BRANCH))
                .commit(&table)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn test_remove_branch_on_tag_fails() {
        let table = table_with(
            vec![snapshot(1, None, 35, TS + 1), snapshot(2, None, 36, TS + 2)],
            vec![(MAIN_BRANCH, branch(1)), ("t", tag(2))],
        );
        assert!(
            Arc::new(action().remove_branch("t"))
                .commit(&table)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn test_remove_tag_on_branch_fails() {
        let table = table_with(
            vec![snapshot(1, None, 35, TS + 1), snapshot(2, None, 36, TS + 2)],
            vec![(MAIN_BRANCH, branch(1)), ("b", branch(2))],
        );
        assert!(
            Arc::new(action().remove_tag("b"))
                .commit(&table)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn test_replace_branch() {
        let table = table_with(
            vec![snapshot(1, None, 35, TS + 1), snapshot(2, None, 36, TS + 2)],
            vec![(MAIN_BRANCH, branch(1)), ("b", branch(1))],
        );
        let (updates, requirements) = commit(&table, action().replace_branch("b", 2)).await;
        assert_eq!(set_ref(&updates, "b"), Some(&branch(2)));
        assert_eq!(requirement_for(&requirements, "b"), Some(&Some(1)));
    }

    #[tokio::test]
    async fn test_replace_tag() {
        let table = table_with(
            vec![snapshot(1, None, 35, TS + 1), snapshot(2, None, 36, TS + 2)],
            vec![(MAIN_BRANCH, branch(1)), ("t", tag(1))],
        );
        let (updates, _) = commit(&table, action().replace_tag("t", 2)).await;
        assert_eq!(set_ref(&updates, "t"), Some(&tag(2)));
    }

    #[tokio::test]
    async fn test_replace_branch_with_ref() {
        let table = table_with(
            vec![snapshot(1, None, 35, TS + 1), snapshot(2, None, 36, TS + 2)],
            vec![(MAIN_BRANCH, branch(2)), ("b", branch(1))],
        );
        let (updates, requirements) =
            commit(&table, action().replace_branch_with_ref("b", MAIN_BRANCH)).await;
        // `b` now points at main's snapshot (2).
        assert_eq!(set_ref(&updates, "b"), Some(&branch(2)));
        assert_eq!(requirement_for(&requirements, "b"), Some(&Some(1)));
        // `main` is only the source: not updated and not asserted (only changed refs are asserted).
        assert!(!removed_ref(&updates, MAIN_BRANCH));
        assert!(set_ref(&updates, MAIN_BRANCH).is_none());
        assert!(requirement_for(&requirements, MAIN_BRANCH).is_none());
    }

    #[tokio::test]
    async fn test_replace_branch_with_ref_to_tag() {
        // `to` may be a tag; `b` moves to the tag's snapshot.
        let table = table_with(
            vec![snapshot(1, None, 35, TS + 1), snapshot(2, None, 36, TS + 2)],
            vec![(MAIN_BRANCH, branch(1)), ("b", branch(1)), ("t", tag(2))],
        );
        let (updates, _) = commit(&table, action().replace_branch_with_ref("b", "t")).await;
        assert_eq!(set_ref(&updates, "b"), Some(&branch(2)));
    }

    #[tokio::test]
    async fn test_fast_forward_to_tag() {
        // 1 -> 2 -> 3 ; tag `t` at 3, `b` at 1 fast-forwards to it.
        let table = table_with(
            vec![
                snapshot(1, None, 35, TS + 1),
                snapshot(2, Some(1), 36, TS + 2),
                snapshot(3, Some(2), 37, TS + 3),
            ],
            vec![(MAIN_BRANCH, branch(3)), ("b", branch(1)), ("t", tag(3))],
        );
        let (updates, _) = commit(&table, action().fast_forward_branch("b", "t")).await;
        assert_eq!(set_ref(&updates, "b"), Some(&branch(3)));
    }

    #[tokio::test]
    async fn test_replace_branch_with_ref_from_tag_fails() {
        let table = table_with(
            vec![snapshot(1, None, 35, TS + 1), snapshot(2, None, 36, TS + 2)],
            vec![(MAIN_BRANCH, branch(2)), ("t", tag(1))],
        );
        assert!(
            Arc::new(action().replace_branch_with_ref("t", MAIN_BRANCH))
                .commit(&table)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn test_fast_forward_from_tag_fails() {
        let table = table_with(
            vec![
                snapshot(1, None, 35, TS + 1),
                snapshot(2, Some(1), 36, TS + 2),
            ],
            vec![(MAIN_BRANCH, branch(2)), ("t", tag(1))],
        );
        assert!(
            Arc::new(action().fast_forward_branch("t", MAIN_BRANCH))
                .commit(&table)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn test_replace_branch_with_ref_missing_destination_fails() {
        let table = table_main_chain();
        assert!(
            Arc::new(action().replace_branch_with_ref("b", "does-not-exist"))
                .commit(&table)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn test_replace_branch_with_ref_creates_missing_source() {
        let table = table_main_chain();
        // `from` ("b") does not exist, so it is created as a branch at main's snapshot (3).
        let (updates, requirements) =
            commit(&table, action().replace_branch_with_ref("b", MAIN_BRANCH)).await;
        assert_eq!(set_ref(&updates, "b"), Some(&branch(3)));
        // No base ref for `b`, so it asserts absence.
        assert_eq!(requirement_for(&requirements, "b"), Some(&None));
        // The source `main` is not asserted (only changed refs are).
        assert!(requirement_for(&requirements, MAIN_BRANCH).is_none());
    }

    #[tokio::test]
    async fn test_transient_source_ref_has_no_requirement() {
        let table = table_with(
            vec![snapshot(1, None, 35, TS + 1), snapshot(2, None, 36, TS + 2)],
            vec![(MAIN_BRANCH, branch(1)), ("b", branch(1))],
        );
        // `tmp` is created and removed within the transaction, used only as a source for `b`.
        let (updates, requirements) = commit(
            &table,
            action()
                .create_branch("tmp", 2)
                .replace_branch_with_ref("b", "tmp")
                .remove_branch("tmp"),
        )
        .await;
        // `b` points at `tmp`'s snapshot (2).
        assert_eq!(set_ref(&updates, "b"), Some(&branch(2)));
        assert_eq!(requirement_for(&requirements, "b"), Some(&Some(1)));
        // `tmp` nets to absent: no update, and no requirement since it never existed in base.
        assert!(set_ref(&updates, "tmp").is_none());
        assert!(!removed_ref(&updates, "tmp"));
        assert!(requirement_for(&requirements, "tmp").is_none());
    }

    #[tokio::test]
    async fn test_fast_forward_success() {
        // 1 -> 2 -> 3 ; `b` at 1 fast-forwards to main (3), since 1 is an ancestor of 3.
        let table = table_with(
            vec![
                snapshot(1, None, 35, TS + 1),
                snapshot(2, Some(1), 36, TS + 2),
                snapshot(3, Some(2), 37, TS + 3),
            ],
            vec![(MAIN_BRANCH, branch(3)), ("b", branch(1))],
        );
        let (updates, _) = commit(&table, action().fast_forward_branch("b", MAIN_BRANCH)).await;
        assert_eq!(set_ref(&updates, "b"), Some(&branch(3)));
    }

    #[tokio::test]
    async fn test_fast_forward_non_ancestor_fails() {
        // `b` at 2 and main at 3 share parent 1: 2 is not an ancestor of 3.
        let table = table_with(
            vec![
                snapshot(1, None, 35, TS + 1),
                snapshot(2, Some(1), 36, TS + 2),
                snapshot(3, Some(1), 37, TS + 3),
            ],
            vec![(MAIN_BRANCH, branch(3)), ("b", branch(2))],
        );
        assert!(
            Arc::new(action().fast_forward_branch("b", MAIN_BRANCH))
                .commit(&table)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn test_noop_replace_branch_with_ref_emits_nothing() {
        // `b` already points at main's snapshot, so there is nothing to update or assert.
        let table = table_with(
            vec![snapshot(1, None, 35, TS + 1), snapshot(2, None, 36, TS + 2)],
            vec![(MAIN_BRANCH, branch(2)), ("b", branch(2))],
        );
        let (updates, requirements) =
            commit(&table, action().replace_branch_with_ref("b", MAIN_BRANCH)).await;
        assert!(updates.is_empty());
        assert!(requirements.is_empty());
    }

    #[tokio::test]
    async fn test_noop_fast_forward_emits_nothing() {
        // A fast-forward to where `b` already is records nothing, so it can't conflict with a
        // concurrent move of `main`.
        let table = table_with(
            vec![
                snapshot(1, None, 35, TS + 1),
                snapshot(2, Some(1), 36, TS + 2),
            ],
            vec![(MAIN_BRANCH, branch(2)), ("b", branch(2))],
        );
        let (updates, requirements) =
            commit(&table, action().fast_forward_branch("b", MAIN_BRANCH)).await;
        assert!(updates.is_empty());
        assert!(requirements.is_empty());
    }

    #[tokio::test]
    async fn test_retention_updates() {
        let table = table_with(vec![snapshot(1, None, 35, TS + 1)], vec![
            (MAIN_BRANCH, branch(1)),
            ("b", branch(1)),
        ]);
        let (updates, _) = commit(
            &table,
            action()
                .set_min_snapshots_to_keep("b", 3)
                .set_max_snapshot_age_ms("b", 1000)
                .set_max_ref_age_ms("b", 2000),
        )
        .await;
        assert_eq!(
            set_ref(&updates, "b"),
            Some(&SnapshotReference {
                snapshot_id: 1,
                retention: SnapshotRetention::Branch {
                    min_snapshots_to_keep: Some(3),
                    max_snapshot_age_ms: Some(1000),
                    max_ref_age_ms: Some(2000),
                },
            })
        );
    }

    #[tokio::test]
    async fn test_set_max_ref_age_on_tag() {
        let table = table_with(vec![snapshot(1, None, 35, TS + 1)], vec![
            (MAIN_BRANCH, branch(1)),
            ("t", tag(1)),
        ]);
        let (updates, _) = commit(&table, action().set_max_ref_age_ms("t", 5000)).await;
        assert_eq!(
            set_ref(&updates, "t"),
            Some(&SnapshotReference {
                snapshot_id: 1,
                retention: SnapshotRetention::Tag {
                    max_ref_age_ms: Some(5000),
                },
            })
        );
    }

    #[tokio::test]
    async fn test_branch_only_retention_on_tag_fails() {
        let table = table_with(vec![snapshot(1, None, 35, TS + 1)], vec![
            (MAIN_BRANCH, branch(1)),
            ("t", tag(1)),
        ]);
        assert!(
            Arc::new(action().set_min_snapshots_to_keep("t", 2))
                .commit(&table)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn test_negative_retention_value_fails() {
        let table = table_main_chain();
        assert!(
            Arc::new(action().set_max_ref_age_ms(MAIN_BRANCH, -1))
                .commit(&table)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn test_rename_branch() {
        let table = table_with(
            vec![snapshot(1, None, 35, TS + 1), snapshot(2, None, 36, TS + 2)],
            vec![(MAIN_BRANCH, branch(1)), ("b", branch(2))],
        );
        let (updates, requirements) = commit(&table, action().rename_branch("b", "c")).await;
        assert!(removed_ref(&updates, "b"));
        assert_eq!(set_ref(&updates, "c"), Some(&branch(2)));
        assert_eq!(requirement_for(&requirements, "b"), Some(&Some(2)));
        assert_eq!(requirement_for(&requirements, "c"), Some(&None));
    }

    #[tokio::test]
    async fn test_rename_main_fails() {
        let table = table_main_chain();
        assert!(
            Arc::new(action().rename_branch(MAIN_BRANCH, "c"))
                .commit(&table)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn test_rename_to_existing_fails() {
        let table = table_with(
            vec![snapshot(1, None, 35, TS + 1), snapshot(2, None, 36, TS + 2)],
            vec![(MAIN_BRANCH, branch(1)), ("b", branch(2))],
        );
        assert!(
            Arc::new(action().rename_branch("b", MAIN_BRANCH))
                .commit(&table)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn test_chained_ops_minimal_diff() {
        let table = table_main_chain();
        // create("a") then rename("a","b"): net effect is only a new ref `b`.
        let (updates, requirements) = commit(
            &table,
            action().create_branch("a", 2).rename_branch("a", "b"),
        )
        .await;
        // `a` was transient: no update and no requirement for it.
        assert!(!removed_ref(&updates, "a"));
        assert!(set_ref(&updates, "a").is_none());
        assert!(requirement_for(&requirements, "a").is_none());
        // Only `b` is set.
        assert_eq!(set_ref(&updates, "b"), Some(&branch(2)));
        assert_eq!(
            updates
                .iter()
                .filter(|u| matches!(u, TableUpdate::SetSnapshotRef { .. }))
                .count(),
            1
        );
        // `b` is absent in base, so it asserts absence.
        assert_eq!(requirement_for(&requirements, "b"), Some(&None));
    }

    #[tokio::test]
    async fn test_net_noop_emits_nothing() {
        let table = table_main_chain();
        let (updates, requirements) =
            commit(&table, action().create_branch("a", 2).remove_branch("a")).await;
        assert!(updates.is_empty());
        assert!(requirements.is_empty());
    }

    #[tokio::test]
    async fn test_requirements_diff_based_and_deduped() {
        let table = table_with(
            vec![snapshot(1, None, 35, TS + 1), snapshot(2, None, 36, TS + 2)],
            vec![(MAIN_BRANCH, branch(1)), ("b", branch(1))],
        );
        // Two ops on `b`: replace then retention. There must be a single requirement for `b`.
        let (_, requirements) = commit(
            &table,
            action()
                .replace_branch("b", 2)
                .set_max_ref_age_ms("b", 1000),
        )
        .await;
        let b_requirements = requirements
            .iter()
            .filter(
                |r| matches!(r, TableRequirement::RefSnapshotIdMatch { r#ref, .. } if r#ref == "b"),
            )
            .count();
        assert_eq!(b_requirements, 1);
        assert_eq!(requirement_for(&requirements, "b"), Some(&Some(1)));
        // Always includes a UuidMatch.
        assert!(
            requirements
                .iter()
                .any(|r| matches!(r, TableRequirement::UuidMatch { .. }))
        );
    }

    #[tokio::test]
    async fn test_retention_only_update_preserves_reference() {
        let table = table_with(vec![snapshot(1, None, 35, TS + 1)], vec![
            (MAIN_BRANCH, branch(1)),
            ("b", branch(1)),
        ]);
        let (updates, requirements) = commit(&table, action().set_max_ref_age_ms("b", 1000)).await;
        // Same snapshot id, only retention changed — still a SetSnapshotRef preserving the id.
        assert_eq!(
            set_ref(&updates, "b"),
            Some(&SnapshotReference {
                snapshot_id: 1,
                retention: SnapshotRetention::Branch {
                    min_snapshots_to_keep: None,
                    max_snapshot_age_ms: None,
                    max_ref_age_ms: Some(1000),
                },
            })
        );
        assert_eq!(requirement_for(&requirements, "b"), Some(&Some(1)));
    }

    #[tokio::test]
    async fn test_round_trip_through_builder() {
        let table = table_with(
            vec![snapshot(1, None, 35, TS + 1), snapshot(2, None, 36, TS + 2)],
            vec![(MAIN_BRANCH, branch(1)), ("old", branch(2))],
        );
        let (updates, _) = commit(
            &table,
            action()
                .create_tag("t", 2)
                .rename_branch("old", "new")
                .replace_branch(MAIN_BRANCH, 2),
        )
        .await;

        let refs = apply_updates(&table, updates);
        assert_eq!(refs.get(MAIN_BRANCH), Some(&branch(2)));
        assert_eq!(refs.get("new"), Some(&branch(2)));
        assert_eq!(refs.get("t"), Some(&tag(2)));
        assert!(!refs.contains_key("old"));
    }

    #[tokio::test]
    async fn test_apply_registers_action() {
        let table = table_main_chain();
        let tx = Transaction::new(&table);
        let tx = tx.manage_snapshots().create_tag("t", 1).apply(tx).unwrap();
        assert_eq!(tx.actions.len(), 1);
    }
}

/// Drives updates *and* requirements through a real `MemoryCatalog` to check the emitted
/// requirements are enforced on commit.
///
/// We hand a `TableCommit` straight to `Catalog::update_table` instead of using
/// `Transaction::commit`, which would reload the table and rebuild the action against fresh
/// metadata on each attempt — hiding the conflict in a single-threaded test.
#[cfg(test)]
mod e2e_tests {
    use std::sync::Arc;

    use crate::memory::tests::new_memory_catalog;
    use crate::spec::{
        DataContentType, DataFile, DataFileBuilder, DataFileFormat, Literal, MAIN_BRANCH, Struct,
    };
    use crate::table::Table;
    use crate::transaction::Transaction;
    use crate::transaction::action::{ApplyTransactionAction, TransactionAction};
    use crate::transaction::manage_snapshots::ManageSnapshotsAction;
    use crate::transaction::tests::make_v3_minimal_table_in_catalog;
    use crate::{Catalog, ErrorKind, TableCommit};

    fn file_with_rows(record_count: u64) -> DataFile {
        DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(format!("test/{record_count}.parquet"))
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(record_count)
            .partition(Struct::from_iter([Some(Literal::long(0))]))
            .partition_spec_id(0)
            .build()
            .unwrap()
    }

    /// Builds a catalog with two snapshots (s1, s2) where `main` is at s2 and branch `b` at s1.
    /// The returned table is the current catalog state and holds both snapshots.
    async fn seed() -> (impl Catalog, Table, i64, i64) {
        let catalog = new_memory_catalog().await;
        let t0 = make_v3_minimal_table_in_catalog(&catalog).await;

        let tx = Transaction::new(&t0);
        let t1 = tx
            .fast_append()
            .add_data_files(vec![file_with_rows(10)])
            .apply(tx)
            .unwrap()
            .commit(&catalog)
            .await
            .unwrap();
        let s1 = t1.metadata().current_snapshot().unwrap().snapshot_id();

        // Pin branch `b` at s1 before main advances.
        let tx = Transaction::new(&t1);
        let t1b = tx
            .manage_snapshots()
            .create_branch("b", s1)
            .apply(tx)
            .unwrap()
            .commit(&catalog)
            .await
            .unwrap();

        let tx = Transaction::new(&t1b);
        let t2 = tx
            .fast_append()
            .add_data_files(vec![file_with_rows(20)])
            .apply(tx)
            .unwrap()
            .commit(&catalog)
            .await
            .unwrap();
        let s2 = t2.metadata().current_snapshot().unwrap().snapshot_id();

        (catalog, t2, s1, s2)
    }

    /// Builds a `TableCommit` (updates + requirements) from an action against `table`'s metadata.
    async fn run_action(table: &Table, action: ManageSnapshotsAction) -> TableCommit {
        let mut ac = Arc::new(action).commit(table).await.unwrap();
        TableCommit::builder()
            .ident(table.identifier().to_owned())
            .updates(ac.take_updates())
            .requirements(ac.take_requirements())
            .build()
    }

    // The `b == s1` requirement is rejected once another writer moves `b`.
    #[tokio::test]
    async fn test_e2e_conflict_on_concurrent_ref_move() {
        let (catalog, table, _s1, s2) = seed().await;
        let commit = run_action(&table, ManageSnapshotsAction::new().replace_branch("b", s2)).await;

        let tx = Transaction::new(&table);
        tx.manage_snapshots()
            .replace_branch("b", s2)
            .apply(tx)
            .unwrap()
            .commit(&catalog)
            .await
            .unwrap();

        let err = catalog.update_table(commit).await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::CatalogCommitConflicts);
    }

    // The "`t` must not exist" requirement is rejected once another writer creates `t`.
    #[tokio::test]
    async fn test_e2e_conflict_on_concurrent_tag_create() {
        let (catalog, table, s1, _s2) = seed().await;
        let commit = run_action(&table, ManageSnapshotsAction::new().create_tag("t", s1)).await;

        let tx = Transaction::new(&table);
        tx.manage_snapshots()
            .create_tag("t", s1)
            .apply(tx)
            .unwrap()
            .commit(&catalog)
            .await
            .unwrap();

        let err = catalog.update_table(commit).await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::CatalogCommitConflicts);
    }

    // A commit whose requirements still hold applies and persists.
    #[tokio::test]
    async fn test_e2e_commit_applies_through_catalog() {
        let (catalog, table, s1, s2) = seed().await;
        let commit = run_action(
            &table,
            ManageSnapshotsAction::new()
                .replace_branch("b", s2)
                .create_tag("t", s1),
        )
        .await;

        let updated = catalog.update_table(commit).await.unwrap();
        let refs = updated.metadata().refs();
        assert_eq!(refs.get("b").unwrap().snapshot_id, s2);
        assert_eq!(refs.get("t").unwrap().snapshot_id, s1);
        assert_eq!(refs.get(MAIN_BRANCH).unwrap().snapshot_id, s2);
    }
}
