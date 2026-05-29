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
use std::sync::Arc;

use async_trait::async_trait;

use crate::spec::TableMetadata;
use crate::table::Table;
use crate::transaction::action::{ActionCommit, TransactionAction};
use crate::{Error, ErrorKind, Result, TableRequirement, TableUpdate};

/// Default number of most recent snapshots to retain when none is specified.
const DEFAULT_RETAIN_LAST: usize = 1;

/// A transaction action that removes snapshots from table metadata.
///
/// This only rewrites metadata; the now-unreferenced data and metadata files are left untouched.
/// Physical file cleanup is the responsibility of a higher-level maintenance operation built on
/// top of this action.
///
/// Selection follows Java `RemoveSnapshots`, with a simpler retention model:
/// - Explicit ids ([`expire_snapshot_ids`](Self::expire_snapshot_ids)) and age-based expiry
///   ([`expire_older_than_ms`](Self::expire_older_than_ms)) are combined: a snapshot is expired
///   if it is named explicitly *or* it is older than the cutoff.
/// - [`retain_last`](Self::retain_last) keeps the most recent snapshots even when they are older
///   than the cutoff; it does not protect snapshots named explicitly.
/// - Snapshots referenced by a branch or tag (including the current snapshot) are never expired,
///   and naming such an id explicitly is an error, since
///   [`remove_snapshots`](crate::spec::TableMetadataBuilder::remove_snapshots) would otherwise
///   drop the ref silently.
///
/// Per-ref retention windows (`max_snapshot_age_ms`, `max_ref_age_ms`, per-branch
/// `min_snapshots_to_keep`) are not yet implemented.
pub struct ExpireSnapshotsAction {
    snapshot_ids: Vec<i64>,
    older_than_ms: Option<i64>,
    retain_last: Option<usize>,
}

impl ExpireSnapshotsAction {
    pub(crate) fn new() -> Self {
        Self {
            snapshot_ids: vec![],
            older_than_ms: None,
            retain_last: None,
        }
    }

    /// Expire these snapshot ids in addition to any age-based selection.
    ///
    /// An id that is still referenced by a branch or tag cannot be expired and causes
    /// [`commit`](TransactionAction::commit) to fail.
    pub fn expire_snapshot_ids(mut self, snapshot_ids: Vec<i64>) -> Self {
        self.snapshot_ids = snapshot_ids;
        self
    }

    /// Expire snapshots whose timestamp is strictly older than `older_than_ms`.
    pub fn expire_older_than_ms(mut self, older_than_ms: i64) -> Self {
        self.older_than_ms = Some(older_than_ms);
        self
    }

    /// Keep at least the `retain_last` most recent snapshots when expiring by age (defaults to 1).
    ///
    /// This only bounds [`expire_older_than_ms`](Self::expire_older_than_ms); it has no effect on
    /// its own and does not protect snapshots named via
    /// [`expire_snapshot_ids`](Self::expire_snapshot_ids).
    pub fn retain_last(mut self, retain_last: usize) -> Self {
        self.retain_last = Some(retain_last);
        self
    }

    fn snapshot_ids_to_expire(&self, table: &Table) -> Result<Vec<i64>> {
        let metadata = table.metadata();
        let protected = Self::protected_snapshot_ids(metadata);
        let existing: HashSet<i64> = metadata.snapshots().map(|s| s.snapshot_id()).collect();

        let mut to_expire: HashSet<i64> = HashSet::new();

        // Explicit ids are expired regardless of age, but one still referenced by a branch or tag
        // cannot be expired (Java's RemoveSnapshots errors rather than silently dropping the ref).
        for id in &self.snapshot_ids {
            if protected.contains(id) {
                return Err(Self::reference_error(metadata, *id));
            }
            if existing.contains(id) {
                to_expire.insert(*id);
            }
        }

        // Age-based expiry, additive with the explicit ids. `retain_last` keeps the most recent
        // snapshots even when older than the cutoff. Without a cutoff there is no age expiry.
        if let Some(older_than_ms) = self.older_than_ms {
            let retain_last = self.retain_last.unwrap_or(DEFAULT_RETAIN_LAST);
            let mut snapshots: Vec<_> = metadata.snapshots().cloned().collect();
            if snapshots.len() > retain_last {
                snapshots.sort_by_key(|s| s.timestamp_ms());
                snapshots.truncate(snapshots.len() - retain_last);
                for snapshot in snapshots {
                    if snapshot.timestamp_ms() < older_than_ms
                        && !protected.contains(&snapshot.snapshot_id())
                    {
                        to_expire.insert(snapshot.snapshot_id());
                    }
                }
            }
        }

        let mut snapshot_ids: Vec<i64> = to_expire.into_iter().collect();
        snapshot_ids.sort_unstable();
        Ok(snapshot_ids)
    }

    /// Snapshot ids that must never be expired: the current snapshot and every branch head and tag
    /// target. Removing any of these would silently drop a ref in
    /// [`remove_snapshots`](crate::spec::TableMetadataBuilder::remove_snapshots).
    fn protected_snapshot_ids(metadata: &TableMetadata) -> HashSet<i64> {
        let mut ids: HashSet<i64> = metadata.refs.values().map(|r| r.snapshot_id).collect();
        if let Some(current) = metadata.current_snapshot_id() {
            ids.insert(current);
        }
        ids
    }

    fn reference_error(metadata: &TableMetadata, snapshot_id: i64) -> Error {
        if metadata.current_snapshot_id() == Some(snapshot_id) {
            return Error::new(ErrorKind::DataInvalid, "Cannot expire the current snapshot");
        }
        let refs: Vec<&str> = metadata
            .refs
            .iter()
            .filter(|(_, r)| r.snapshot_id == snapshot_id)
            .map(|(name, _)| name.as_str())
            .collect();
        Error::new(
            ErrorKind::DataInvalid,
            format!("Cannot expire snapshot {snapshot_id}: still referenced by {refs:?}"),
        )
    }
}

#[async_trait]
impl TransactionAction for ExpireSnapshotsAction {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        let snapshot_ids = self.snapshot_ids_to_expire(table)?;

        if snapshot_ids.is_empty() {
            return Ok(ActionCommit::new(vec![], vec![]));
        }

        Ok(ActionCommit::new(
            vec![TableUpdate::RemoveSnapshots { snapshot_ids }],
            vec![TableRequirement::UuidMatch {
                uuid: table.metadata().uuid(),
            }],
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::TableUpdate;
    use crate::transaction::Transaction;
    use crate::transaction::action::{ApplyTransactionAction, TransactionAction};
    use crate::transaction::expire_snapshots::ExpireSnapshotsAction;
    use crate::transaction::tests::make_v2_table;

    // `make_v2_table` carries an older snapshot (ts 1515100955770) and a current
    // snapshot (ts 1555100955770).
    const OLD_SNAPSHOT: i64 = 3051729675574597004;
    const CURRENT_SNAPSHOT: i64 = 3055729675574597004;

    async fn removed_ids(action: ExpireSnapshotsAction) -> Vec<i64> {
        let table = make_v2_table();
        let mut commit = Arc::new(action).commit(&table).await.unwrap();
        match commit.take_updates().into_iter().next() {
            Some(TableUpdate::RemoveSnapshots { snapshot_ids }) => snapshot_ids,
            None => vec![],
            other => panic!("unexpected update: {other:?}"),
        }
    }

    fn action() -> ExpireSnapshotsAction {
        ExpireSnapshotsAction::new()
    }

    #[tokio::test]
    async fn test_expire_explicit_snapshot_id() {
        assert_eq!(
            removed_ids(action().expire_snapshot_ids(vec![OLD_SNAPSHOT])).await,
            vec![OLD_SNAPSHOT]
        );
    }

    #[tokio::test]
    async fn test_explicit_unknown_id_is_ignored() {
        assert!(
            removed_ids(action().expire_snapshot_ids(vec![42]))
                .await
                .is_empty()
        );
    }

    #[tokio::test]
    async fn test_cannot_expire_current_snapshot() {
        let table = make_v2_table();
        let action = action().expire_snapshot_ids(vec![CURRENT_SNAPSHOT]);
        assert!(Arc::new(action).commit(&table).await.is_err());
    }

    /// `make_v2_table` with a tag pointing at the older snapshot.
    fn table_with_tag_on_old() -> crate::table::Table {
        use crate::spec::{SnapshotReference, SnapshotRetention};

        let table = make_v2_table();
        let metadata = table
            .metadata()
            .clone()
            .into_builder(None)
            .set_ref("history-tag", SnapshotReference {
                snapshot_id: OLD_SNAPSHOT,
                retention: SnapshotRetention::Tag {
                    max_ref_age_ms: None,
                },
            })
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        table.with_metadata(Arc::new(metadata))
    }

    #[tokio::test]
    async fn test_cannot_expire_tagged_snapshot_explicitly() {
        let table = table_with_tag_on_old();
        let action = action().expire_snapshot_ids(vec![OLD_SNAPSHOT]);
        assert!(Arc::new(action).commit(&table).await.is_err());
    }

    #[tokio::test]
    async fn test_age_expiry_skips_tagged_snapshot() {
        let table = table_with_tag_on_old();
        let mut commit = Arc::new(action().expire_older_than_ms(i64::MAX))
            .commit(&table)
            .await
            .unwrap();
        // Both snapshots are referenced (current + tag), so nothing is expired.
        assert!(commit.take_updates().is_empty());
    }

    #[tokio::test]
    async fn test_retain_last_default_expires_older_non_current() {
        assert_eq!(
            removed_ids(action().expire_older_than_ms(i64::MAX)).await,
            vec![OLD_SNAPSHOT]
        );
    }

    #[tokio::test]
    async fn test_retain_last_noop_when_enough_retained() {
        assert!(removed_ids(action().retain_last(5)).await.is_empty());
    }

    #[tokio::test]
    async fn test_older_than_excludes_newer_snapshots() {
        // Threshold older than every snapshot -> nothing qualifies.
        assert!(
            removed_ids(action().expire_older_than_ms(1))
                .await
                .is_empty()
        );
    }

    #[tokio::test]
    async fn test_apply_registers_action() {
        let table = make_v2_table();
        let tx = Transaction::new(&table);
        let tx = tx
            .expire_snapshots()
            .expire_snapshot_ids(vec![OLD_SNAPSHOT])
            .apply(tx)
            .unwrap();
        assert_eq!(tx.actions.len(), 1);
    }
}
