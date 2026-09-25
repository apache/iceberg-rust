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

use crate::spec::{SnapshotRef, TableMetadataRef};
use crate::{Error, ErrorKind, Result};

struct Ancestors {
    next: Option<SnapshotRef>,
    get_snapshot: Box<dyn Fn(i64) -> Option<SnapshotRef> + Send>,
    visited: HashSet<i64>,
}

impl Iterator for Ancestors {
    type Item = SnapshotRef;

    fn next(&mut self) -> Option<Self::Item> {
        let snapshot = self.next.take()?;
        // corrupt metadata with a parent cycle must not hang the traversal
        if !self.visited.insert(snapshot.snapshot_id()) {
            return None;
        }
        self.next = snapshot
            .parent_snapshot_id()
            .and_then(|id| (self.get_snapshot)(id));
        Some(snapshot)
    }
}

/// Iterate starting from `snapshot_id` (inclusive) to the root snapshot.
///
/// The iterator visits each snapshot at most once, so it terminates even on
/// corrupt metadata whose parent pointers form a cycle.
pub fn ancestors_of(
    table_metadata: &TableMetadataRef,
    snapshot_id: i64,
) -> impl Iterator<Item = SnapshotRef> + Send {
    let initial = table_metadata.snapshot_by_id(snapshot_id).cloned();
    let table_metadata = table_metadata.clone();
    Ancestors {
        next: initial,
        get_snapshot: Box::new(move |id| table_metadata.snapshot_by_id(id).cloned()),
        visited: HashSet::new(),
    }
}

/// Iterate starting from `latest_snapshot_id` (inclusive) to `oldest_snapshot_id` (exclusive).
///
/// Note: if `oldest_snapshot_id` is `Some(id)` but `id` is not actually an
/// ancestor of `latest_snapshot_id`, the walk is never stopped and this yields
/// *all* ancestors of `latest_snapshot_id` down to the root. Callers that treat
/// `oldest_snapshot_id` as a lower bound must validate the lineage themselves.
pub fn ancestors_between(
    table_metadata: &TableMetadataRef,
    latest_snapshot_id: i64,
    oldest_snapshot_id: Option<i64>,
) -> impl Iterator<Item = SnapshotRef> + Send {
    ancestors_of(table_metadata, latest_snapshot_id).take_while(move |snapshot| {
        oldest_snapshot_id
            .map(|id| snapshot.snapshot_id() != id)
            .unwrap_or(true)
    })
}

/// Resolve the snapshot ID from the latest main-history entry at or before
/// `timestamp_ms` (milliseconds since the Unix epoch).
///
/// Equal timestamps select the first entry. Returns [`ErrorKind::DataInvalid`]
/// if no matching history exists. The returned snapshot may have expired, so
/// [`snapshot_by_id`](crate::spec::TableMetadata::snapshot_by_id) can still return `None`.
pub fn snapshot_id_as_of_time(table_metadata: &TableMetadataRef, timestamp_ms: i64) -> Result<i64> {
    let best = table_metadata
        .history()
        .iter()
        .filter(|entry| entry.timestamp_ms() <= timestamp_ms)
        .reduce(|best, entry| {
            // Keep the first entry on ties, matching Java's timestamp selection.
            if entry.timestamp_ms() > best.timestamp_ms() {
                entry
            } else {
                best
            }
        });
    best.map(|entry| entry.snapshot_id).ok_or_else(|| {
        Error::new(
            ErrorKind::DataInvalid,
            format!("No snapshot history at or before timestamp {timestamp_ms} ms"),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scan::tests::TableTestFixture;
    use crate::spec::SnapshotLog;

    // Five snapshots chained as: S1 (root) -> S2 -> S3 -> S4 -> S5 (current)
    const S1: i64 = 3051729675574597004;
    const S2: i64 = 3055729675574597004;
    const S3: i64 = 3056729675574597004;
    const S4: i64 = 3057729675574597004;
    const S5: i64 = 3059729675574597004;

    fn metadata() -> TableMetadataRef {
        let fixture = TableTestFixture::new_with_deep_history();
        std::sync::Arc::new(fixture.table.metadata().clone())
    }

    type History = [(i64, i64)];

    fn metadata_with_history(history: &History) -> TableMetadataRef {
        let mut metadata = metadata().as_ref().clone();
        metadata.snapshot_log = history
            .iter()
            .map(|&(timestamp_ms, snapshot_id)| SnapshotLog {
                timestamp_ms,
                snapshot_id,
            })
            .collect();
        metadata.into()
    }

    #[test]
    fn test_snapshot_id_as_of_time() {
        let cases: &[(&History, i64, i64)] = &[
            (&[(1000, S1), (2000, S2)], 1000, S1),
            (&[(1000, S1), (2000, S2)], 1500, S1),
            (&[(1000, S1), (2000, S2)], 2000, S2),
            (&[(1000, S1), (2000, S2)], 3000, S2),
            // Rollback records when an existing snapshot becomes current again.
            (&[(1000, S1), (2000, S2), (3000, S1)], 3500, S1),
            // The first equal maximum wins; max_by_key would pick S3.
            (&[(1000, S1), (2000, S2), (2000, S3)], 2000, S2),
            // Clock skew means the log need not be sorted by timestamp.
            (&[(1000, S1), (3000, S2), (2500, S3)], 3500, S2),
            (&[(1000, S1), (3000, S2), (2500, S3)], 2600, S3),
            (&[(-2000, S1), (-1000, S2)], -1500, S1),
            (&[(i64::MIN, S1), (0, S2)], i64::MIN, S1),
            (&[(0, S1), (i64::MAX, S2)], i64::MAX, S2),
        ];
        for &(history, timestamp_ms, expected) in cases {
            let metadata = metadata_with_history(history);
            assert_eq!(
                snapshot_id_as_of_time(&metadata, timestamp_ms).unwrap(),
                expected,
                "timestamp {timestamp_ms}, history {history:?}"
            );
        }
    }

    #[test]
    fn test_snapshot_id_as_of_time_without_history_at_timestamp() {
        let cases: &[(&History, i64)] = &[
            (&[], 1000),
            (&[(1000, S1)], 999),
            (&[(3000, S1)], 1500), // Expired prefix; S1 is still retained.
        ];
        for &(history, timestamp_ms) in cases {
            let metadata = metadata_with_history(history);
            assert!(metadata.snapshot_by_id(S1).is_some());
            let err = snapshot_id_as_of_time(&metadata, timestamp_ms).unwrap_err();
            assert_eq!(err.kind(), ErrorKind::DataInvalid);
            assert!(err.message().contains(&timestamp_ms.to_string()));
        }
    }

    #[test]
    fn test_snapshot_id_as_of_time_ignores_snapshots_outside_main_history() {
        let mut metadata = metadata().as_ref().clone();
        metadata.snapshot_log.truncate(2);
        // Newer retained snapshots may be staged or belong only to another branch.
        assert!(metadata.snapshot_by_id(S5).is_some());
        assert_eq!(
            snapshot_id_as_of_time(&metadata.into(), i64::MAX).unwrap(),
            S2
        );
    }

    // --- ancestors_of ---

    #[test]
    fn test_ancestors_of_nonexistent_snapshot_returns_empty() {
        let meta = metadata();
        let ids: Vec<i64> = ancestors_of(&meta, 999).map(|s| s.snapshot_id()).collect();
        assert!(ids.is_empty());
    }

    #[test]
    fn test_ancestors_of_root_returns_only_root() {
        let meta = metadata();
        let ids: Vec<i64> = ancestors_of(&meta, S1).map(|s| s.snapshot_id()).collect();
        assert_eq!(ids, vec![S1]);
    }

    #[test]
    fn test_ancestors_of_leaf_returns_full_chain() {
        let meta = metadata();
        let ids: Vec<i64> = ancestors_of(&meta, S5).map(|s| s.snapshot_id()).collect();
        assert_eq!(ids, vec![S5, S4, S3, S2, S1]);
    }

    #[test]
    fn test_ancestors_of_mid_chain_returns_partial_chain() {
        let meta = metadata();
        let ids: Vec<i64> = ancestors_of(&meta, S3).map(|s| s.snapshot_id()).collect();
        assert_eq!(ids, vec![S3, S2, S1]);
    }

    #[test]
    fn test_ancestors_of_second_snapshot() {
        let meta = metadata();
        let ids: Vec<i64> = ancestors_of(&meta, S2).map(|s| s.snapshot_id()).collect();
        assert_eq!(ids, vec![S2, S1]);
    }

    // --- ancestors_between ---

    #[test]
    fn test_ancestors_between_same_id_returns_empty() {
        let meta = metadata();
        let ids: Vec<i64> = ancestors_between(&meta, S3, Some(S3))
            .map(|s| s.snapshot_id())
            .collect();
        assert!(ids.is_empty());
    }

    #[test]
    fn test_ancestors_between_no_oldest_returns_all_ancestors() {
        let meta = metadata();
        let ids: Vec<i64> = ancestors_between(&meta, S5, None)
            .map(|s| s.snapshot_id())
            .collect();
        assert_eq!(ids, vec![S5, S4, S3, S2, S1]);
    }

    #[test]
    fn test_ancestors_between_excludes_oldest_snapshot() {
        let meta = metadata();
        // S5 down to (but not including) S2
        let ids: Vec<i64> = ancestors_between(&meta, S5, Some(S2))
            .map(|s| s.snapshot_id())
            .collect();
        assert_eq!(ids, vec![S5, S4, S3]);
    }

    #[test]
    fn test_ancestors_between_adjacent_snapshots() {
        let meta = metadata();
        // S3 down to (but not including) S2 — only S3 itself
        let ids: Vec<i64> = ancestors_between(&meta, S3, Some(S2))
            .map(|s| s.snapshot_id())
            .collect();
        assert_eq!(ids, vec![S3]);
    }

    #[test]
    fn test_ancestors_between_leaf_and_root() {
        let meta = metadata();
        // S5 down to (but not including) S1
        let ids: Vec<i64> = ancestors_between(&meta, S5, Some(S1))
            .map(|s| s.snapshot_id())
            .collect();
        assert_eq!(ids, vec![S5, S4, S3, S2]);
    }

    #[test]
    fn test_ancestors_between_nonexistent_oldest_returns_full_chain() {
        let meta = metadata();
        // oldest_snapshot_id doesn't exist in the chain, so take_while never stops
        let ids: Vec<i64> = ancestors_between(&meta, S5, Some(999))
            .map(|s| s.snapshot_id())
            .collect();
        assert_eq!(ids, vec![S5, S4, S3, S2, S1]);
    }

    #[test]
    fn test_ancestors_between_nonexistent_latest_returns_empty() {
        let meta = metadata();
        let ids: Vec<i64> = ancestors_between(&meta, 999, Some(S1))
            .map(|s| s.snapshot_id())
            .collect();
        assert!(ids.is_empty());
    }
}
