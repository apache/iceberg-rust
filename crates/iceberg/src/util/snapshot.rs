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

use crate::spec::{SnapshotRef, TableMetadataRef};

struct Ancestors {
    next: Option<SnapshotRef>,
    get_snapshot: Box<dyn Fn(i64) -> Option<SnapshotRef> + Send>,
}

impl Iterator for Ancestors {
    type Item = SnapshotRef;

    fn next(&mut self) -> Option<Self::Item> {
        let snapshot = self.next.take()?;
        self.next = snapshot
            .parent_snapshot_id()
            .and_then(|id| (self.get_snapshot)(id));
        Some(snapshot)
    }
}

/// Iterate starting from `snapshot_id` (inclusive) to the root snapshot.
pub fn ancestors_of(
    table_metadata: &TableMetadataRef,
    snapshot_id: i64,
) -> impl Iterator<Item = SnapshotRef> + Send {
    let initial = table_metadata.snapshot_by_id(snapshot_id).cloned();
    let table_metadata = table_metadata.clone();
    Ancestors {
        next: initial,
        get_snapshot: Box::new(move |id| table_metadata.snapshot_by_id(id).cloned()),
    }
}

/// Iterate starting from `latest_snapshot_id` (inclusive) to `oldest_snapshot_id` (exclusive).
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scan::tests::TableTestFixture;

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
