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
        let result = snapshot.clone();
        self.next = snapshot
            .parent_snapshot_id()
            .and_then(|id| (self.get_snapshot)(id));
        Some(result)
    }
}

/// Iterate starting from `snapshot` (inclusive) to the root snapshot.
pub fn ancestors_of(
    table_metadata: &TableMetadataRef,
    snapshot: i64,
) -> Box<dyn Iterator<Item = SnapshotRef> + Send> {
    if let Some(snapshot) = table_metadata.snapshot_by_id(snapshot) {
        let table_metadata = table_metadata.clone();
        Box::new(Ancestors {
            next: Some(snapshot.clone()),
            get_snapshot: Box::new(move |id| table_metadata.snapshot_by_id(id).cloned()),
        })
    } else {
        Box::new(std::iter::empty())
    }
}

/// Iterate starting from `snapshot` (inclusive) to `oldest_snapshot_id` (exclusive).
pub fn ancestors_between(
    table_metadata: &TableMetadataRef,
    latest_snapshot_id: i64,
    oldest_snapshot_id: Option<i64>,
) -> Box<dyn Iterator<Item = SnapshotRef> + Send> {
    let Some(oldest_snapshot_id) = oldest_snapshot_id else {
        return Box::new(ancestors_of(table_metadata, latest_snapshot_id));
    };

    if latest_snapshot_id == oldest_snapshot_id {
        return Box::new(std::iter::empty());
    }

    Box::new(
        ancestors_of(table_metadata, latest_snapshot_id)
            .take_while(move |snapshot| snapshot.snapshot_id() != oldest_snapshot_id),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scan::tests::TableTestFixture;

    // The TableTestFixture has two snapshots:
    //   root:  id = 3051729675574597004, no parent
    //   child: id = 3055729675574597004, parent = root
    const ROOT_ID: i64 = 3051729675574597004;
    const CHILD_ID: i64 = 3055729675574597004;

    fn metadata() -> TableMetadataRef {
        let fixture = TableTestFixture::new();
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
        let ids: Vec<i64> = ancestors_of(&meta, ROOT_ID)
            .map(|s| s.snapshot_id())
            .collect();
        assert_eq!(ids, vec![ROOT_ID]);
    }

    #[test]
    fn test_ancestors_of_child_returns_chain_newest_first() {
        let meta = metadata();
        let ids: Vec<i64> = ancestors_of(&meta, CHILD_ID)
            .map(|s| s.snapshot_id())
            .collect();
        assert_eq!(ids, vec![CHILD_ID, ROOT_ID]);
    }

    // --- ancestors_between ---

    #[test]
    fn test_ancestors_between_same_id_returns_empty() {
        let meta = metadata();
        let ids: Vec<i64> = ancestors_between(&meta, CHILD_ID, Some(CHILD_ID))
            .map(|s| s.snapshot_id())
            .collect();
        assert!(ids.is_empty());
    }

    #[test]
    fn test_ancestors_between_no_oldest_returns_all_ancestors() {
        let meta = metadata();
        let ids: Vec<i64> = ancestors_between(&meta, CHILD_ID, None)
            .map(|s| s.snapshot_id())
            .collect();
        assert_eq!(ids, vec![CHILD_ID, ROOT_ID]);
    }

    #[test]
    fn test_ancestors_between_excludes_oldest_snapshot() {
        let meta = metadata();
        // From child up to (but not including) root.
        let ids: Vec<i64> = ancestors_between(&meta, CHILD_ID, Some(ROOT_ID))
            .map(|s| s.snapshot_id())
            .collect();
        assert_eq!(ids, vec![CHILD_ID]);
    }
}
