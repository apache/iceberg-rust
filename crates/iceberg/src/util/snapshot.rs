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
