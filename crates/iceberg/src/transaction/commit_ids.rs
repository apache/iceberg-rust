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

use std::sync::OnceLock;

use uuid::Uuid;

use crate::table::Table;
use crate::transaction::snapshot::SnapshotProducer;

/// Snapshot id and commit UUID held behind `OnceLock`s so they survive OCC retries.
///
/// Each merging action holds one `CommitIds`. The first `commit()` call generates
/// both values; subsequent retries observe the same pair, which is the contract the
/// catalog requires for idempotent retries (matching path planning, manifest list
/// filenames, and snapshot identity across attempts).
pub(crate) struct CommitIds {
    snapshot_id: OnceLock<i64>,
    commit_uuid: OnceLock<Uuid>,
}

impl CommitIds {
    pub(crate) fn new() -> Self {
        Self {
            snapshot_id: OnceLock::new(),
            commit_uuid: OnceLock::new(),
        }
    }

    pub(crate) fn snapshot_id(&self, table: &Table) -> i64 {
        *self
            .snapshot_id
            .get_or_init(|| SnapshotProducer::generate_unique_snapshot_id(table))
    }

    pub(crate) fn commit_uuid(&self) -> Uuid {
        *self.commit_uuid.get_or_init(Uuid::now_v7)
    }
}

#[cfg(test)]
mod tests {
    use super::CommitIds;
    use crate::transaction::tests::make_v2_minimal_table;

    #[test]
    fn snapshot_id_is_idempotent() {
        let table = make_v2_minimal_table();
        let ids = CommitIds::new();
        assert_eq!(ids.snapshot_id(&table), ids.snapshot_id(&table));
    }

    #[test]
    fn commit_uuid_is_idempotent() {
        let ids = CommitIds::new();
        assert_eq!(ids.commit_uuid(), ids.commit_uuid());
    }
}
