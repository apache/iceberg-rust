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

use arrow_array::builder::{BooleanBuilder, PrimitiveBuilder};
use arrow_array::types::{Int64Type, TimestampMillisecondType};
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use futures::{stream, StreamExt};

use crate::scan::ArrowRecordBatchStream;
use crate::spec::{Snapshot, TableMetadata};
use crate::table::Table;
use crate::Result;

/// History table.
///
/// Shows how the table's current snapshot has changed over time and when each
/// snapshot became the current snapshot.
///
/// Unlike the [Snapshots][SnapshotsTable], this metadata table has less detail
/// per snapshot but includes ancestry information of the current snapshot.
///
/// `is_current_ancestor` indicates whether the snapshot is an ancestor of the
/// current snapshot. If `false`, then the snapshot was rolled back.
pub struct HistoryTable<'a> {
    table: &'a Table,
}

impl<'a> HistoryTable<'a> {
    /// Create a new History table instance.
    pub fn new(table: &'a Table) -> Self {
        Self { table }
    }

    /// Return the schema of the history table.
    pub fn schema(&self) -> Schema {
        Schema::new(vec![
            Field::new(
                "made_current_at",
                DataType::Timestamp(TimeUnit::Millisecond, Some("+00:00".into())),
                false,
            ),
            Field::new("snapshot_id", DataType::Int64, false),
            Field::new("parent_id", DataType::Int64, true),
            Field::new("is_current_ancestor", DataType::Boolean, false),
        ])
    }

    /// Scan the history table.
    pub async fn scan(&self) -> Result<ArrowRecordBatchStream> {
        let table_metadata = self.table.metadata();
        let ancestors_by_snapshot_id: HashSet<i64> =
            SnapshotAncestors::from_current_snapshot(table_metadata)
                .map(|snapshot| snapshot.snapshot_id())
                .collect();

        let mut made_current_at =
            PrimitiveBuilder::<TimestampMillisecondType>::new().with_timezone("+00:00");
        let mut snapshot_id = PrimitiveBuilder::<Int64Type>::new();
        let mut parent_id = PrimitiveBuilder::<Int64Type>::new();
        let mut is_current_ancestor = BooleanBuilder::new();

        for snapshot in table_metadata.snapshots() {
            made_current_at.append_value(snapshot.timestamp_ms());
            snapshot_id.append_value(snapshot.snapshot_id());
            parent_id.append_option(snapshot.parent_snapshot_id());
            is_current_ancestor
                .append_value(ancestors_by_snapshot_id.contains(&snapshot.snapshot_id()));
        }

        let batch = RecordBatch::try_new(Arc::new(Self::schema(self)), vec![
            Arc::new(made_current_at.finish()),
            Arc::new(snapshot_id.finish()),
            Arc::new(parent_id.finish()),
            Arc::new(is_current_ancestor.finish()),
        ])?;

        Ok(stream::iter(vec![Ok(batch)]).boxed())
    }
}

/// Utility to iterate parent-by-parent over the ancestors of a snapshot.
struct SnapshotAncestors<'a> {
    table_metadata: &'a TableMetadata,
    snapshot: Option<&'a Snapshot>,
}

impl<'a> SnapshotAncestors<'a> {
    fn from_current_snapshot(table_metadata: &'a TableMetadata) -> Self {
        SnapshotAncestors {
            table_metadata,
            snapshot: table_metadata.current_snapshot().map(|s| s.as_ref()),
        }
    }
}

impl<'a> Iterator for SnapshotAncestors<'a> {
    type Item = &'a Snapshot;

    /// Return the current `snapshot` and move this iterator to the parent snapshot.
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(snapshot) = self.snapshot {
            let parent = match snapshot.parent_snapshot_id() {
                Some(parent_snapshot_id) => self
                    .table_metadata
                    .snapshot_by_id(parent_snapshot_id)
                    .map(|s| s.as_ref()),
                None => None,
            };
            self.snapshot = parent;
            Some(snapshot)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use expect_test::expect;

    use crate::inspect::metadata_table::tests::check_record_batches;
    use crate::scan::tests::TableTestFixture;

    #[tokio::test]
    async fn test_history_table() {
        let table = TableTestFixture::new().table;

        let batch_stream = table.inspect().history().scan().await.unwrap();

        check_record_batches(
            batch_stream,
            expect![[r#"
                Field { name: "made_current_at", data_type: Timestamp(Millisecond, Some("+00:00")), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} },
                Field { name: "snapshot_id", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} },
                Field { name: "parent_id", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} },
                Field { name: "is_current_ancestor", data_type: Boolean, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }"#]],
            expect![[r#"
                made_current_at: PrimitiveArray<Timestamp(Millisecond, Some("+00:00"))>
                [
                  2018-01-04T21:22:35.770+00:00,
                  2019-04-12T20:29:15.770+00:00,
                ],
                snapshot_id: PrimitiveArray<Int64>
                [
                  3051729675574597004,
                  3055729675574597004,
                ],
                parent_id: PrimitiveArray<Int64>
                [
                  null,
                  3051729675574597004,
                ],
                is_current_ancestor: BooleanArray
                [
                  true,
                  true,
                ]"#]],
            &[],
            Some("made_current_at"),
        ).await;
    }
}
