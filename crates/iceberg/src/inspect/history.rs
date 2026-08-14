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

//! History table: the chronological log of every snapshot that was ever the
//! table's current snapshot.
//!
//! Each row is one entry of the metadata snapshot-log and answers two questions
//! about the table's lineage: *when* a snapshot became current, and *whether*
//! it is still an ancestor of the current state. Snapshots that were rolled back
//! remain visible in the log yet are flagged `is_current_ancestor = false`, so
//! the live lineage stays distinguishable from abandoned history.
//!
//! References:
//! - <https://github.com/apache/iceberg/blob/ac865e334e143dfd9e33011d8cf710b46d91f1e5/core/src/main/java/org/apache/iceberg/HistoryTable.java#L50-L54>

use std::collections::HashSet;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_array::builder::{BooleanBuilder, PrimitiveBuilder};
use arrow_array::types::{Int64Type, TimestampMicrosecondType};
use futures::{StreamExt, stream};

use crate::Result;
use crate::arrow::{UTC_TIME_ZONE, schema_to_arrow_schema};
use crate::scan::ArrowRecordBatchStream;
use crate::spec::{NestedField, PrimitiveType, Type};
use crate::table::Table;
use crate::util::snapshot::ancestors_of;

/// History table.
pub struct HistoryTable<'a> {
    table: &'a Table,
}

impl<'a> HistoryTable<'a> {
    /// Create a new History table instance.
    pub fn new(table: &'a Table) -> Self {
        Self { table }
    }

    /// Returns the iceberg schema of the history table.
    pub fn schema(&self) -> crate::spec::Schema {
        let fields = vec![
            NestedField::required(
                1,
                "made_current_at",
                Type::Primitive(PrimitiveType::Timestamptz),
            ),
            NestedField::required(2, "snapshot_id", Type::Primitive(PrimitiveType::Long)),
            NestedField::optional(3, "parent_id", Type::Primitive(PrimitiveType::Long)),
            NestedField::required(
                4,
                "is_current_ancestor",
                Type::Primitive(PrimitiveType::Boolean),
            ),
        ];
        crate::spec::Schema::builder()
            .with_fields(fields.into_iter().map(|f| f.into()))
            .build()
            .unwrap()
    }

    /// Scans the history table.
    pub async fn scan(&self) -> Result<ArrowRecordBatchStream> {
        let schema = schema_to_arrow_schema(&self.schema())?;
        let metadata = self.table.metadata();

        // Walk the current snapshot's parent chain once: membership in this set
        // is what tells the live lineage apart from rolled-back history entries.
        let ancestors: HashSet<i64> = metadata
            .current_snapshot_id()
            .map(|id| {
                ancestors_of(&self.table.metadata_ref(), id)
                    .map(|snapshot| snapshot.snapshot_id())
                    .collect()
            })
            .unwrap_or_default();

        let mut made_current_at =
            PrimitiveBuilder::<TimestampMicrosecondType>::new().with_timezone(UTC_TIME_ZONE);
        let mut snapshot_id = PrimitiveBuilder::<Int64Type>::new();
        let mut parent_id = PrimitiveBuilder::<Int64Type>::new();
        let mut is_current_ancestor = BooleanBuilder::new();

        for entry in metadata.history() {
            made_current_at.append_value(entry.timestamp_ms.saturating_mul(1000)); // ms -> µs
            snapshot_id.append_value(entry.snapshot_id);
            parent_id.append_option(
                metadata
                    .snapshot_by_id(entry.snapshot_id)
                    .and_then(|snapshot| snapshot.parent_snapshot_id()),
            );
            is_current_ancestor.append_value(ancestors.contains(&entry.snapshot_id));
        }

        let batch = RecordBatch::try_new(Arc::new(schema), vec![
            Arc::new(made_current_at.finish()),
            Arc::new(snapshot_id.finish()),
            Arc::new(parent_id.finish()),
            Arc::new(is_current_ancestor.finish()),
        ])?;

        Ok(stream::iter(vec![Ok(batch)]).boxed())
    }
}

#[cfg(test)]
mod tests {
    use expect_test::expect;
    use futures::TryStreamExt;

    use crate::TableIdent;
    use crate::io::FileIO;
    use crate::scan::tests::TableTestFixture;
    use crate::spec::TableMetadata;
    use crate::table::Table;
    use crate::test_utils::{check_record_batches, test_runtime};

    fn table_from_metadata_json(metadata_json: &str) -> Table {
        let metadata = serde_json::from_str::<TableMetadata>(metadata_json).unwrap();
        Table::builder()
            .metadata(metadata)
            .identifier(TableIdent::from_strs(["db", "history"]).unwrap())
            .file_io(FileIO::new_with_fs())
            .metadata_location("s3://bucket/test/location/metadata/v3.json")
            .runtime(test_runtime())
            .build()
            .unwrap()
    }

    #[tokio::test]
    async fn test_history_table() {
        let table = TableTestFixture::new().table;

        let batch_stream = table.inspect().history().scan().await.unwrap();

        check_record_batches(
            batch_stream.try_collect::<Vec<_>>().await.unwrap(),
            expect![[r#"
                Field { "made_current_at": Timestamp(µs, "+00:00"), metadata: {"PARQUET:field_id": "1"} },
                Field { "snapshot_id": Int64, metadata: {"PARQUET:field_id": "2"} },
                Field { "parent_id": nullable Int64, metadata: {"PARQUET:field_id": "3"} },
                Field { "is_current_ancestor": Boolean, metadata: {"PARQUET:field_id": "4"} }"#]],
            expect![[r#"
                made_current_at: PrimitiveArray<Timestamp(µs, "+00:00")>
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
        );
    }

    /// A rolled-back snapshot (S2) stays in the log but drops out of the current
    /// lineage: the current snapshot (S3) descends from S1, so only S1 and S3 are
    /// current ancestors while S2 is flagged `false`. The log also keeps an entry
    /// for an expired snapshot (99, absent from `snapshots`) which renders with a
    /// null parent and `is_current_ancestor = false`, and a second entry for S1
    /// (a roll-forward makes a snapshot current again) which yields one row per
    /// log entry.
    #[tokio::test]
    async fn test_history_table_with_rolled_back_snapshot() {
        let metadata_json = r#"{
            "format-version": 2,
            "table-uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c1",
            "location": "s3://bucket/test/location",
            "last-sequence-number": 3,
            "last-updated-ms": 1600000000000,
            "last-column-id": 1,
            "current-schema-id": 0,
            "schemas": [
                {"type": "struct", "schema-id": 0, "fields": [{"id": 1, "name": "x", "required": true, "type": "long"}]}
            ],
            "default-spec-id": 0,
            "partition-specs": [{"spec-id": 0, "fields": []}],
            "last-partition-id": 999,
            "default-sort-order-id": 0,
            "sort-orders": [{"order-id": 0, "fields": []}],
            "properties": {},
            "current-snapshot-id": 3,
            "snapshots": [
                {
                    "snapshot-id": 1,
                    "timestamp-ms": 1515100955770,
                    "sequence-number": 1,
                    "summary": {"operation": "append"},
                    "manifest-list": "s3://bucket/metadata/snap-1.avro",
                    "schema-id": 0
                },
                {
                    "snapshot-id": 2,
                    "parent-snapshot-id": 1,
                    "timestamp-ms": 1555100955770,
                    "sequence-number": 2,
                    "summary": {"operation": "append"},
                    "manifest-list": "s3://bucket/metadata/snap-2.avro",
                    "schema-id": 0
                },
                {
                    "snapshot-id": 3,
                    "parent-snapshot-id": 1,
                    "timestamp-ms": 1600000000000,
                    "sequence-number": 3,
                    "summary": {"operation": "append"},
                    "manifest-list": "s3://bucket/metadata/snap-3.avro",
                    "schema-id": 0
                }
            ],
            "snapshot-log": [
                {"snapshot-id": 99, "timestamp-ms": 1500000000000},
                {"snapshot-id": 1, "timestamp-ms": 1515100955770},
                {"snapshot-id": 2, "timestamp-ms": 1555100955770},
                {"snapshot-id": 1, "timestamp-ms": 1580000000000},
                {"snapshot-id": 3, "timestamp-ms": 1600000000000}
            ],
            "metadata-log": [],
            "refs": {"main": {"snapshot-id": 3, "type": "branch"}}
        }"#;

        let table = table_from_metadata_json(metadata_json);

        let batch_stream = table.inspect().history().scan().await.unwrap();

        check_record_batches(
            batch_stream.try_collect::<Vec<_>>().await.unwrap(),
            expect![[r#"
                Field { "made_current_at": Timestamp(µs, "+00:00"), metadata: {"PARQUET:field_id": "1"} },
                Field { "snapshot_id": Int64, metadata: {"PARQUET:field_id": "2"} },
                Field { "parent_id": nullable Int64, metadata: {"PARQUET:field_id": "3"} },
                Field { "is_current_ancestor": Boolean, metadata: {"PARQUET:field_id": "4"} }"#]],
            expect![[r#"
                made_current_at: PrimitiveArray<Timestamp(µs, "+00:00")>
                [
                  2017-07-14T02:40:00+00:00,
                  2018-01-04T21:22:35.770+00:00,
                  2019-04-12T20:29:15.770+00:00,
                  2020-01-26T00:53:20+00:00,
                  2020-09-13T12:26:40+00:00,
                ],
                snapshot_id: PrimitiveArray<Int64>
                [
                  99,
                  1,
                  2,
                  1,
                  3,
                ],
                parent_id: PrimitiveArray<Int64>
                [
                  null,
                  null,
                  1,
                  null,
                  1,
                ],
                is_current_ancestor: BooleanArray
                [
                  false,
                  true,
                  false,
                  true,
                  true,
                ]"#]],
            &[],
            Some("made_current_at"),
        );
    }

    /// Corrupt metadata whose parent pointers form a cycle (S1 and S2 claim each
    /// other as parent) must not hang the scan: the ancestor traversal visits each
    /// snapshot once, so both log entries still render, flagged as ancestors.
    #[tokio::test]
    async fn test_history_table_with_parent_cycle() {
        let metadata_json = r#"{
            "format-version": 2,
            "table-uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c1",
            "location": "s3://bucket/test/location",
            "last-sequence-number": 2,
            "last-updated-ms": 1555100955770,
            "last-column-id": 1,
            "current-schema-id": 0,
            "schemas": [
                {"type": "struct", "schema-id": 0, "fields": [{"id": 1, "name": "x", "required": true, "type": "long"}]}
            ],
            "default-spec-id": 0,
            "partition-specs": [{"spec-id": 0, "fields": []}],
            "last-partition-id": 999,
            "default-sort-order-id": 0,
            "sort-orders": [{"order-id": 0, "fields": []}],
            "properties": {},
            "current-snapshot-id": 2,
            "snapshots": [
                {
                    "snapshot-id": 1,
                    "parent-snapshot-id": 2,
                    "timestamp-ms": 1515100955770,
                    "sequence-number": 1,
                    "summary": {"operation": "append"},
                    "manifest-list": "s3://bucket/metadata/snap-1.avro",
                    "schema-id": 0
                },
                {
                    "snapshot-id": 2,
                    "parent-snapshot-id": 1,
                    "timestamp-ms": 1555100955770,
                    "sequence-number": 2,
                    "summary": {"operation": "append"},
                    "manifest-list": "s3://bucket/metadata/snap-2.avro",
                    "schema-id": 0
                }
            ],
            "snapshot-log": [
                {"snapshot-id": 1, "timestamp-ms": 1515100955770},
                {"snapshot-id": 2, "timestamp-ms": 1555100955770}
            ],
            "metadata-log": [],
            "refs": {"main": {"snapshot-id": 2, "type": "branch"}}
        }"#;

        let table = table_from_metadata_json(metadata_json);

        let batch_stream = table.inspect().history().scan().await.unwrap();

        check_record_batches(
            batch_stream.try_collect::<Vec<_>>().await.unwrap(),
            expect![[r#"
                Field { "made_current_at": Timestamp(µs, "+00:00"), metadata: {"PARQUET:field_id": "1"} },
                Field { "snapshot_id": Int64, metadata: {"PARQUET:field_id": "2"} },
                Field { "parent_id": nullable Int64, metadata: {"PARQUET:field_id": "3"} },
                Field { "is_current_ancestor": Boolean, metadata: {"PARQUET:field_id": "4"} }"#]],
            expect![[r#"
                made_current_at: PrimitiveArray<Timestamp(µs, "+00:00")>
                [
                  2018-01-04T21:22:35.770+00:00,
                  2019-04-12T20:29:15.770+00:00,
                ],
                snapshot_id: PrimitiveArray<Int64>
                [
                  1,
                  2,
                ],
                parent_id: PrimitiveArray<Int64>
                [
                  2,
                  1,
                ],
                is_current_ancestor: BooleanArray
                [
                  true,
                  true,
                ]"#]],
            &[],
            Some("made_current_at"),
        );
    }

    /// An empty snapshot-log must yield a zero-row batch that still carries the
    /// full history schema.
    #[tokio::test]
    async fn test_history_table_with_empty_snapshot_log() {
        let metadata_json = r#"{
            "format-version": 2,
            "table-uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c1",
            "location": "s3://bucket/test/location",
            "last-sequence-number": 1,
            "last-updated-ms": 1515100955770,
            "last-column-id": 1,
            "current-schema-id": 0,
            "schemas": [
                {"type": "struct", "schema-id": 0, "fields": [{"id": 1, "name": "x", "required": true, "type": "long"}]}
            ],
            "default-spec-id": 0,
            "partition-specs": [{"spec-id": 0, "fields": []}],
            "last-partition-id": 999,
            "default-sort-order-id": 0,
            "sort-orders": [{"order-id": 0, "fields": []}],
            "properties": {},
            "current-snapshot-id": 1,
            "snapshots": [
                {
                    "snapshot-id": 1,
                    "timestamp-ms": 1515100955770,
                    "sequence-number": 1,
                    "summary": {"operation": "append"},
                    "manifest-list": "s3://bucket/metadata/snap-1.avro",
                    "schema-id": 0
                }
            ],
            "snapshot-log": [],
            "metadata-log": [],
            "refs": {"main": {"snapshot-id": 1, "type": "branch"}}
        }"#;

        let table = table_from_metadata_json(metadata_json);

        let batch_stream = table.inspect().history().scan().await.unwrap();

        check_record_batches(
            batch_stream.try_collect::<Vec<_>>().await.unwrap(),
            expect![[r#"
                Field { "made_current_at": Timestamp(µs, "+00:00"), metadata: {"PARQUET:field_id": "1"} },
                Field { "snapshot_id": Int64, metadata: {"PARQUET:field_id": "2"} },
                Field { "parent_id": nullable Int64, metadata: {"PARQUET:field_id": "3"} },
                Field { "is_current_ancestor": Boolean, metadata: {"PARQUET:field_id": "4"} }"#]],
            expect![[r#"
                made_current_at: PrimitiveArray<Timestamp(µs, "+00:00")>
                [
                ],
                snapshot_id: PrimitiveArray<Int64>
                [
                ],
                parent_id: PrimitiveArray<Int64>
                [
                ],
                is_current_ancestor: BooleanArray
                [
                ]"#]],
            &[],
            Some("made_current_at"),
        );
    }

    /// Without a current snapshot (`current-snapshot-id: -1`) there is no live
    /// lineage, so every log entry renders with `is_current_ancestor = false`.
    #[tokio::test]
    async fn test_history_table_without_current_snapshot() {
        let metadata_json = r#"{
            "format-version": 2,
            "table-uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c1",
            "location": "s3://bucket/test/location",
            "last-sequence-number": 2,
            "last-updated-ms": 1555100955770,
            "last-column-id": 1,
            "current-schema-id": 0,
            "schemas": [
                {"type": "struct", "schema-id": 0, "fields": [{"id": 1, "name": "x", "required": true, "type": "long"}]}
            ],
            "default-spec-id": 0,
            "partition-specs": [{"spec-id": 0, "fields": []}],
            "last-partition-id": 999,
            "default-sort-order-id": 0,
            "sort-orders": [{"order-id": 0, "fields": []}],
            "properties": {},
            "current-snapshot-id": -1,
            "snapshots": [
                {
                    "snapshot-id": 1,
                    "timestamp-ms": 1515100955770,
                    "sequence-number": 1,
                    "summary": {"operation": "append"},
                    "manifest-list": "s3://bucket/metadata/snap-1.avro",
                    "schema-id": 0
                },
                {
                    "snapshot-id": 2,
                    "parent-snapshot-id": 1,
                    "timestamp-ms": 1555100955770,
                    "sequence-number": 2,
                    "summary": {"operation": "append"},
                    "manifest-list": "s3://bucket/metadata/snap-2.avro",
                    "schema-id": 0
                }
            ],
            "snapshot-log": [
                {"snapshot-id": 1, "timestamp-ms": 1515100955770},
                {"snapshot-id": 2, "timestamp-ms": 1555100955770}
            ],
            "metadata-log": [],
            "refs": {}
        }"#;

        let table = table_from_metadata_json(metadata_json);

        let batch_stream = table.inspect().history().scan().await.unwrap();

        check_record_batches(
            batch_stream.try_collect::<Vec<_>>().await.unwrap(),
            expect![[r#"
                Field { "made_current_at": Timestamp(µs, "+00:00"), metadata: {"PARQUET:field_id": "1"} },
                Field { "snapshot_id": Int64, metadata: {"PARQUET:field_id": "2"} },
                Field { "parent_id": nullable Int64, metadata: {"PARQUET:field_id": "3"} },
                Field { "is_current_ancestor": Boolean, metadata: {"PARQUET:field_id": "4"} }"#]],
            expect![[r#"
                made_current_at: PrimitiveArray<Timestamp(µs, "+00:00")>
                [
                  2018-01-04T21:22:35.770+00:00,
                  2019-04-12T20:29:15.770+00:00,
                ],
                snapshot_id: PrimitiveArray<Int64>
                [
                  1,
                  2,
                ],
                parent_id: PrimitiveArray<Int64>
                [
                  null,
                  1,
                ],
                is_current_ancestor: BooleanArray
                [
                  false,
                  false,
                ]"#]],
            &[],
            Some("made_current_at"),
        );
    }
}
