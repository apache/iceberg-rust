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

use arrow_array::RecordBatch;
use arrow_array::builder::{BooleanBuilder, PrimitiveBuilder};
use arrow_array::types::{Int64Type, TimestampMicrosecondType};
use futures::{StreamExt, stream};

use crate::Result;
use crate::arrow::schema_to_arrow_schema;
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

        let mut made_current_at =
            PrimitiveBuilder::<TimestampMicrosecondType>::new().with_timezone("+00:00");
        let mut snapshot_id = PrimitiveBuilder::<Int64Type>::new();
        let mut parent_id = PrimitiveBuilder::<Int64Type>::new();
        let mut is_current_ancestor = BooleanBuilder::new();

        let table_metadata = self.table.metadata_ref();
        let current_ancestor_ids = self
            .table
            .metadata()
            .current_snapshot()
            .map(|snapshot| {
                ancestors_of(&table_metadata, snapshot.snapshot_id())
                    .map(|snapshot| snapshot.snapshot_id())
                    .collect::<HashSet<_>>()
            })
            .unwrap_or_default();

        for history_entry in self.table.metadata().history() {
            made_current_at.append_value(history_entry.timestamp_ms() * 1000);
            snapshot_id.append_value(history_entry.snapshot_id);
            parent_id.append_option(
                self.table
                    .metadata()
                    .snapshot_by_id(history_entry.snapshot_id)
                    .and_then(|snapshot| snapshot.parent_snapshot_id()),
            );
            is_current_ancestor
                .append_value(current_ancestor_ids.contains(&history_entry.snapshot_id));
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

    use crate::scan::tests::TableTestFixture;
    use crate::test_utils::check_record_batches;

    #[tokio::test]
    async fn test_history_table() {
        let table = TableTestFixture::new_with_deep_history().table;

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
                  2019-11-30T08:02:35.770+00:00,
                  2020-07-18T19:35:55.770+00:00,
                  2020-10-14T01:22:53.590+00:00,
                ],
                snapshot_id: PrimitiveArray<Int64>
                [
                  3051729675574597004,
                  3055729675574597004,
                  3056729675574597004,
                  3057729675574597004,
                  3059729675574597004,
                ],
                parent_id: PrimitiveArray<Int64>
                [
                  null,
                  3051729675574597004,
                  3055729675574597004,
                  3056729675574597004,
                  3057729675574597004,
                ],
                is_current_ancestor: BooleanArray
                [
                  true,
                  true,
                  true,
                  true,
                  true,
                ]"#]],
            &[],
            Some("made_current_at"),
        );
    }
}
