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

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_array::builder::{PrimitiveBuilder, StringBuilder};
use arrow_array::types::{Int32Type, Int64Type, TimestampMicrosecondType};
use futures::{StreamExt, stream};

use crate::Result;
use crate::arrow::schema_to_arrow_schema;
use crate::scan::ArrowRecordBatchStream;
use crate::spec::{NestedField, PrimitiveType, Type};
use crate::table::Table;

/// Metadata log entries table.
///
/// Shows the table's metadata log history.
pub struct MetadataLogEntriesTable<'a> {
    table: &'a Table,
}

impl<'a> MetadataLogEntriesTable<'a> {
    /// Create a new MetadataLogEntries table instance.
    pub fn new(table: &'a Table) -> Self {
        Self { table }
    }

    /// Returns the iceberg schema of the metadata log entries table.
    pub fn schema(&self) -> crate::spec::Schema {
        let fields = vec![
            NestedField::required(1, "timestamp", Type::Primitive(PrimitiveType::Timestamptz)),
            NestedField::required(2, "file", Type::Primitive(PrimitiveType::String)),
            NestedField::optional(
                3,
                "latest_snapshot_id",
                Type::Primitive(PrimitiveType::Long),
            ),
            NestedField::optional(4, "latest_schema_id", Type::Primitive(PrimitiveType::Int)),
            NestedField::optional(
                5,
                "latest_sequence_number",
                Type::Primitive(PrimitiveType::Long),
            ),
        ];
        crate::spec::Schema::builder()
            .with_fields(fields.into_iter().map(|f| f.into()))
            .build()
            .unwrap()
    }

    /// Scans the metadata log entries table.
    pub async fn scan(&self) -> Result<ArrowRecordBatchStream> {
        let schema = schema_to_arrow_schema(&self.schema())?;
        let metadata = self.table.metadata();

        let mut timestamp =
            PrimitiveBuilder::<TimestampMicrosecondType>::new().with_timezone("+00:00");
        let mut file = StringBuilder::new();
        let mut latest_snapshot_id = PrimitiveBuilder::<Int64Type>::new();
        let mut latest_schema_id = PrimitiveBuilder::<Int32Type>::new();
        let mut latest_sequence_number = PrimitiveBuilder::<Int64Type>::new();

        // Historical entries from the metadata log
        for log_entry in metadata.metadata_log() {
            timestamp.append_value(log_entry.timestamp_ms * 1000);
            file.append_value(&log_entry.metadata_file);
            latest_snapshot_id.append_null();
            latest_schema_id.append_null();
            latest_sequence_number.append_null();
        }

        // Current metadata entry
        if let Some(metadata_location) = self.table.metadata_location() {
            timestamp.append_value(metadata.last_updated_ms * 1000);
            file.append_value(metadata_location);
            latest_snapshot_id.append_option(metadata.current_snapshot_id);
            latest_schema_id.append_value(metadata.current_schema_id);
            latest_sequence_number.append_value(metadata.last_sequence_number);
        }

        let batch = RecordBatch::try_new(Arc::new(schema), vec![
            Arc::new(timestamp.finish()),
            Arc::new(file.finish()),
            Arc::new(latest_snapshot_id.finish()),
            Arc::new(latest_schema_id.finish()),
            Arc::new(latest_sequence_number.finish()),
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
    async fn test_metadata_log_entries_table() {
        let table = TableTestFixture::new().table;

        let batch_stream = table.inspect().metadata_log_entries().scan().await.unwrap();

        check_record_batches(
            batch_stream.try_collect::<Vec<_>>().await.unwrap(),
            expect![[r#"
                Field { "timestamp": Timestamp(µs, "+00:00"), metadata: {"PARQUET:field_id": "1"} },
                Field { "file": Utf8, metadata: {"PARQUET:field_id": "2"} },
                Field { "latest_snapshot_id": nullable Int64, metadata: {"PARQUET:field_id": "3"} },
                Field { "latest_schema_id": nullable Int32, metadata: {"PARQUET:field_id": "4"} },
                Field { "latest_sequence_number": nullable Int64, metadata: {"PARQUET:field_id": "5"} }"#]],
            expect![[r#"
                timestamp: PrimitiveArray<Timestamp(µs, "+00:00")>
                [
                  1970-01-01T00:25:15.100+00:00,
                  2020-10-14T01:22:53.590+00:00,
                ],
                file: (skipped),
                latest_snapshot_id: PrimitiveArray<Int64>
                [
                  null,
                  3055729675574597004,
                ],
                latest_schema_id: PrimitiveArray<Int32>
                [
                  null,
                  1,
                ],
                latest_sequence_number: PrimitiveArray<Int64>
                [
                  null,
                  34,
                ]"#]],
            &["file"],
            Some("timestamp"),
        );
    }
}
