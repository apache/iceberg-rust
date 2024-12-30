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

//! Metadata table api.

use std::sync::Arc;

use arrow_array::builder::{MapBuilder, PrimitiveBuilder, StringBuilder};
use arrow_array::types::{Int64Type, TimestampMillisecondType};
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema, TimeUnit};

use crate::spec::TableMetadata;
use crate::table::Table;
use crate::Result;

/// Metadata table is used to inspect a table's history, snapshots, and other metadata as a table.
///
/// References:
/// - <https://github.com/apache/iceberg/blob/ac865e334e143dfd9e33011d8cf710b46d91f1e5/core/src/main/java/org/apache/iceberg/MetadataTableType.java#L23-L39>
/// - <https://iceberg.apache.org/docs/latest/spark-queries/#querying-with-sql>
/// - <https://py.iceberg.apache.org/api/#inspecting-tables>
#[derive(Debug)]
pub struct MetadataTable(Table);

impl MetadataTable {
    /// Creates a new metadata scan.
    pub(super) fn new(table: Table) -> Self {
        Self(table)
    }

    /// Get the snapshots table.
    pub fn snapshots(&self) -> SnapshotsTable {
        SnapshotsTable {
            metadata_table: self,
        }
    }

    fn metadata(&self) -> &TableMetadata {
        self.0.metadata()
    }
}

/// Snapshots table.
pub struct SnapshotsTable<'a> {
    metadata_table: &'a MetadataTable,
}

impl<'a> SnapshotsTable<'a> {
    /// Returns the schema of the snapshots table.
    pub fn schema(&self) -> Schema {
        Schema::new(vec![
            Field::new(
                "committed_at",
                DataType::Timestamp(TimeUnit::Millisecond, Some("+00:00".into())),
                false,
            ),
            Field::new("snapshot_id", DataType::Int64, false),
            Field::new("parent_id", DataType::Int64, true),
            Field::new("operation", DataType::Utf8, false),
            Field::new("manifest_list", DataType::Utf8, false),
            Field::new(
                "summary",
                DataType::Map(
                    Arc::new(Field::new(
                        "entries",
                        DataType::Struct(
                            vec![
                                Field::new("keys", DataType::Utf8, false),
                                Field::new("values", DataType::Utf8, true),
                            ]
                            .into(),
                        ),
                        false,
                    )),
                    false,
                ),
                false,
            ),
        ])
    }

    /// Scans the snapshots table.
    pub fn scan(&self) -> Result<RecordBatch> {
        let mut committed_at =
            PrimitiveBuilder::<TimestampMillisecondType>::new().with_timezone("+00:00");
        let mut snapshot_id = PrimitiveBuilder::<Int64Type>::new();
        let mut parent_id = PrimitiveBuilder::<Int64Type>::new();
        let mut operation = StringBuilder::new();
        let mut manifest_list = StringBuilder::new();
        let mut summary = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());

        for snapshot in self.metadata_table.metadata().snapshots() {
            committed_at.append_value(snapshot.timestamp_ms());
            snapshot_id.append_value(snapshot.snapshot_id());
            parent_id.append_option(snapshot.parent_snapshot_id());
            manifest_list.append_value(snapshot.manifest_list());
            operation.append_value(snapshot.summary().operation.as_str());
            for (key, value) in &snapshot.summary().additional_properties {
                summary.keys().append_value(key);
                summary.values().append_value(value);
            }
            summary.append(true)?;
        }

        Ok(RecordBatch::try_new(
            Arc::new(self.schema()),
            vec![
                Arc::new(committed_at.finish()),
                Arc::new(snapshot_id.finish()),
                Arc::new(parent_id.finish()),
                Arc::new(operation.finish()),
                Arc::new(manifest_list.finish()),
                Arc::new(summary.finish()),
            ],
        )?)
    }
}

#[cfg(test)]
mod tests {
    use expect_test::{expect, Expect};
    use itertools::Itertools;

    use super::*;
    use crate::scan::tests::TableTestFixture;

    /// Snapshot testing to check the resulting record batch.
    ///
    /// - `expected_schema/data`: put `expect![[""]]` as a placeholder,
    ///   and then run test with `UPDATE_EXPECT=1 cargo test` to automatically update the result,
    ///   or use rust-analyzer (see [video](https://github.com/rust-analyzer/expect-test)).
    ///   Check the doc of [`expect_test`] for more details.
    /// - `ignore_check_columns`: Some columns are not stable, so we can skip them.
    /// - `sort_column`: The order of the data might be non-deterministic, so we can sort it by a column.
    fn check_record_batch(
        record_batch: RecordBatch,
        expected_schema: Expect,
        expected_data: Expect,
        ignore_check_columns: &[&str],
        sort_column: Option<&str>,
    ) {
        let mut columns = record_batch.columns().to_vec();
        if let Some(sort_column) = sort_column {
            let column = record_batch.column_by_name(sort_column).unwrap();
            let indices = arrow_ord::sort::sort_to_indices(column, None, None).unwrap();
            columns = columns
                .iter()
                .map(|column| arrow_select::take::take(column.as_ref(), &indices, None).unwrap())
                .collect_vec();
        }

        expected_schema.assert_eq(&format!(
            "{}",
            record_batch.schema().fields().iter().format(",\n")
        ));
        expected_data.assert_eq(&format!(
            "{}",
            record_batch
                .schema()
                .fields()
                .iter()
                .zip_eq(columns)
                .map(|(field, column)| {
                    if ignore_check_columns.contains(&field.name().as_str()) {
                        format!("{}: (skipped)", field.name())
                    } else {
                        format!("{}: {:?}", field.name(), column)
                    }
                })
                .format(",\n")
        ));
    }

    #[test]
    fn test_snapshots_table() {
        let table = TableTestFixture::new().table;
        let record_batch = table.metadata_table().snapshots().scan().unwrap();
        check_record_batch(
            record_batch,
            expect![[r#"
                Field { name: "committed_at", data_type: Timestamp(Millisecond, Some("+00:00")), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} },
                Field { name: "snapshot_id", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} },
                Field { name: "parent_id", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} },
                Field { name: "operation", data_type: Utf8, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} },
                Field { name: "manifest_list", data_type: Utf8, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} },
                Field { name: "summary", data_type: Map(Field { name: "entries", data_type: Struct([Field { name: "keys", data_type: Utf8, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "values", data_type: Utf8, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, false), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }"#]],
            expect![[r#"
                committed_at: PrimitiveArray<Timestamp(Millisecond, Some("+00:00"))>
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
                operation: StringArray
                [
                  "append",
                  "append",
                ],
                manifest_list: (skipped),
                summary: MapArray
                [
                  StructArray
                -- validity: 
                [
                ]
                [
                -- child 0: "keys" (Utf8)
                StringArray
                [
                ]
                -- child 1: "values" (Utf8)
                StringArray
                [
                ]
                ],
                  StructArray
                -- validity: 
                [
                ]
                [
                -- child 0: "keys" (Utf8)
                StringArray
                [
                ]
                -- child 1: "values" (Utf8)
                StringArray
                [
                ]
                ],
                ]"#]],
            &["manifest_list"],
            Some("committed_at"),
        );
    }
}
