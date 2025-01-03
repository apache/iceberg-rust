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

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::builder::{
    BooleanBuilder, ListBuilder, MapBuilder, PrimitiveBuilder, StringBuilder, StructBuilder,
};
use arrow_array::types::{Int32Type, Int64Type, TimestampMillisecondType};
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Fields, Schema, TimeUnit};

use crate::arrow::schema_to_arrow_schema;
use crate::spec::{ListType, NestedField, PrimitiveType, StructType, Type};
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
        SnapshotsTable { table: &self.0 }
    }

    /// Get the manifests table.
    pub fn manifests(&self) -> ManifestsTable {
        ManifestsTable { table: &self.0 }
    }
}

/// Snapshots table.
pub struct SnapshotsTable<'a> {
    table: &'a Table,
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

        for snapshot in self.table.metadata().snapshots() {
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

        Ok(RecordBatch::try_new(Arc::new(self.schema()), vec![
            Arc::new(committed_at.finish()),
            Arc::new(snapshot_id.finish()),
            Arc::new(parent_id.finish()),
            Arc::new(operation.finish()),
            Arc::new(manifest_list.finish()),
            Arc::new(summary.finish()),
        ])?)
    }
}

/// Manifests table.
pub struct ManifestsTable<'a> {
    table: &'a Table,
}

impl<'a> ManifestsTable<'a> {
    /// Returns the iceberg schema of the manifests table.
    pub fn schema(&self) -> crate::spec::Schema {
        let fields = vec![
            NestedField::new(14, "content", Type::Primitive(PrimitiveType::Int), true),
            NestedField::new(1, "path", Type::Primitive(PrimitiveType::String), true),
            NestedField::new(2, "length", Type::Primitive(PrimitiveType::Long), true),
            NestedField::new(
                3,
                "partition_spec_id",
                Type::Primitive(PrimitiveType::Int),
                true,
            ),
            NestedField::new(
                4,
                "added_snapshot_id",
                Type::Primitive(PrimitiveType::Long),
                true,
            ),
            NestedField::new(
                5,
                "added_data_files_count",
                Type::Primitive(PrimitiveType::Int),
                true,
            ),
            NestedField::new(
                6,
                "existing_data_files_count",
                Type::Primitive(PrimitiveType::Int),
                true,
            ),
            NestedField::new(
                7,
                "deleted_data_files_count",
                Type::Primitive(PrimitiveType::Int),
                true,
            ),
            NestedField::new(
                15,
                "added_delete_files_count",
                Type::Primitive(PrimitiveType::Int),
                true,
            ),
            NestedField::new(
                16,
                "existing_delete_files_count",
                Type::Primitive(PrimitiveType::Int),
                true,
            ),
            NestedField::new(
                17,
                "deleted_delete_files_count",
                Type::Primitive(PrimitiveType::Int),
                true,
            ),
            NestedField::new(
                8,
                "partition_summaries",
                Type::List(ListType {
                    element_field: Arc::new(NestedField::new(
                        9,
                        "item",
                        Type::Struct(StructType::new(vec![
                            Arc::new(NestedField::new(
                                10,
                                "contains_null",
                                Type::Primitive(PrimitiveType::Boolean),
                                true,
                            )),
                            Arc::new(NestedField::new(
                                11,
                                "contains_nan",
                                Type::Primitive(PrimitiveType::Boolean),
                                false,
                            )),
                            Arc::new(NestedField::new(
                                12,
                                "lower_bound",
                                Type::Primitive(PrimitiveType::String),
                                false,
                            )),
                            Arc::new(NestedField::new(
                                13,
                                "upper_bound",
                                Type::Primitive(PrimitiveType::String),
                                false,
                            )),
                        ])),
                        true,
                    )),
                }),
                true,
            ),
        ];

        crate::spec::Schema::builder()
            .with_fields(fields.into_iter().map(|f| f.into()))
            .build()
            .unwrap()
    }

    /// Scans the manifests table.
    pub async fn scan(&self) -> Result<RecordBatch> {
        let schema = schema_to_arrow_schema(&self.schema())?;
        let partition_summary_fields = if let DataType::List(list_type) =
            schema.field_with_name("partition_summaries")?.data_type()
        {
            if let DataType::Struct(fields) = list_type.data_type() {
                fields.to_vec()
            } else {
                unreachable!()
            }
        } else {
            unreachable!()
        };

        let mut content = PrimitiveBuilder::<Int32Type>::new();
        let mut path = StringBuilder::new();
        let mut length = PrimitiveBuilder::<Int64Type>::new();
        let mut partition_spec_id = PrimitiveBuilder::<Int32Type>::new();
        let mut added_snapshot_id = PrimitiveBuilder::<Int64Type>::new();
        let mut added_data_files_count = PrimitiveBuilder::<Int32Type>::new();
        let mut existing_data_files_count = PrimitiveBuilder::<Int32Type>::new();
        let mut deleted_data_files_count = PrimitiveBuilder::<Int32Type>::new();
        let mut added_delete_files_count = PrimitiveBuilder::<Int32Type>::new();
        let mut existing_delete_files_count = PrimitiveBuilder::<Int32Type>::new();
        let mut deleted_delete_files_count = PrimitiveBuilder::<Int32Type>::new();
        let mut partition_summaries = ListBuilder::new(StructBuilder::from_fields(
            Fields::from(partition_summary_fields.clone()),
            0,
        ))
        .with_field(Arc::new(
            Field::new_struct("item", partition_summary_fields, false).with_metadata(
                HashMap::from([("PARQUET:field_id".to_string(), "9".to_string())]),
            ),
        ));

        if let Some(snapshot) = self.table.metadata().current_snapshot() {
            let manifest_list = snapshot
                .load_manifest_list(self.table.file_io(), &self.table.metadata_ref())
                .await?;
            for manifest in manifest_list.entries() {
                content.append_value(manifest.content as i32);
                path.append_value(manifest.manifest_path.clone());
                length.append_value(manifest.manifest_length);
                partition_spec_id.append_value(manifest.partition_spec_id);
                added_snapshot_id.append_value(manifest.added_snapshot_id);
                added_data_files_count.append_value(manifest.added_files_count.unwrap_or(0) as i32);
                existing_data_files_count
                    .append_value(manifest.existing_files_count.unwrap_or(0) as i32);
                deleted_data_files_count
                    .append_value(manifest.deleted_files_count.unwrap_or(0) as i32);
                added_delete_files_count
                    .append_value(manifest.added_files_count.unwrap_or(0) as i32);
                existing_delete_files_count
                    .append_value(manifest.existing_files_count.unwrap_or(0) as i32);
                deleted_delete_files_count
                    .append_value(manifest.deleted_files_count.unwrap_or(0) as i32);

                let partition_summaries_builder = partition_summaries.values();
                for summary in &manifest.partitions {
                    partition_summaries_builder
                        .field_builder::<BooleanBuilder>(0)
                        .unwrap()
                        .append_value(summary.contains_null);
                    partition_summaries_builder
                        .field_builder::<BooleanBuilder>(1)
                        .unwrap()
                        .append_option(summary.contains_nan);
                    partition_summaries_builder
                        .field_builder::<StringBuilder>(2)
                        .unwrap()
                        .append_option(summary.lower_bound.as_ref().map(|v| v.to_string()));
                    partition_summaries_builder
                        .field_builder::<StringBuilder>(3)
                        .unwrap()
                        .append_option(summary.upper_bound.as_ref().map(|v| v.to_string()));
                    partition_summaries_builder.append(true);
                }
                partition_summaries.append(true);
            }
        }

        Ok(RecordBatch::try_new(Arc::new(schema), vec![
            Arc::new(content.finish()),
            Arc::new(path.finish()),
            Arc::new(length.finish()),
            Arc::new(partition_spec_id.finish()),
            Arc::new(added_snapshot_id.finish()),
            Arc::new(added_data_files_count.finish()),
            Arc::new(existing_data_files_count.finish()),
            Arc::new(deleted_data_files_count.finish()),
            Arc::new(added_delete_files_count.finish()),
            Arc::new(existing_delete_files_count.finish()),
            Arc::new(deleted_delete_files_count.finish()),
            Arc::new(partition_summaries.finish()),
        ])?)
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

    #[tokio::test]
    async fn test_manifests_table() {
        let mut fixture = TableTestFixture::new();
        fixture.setup_manifest_files().await;

        let record_batch = fixture
            .table
            .metadata_table()
            .manifests()
            .scan()
            .await
            .unwrap();

        check_record_batch(
            record_batch,
            expect![[r#"
                Field { name: "content", data_type: Int32, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "14"} },
                Field { name: "path", data_type: Utf8, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1"} },
                Field { name: "length", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "2"} },
                Field { name: "partition_spec_id", data_type: Int32, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "3"} },
                Field { name: "added_snapshot_id", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "4"} },
                Field { name: "added_data_files_count", data_type: Int32, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "5"} },
                Field { name: "existing_data_files_count", data_type: Int32, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "6"} },
                Field { name: "deleted_data_files_count", data_type: Int32, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "7"} },
                Field { name: "added_delete_files_count", data_type: Int32, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "15"} },
                Field { name: "existing_delete_files_count", data_type: Int32, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "16"} },
                Field { name: "deleted_delete_files_count", data_type: Int32, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "17"} },
                Field { name: "partition_summaries", data_type: List(Field { name: "item", data_type: Struct([Field { name: "contains_null", data_type: Boolean, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "10"} }, Field { name: "contains_nan", data_type: Boolean, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "11"} }, Field { name: "lower_bound", data_type: Utf8, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "12"} }, Field { name: "upper_bound", data_type: Utf8, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "13"} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "9"} }), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "8"} }"#]],
            expect![[r#"
                content: PrimitiveArray<Int32>
                [
                  0,
                ],
                path: (skipped),
                length: (skipped),
                partition_spec_id: PrimitiveArray<Int32>
                [
                  0,
                ],
                added_snapshot_id: PrimitiveArray<Int64>
                [
                  3055729675574597004,
                ],
                added_data_files_count: PrimitiveArray<Int32>
                [
                  1,
                ],
                existing_data_files_count: PrimitiveArray<Int32>
                [
                  1,
                ],
                deleted_data_files_count: PrimitiveArray<Int32>
                [
                  1,
                ],
                added_delete_files_count: PrimitiveArray<Int32>
                [
                  1,
                ],
                existing_delete_files_count: PrimitiveArray<Int32>
                [
                  1,
                ],
                deleted_delete_files_count: PrimitiveArray<Int32>
                [
                  1,
                ],
                partition_summaries: ListArray
                [
                  StructArray
                -- validity: 
                [
                  valid,
                ]
                [
                -- child 0: "contains_null" (Boolean)
                BooleanArray
                [
                  false,
                ]
                -- child 1: "contains_nan" (Boolean)
                BooleanArray
                [
                  false,
                ]
                -- child 2: "lower_bound" (Utf8)
                StringArray
                [
                  "100",
                ]
                -- child 3: "upper_bound" (Utf8)
                StringArray
                [
                  "300",
                ]
                ],
                ]"#]],
            &["path", "length"],
            Some("path"),
        );
    }
}
