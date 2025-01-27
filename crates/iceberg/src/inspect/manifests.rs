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

use arrow_array::builder::{
    BooleanBuilder, ListBuilder, PrimitiveBuilder, StringBuilder, StructBuilder,
};
use arrow_array::types::{Int32Type, Int64Type, Int8Type};
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Fields, Schema};
use futures::{stream, StreamExt};

use crate::scan::ArrowRecordBatchStream;
use crate::table::Table;
use crate::Result;

/// Manifests table.
pub struct ManifestsTable<'a> {
    table: &'a Table,
}

impl<'a> ManifestsTable<'a> {
    /// Create a new Manifests table instance.
    pub fn new(table: &'a Table) -> Self {
        Self { table }
    }

    fn partition_summary_fields() -> Vec<Field> {
        vec![
            Field::new("contains_null", DataType::Boolean, false),
            Field::new("contains_nan", DataType::Boolean, true),
            Field::new("lower_bound", DataType::Utf8, true),
            Field::new("upper_bound", DataType::Utf8, true),
        ]
    }

    /// Returns the schema of the manifests table.
    pub fn schema(&self) -> Schema {
        Schema::new(vec![
            Field::new("content", DataType::Int8, false),
            Field::new("path", DataType::Utf8, false),
            Field::new("length", DataType::Int64, false),
            Field::new("partition_spec_id", DataType::Int32, false),
            Field::new("added_snapshot_id", DataType::Int64, false),
            Field::new("added_data_files_count", DataType::Int32, false),
            Field::new("existing_data_files_count", DataType::Int32, false),
            Field::new("deleted_data_files_count", DataType::Int32, false),
            Field::new("added_delete_files_count", DataType::Int32, false),
            Field::new("existing_delete_files_count", DataType::Int32, false),
            Field::new("deleted_delete_files_count", DataType::Int32, false),
            Field::new(
                "partition_summaries",
                DataType::List(Arc::new(Field::new_struct(
                    "item",
                    Self::partition_summary_fields(),
                    false,
                ))),
                false,
            ),
        ])
    }

    /// Scans the manifests table.
    pub async fn scan(&self) -> Result<ArrowRecordBatchStream> {
        let mut content = PrimitiveBuilder::<Int8Type>::new();
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
            Fields::from(Self::partition_summary_fields()),
            0,
        ))
        .with_field(Arc::new(Field::new_struct(
            "item",
            Self::partition_summary_fields(),
            false,
        )));

        if let Some(snapshot) = self.table.metadata().current_snapshot() {
            let manifest_list = snapshot
                .load_manifest_list(self.table.file_io(), &self.table.metadata_ref())
                .await?;
            for manifest in manifest_list.entries() {
                content.append_value(manifest.content as i8);
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

        let batch = RecordBatch::try_new(Arc::new(self.schema()), vec![
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
        ])?;

        Ok(stream::iter(vec![Ok(batch)]).boxed())
    }
}

#[cfg(test)]
mod tests {
    use expect_test::expect;

    use crate::inspect::metadata_table::tests::check_record_batches;
    use crate::scan::tests::TableTestFixture;

    #[tokio::test]
    async fn test_manifests_table() {
        let mut fixture = TableTestFixture::new();
        fixture.setup_manifest_files().await;

        let batch_stream = fixture.table.inspect().manifests().scan().await.unwrap();

        check_record_batches(
            batch_stream,
            expect![[r#"
                Field { name: "content", data_type: Int8, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} },
                Field { name: "path", data_type: Utf8, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} },
                Field { name: "length", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} },
                Field { name: "partition_spec_id", data_type: Int32, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} },
                Field { name: "added_snapshot_id", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} },
                Field { name: "added_data_files_count", data_type: Int32, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} },
                Field { name: "existing_data_files_count", data_type: Int32, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} },
                Field { name: "deleted_data_files_count", data_type: Int32, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} },
                Field { name: "added_delete_files_count", data_type: Int32, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} },
                Field { name: "existing_delete_files_count", data_type: Int32, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} },
                Field { name: "deleted_delete_files_count", data_type: Int32, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} },
                Field { name: "partition_summaries", data_type: List(Field { name: "item", data_type: Struct([Field { name: "contains_null", data_type: Boolean, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "contains_nan", data_type: Boolean, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "lower_bound", data_type: Utf8, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "upper_bound", data_type: Utf8, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }"#]],
            expect![[r#"
                content: PrimitiveArray<Int8>
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
        ).await;
    }
}
