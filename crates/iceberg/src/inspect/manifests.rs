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

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::builder::{
    BooleanBuilder, GenericListBuilder, ListBuilder, PrimitiveBuilder, StringBuilder, StructBuilder,
};
use arrow_array::types::{Int32Type, Int64Type};
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Fields};
use futures::{stream, StreamExt};

use crate::arrow::schema_to_arrow_schema;
use crate::scan::ArrowRecordBatchStream;
use crate::spec::{FieldSummary, ListType, NestedField, PrimitiveType, StructType, Type};
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
    pub async fn scan(&self) -> Result<ArrowRecordBatchStream> {
        let schema = schema_to_arrow_schema(&self.schema())?;

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
        let mut partition_summaries = self.partition_summary_builder()?;

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
                self.append_partition_summaries(&mut partition_summaries, &manifest.partitions);
            }
        }

        let batch = RecordBatch::try_new(Arc::new(schema), vec![
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

    fn partition_summary_builder(&self) -> Result<GenericListBuilder<i32, StructBuilder>> {
        let schema = schema_to_arrow_schema(&self.schema())?;
        let partition_summary_fields =
            match schema.field_with_name("partition_summaries")?.data_type() {
                DataType::List(list_type) => match list_type.data_type() {
                    DataType::Struct(fields) => fields.to_vec(),
                    _ => unreachable!(),
                },
                _ => unreachable!(),
            };

        let partition_summaries = ListBuilder::new(StructBuilder::from_fields(
            Fields::from(partition_summary_fields.clone()),
            0,
        ))
        .with_field(Arc::new(
            Field::new_struct("item", partition_summary_fields, false).with_metadata(
                HashMap::from([("PARQUET:field_id".to_string(), "9".to_string())]),
            ),
        ));

        Ok(partition_summaries)
    }

    fn append_partition_summaries(
        &self,
        builder: &mut GenericListBuilder<i32, StructBuilder>,
        partitions: &[FieldSummary],
    ) {
        let partition_summaries_builder = builder.values();
        for summary in partitions {
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
        builder.append(true);
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

        let record_batch = fixture.table.inspect().manifests().scan().await.unwrap();

        check_record_batches(
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
        ).await;
    }
}
