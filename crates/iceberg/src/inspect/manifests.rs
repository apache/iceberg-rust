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
use arrow_array::types::{Int32Type, Int64Type};
use futures::{StreamExt, stream};

use super::partition_summary::{append_partition_summaries, partition_summary_builder};
use crate::Result;
use crate::arrow::schema_to_arrow_schema;
use crate::scan::ArrowRecordBatchStream;
use crate::spec::{ListType, ManifestContentType, NestedField, PrimitiveType, StructType, Type};
use crate::table::Table;

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
            .expect("manifests metadata table schema is statically valid")
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
        let mut partition_summaries = partition_summary_builder(&self.schema())?;

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

                // Counts are CONTENT-GATED, mirroring Java `ManifestsTable.manifestFileToRow`: a DATA
                // manifest carries its added/existing/deleted counts in the *_data_files_count columns
                // (the *_delete_files_count columns are 0), and a DELETE manifest the reverse.
                let added = manifest.added_files_count.unwrap_or(0) as i32;
                let existing = manifest.existing_files_count.unwrap_or(0) as i32;
                let deleted = manifest.deleted_files_count.unwrap_or(0) as i32;
                let is_data = manifest.content == ManifestContentType::Data;
                added_data_files_count.append_value(if is_data { added } else { 0 });
                existing_data_files_count.append_value(if is_data { existing } else { 0 });
                deleted_data_files_count.append_value(if is_data { deleted } else { 0 });
                added_delete_files_count.append_value(if is_data { 0 } else { added });
                existing_delete_files_count.append_value(if is_data { 0 } else { existing });
                deleted_delete_files_count.append_value(if is_data { 0 } else { deleted });

                let spec = self
                    .table
                    .metadata()
                    .partition_spec_by_id(manifest.partition_spec_id)
                    .ok_or_else(|| {
                        crate::Error::new(
                            crate::ErrorKind::DataInvalid,
                            format!(
                                "partition spec id {} referenced by manifest {} not found",
                                manifest.partition_spec_id, manifest.manifest_path
                            ),
                        )
                    })?;
                let spec_struct = spec.partition_type(self.table.metadata().current_schema())?;
                append_partition_summaries(
                    &mut partition_summaries,
                    manifest.partitions.as_deref().unwrap_or(&[]),
                    &spec_struct,
                )?;
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
}

#[cfg(test)]
mod tests {
    use expect_test::expect;
    use futures::TryStreamExt;

    use crate::scan::tests::TableTestFixture;
    use crate::test_utils::check_record_batches;

    #[tokio::test]
    async fn test_manifests_table() {
        let mut fixture = TableTestFixture::new();
        fixture.setup_manifest_files().await;

        let record_batch = fixture.table.inspect().manifests().scan().await.unwrap();

        check_record_batches(
            record_batch.try_collect::<Vec<_>>().await.unwrap(),
            expect![[r#"
                Field { "content": Int32, metadata: {"PARQUET:field_id": "14"} },
                Field { "path": Utf8, metadata: {"PARQUET:field_id": "1"} },
                Field { "length": Int64, metadata: {"PARQUET:field_id": "2"} },
                Field { "partition_spec_id": Int32, metadata: {"PARQUET:field_id": "3"} },
                Field { "added_snapshot_id": Int64, metadata: {"PARQUET:field_id": "4"} },
                Field { "added_data_files_count": Int32, metadata: {"PARQUET:field_id": "5"} },
                Field { "existing_data_files_count": Int32, metadata: {"PARQUET:field_id": "6"} },
                Field { "deleted_data_files_count": Int32, metadata: {"PARQUET:field_id": "7"} },
                Field { "added_delete_files_count": Int32, metadata: {"PARQUET:field_id": "15"} },
                Field { "existing_delete_files_count": Int32, metadata: {"PARQUET:field_id": "16"} },
                Field { "deleted_delete_files_count": Int32, metadata: {"PARQUET:field_id": "17"} },
                Field { "partition_summaries": List(non-null Struct("contains_null": non-null Boolean, metadata: {"PARQUET:field_id": "10"}, "contains_nan": Boolean, metadata: {"PARQUET:field_id": "11"}, "lower_bound": Utf8, metadata: {"PARQUET:field_id": "12"}, "upper_bound": Utf8, metadata: {"PARQUET:field_id": "13"}), metadata: {"PARQUET:field_id": "9"}), metadata: {"PARQUET:field_id": "8"} }"#]],
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
                  0,
                ],
                existing_delete_files_count: PrimitiveArray<Int32>
                [
                  0,
                ],
                deleted_delete_files_count: PrimitiveArray<Int32>
                [
                  0,
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
                  "1",
                ]
                -- child 3: "upper_bound" (Utf8)
                StringArray
                [
                  "1",
                ]
                ],
                ]"#]],
            &["path", "length"],
            Some("path"),
        );
    }
}
