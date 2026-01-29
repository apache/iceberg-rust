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
use arrow_array::types::Int64Type;
use futures::{StreamExt, stream};

use crate::Result;
use crate::arrow::schema_to_arrow_schema;
use crate::scan::ArrowRecordBatchStream;
use crate::spec::{NestedField, PrimitiveType, SnapshotRetention, Type};
use crate::table::Table;

/// Refs metadata table.
///
/// Shows all named references (branches and tags) for the table.
pub struct RefsTable<'a> {
    table: &'a Table,
}

impl<'a> RefsTable<'a> {
    /// Create a new Refs table instance.
    pub fn new(table: &'a Table) -> Self {
        Self { table }
    }

    /// Returns the iceberg schema of the refs table.
    pub fn schema(&self) -> crate::spec::Schema {
        let fields = vec![
            NestedField::required(1, "name", Type::Primitive(PrimitiveType::String)),
            NestedField::required(2, "type", Type::Primitive(PrimitiveType::String)),
            NestedField::required(3, "snapshot_id", Type::Primitive(PrimitiveType::Long)),
            NestedField::optional(
                4,
                "max_reference_age_in_ms",
                Type::Primitive(PrimitiveType::Long),
            ),
            NestedField::optional(
                5,
                "min_snapshots_to_keep",
                Type::Primitive(PrimitiveType::Long),
            ),
            NestedField::optional(
                6,
                "max_snapshot_age_in_ms",
                Type::Primitive(PrimitiveType::Long),
            ),
        ];
        crate::spec::Schema::builder()
            .with_fields(fields.into_iter().map(|f| f.into()))
            .build()
            .unwrap()
    }

    /// Scans the refs table.
    pub async fn scan(&self) -> Result<ArrowRecordBatchStream> {
        let schema = schema_to_arrow_schema(&self.schema())?;

        let mut name_builder = StringBuilder::new();
        let mut type_builder = StringBuilder::new();
        let mut snapshot_id_builder = PrimitiveBuilder::<Int64Type>::new();
        let mut max_ref_age_builder = PrimitiveBuilder::<Int64Type>::new();
        let mut min_snapshots_builder = PrimitiveBuilder::<Int64Type>::new();
        let mut max_snapshot_age_builder = PrimitiveBuilder::<Int64Type>::new();

        for (ref_name, snapshot_ref) in &self.table.metadata().refs {
            name_builder.append_value(ref_name);
            snapshot_id_builder.append_value(snapshot_ref.snapshot_id);

            match &snapshot_ref.retention {
                SnapshotRetention::Branch {
                    min_snapshots_to_keep,
                    max_snapshot_age_ms,
                    max_ref_age_ms,
                } => {
                    type_builder.append_value("branch");
                    max_ref_age_builder.append_option(*max_ref_age_ms);
                    min_snapshots_builder.append_option(min_snapshots_to_keep.map(|v| v as i64));
                    max_snapshot_age_builder.append_option(*max_snapshot_age_ms);
                }
                SnapshotRetention::Tag { max_ref_age_ms } => {
                    type_builder.append_value("tag");
                    max_ref_age_builder.append_option(*max_ref_age_ms);
                    min_snapshots_builder.append_null();
                    max_snapshot_age_builder.append_null();
                }
            }
        }

        let batch = RecordBatch::try_new(Arc::new(schema), vec![
            Arc::new(name_builder.finish()),
            Arc::new(type_builder.finish()),
            Arc::new(snapshot_id_builder.finish()),
            Arc::new(max_ref_age_builder.finish()),
            Arc::new(min_snapshots_builder.finish()),
            Arc::new(max_snapshot_age_builder.finish()),
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
    async fn test_refs_table() {
        let table = TableTestFixture::new().table;

        let batch_stream = table.inspect().refs().scan().await.unwrap();

        check_record_batches(
            batch_stream.try_collect::<Vec<_>>().await.unwrap(),
            expect![[r#"
                Field { "name": Utf8, metadata: {"PARQUET:field_id": "1"} },
                Field { "type": Utf8, metadata: {"PARQUET:field_id": "2"} },
                Field { "snapshot_id": Int64, metadata: {"PARQUET:field_id": "3"} },
                Field { "max_reference_age_in_ms": nullable Int64, metadata: {"PARQUET:field_id": "4"} },
                Field { "min_snapshots_to_keep": nullable Int64, metadata: {"PARQUET:field_id": "5"} },
                Field { "max_snapshot_age_in_ms": nullable Int64, metadata: {"PARQUET:field_id": "6"} }"#]],
            expect![[r#"
                name: StringArray
                [
                  "main",
                  "test",
                ],
                type: StringArray
                [
                  "branch",
                  "tag",
                ],
                snapshot_id: PrimitiveArray<Int64>
                [
                  3055729675574597004,
                  3051729675574597004,
                ],
                max_reference_age_in_ms: PrimitiveArray<Int64>
                [
                  null,
                  10000000,
                ],
                min_snapshots_to_keep: PrimitiveArray<Int64>
                [
                  null,
                  null,
                ],
                max_snapshot_age_in_ms: PrimitiveArray<Int64>
                [
                  null,
                  null,
                ]"#]],
            &[],
            Some("name"),
        );
    }
}
