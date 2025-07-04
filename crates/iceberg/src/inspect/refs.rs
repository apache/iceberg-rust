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

use arrow_array::RecordBatch;
use arrow_array::builder::{PrimitiveBuilder, StringBuilder};
use arrow_array::types::{Int32Type, Int64Type};
use futures::{StreamExt, stream};

use crate::Result;
use crate::arrow::schema_to_arrow_schema;
use crate::scan::ArrowRecordBatchStream;
use crate::spec::{NestedField, PrimitiveType, SnapshotReference, SnapshotRetention, Type};
use crate::table::Table;

/// Refs table.
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
                Type::Primitive(PrimitiveType::Int),
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

        let mut names = StringBuilder::new();
        let mut ref_type = StringBuilder::new();
        let mut snapshot_id = PrimitiveBuilder::<Int64Type>::new();
        let mut max_reference_age_in_ms = PrimitiveBuilder::<Int64Type>::new();
        let mut min_keep = PrimitiveBuilder::<Int32Type>::new();
        let mut max_snapshot_age_in_ms = PrimitiveBuilder::<Int64Type>::new();

        let refs: &HashMap<String, SnapshotReference> = &self.table.metadata().refs;
        for (name, snapshot_ref) in refs {
            names.append_value(name);
            snapshot_id.append_value(snapshot_ref.snapshot_id);

            match &snapshot_ref.retention {
                SnapshotRetention::Branch {
                    min_snapshots_to_keep,
                    max_snapshot_age_ms,
                    max_ref_age_ms,
                } => {
                    ref_type.append_value("BRANCH");
                    max_reference_age_in_ms.append_option(*max_ref_age_ms);
                    min_keep.append_option(*min_snapshots_to_keep);
                    max_snapshot_age_in_ms.append_option(*max_snapshot_age_ms);
                }
                SnapshotRetention::Tag { max_ref_age_ms } => {
                    ref_type.append_value("TAG");
                    max_reference_age_in_ms.append_option(*max_ref_age_ms);
                    min_keep.append_null();
                    max_snapshot_age_in_ms.append_null();
                }
            }
        }

        let batch = RecordBatch::try_new(Arc::new(schema), vec![
            Arc::new(names.finish()),
            Arc::new(ref_type.finish()),
            Arc::new(snapshot_id.finish()),
            Arc::new(max_reference_age_in_ms.finish()),
            Arc::new(min_keep.finish()),
            Arc::new(max_snapshot_age_in_ms.finish()),
        ])?;

        Ok(stream::iter(vec![Ok(batch)]).boxed())
    }
}

#[cfg(test)]
mod tests {
    use expect_test::expect;

    use crate::inspect::metadata_table::tests::check_record_batches;
    use crate::scan::tests::TableTestFixture;
    use crate::spec::{SnapshotReference, SnapshotRetention};

    #[tokio::test]
    async fn test_refs_table() {
        let table = TableTestFixture::new().table;

        let snapshot_id = table
            .metadata()
            .snapshots()
            .next()
            .map(|s| s.snapshot_id())
            .expect("Table should have at least one snapshot");

        SnapshotReference::new(snapshot_id, SnapshotRetention::Branch {
            min_snapshots_to_keep: Some(10),
            max_snapshot_age_ms: Some(86400000),
            max_ref_age_ms: Some(604800000),
        });

        let batch_stream = table.inspect().refs().scan().await.unwrap();

        check_record_batches(
        batch_stream,
        expect![[r#"Field { name: "name", data_type: Utf8, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1"} },
Field { name: "type", data_type: Utf8, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "2"} },
Field { name: "snapshot_id", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "3"} },
Field { name: "max_reference_age_in_ms", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "4"} },
Field { name: "min_snapshots_to_keep", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "5"} },
Field { name: "max_snapshot_age_in_ms", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "6"} }"#]],
        expect![[r#"name: StringArray
[
  "test",
  "main",
],
type: StringArray
[
  "TAG",
  "BRANCH",
],
snapshot_id: PrimitiveArray<Int64>
[
  3051729675574597004,
  3055729675574597004,
],
max_reference_age_in_ms: PrimitiveArray<Int64>
[
  10000000,
  null,
],
min_snapshots_to_keep: PrimitiveArray<Int32>
[
  null,
  null,
],
max_snapshot_age_in_ms: PrimitiveArray<Int64>
[
  null,
  null,
]"#]],
        &[], // No columns to skip initially
        None, // No sort column initially
    ).await;
    }
}
