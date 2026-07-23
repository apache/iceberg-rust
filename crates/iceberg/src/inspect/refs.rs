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

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_array::builder::{PrimitiveBuilder, StringBuilder};
use arrow_array::types::{Int32Type, Int64Type};
use futures::{StreamExt, stream};

use crate::Result;
use crate::arrow::schema_to_arrow_schema;
use crate::scan::ArrowRecordBatchStream;
use crate::spec::{NestedField, PrimitiveType, SnapshotRetention, Type};
use crate::table::Table;

/// Refs table.
///
/// Contains one row for every named reference in the table metadata's `refs` map — branches
/// and tags — sorted by reference name. Each row carries the referenced snapshot id, the
/// reference type (`"BRANCH"` or `"TAG"`, uppercase, matching the Java implementation rather
/// than the lowercase spec serialization), and the retention fields: `max_reference_age_in_ms`
/// for both kinds, plus `min_snapshots_to_keep` and `max_snapshot_age_in_ms` for branches only
/// (null for tags).
///
/// Reference: <https://github.com/apache/iceberg/blob/main/core/src/main/java/org/apache/iceberg/RefsTable.java>
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

        let mut name = StringBuilder::new();
        let mut ref_type = StringBuilder::new();
        let mut snapshot_id = PrimitiveBuilder::<Int64Type>::new();
        let mut max_reference_age_in_ms = PrimitiveBuilder::<Int64Type>::new();
        let mut min_snapshots_to_keep = PrimitiveBuilder::<Int32Type>::new();
        let mut max_snapshot_age_in_ms = PrimitiveBuilder::<Int64Type>::new();

        // Collect into a `BTreeMap` first so the resulting rows are ordered
        // deterministically by ref name, since `TableMetadata::refs` is a `HashMap`.
        let refs: BTreeMap<_, _> = self.table.metadata().refs.iter().collect();
        for (ref_name, snapshot_ref) in refs {
            name.append_value(ref_name);
            snapshot_id.append_value(snapshot_ref.snapshot_id);
            match &snapshot_ref.retention {
                SnapshotRetention::Branch {
                    min_snapshots_to_keep: min_snapshots,
                    max_snapshot_age_ms,
                    max_ref_age_ms,
                } => {
                    ref_type.append_value("BRANCH");
                    max_reference_age_in_ms.append_option(*max_ref_age_ms);
                    min_snapshots_to_keep.append_option(*min_snapshots);
                    max_snapshot_age_in_ms.append_option(*max_snapshot_age_ms);
                }
                SnapshotRetention::Tag { max_ref_age_ms } => {
                    ref_type.append_value("TAG");
                    max_reference_age_in_ms.append_option(*max_ref_age_ms);
                    min_snapshots_to_keep.append_null();
                    max_snapshot_age_in_ms.append_null();
                }
            }
        }

        let batch = RecordBatch::try_new(Arc::new(schema), vec![
            Arc::new(name.finish()),
            Arc::new(ref_type.finish()),
            Arc::new(snapshot_id.finish()),
            Arc::new(max_reference_age_in_ms.finish()),
            Arc::new(min_snapshots_to_keep.finish()),
            Arc::new(max_snapshot_age_in_ms.finish()),
        ])?;

        Ok(stream::iter(vec![Ok(batch)]).boxed())
    }
}

#[cfg(test)]
mod tests {
    use expect_test::expect;
    use futures::TryStreamExt;
    use tempfile::TempDir;

    use crate::TableIdent;
    use crate::io::FileIO;
    use crate::scan::tests::TableTestFixture;
    use crate::spec::TableMetadata;
    use crate::table::Table;
    use crate::test_utils::{check_record_batches, test_runtime};

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
                Field { "min_snapshots_to_keep": nullable Int32, metadata: {"PARQUET:field_id": "5"} },
                Field { "max_snapshot_age_in_ms": nullable Int64, metadata: {"PARQUET:field_id": "6"} }"#]],
            expect![[r#"
                name: StringArray
                [
                  "main",
                  "test",
                ],
                type: StringArray
                [
                  "BRANCH",
                  "TAG",
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
            &[],
            Some("name"),
        );
    }

    #[tokio::test]
    async fn test_refs_table_retention_fields() {
        // Same shape as `testdata/example_table_metadata_v2.json`, but with the
        // "main" branch carrying explicit retention settings instead of the
        // implicit ref that `TableMetadata::try_normalize` would construct.
        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().join("table1");
        let table_metadata1_location = table_location.join("metadata/v1.json");

        let metadata_json = format!(
            r#"{{
              "format-version": 2,
              "table-uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c1",
              "location": "{table_location}",
              "last-sequence-number": 34,
              "last-updated-ms": 1602638573590,
              "last-column-id": 3,
              "current-schema-id": 1,
              "schemas": [
                {{
                  "type": "struct",
                  "schema-id": 0,
                  "fields": [
                    {{"id": 1, "name": "x", "required": true, "type": "long"}}
                  ]}},
                {{
                  "type": "struct",
                  "schema-id": 1,
                  "identifier-field-ids": [1, 2],
                  "fields": [
                    {{"id": 1, "name": "x", "required": true, "type": "long"}},
                    {{"id": 2, "name": "y", "required": true, "type": "long", "doc": "comment"}},
                    {{"id": 3, "name": "z", "required": true, "type": "long"}},
                    {{"id": 4, "name": "a", "required": true, "type": "string"}},
                    {{"id": 5, "name": "dbl", "required": true, "type": "double"}},
                    {{"id": 6, "name": "i32", "required": true, "type": "int"}},
                    {{"id": 7, "name": "i64", "required": true, "type": "long"}},
                    {{"id": 8, "name": "bool", "required": true, "type": "boolean"}}
                  ]
                }}
              ],
              "default-spec-id": 0,
              "partition-specs": [
                {{
                  "spec-id": 0,
                  "fields": [
                    {{"name": "x", "transform": "identity", "source-id": 1, "field-id": 1000}}
                  ]
                }}
              ],
              "last-partition-id": 1000,
              "default-sort-order-id": 3,
              "sort-orders": [
                {{
                  "order-id": 3,
                  "fields": [
                    {{"transform": "identity", "source-id": 2, "direction": "asc", "null-order": "nulls-first"}},
                    {{"transform": "bucket[4]", "source-id": 3, "direction": "desc", "null-order": "nulls-last"}}
                  ]
                }}
              ],
              "properties": {{"read.split.target.size": "134217728"}},
              "current-snapshot-id": 3055729675574597004,
              "snapshots": [
                {{
                  "snapshot-id": 3051729675574597004,
                  "timestamp-ms": 1515100955770,
                  "sequence-number": 0,
                  "summary": {{"operation": "append"}},
                  "manifest-list": "{table_location}/metadata/manifests_list_1.avro"
                }},
                {{
                  "snapshot-id": 3055729675574597004,
                  "parent-snapshot-id": 3051729675574597004,
                  "timestamp-ms": 1555100955770,
                  "sequence-number": 1,
                  "summary": {{"operation": "append"}},
                  "manifest-list": "{table_location}/metadata/manifests_list_2.avro",
                  "schema-id": 1
                }}
              ],
              "snapshot-log": [
                {{"snapshot-id": 3051729675574597004, "timestamp-ms": 1515100955770}},
                {{"snapshot-id": 3055729675574597004, "timestamp-ms": 1555100955770}}
              ],
              "metadata-log": [{{"metadata-file": "{table_location}/metadata/v1.json", "timestamp-ms": 1515100}}],
              "refs": {{
                "main": {{
                  "snapshot-id": 3055729675574597004,
                  "type": "branch",
                  "min-snapshots-to-keep": 5,
                  "max-snapshot-age-ms": 86400000,
                  "max-ref-age-ms": 259200000
                }},
                "test": {{"snapshot-id": 3051729675574597004, "type": "tag", "max-ref-age-ms": 10000000}}
              }}
            }}"#,
            table_location = table_location.to_str().unwrap(),
        );

        let table_metadata = serde_json::from_str::<TableMetadata>(&metadata_json).unwrap();

        let table = Table::builder()
            .metadata(table_metadata)
            .identifier(TableIdent::from_strs(["db", "table1"]).unwrap())
            .file_io(FileIO::new_with_fs())
            .metadata_location(table_metadata1_location.as_os_str().to_str().unwrap())
            .runtime(test_runtime())
            .build()
            .unwrap();

        let batch_stream = table.inspect().refs().scan().await.unwrap();

        check_record_batches(
            batch_stream.try_collect::<Vec<_>>().await.unwrap(),
            expect![[r#"
                Field { "name": Utf8, metadata: {"PARQUET:field_id": "1"} },
                Field { "type": Utf8, metadata: {"PARQUET:field_id": "2"} },
                Field { "snapshot_id": Int64, metadata: {"PARQUET:field_id": "3"} },
                Field { "max_reference_age_in_ms": nullable Int64, metadata: {"PARQUET:field_id": "4"} },
                Field { "min_snapshots_to_keep": nullable Int32, metadata: {"PARQUET:field_id": "5"} },
                Field { "max_snapshot_age_in_ms": nullable Int64, metadata: {"PARQUET:field_id": "6"} }"#]],
            expect![[r#"
                name: StringArray
                [
                  "main",
                  "test",
                ],
                type: StringArray
                [
                  "BRANCH",
                  "TAG",
                ],
                snapshot_id: PrimitiveArray<Int64>
                [
                  3055729675574597004,
                  3051729675574597004,
                ],
                max_reference_age_in_ms: PrimitiveArray<Int64>
                [
                  259200000,
                  10000000,
                ],
                min_snapshots_to_keep: PrimitiveArray<Int32>
                [
                  5,
                  null,
                ],
                max_snapshot_age_in_ms: PrimitiveArray<Int64>
                [
                  86400000,
                  null,
                ]"#]],
            &[],
            Some("name"),
        );
    }
}
