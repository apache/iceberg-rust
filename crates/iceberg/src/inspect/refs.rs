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
use arrow_array::builder::{Int32Builder, Int64Builder, StringBuilder};
use futures::{StreamExt, stream};

use crate::Result;
use crate::arrow::schema_to_arrow_schema;
use crate::scan::ArrowRecordBatchStream;
use crate::spec::{NestedField, PrimitiveType, SnapshotRetention, Type};
use crate::table::Table;

/// Snapshot references table.
pub struct RefsTable<'a> {
    table: &'a Table,
}

impl<'a> RefsTable<'a> {
    pub(crate) fn new(table: &'a Table) -> Self {
        Self { table }
    }

    /// Returns the Iceberg schema of the references table.
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
            .with_fields(fields.into_iter().map(Arc::new))
            .build()
            .unwrap()
    }

    /// Scans the references in the loaded table metadata.
    pub async fn scan(&self) -> Result<ArrowRecordBatchStream> {
        let schema = schema_to_arrow_schema(&self.schema())?;
        let mut name = StringBuilder::new();
        let mut ref_type = StringBuilder::new();
        let mut snapshot_id = Int64Builder::new();
        let mut max_reference_age_in_ms = Int64Builder::new();
        let mut min_snapshots_to_keep = Int32Builder::new();
        let mut max_snapshot_age_in_ms = Int64Builder::new();

        for (ref_name, reference) in &self.table.metadata().refs {
            name.append_value(ref_name);
            snapshot_id.append_value(reference.snapshot_id);
            match reference.retention {
                SnapshotRetention::Branch {
                    min_snapshots_to_keep: min_snapshots,
                    max_snapshot_age_ms,
                    max_ref_age_ms,
                } => {
                    ref_type.append_value("BRANCH");
                    max_reference_age_in_ms.append_option(max_ref_age_ms);
                    min_snapshots_to_keep.append_option(min_snapshots);
                    max_snapshot_age_in_ms.append_option(max_snapshot_age_ms);
                }
                SnapshotRetention::Tag { max_ref_age_ms } => {
                    ref_type.append_value("TAG");
                    max_reference_age_in_ms.append_option(max_ref_age_ms);
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
    use std::collections::HashMap;

    use arrow_array::{Int32Array, Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use futures::TryStreamExt;

    use super::*;
    use crate::inspect::MetadataTableType;
    use crate::spec::SnapshotReference;
    use crate::test_utils::scan::TableTestFixture;
    use crate::test_utils::test_runtime;

    fn expected_schema() -> Schema {
        Schema::new(
            vec![
                Field::new("name", DataType::Utf8, false),
                Field::new("type", DataType::Utf8, false),
                Field::new("snapshot_id", DataType::Int64, false),
                Field::new("max_reference_age_in_ms", DataType::Int64, true),
                Field::new("min_snapshots_to_keep", DataType::Int32, true),
                Field::new("max_snapshot_age_in_ms", DataType::Int64, true),
            ]
            .into_iter()
            .enumerate()
            .map(|(i, field)| {
                field.with_metadata(HashMap::from([(
                    "PARQUET:field_id".to_string(),
                    (i + 1).to_string(),
                )]))
            })
            .collect::<Vec<_>>(),
        )
    }

    #[tokio::test]
    async fn refs_include_branches_tags_and_nullable_retention() {
        let fixture = TableTestFixture::new();
        let mut metadata = fixture.table.metadata().clone();
        let current_id = metadata.current_snapshot_id().unwrap();
        let older_id = 3051729675574597004;
        metadata.refs = HashMap::from([
            (
                "main".to_string(),
                SnapshotReference::new(current_id, SnapshotRetention::branch(None, None, None)),
            ),
            (
                "branch".to_string(),
                SnapshotReference::new(
                    older_id,
                    SnapshotRetention::branch(Some(3), Some(86_400_000), Some(604_800_000)),
                ),
            ),
            (
                "tag".to_string(),
                SnapshotReference::new(older_id, SnapshotRetention::Tag {
                    max_ref_age_ms: Some(172_800_000),
                }),
            ),
            (
                "tag_without_retention".to_string(),
                SnapshotReference::new(current_id, SnapshotRetention::Tag {
                    max_ref_age_ms: None,
                }),
            ),
        ]);
        let table = Table::builder()
            .metadata(metadata)
            .identifier(fixture.table.identifier().clone())
            .file_io(fixture.table.file_io().clone())
            .runtime(test_runtime())
            .build()
            .unwrap();
        let batches: Vec<_> = table
            .inspect()
            .refs()
            .scan()
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        let batch = &batches[0];
        let indices = arrow_ord::sort::sort_to_indices(batch.column(0), None, None).unwrap();
        let actual = arrow_select::take::take_record_batch(batch, &indices).unwrap();
        let expected = RecordBatch::try_new(Arc::new(expected_schema()), vec![
            Arc::new(StringArray::from(vec![
                "branch",
                "main",
                "tag",
                "tag_without_retention",
            ])),
            Arc::new(StringArray::from(vec!["BRANCH", "BRANCH", "TAG", "TAG"])),
            Arc::new(Int64Array::from(vec![
                older_id, current_id, older_id, current_id,
            ])),
            Arc::new(Int64Array::from(vec![
                Some(604_800_000),
                None,
                Some(172_800_000),
                None,
            ])),
            Arc::new(Int32Array::from(vec![Some(3), None, None, None])),
            Arc::new(Int64Array::from(vec![Some(86_400_000), None, None, None])),
        ])
        .unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(actual, expected);
    }

    #[tokio::test]
    async fn refs_include_implicit_main() {
        let fixture = TableTestFixture::new();
        let batches: Vec<_> = fixture
            .table
            .inspect()
            .refs()
            .scan()
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        let names = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(names.iter().any(|name| name == Some("main")));
    }

    #[tokio::test]
    async fn refs_are_empty_without_snapshots() {
        let fixture = TableTestFixture::new_empty();
        let batches: Vec<_> = fixture
            .table
            .inspect()
            .refs()
            .scan()
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert_eq!(batches, vec![RecordBatch::new_empty(Arc::new(
            expected_schema()
        ))]);
    }

    #[test]
    fn refs_metadata_type_round_trips() {
        assert!(matches!(
            MetadataTableType::try_from("refs"),
            Ok(MetadataTableType::Refs)
        ));
        assert_eq!(MetadataTableType::Refs.as_str(), "refs");
        assert!(MetadataTableType::all_types().any(|kind| matches!(kind, MetadataTableType::Refs)));
    }
}
