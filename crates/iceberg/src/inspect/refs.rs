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

/// Refs table.
///
/// Exposes a table's known snapshot references (branches + tags) as rows. Mirrors Java `RefsTable`.
pub struct RefsTable<'a> {
    table: &'a Table,
}

impl<'a> RefsTable<'a> {
    /// Create a new Refs table instance.
    pub fn new(table: &'a Table) -> Self {
        Self { table }
    }

    /// Returns the iceberg schema of the refs table.
    ///
    /// Field ids mirror Java `RefsTable.SNAPSHOT_REF_SCHEMA`.
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
            .expect("refs metadata table schema is statically valid")
    }

    /// Scans the refs table.
    pub async fn scan(&self) -> Result<ArrowRecordBatchStream> {
        let schema = schema_to_arrow_schema(&self.schema())?;
        let metadata = self.table.metadata();

        let mut name = StringBuilder::new();
        let mut ref_type = StringBuilder::new();
        let mut snapshot_id = Int64Builder::new();
        let mut max_reference_age_in_ms = Int64Builder::new();
        let mut min_snapshots_to_keep = Int32Builder::new();
        let mut max_snapshot_age_in_ms = Int64Builder::new();

        // Sort by ref name for deterministic output (the underlying map is unordered).
        let mut refs: Vec<_> = metadata.refs.iter().collect();
        refs.sort_by(|(a, _), (b, _)| a.cmp(b));

        for (ref_name, reference) in refs {
            name.append_value(ref_name);
            snapshot_id.append_value(reference.snapshot_id);
            match &reference.retention {
                SnapshotRetention::Branch {
                    min_snapshots_to_keep: min,
                    max_snapshot_age_ms: max_age,
                    max_ref_age_ms: max_ref,
                } => {
                    ref_type.append_value("BRANCH");
                    max_reference_age_in_ms.append_option(*max_ref);
                    min_snapshots_to_keep.append_option(*min);
                    max_snapshot_age_in_ms.append_option(*max_age);
                }
                SnapshotRetention::Tag {
                    max_ref_age_ms: max_ref,
                } => {
                    ref_type.append_value("TAG");
                    max_reference_age_in_ms.append_option(*max_ref);
                    // Tags carry only max_reference_age; the branch-only fields are always NULL.
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
    use std::fs::File;
    use std::io::BufReader;
    use std::sync::Arc;

    use arrow_array::Array;
    use arrow_array::cast::AsArray;
    use arrow_array::types::{Int32Type, Int64Type};
    use expect_test::expect;
    use futures::TryStreamExt;

    use crate::TableIdent;
    use crate::io::FileIO;
    use crate::spec::{SnapshotReference, SnapshotRetention, TableMetadata};
    use crate::table::Table;
    use crate::test_utils::check_record_batches;

    const CURRENT: i64 = 3055729675574597004;
    const ROOT: i64 = 3051729675574597004;

    /// Builds a `Table` from `TableMetadataV2Valid.json` (local copy of `transaction::tests::make_v2_table`,
    /// which is private to `transaction/`). Has only the synthesized `main` branch.
    fn make_v2_table() -> Table {
        let file = File::open(format!(
            "{}/testdata/table_metadata/{}",
            env!("CARGO_MANIFEST_DIR"),
            "TableMetadataV2Valid.json"
        ))
        .unwrap();
        let reader = BufReader::new(file);
        let resp = serde_json::from_reader::<_, TableMetadata>(reader).unwrap();

        Table::builder()
            .metadata(resp)
            .metadata_location("s3://bucket/test/location/metadata/v1.json".to_string())
            .identifier(TableIdent::from_strs(["ns1", "test1"]).unwrap())
            .file_io(FileIO::new_with_memory())
            .build()
            .unwrap()
    }

    async fn scan_to_batches(table: &Table) -> Vec<arrow_array::RecordBatch> {
        table
            .inspect()
            .refs()
            .scan()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
    }

    /// `make_v2_table()` (which has only the synthesized `main` branch) plus a `dev` branch WITH retention
    /// set (min_snapshots_to_keep + max_snapshot_age_ms + max_ref_age_ms) and a `release` tag with only
    /// max_ref_age set. Exercises BRANCH-with-retention, TAG-only-max-ref-age, and NULL-where-unset.
    fn table_with_branch_and_tag() -> Table {
        let base = make_v2_table();
        let metadata: TableMetadata = base.metadata().clone();
        let metadata = metadata
            .into_builder(None)
            .set_ref(
                "dev",
                SnapshotReference::new(
                    CURRENT,
                    SnapshotRetention::branch(Some(2), Some(86_400_000), Some(604_800_000)),
                ),
            )
            .expect("add dev branch")
            .set_ref(
                "release",
                SnapshotReference::new(ROOT, SnapshotRetention::Tag {
                    max_ref_age_ms: Some(259_200_000),
                }),
            )
            .expect("add release tag")
            .build()
            .expect("build")
            .metadata;
        base.with_metadata(Arc::new(metadata))
    }

    /// RISK: the Arrow schema (columns + field ids + types) must match Java `SNAPSHOT_REF_SCHEMA`.
    #[tokio::test]
    async fn test_refs_table_arrow_schema_columns_field_ids_and_types() {
        let table = table_with_branch_and_tag();
        let batches = scan_to_batches(&table).await;
        check_record_batches(
            batches,
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
                  "dev",
                  "main",
                  "release",
                ],
                type: StringArray
                [
                  "BRANCH",
                  "BRANCH",
                  "TAG",
                ],
                snapshot_id: PrimitiveArray<Int64>
                [
                  3055729675574597004,
                  3055729675574597004,
                  3051729675574597004,
                ],
                max_reference_age_in_ms: PrimitiveArray<Int64>
                [
                  604800000,
                  null,
                  259200000,
                ],
                min_snapshots_to_keep: PrimitiveArray<Int32>
                [
                  2,
                  null,
                  null,
                ],
                max_snapshot_age_in_ms: PrimitiveArray<Int64>
                [
                  86400000,
                  null,
                  null,
                ]"#]],
            &[],
            Some("name"),
        );
    }

    /// RISK: `type` must be BRANCH for a branch and TAG for a tag; one row per ref.
    #[tokio::test]
    async fn test_refs_table_type_branch_vs_tag_and_row_per_ref() {
        let table = table_with_branch_and_tag();
        let batches = scan_to_batches(&table).await;
        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 3, "main, dev, release");

        let name = batch.column(0).as_string::<i32>();
        let ref_type = batch.column(1).as_string::<i32>();
        let mut types = std::collections::HashMap::new();
        for i in 0..batch.num_rows() {
            types.insert(name.value(i).to_string(), ref_type.value(i).to_string());
        }
        assert_eq!(types.get("main").map(String::as_str), Some("BRANCH"));
        assert_eq!(types.get("dev").map(String::as_str), Some("BRANCH"));
        assert_eq!(types.get("release").map(String::as_str), Some("TAG"));
    }

    /// RISK: retention columns — a branch with retention set populates all three; an unset branch (main)
    /// is NULL everywhere; a tag populates ONLY max_reference_age, NULL for the branch-only fields.
    #[tokio::test]
    async fn test_refs_table_retention_populated_and_null_per_kind() {
        let table = table_with_branch_and_tag();
        let batches = scan_to_batches(&table).await;
        let batch = &batches[0];

        let name = batch.column(0).as_string::<i32>();
        let max_ref_age = batch.column(3).as_primitive::<Int64Type>();
        let min_keep = batch.column(4).as_primitive::<Int32Type>();
        let max_snap_age = batch.column(5).as_primitive::<Int64Type>();

        let mut idx = std::collections::HashMap::new();
        for i in 0..batch.num_rows() {
            idx.insert(name.value(i).to_string(), i);
        }

        // dev: branch with all retention set.
        let dev = idx["dev"];
        assert_eq!(max_ref_age.value(dev), 604_800_000);
        assert_eq!(min_keep.value(dev), 2);
        assert_eq!(max_snap_age.value(dev), 86_400_000);

        // main: branch with no retention → all NULL.
        let main = idx["main"];
        assert!(max_ref_age.is_null(main));
        assert!(min_keep.is_null(main));
        assert!(max_snap_age.is_null(main));

        // release: tag → only max_reference_age set; branch-only fields NULL.
        let release = idx["release"];
        assert_eq!(max_ref_age.value(release), 259_200_000);
        assert!(
            min_keep.is_null(release),
            "tag has no min_snapshots_to_keep"
        );
        assert!(
            max_snap_age.is_null(release),
            "tag has no max_snapshot_age_in_ms"
        );
    }
}
