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

use std::collections::HashSet;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_array::builder::{BooleanBuilder, PrimitiveBuilder};
use arrow_array::types::{Int64Type, TimestampMicrosecondType};
use futures::{StreamExt, stream};

use crate::Result;
use crate::arrow::schema_to_arrow_schema;
use crate::scan::ArrowRecordBatchStream;
use crate::spec::{NestedField, PrimitiveType, Type};
use crate::table::Table;

/// History table.
///
/// Exposes a table's history as rows — one per entry in the table's snapshot log (which records each
/// update to the table's current snapshot). Mirrors Java `HistoryTable`.
pub struct HistoryTable<'a> {
    table: &'a Table,
}

impl<'a> HistoryTable<'a> {
    /// Create a new History table instance.
    pub fn new(table: &'a Table) -> Self {
        Self { table }
    }

    /// Returns the iceberg schema of the history table.
    ///
    /// Field ids mirror Java `HistoryTable.HISTORY_SCHEMA`.
    pub fn schema(&self) -> crate::spec::Schema {
        let fields = vec![
            NestedField::required(
                1,
                "made_current_at",
                Type::Primitive(PrimitiveType::Timestamptz),
            ),
            NestedField::required(2, "snapshot_id", Type::Primitive(PrimitiveType::Long)),
            NestedField::optional(3, "parent_id", Type::Primitive(PrimitiveType::Long)),
            NestedField::required(
                4,
                "is_current_ancestor",
                Type::Primitive(PrimitiveType::Boolean),
            ),
        ];
        crate::spec::Schema::builder()
            .with_fields(fields.into_iter().map(|f| f.into()))
            .build()
            .expect("history metadata table schema is statically valid")
    }

    /// Scans the history table.
    pub async fn scan(&self) -> Result<ArrowRecordBatchStream> {
        let schema = schema_to_arrow_schema(&self.schema())?;
        let metadata = self.table.metadata();

        // The set of snapshot ids that are ancestors of (or are) the current snapshot — i.e. the
        // parent chain walked from `current_snapshot_id`. Mirrors Java `SnapshotUtil.currentAncestorIds`.
        let current_ancestors = current_ancestor_ids(metadata);

        let mut made_current_at =
            PrimitiveBuilder::<TimestampMicrosecondType>::new().with_timezone("+00:00");
        let mut snapshot_id = PrimitiveBuilder::<Int64Type>::new();
        let mut parent_id = PrimitiveBuilder::<Int64Type>::new();
        let mut is_current_ancestor = BooleanBuilder::new();

        for entry in metadata.history() {
            made_current_at.append_value(entry.timestamp_ms * 1000);
            snapshot_id.append_value(entry.snapshot_id);
            // `parent_id` is the SNAPSHOT's parent (nullable), not the previous log entry.
            parent_id.append_option(
                metadata
                    .snapshot_by_id(entry.snapshot_id)
                    .and_then(|s| s.parent_snapshot_id()),
            );
            is_current_ancestor.append_value(current_ancestors.contains(&entry.snapshot_id));
        }

        let batch = RecordBatch::try_new(Arc::new(schema), vec![
            Arc::new(made_current_at.finish()),
            Arc::new(snapshot_id.finish()),
            Arc::new(parent_id.finish()),
            Arc::new(is_current_ancestor.finish()),
        ])?;

        Ok(stream::iter(vec![Ok(batch)]).boxed())
    }
}

/// Returns the set of snapshot ids reachable from the table's current snapshot by following
/// `parent_snapshot_id` (inclusive of the current snapshot). Mirrors Java
/// `SnapshotUtil.currentAncestorIds(table)`.
fn current_ancestor_ids(metadata: &crate::spec::TableMetadata) -> HashSet<i64> {
    let mut ids = HashSet::new();
    let mut current = metadata.current_snapshot_id();
    while let Some(id) = current {
        if !ids.insert(id) {
            // Defensive: a malformed parent cycle would otherwise loop forever.
            break;
        }
        current = metadata
            .snapshot_by_id(id)
            .and_then(|s| s.parent_snapshot_id());
    }
    ids
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fs::File;
    use std::io::BufReader;
    use std::sync::Arc;

    use arrow_array::Array;
    use arrow_array::cast::AsArray;
    use arrow_array::types::{Int64Type, TimestampMicrosecondType};
    use expect_test::expect;
    use futures::TryStreamExt;

    use crate::TableIdent;
    use crate::io::FileIO;
    use crate::spec::{
        Operation, Snapshot, SnapshotReference, SnapshotRetention, Summary, TableMetadata,
    };
    use crate::table::Table;
    use crate::test_utils::check_record_batches;

    /// Builds a `Table` from the committed `TableMetadataV2Valid.json` fixture (2 snapshots: ROOT → CURRENT,
    /// a 2-entry snapshot log, an empty metadata log, no explicit refs → `main` synthesized). Local copy of
    /// `transaction::tests::make_v2_table` (that module is private to `transaction/`).
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

    // From TableMetadataV2Valid.json: main -> CURRENT (3055...), whose parent is the root (3051...).
    const CURRENT: i64 = 3055729675574597004;
    const ROOT: i64 = 3051729675574597004;
    // A snapshot grafted as a SIBLING of CURRENT (also a child of ROOT): it has a snapshot-log entry but
    // is NOT in main's current ancestry — the only way to make `is_current_ancestor` false.
    const SIBLING: i64 = 3060729675574597004;

    async fn scan_to_batches(table: &Table) -> Vec<arrow_array::RecordBatch> {
        table
            .inspect()
            .history()
            .scan()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
    }

    /// `make_v2_table()` evolved so the snapshot log contains a SIBLING that is NOT a current-ancestor.
    ///
    /// This must be done in TWO separate commits: a single transaction that makes `main` point at SIBLING
    /// then rolls it back to CURRENT would treat SIBLING as an *intermediate* snapshot and drop it from the
    /// snapshot log (`TableMetadataBuilder::update_snapshot_log`, mirroring Java). So commit 1 grafts SIBLING
    /// and makes `main` point at it (SIBLING is now the persisted current snapshot, in the log); commit 2
    /// rolls `main` back to CURRENT. The final current parent chain is {CURRENT, ROOT}; SIBLING remains a
    /// log row but is forked off, so its `is_current_ancestor` must be false — the only non-trivial column.
    fn forked_table() -> Table {
        let base = make_v2_table();
        let sibling = Snapshot::builder()
            .with_snapshot_id(SIBLING)
            .with_parent_snapshot_id(Some(ROOT))
            .with_sequence_number(35)
            .with_timestamp_ms(1700000000000)
            .with_manifest_list("/tmp/sibling-manifest-list.avro")
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: HashMap::new(),
            })
            .with_schema_id(1)
            .build();

        // Commit 1: graft SIBLING and make it the current snapshot (its own commit, so it is not
        // intermediate and stays in the snapshot log).
        let after_sibling = base
            .metadata()
            .clone()
            .into_builder(None)
            .add_snapshot(sibling)
            .expect("add sibling")
            .set_ref(
                "main",
                SnapshotReference::new(SIBLING, SnapshotRetention::branch(None, None, None)),
            )
            .expect("set main to sibling")
            .build()
            .expect("build after sibling")
            .metadata;

        // Commit 2: roll `main` back to CURRENT. SIBLING is now a historical log entry, not intermediate.
        let metadata = after_sibling
            .into_builder(None)
            .set_ref(
                "main",
                SnapshotReference::new(CURRENT, SnapshotRetention::branch(None, None, None)),
            )
            .expect("roll main back to current")
            .build()
            .expect("build forked")
            .metadata;
        base.with_metadata(Arc::new(metadata))
    }

    /// RISK: the Arrow schema (columns + field ids + types) must match Java `HISTORY_SCHEMA`.
    #[tokio::test]
    async fn test_history_table_arrow_schema_columns_field_ids_and_types() {
        let table = make_v2_table();
        let batches = table
            .inspect()
            .history()
            .scan()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        check_record_batches(
            batches,
            expect![[r#"
                Field { "made_current_at": Timestamp(µs, "+00:00"), metadata: {"PARQUET:field_id": "1"} },
                Field { "snapshot_id": Int64, metadata: {"PARQUET:field_id": "2"} },
                Field { "parent_id": nullable Int64, metadata: {"PARQUET:field_id": "3"} },
                Field { "is_current_ancestor": Boolean, metadata: {"PARQUET:field_id": "4"} }"#]],
            expect![[r#"
                made_current_at: PrimitiveArray<Timestamp(µs, "+00:00")>
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
                is_current_ancestor: BooleanArray
                [
                  true,
                  true,
                ]"#]],
            &[],
            Some("made_current_at"),
        );
    }

    /// RISK: one row per snapshot-log entry; `snapshot_id`/`parent_id` track the SNAPSHOT (parent of the
    /// snapshot, not the previous log row). The straight-line fixture has both snapshots as current-ancestors.
    #[tokio::test]
    async fn test_history_table_row_per_log_entry_with_snapshot_parent() {
        let table = make_v2_table();
        let batches = scan_to_batches(&table).await;
        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 2);

        let snapshot_id = batch.column(1).as_primitive::<Int64Type>();
        let parent_id = batch.column(2).as_primitive::<Int64Type>();
        // Log order = ROOT then CURRENT.
        assert_eq!(snapshot_id.value(0), ROOT);
        assert!(parent_id.is_null(0)); // ROOT has no parent.
        assert_eq!(snapshot_id.value(1), CURRENT);
        assert_eq!(parent_id.value(1), ROOT); // CURRENT's parent is ROOT.
    }

    /// RISK (the only non-trivial column): a snapshot that is NOT on the current parent chain must have
    /// `is_current_ancestor == false`, while the current snapshot + its ancestors are true.
    #[tokio::test]
    async fn test_history_table_is_current_ancestor_false_for_forked_snapshot() {
        let table = forked_table();
        let batches = scan_to_batches(&table).await;
        let batch = &batches[0];
        // Log entries: ROOT, CURRENT, SIBLING, CURRENT (the rollback re-stamps CURRENT) → 4 rows.
        assert_eq!(
            batch.num_rows(),
            4,
            "ROOT, CURRENT, SIBLING, CURRENT(rollback)"
        );

        let snapshot_id = batch.column(1).as_primitive::<Int64Type>();
        let is_ancestor = batch.column(3).as_boolean();
        // Every row's is_current_ancestor must be consistent for a given snapshot id; collect per id.
        let mut by_id = std::collections::HashMap::new();
        for i in 0..batch.num_rows() {
            let prev = by_id.insert(snapshot_id.value(i), is_ancestor.value(i));
            if let Some(prev) = prev {
                assert_eq!(
                    prev,
                    is_ancestor.value(i),
                    "duplicate log rows for one snapshot must agree on is_current_ancestor"
                );
            }
        }
        // ROOT and CURRENT are on main's parent chain → true; SIBLING is forked off → false.
        assert_eq!(by_id.get(&ROOT), Some(&true));
        assert_eq!(by_id.get(&CURRENT), Some(&true));
        assert_eq!(by_id.get(&SIBLING), Some(&false));
    }

    /// RISK: `made_current_at` is the snapshot-LOG timestamp (millis) converted to micros UTC, not the
    /// snapshot's own timestamp (here they coincide, but the conversion factor must be 1000).
    #[tokio::test]
    async fn test_history_table_made_current_at_is_log_timestamp_in_micros() {
        let table = make_v2_table();
        let batches = scan_to_batches(&table).await;
        let batch = &batches[0];
        let made_current_at = batch.column(0).as_primitive::<TimestampMicrosecondType>();
        // ROOT snapshot-log entry: timestamp-ms 1515100955770 → micros 1515100955770000.
        assert_eq!(made_current_at.value(0), 1515100955770 * 1000);
        assert_eq!(made_current_at.value(1), 1555100955770 * 1000);
    }
}
