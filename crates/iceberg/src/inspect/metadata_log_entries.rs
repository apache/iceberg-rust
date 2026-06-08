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
use arrow_array::builder::{Int32Builder, Int64Builder, PrimitiveBuilder, StringBuilder};
use arrow_array::types::TimestampMicrosecondType;
use futures::{StreamExt, stream};

use crate::Result;
use crate::arrow::schema_to_arrow_schema;
use crate::scan::ArrowRecordBatchStream;
use crate::spec::{NestedField, PrimitiveType, TableMetadata, Type};
use crate::table::Table;

/// Metadata log entries table.
///
/// Exposes each entry in a table's metadata log (the previous metadata files plus the current one) as a
/// row. Mirrors Java `MetadataLogEntriesTable`.
pub struct MetadataLogEntriesTable<'a> {
    table: &'a Table,
}

impl<'a> MetadataLogEntriesTable<'a> {
    /// Create a new Metadata Log Entries table instance.
    pub fn new(table: &'a Table) -> Self {
        Self { table }
    }

    /// Returns the iceberg schema of the metadata log entries table.
    ///
    /// Field ids mirror Java `MetadataLogEntriesTable.METADATA_LOG_ENTRIES_SCHEMA`.
    pub fn schema(&self) -> crate::spec::Schema {
        let fields = vec![
            NestedField::required(1, "timestamp", Type::Primitive(PrimitiveType::Timestamptz)),
            NestedField::required(2, "file", Type::Primitive(PrimitiveType::String)),
            NestedField::optional(
                3,
                "latest_snapshot_id",
                Type::Primitive(PrimitiveType::Long),
            ),
            NestedField::optional(4, "latest_schema_id", Type::Primitive(PrimitiveType::Int)),
            NestedField::optional(
                5,
                "latest_sequence_number",
                Type::Primitive(PrimitiveType::Long),
            ),
        ];
        crate::spec::Schema::builder()
            .with_fields(fields.into_iter().map(|f| f.into()))
            .build()
            .expect("metadata log entries metadata table schema is statically valid")
    }

    /// Scans the metadata log entries table.
    pub async fn scan(&self) -> Result<ArrowRecordBatchStream> {
        let schema = schema_to_arrow_schema(&self.schema())?;
        let metadata = self.table.metadata();

        let mut timestamp =
            PrimitiveBuilder::<TimestampMicrosecondType>::new().with_timezone("+00:00");
        let mut file = StringBuilder::new();
        let mut latest_snapshot_id = Int64Builder::new();
        let mut latest_schema_id = Int32Builder::new();
        let mut latest_sequence_number = Int64Builder::new();

        // The log = previous metadata files + a synthetic final entry for the CURRENT metadata file
        // (Java appends `MetadataLogEntry(lastUpdatedMillis, metadataFileLocation)`).
        let current_file = self.table.metadata_location().unwrap_or("");
        let entries = metadata
            .metadata_log()
            .iter()
            .map(|e| (e.timestamp_ms, e.metadata_file.as_str()))
            .chain(std::iter::once((metadata.last_updated_ms(), current_file)));

        for (timestamp_ms, metadata_file) in entries {
            timestamp.append_value(timestamp_ms * 1000);
            file.append_value(metadata_file);

            // `latest_*` come from the snapshot that was current at this metadata-log entry's timestamp:
            // the last snapshot-log entry with `made_current_at <= timestamp` (Java
            // `SnapshotUtil.snapshotIdAsOfTime`). NULL when no snapshot is at/older than the timestamp
            // (e.g. a table-creation metadata file written before the first snapshot).
            match snapshot_id_as_of_time(metadata, timestamp_ms) {
                Some(snapshot_id) => {
                    latest_snapshot_id.append_value(snapshot_id);
                    let snapshot = metadata.snapshot_by_id(snapshot_id);
                    latest_schema_id.append_option(snapshot.and_then(|s| s.schema_id()));
                    latest_sequence_number.append_option(snapshot.map(|s| s.sequence_number()));
                }
                None => {
                    latest_snapshot_id.append_null();
                    latest_schema_id.append_null();
                    latest_sequence_number.append_null();
                }
            }
        }

        let batch = RecordBatch::try_new(Arc::new(schema), vec![
            Arc::new(timestamp.finish()),
            Arc::new(file.finish()),
            Arc::new(latest_snapshot_id.finish()),
            Arc::new(latest_schema_id.finish()),
            Arc::new(latest_sequence_number.finish()),
        ])?;

        Ok(stream::iter(vec![Ok(batch)]).boxed())
    }
}

/// Returns the id of the snapshot that was current at `timestamp_ms` — the last snapshot-log entry whose
/// `timestamp_ms` is `<=` the argument — or `None` if no snapshot is at/older than that time. Mirrors Java
/// `SnapshotUtil.nullableSnapshotIdAsOfTime`.
fn snapshot_id_as_of_time(metadata: &TableMetadata, timestamp_ms: i64) -> Option<i64> {
    let mut snapshot_id = None;
    for entry in metadata.history() {
        if entry.timestamp_ms <= timestamp_ms {
            snapshot_id = Some(entry.snapshot_id);
        }
    }
    snapshot_id
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::io::BufReader;
    use std::sync::Arc;

    use arrow_array::Array;
    use arrow_array::cast::AsArray;
    use arrow_array::types::{Int32Type, Int64Type, TimestampMicrosecondType};
    use expect_test::expect;
    use futures::TryStreamExt;

    use crate::TableIdent;
    use crate::io::FileIO;
    use crate::spec::{MetadataLog, TableMetadata};
    use crate::table::Table;
    use crate::test_utils::check_record_batches;

    const CURRENT: i64 = 3055729675574597004;
    const ROOT: i64 = 3051729675574597004;

    /// Builds a `Table` from `TableMetadataV2Valid.json` (local copy of `transaction::tests::make_v2_table`,
    /// which is private to `transaction/`). Has an EMPTY metadata log out of the box.
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
    // The two snapshot-log timestamps from TableMetadataV2Valid.json.
    const ROOT_TS: i64 = 1515100955770;
    const CURRENT_TS: i64 = 1555100955770;

    async fn scan_to_batches(table: &Table) -> Vec<arrow_array::RecordBatch> {
        table
            .inspect()
            .metadata_log_entries()
            .scan()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
    }

    /// `make_v2_table()` (empty metadata-log) augmented with two previous metadata files in the log so the
    /// table looks like it has been committed several times. The two prior entries straddle the snapshot
    /// timestamps: one BEFORE any snapshot (creation-time → latest_* NULL), one AFTER ROOT but before
    /// CURRENT (→ ROOT). The synthetic current entry (last_updated_ms) is after CURRENT (→ CURRENT).
    fn table_with_metadata_log() -> Table {
        let base = make_v2_table();
        // The metadata-log is a `pub(crate)` field (reachable from this in-crate test). Set it explicitly
        // so the entries deterministically straddle the snapshot timestamps — that is exactly what the
        // `latest_*` resolution must key off, and it avoids depending on wall-clock commit timestamps.
        let mut meta: TableMetadata = base.metadata().clone();
        meta.metadata_log = vec![
            MetadataLog {
                metadata_file: "s3://bucket/metadata/00000-creation.metadata.json".to_string(),
                timestamp_ms: ROOT_TS - 1000, // before any snapshot
            },
            MetadataLog {
                metadata_file: "s3://bucket/metadata/00001-after-root.metadata.json".to_string(),
                timestamp_ms: ROOT_TS + 1000, // after ROOT, before CURRENT
            },
        ];
        base.with_metadata(Arc::new(meta))
    }

    /// RISK: the Arrow schema (columns + field ids + types) must match Java `METADATA_LOG_ENTRIES_SCHEMA`.
    #[tokio::test]
    async fn test_metadata_log_entries_table_arrow_schema_columns_field_ids_and_types() {
        let table = make_v2_table();
        let batches = scan_to_batches(&table).await;
        check_record_batches(
            batches,
            expect![[r#"
                Field { "timestamp": Timestamp(µs, "+00:00"), metadata: {"PARQUET:field_id": "1"} },
                Field { "file": Utf8, metadata: {"PARQUET:field_id": "2"} },
                Field { "latest_snapshot_id": nullable Int64, metadata: {"PARQUET:field_id": "3"} },
                Field { "latest_schema_id": nullable Int32, metadata: {"PARQUET:field_id": "4"} },
                Field { "latest_sequence_number": nullable Int64, metadata: {"PARQUET:field_id": "5"} }"#]],
            expect![[r#"
                timestamp: PrimitiveArray<Timestamp(µs, "+00:00")>
                [
                  2020-10-14T01:22:53.590+00:00,
                ],
                file: (skipped),
                latest_snapshot_id: PrimitiveArray<Int64>
                [
                  3055729675574597004,
                ],
                latest_schema_id: PrimitiveArray<Int32>
                [
                  1,
                ],
                latest_sequence_number: PrimitiveArray<Int64>
                [
                  1,
                ]"#]],
            &["file"],
            Some("timestamp"),
        );
    }

    /// RISK: rows = each metadata-log entry PLUS the synthetic current entry; `file` + `timestamp` faithful.
    #[tokio::test]
    async fn test_metadata_log_entries_table_row_per_log_entry_plus_current() {
        let table = table_with_metadata_log();
        let batches = scan_to_batches(&table).await;
        let batch = &batches[0];
        // 2 prior log entries + 1 synthetic current entry.
        assert_eq!(batch.num_rows(), 3);

        let file = batch.column(1).as_string::<i32>();
        let timestamp = batch.column(0).as_primitive::<TimestampMicrosecondType>();

        // The current metadata file is the last row (sorted by timestamp ascending here).
        let mut files: Vec<&str> = (0..batch.num_rows()).map(|i| file.value(i)).collect();
        files.sort();
        assert!(files.contains(&"s3://bucket/metadata/00000-creation.metadata.json"));
        assert!(files.contains(&"s3://bucket/metadata/00001-after-root.metadata.json"));
        // The synthetic entry carries the table's metadata_location.
        assert!(
            files
                .iter()
                .any(|f| f.contains("s3://bucket/test/location/metadata/v1.json"))
        );

        // Timestamps are micros UTC = log millis * 1000. Earliest = creation entry.
        assert_eq!(timestamp.value(0), (ROOT_TS - 1000) * 1000);
    }

    /// RISK (the derived columns): `latest_*` resolve to the snapshot current AT each entry's timestamp —
    /// NULL before the first snapshot, ROOT after ROOT/before CURRENT, CURRENT after CURRENT.
    #[tokio::test]
    async fn test_metadata_log_entries_table_latest_columns_resolve_snapshot_as_of_time() {
        let table = table_with_metadata_log();
        let batches = scan_to_batches(&table).await;
        let batch = &batches[0];

        let timestamp = batch.column(0).as_primitive::<TimestampMicrosecondType>();
        let latest_snapshot_id = batch.column(2).as_primitive::<Int64Type>();
        let latest_schema_id = batch.column(3).as_primitive::<Int32Type>();
        let latest_sequence_number = batch.column(4).as_primitive::<Int64Type>();

        // Index rows by their timestamp (millis).
        let mut by_ts = std::collections::HashMap::new();
        for i in 0..batch.num_rows() {
            by_ts.insert(timestamp.value(i) / 1000, i);
        }

        // Creation entry (before ROOT): no snapshot ≤ ts → all NULL.
        let creation = by_ts[&(ROOT_TS - 1000)];
        assert!(
            latest_snapshot_id.is_null(creation),
            "no snapshot existed at creation time"
        );
        assert!(latest_schema_id.is_null(creation));
        assert!(latest_sequence_number.is_null(creation));

        // After-ROOT entry (ROOT_TS+1000, before CURRENT_TS): the current snapshot is ROOT.
        let after_root = by_ts[&(ROOT_TS + 1000)];
        assert_eq!(latest_snapshot_id.value(after_root), ROOT);
        // ROOT's sequence number is 0 (V1-carryover); schema_id is None in the fixture → NULL.
        assert_eq!(latest_sequence_number.value(after_root), 0);
        assert!(
            latest_schema_id.is_null(after_root),
            "ROOT snapshot has no schema_id in the fixture"
        );

        // Synthetic current entry (last_updated_ms, after CURRENT_TS): the current snapshot is CURRENT.
        let current_ts = table.metadata().last_updated_ms();
        let current = by_ts[&current_ts];
        assert_eq!(latest_snapshot_id.value(current), CURRENT);
        assert_eq!(latest_schema_id.value(current), 1);
        assert_eq!(latest_sequence_number.value(current), 1);
        // Sanity: the current entry's timestamp is at/after CURRENT's snapshot-log timestamp.
        assert!(current_ts >= CURRENT_TS);
    }

    /// RISK (the `<=` boundary in `snapshot_id_as_of_time`, Java `nullableSnapshotIdAsOfTime`): a
    /// metadata-log entry whose timestamp EXACTLY EQUALS a snapshot-log timestamp must resolve TO that
    /// snapshot — the snapshot became current AT that instant. A strict `<` would wrongly resolve such an
    /// entry to the PREVIOUS snapshot (or NULL for the first), assigning the wrong `latest_*` to the row.
    /// This is the only case that distinguishes `<=` from `<`; the other tests offset every entry off the
    /// snapshot timestamps, so they leave the boundary unpinned.
    #[tokio::test]
    async fn test_metadata_log_entries_table_latest_columns_inclusive_of_exact_snapshot_timestamp()
    {
        let base = make_v2_table();
        let mut meta: TableMetadata = base.metadata().clone();
        // Two entries landing EXACTLY on the two snapshot-log timestamps.
        meta.metadata_log = vec![
            MetadataLog {
                metadata_file: "s3://bucket/metadata/00000-at-root.metadata.json".to_string(),
                timestamp_ms: ROOT_TS, // exactly when ROOT became current → must resolve to ROOT
            },
            MetadataLog {
                metadata_file: "s3://bucket/metadata/00001-at-current.metadata.json".to_string(),
                timestamp_ms: CURRENT_TS, // exactly when CURRENT became current → must resolve to CURRENT
            },
        ];
        let table = base.with_metadata(Arc::new(meta));

        let batches = scan_to_batches(&table).await;
        let batch = &batches[0];
        let timestamp = batch.column(0).as_primitive::<TimestampMicrosecondType>();
        let latest_snapshot_id = batch.column(2).as_primitive::<Int64Type>();

        let mut by_ts = std::collections::HashMap::new();
        for i in 0..batch.num_rows() {
            by_ts.insert(timestamp.value(i) / 1000, i);
        }

        // Entry AT ROOT_TS: inclusive resolution picks ROOT, not NULL (which a strict `<` would give).
        let at_root = by_ts[&ROOT_TS];
        assert_eq!(
            latest_snapshot_id.value(at_root),
            ROOT,
            "entry at exactly ROOT's snapshot timestamp must resolve to ROOT (<=, not <)"
        );
        // Entry AT CURRENT_TS: inclusive resolution picks CURRENT, not ROOT (which a strict `<` would give).
        let at_current = by_ts[&CURRENT_TS];
        assert_eq!(
            latest_snapshot_id.value(at_current),
            CURRENT,
            "entry at exactly CURRENT's snapshot timestamp must resolve to CURRENT (<=, not <)"
        );
    }
}
