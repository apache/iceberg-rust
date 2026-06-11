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

//! Java interop test for DELETION-VECTOR merge-on-read scan execution (Increment D1, Direction 1).
//!
//! The sibling of [`interop_scan_exec`]: the merge-on-read mechanism here is a PUFFIN DELETION
//! VECTOR (a `deletion-vector-v1` blob written by Java's production `BaseDVFileWriter`), not a
//! parquet position-delete file. This is the crown-jewel proof that the Rust DV scan READ path —
//! the Puffin routing in `CachingDeleteFileLoader`, the blob framing (length prefix / magic /
//! CRC-32), the portable 64-bit roaring decode, and the keying by `referenced_data_file` — is
//! byte-compatible with what Java 1.10.0 actually writes. Before D1, this scan FAILED: every
//! position delete was routed to the parquet reader, which cannot parse a Puffin file.
//!
//! THE FIXTURE. The Java oracle's `generate-interop-dv` mode writes a REAL table to a temp dir:
//! an unpartitioned **V3** table (deletion vectors require format version 3) with
//!   * data file `00000-data-a.parquet`: rows (10,"a") (20,"b") (30,"c") (40,"d") (50,"e") at
//!     positions 0..4;
//!   * data file `00000-data-b.parquet`: rows (60,"f") (70,"g") (80,"h") — the SIBLING control:
//!     a DV is FILE-scoped, so file B must come through the scan untouched;
//!   * a REAL Puffin deletion vector deleting positions {1, 3} of file A (ids 20 and 40),
//!     committed via `newRowDelta().addDeletes(dv)`.
//!
//! Java materializes its OWN merge-on-read read (`IcebergGenerics`, which loads the DV via
//! `BaseDeleteLoader.readDV`) into `java_dv_scan_rows.json` = {10,30,50,60,70,80}. THIS test
//! loads the same `final.metadata.json`, runs `scan().build()?.to_arrow()`, and asserts the rows
//! EQUAL Java's read — ids 20/40 ABSENT (the DV applied to file A) and 60/70/80 PRESENT (the DV
//! did not leak onto file B).
//!
//! THE ENV GATE. The table is regenerated each run (nothing binary is committed), so this test is
//! gated on `ICEBERG_INTEROP_DV_DIR` (empty string treated as unset). When unset it is a clean
//! NO-OP (runtime early-return, NOT `#[ignore]`) so the offline `cargo test` gate stays green
//! with no Java/Maven. `dev/java-interop/run-interop-dv.sh` runs the REAL comparison — including
//! the sibling byte-level blob-decode pin that lives in
//! `delete_vector::tests::test_dv_blob_decodes_java_written_blob_when_env_set` (a lib test,
//! because the blob decoder is crate-internal).

use std::cmp::Ordering;
use std::fs;
use std::path::PathBuf;

use arrow_array::RecordBatch;
use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use futures::TryStreamExt;
use iceberg::TableIdent;
use iceberg::io::FileIO;
use iceberg::spec::TableMetadata;
use iceberg::table::Table;
use serde::Deserialize;

/// One live row of Java's merge-on-read read: the `id` (long) + nullable `data` string.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
struct ScanRow {
    id: i64,
    data: Option<String>,
}

/// Sort rows by id (then data) for an order-independent comparison.
fn sorted_by_id(mut rows: Vec<ScanRow>) -> Vec<ScanRow> {
    rows.sort_by(|a, b| a.id.cmp(&b.id).then_with(|| cmp_opt(&a.data, &b.data)));
    rows
}

fn cmp_opt(a: &Option<String>, b: &Option<String>) -> Ordering {
    match (a, b) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => Ordering::Less,
        (Some(_), None) => Ordering::Greater,
        (Some(x), Some(y)) => x.cmp(y),
    }
}

/// The temp dir the Java oracle wrote the V3 DV table + JSON rows into. `None` when the env var
/// is unset OR set to the empty string (set-but-empty must not flip the gate on).
fn dv_dir() -> Option<PathBuf> {
    std::env::var_os("ICEBERG_INTEROP_DV_DIR")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
}

/// Load + parse the Java ground-truth rows from `<dir>/java_dv_scan_rows.json`.
fn read_java_rows(dir: &std::path::Path) -> Vec<ScanRow> {
    let path = dir.join("java_dv_scan_rows.json");
    let json = fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("read {}: {error}", path.display()));
    serde_json::from_str::<Vec<ScanRow>>(&json)
        .unwrap_or_else(|error| panic!("parse {}: {error}", path.display()))
}

/// Build a `Table` over the Java-written `final.metadata.json` with a local-filesystem `FileIO`
/// (the commits wrote bare absolute paths).
fn load_table(dir: &std::path::Path) -> Table {
    let metadata_path = dir.join("table/metadata/final.metadata.json");
    let json = fs::read_to_string(&metadata_path)
        .unwrap_or_else(|error| panic!("read {}: {error}", metadata_path.display()));
    let metadata: TableMetadata = serde_json::from_str(&json)
        .unwrap_or_else(|error| panic!("parse {}: {error}", metadata_path.display()));

    Table::builder()
        .metadata(metadata)
        .metadata_location(metadata_path.to_string_lossy().to_string())
        .identifier(TableIdent::from_strs(["interop", "dv_scan"]).expect("valid identifier"))
        .file_io(FileIO::new_with_fs())
        .build()
        .expect("build table from Java-written final.metadata.json")
}

/// Extract the (id, data) rows from one scan batch by column name.
fn extract_rows(batch: &RecordBatch) -> Vec<ScanRow> {
    let id = batch
        .column_by_name("id")
        .expect("id column present")
        .as_primitive::<Int64Type>();
    let data = batch.column_by_name("data").expect("data column present");

    (0..batch.num_rows())
        .map(|i| ScanRow {
            id: id.value(i),
            data: string_value(data, i),
        })
        .collect()
}

/// Read row `i` of a nullable string column, tolerating Utf8 (i32) / LargeUtf8 (i64) offsets.
fn string_value(array: &arrow_array::ArrayRef, i: usize) -> Option<String> {
    use arrow_schema::DataType;
    if array.is_null(i) {
        return None;
    }
    match array.data_type() {
        DataType::Utf8 => Some(array.as_string::<i32>().value(i).to_string()),
        DataType::LargeUtf8 => Some(array.as_string::<i64>().value(i).to_string()),
        other => panic!("unexpected data column arrow type: {other:?}"),
    }
}

/// Risk pinned: the WHOLE deletion-vector scan read path against REAL Java-written bytes —
/// plan-time DV association (`delete_file_index`), the Puffin loader routing, the
/// `deletion-vector-v1` framing + portable roaring decode, and the referenced-data-file keying.
/// A regression in ANY of them either errors this scan or resurrects ids 20/40 / corrupts file B.
#[tokio::test]
async fn test_dv_scan_merge_on_read_matches_java_read() {
    let Some(dir) = dv_dir() else {
        println!(
            "skipping interop_dv_scan — set ICEBERG_INTEROP_DV_DIR \
             (run dev/java-interop/run-interop-dv.sh)"
        );
        return;
    };

    let table = load_table(&dir);

    let batch_stream = table
        .scan()
        .select(["id", "data"])
        .build()
        .expect("build table scan")
        .to_arrow()
        .await
        .expect("scan to_arrow over the Java-written V3 DV table");
    let batches: Vec<RecordBatch> = batch_stream
        .try_collect()
        .await
        .expect("collect scan record batches");

    let rust_rows = sorted_by_id(batches.iter().flat_map(extract_rows).collect());
    let java_rows = sorted_by_id(read_java_rows(&dir));

    assert_eq!(
        rust_rows, java_rows,
        "Rust scan (with the deletion vector applied) must equal Java's own read"
    );

    // The two load-bearing facts, asserted by name so a comparison-shape bug cannot hide them:
    // the DV's positions {1,3} of file A (ids 20/40) are ABSENT, and the SIBLING file B's rows
    // are fully present (the file-scoped DV did not leak across files).
    let ids: Vec<i64> = rust_rows.iter().map(|row| row.id).collect();
    assert!(
        !ids.contains(&20) && !ids.contains(&40),
        "deleted ids 20/40 must be absent, got {ids:?}"
    );
    for sibling_id in [60, 70, 80] {
        assert!(
            ids.contains(&sibling_id),
            "sibling file B row {sibling_id} must survive, got {ids:?}"
        );
    }
}
