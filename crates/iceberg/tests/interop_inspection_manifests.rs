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

//! Java interop test for the MANIFEST-READING inspection tables `files` / `data_files` / `delete_files`
//! (the A1 foundation increment).
//!
//! Unlike the pure-metadata inspection interop ([`interop_inspection`], offline, reading committed JSON
//! fixtures), these tables read REAL ON-DISK AVRO MANIFESTS. The byte-level "read a table Java wrote"
//! proof therefore needs a real table: the Java oracle's `generate-inspection-manifests` mode WRITES a
//! partitioned V2 table to a temp dir via real commits (`newAppend` writes a DATA manifest + manifest-list;
//! `newRowDelta` writes a DELETE manifest), writes `final.metadata.json` to a known path, and materializes
//! the rows of Java's REAL `FilesTable` / `DataFilesTable` / `DeleteFilesTable` (via
//! `MetadataTableUtils.createMetadataTableInstance` + `task.asDataTask().rows()`, each `ManifestReadTask`
//! opening the on-disk AVRO) into `java_files.json` / `java_data_files.json` / `java_delete_files.json`.
//!
//! This test reads the SAME on-disk manifests: it loads `final.metadata.json`, builds a `Table` over a
//! local-filesystem `FileIO` (which resolves the absolute manifest paths the commits wrote), runs
//! `inspect().files()/.data_files()/.delete_files().scan()`, extracts EVERY column except the deferred
//! `readable_metrics` virtual struct, and asserts field-for-field equality against the Java rows
//! ORDER-INDEPENDENTLY (sorted by `file_path`).
//!
//! THE ENV GATE. Because the table is regenerated each run (nothing binary is committed), this test is
//! GATED on `ICEBERG_INTEROP_MANIFEST_DIR`. When the var is UNSET the test is a clean NO-OP (a runtime
//! early-return, NOT `#[ignore]`) so the offline `cargo test` gate stays green with no Java/Docker. The
//! `dev/java-interop/run-inspection-manifests.sh` script sets the var and runs the REAL comparison.
//!
//! DEFERRED — `readable_metrics`. The trailing virtual `readable_metrics` STRUCT column is DERIVED (one
//! per-leaf-column struct of human-readable min/max/counts). Its interior field ordering depends on a JVM
//! HashMap iteration order (a documented divergence), so it is OUT OF SCOPE for A1 — the RAW metric MAPS +
//! bound MAPS this test DOES compare are the load-bearing source those readable values derive from.
//!
//! `file_format` NOW MATCHES JAVA EXACTLY. Rust's `inspect` projection upper-cases the rendered
//! `file_format` column (`PARQUET`/`AVRO`/`ORC`) to match Java's `FilesTable`/`ManifestEntriesTable`, which
//! emit the UPPERCASE `FileFormat` enum NAME via `format.toString()`. The on-disk AVRO stores the lowercase
//! string on BOTH (the manifest serde is unchanged; Java/Rust read each other's lowercase via the
//! case-insensitive `from_str`). So the comparison asserts EXACT equality on `file_format` — no canonicalization.
//!
//! ONE KNOWN, NON-CORRUPTING REPRESENTATION DIVERGENCE (content-identical; surfaced, not hidden). It is a
//! presentation-only difference in how each library's metadata table RENDERS a column; the underlying
//! on-disk manifest value is identical, so it is NOT a production bug and NOT in scope to "fix" here (a fix
//! would be a spec-type change, out of bounds for an interop test). It is collapsed to a canonical form by
//! [`FileRow::canonical`] for the bulk equality AND pinned RAW by a focused assertion so it cannot drift
//! unnoticed:
//!   - ABSENT METRIC/BOUND MAP — empty `{}` vs `null`. Rust's `spec::DataFile` stores the metric/bound maps
//!     as NON-optional `HashMap`, so an absent map projects to an EMPTY map; Java stores `null` and emits
//!     JSON `null`. An empty map and a null map carry identical information (no metrics). Canonicalized by
//!     treating `None` and `Some(empty)` as equal.
//!
//! NO PRODUCTION CHANGE is needed: every OTHER column the Rust `files` family projects matches Java's
//! `FilesTable` row byte-for-byte when both read the same on-disk manifest. (If a column genuinely diverged
//! in CONTENT, the contract is to STOP and report it — never to hide a column. These two are content-equal.)

use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;

use arrow_array::cast::AsArray;
use arrow_array::types::{Float64Type, Int32Type, Int64Type, TimestampMicrosecondType};
use arrow_array::{Array, ArrayRef, RecordBatch, StringArray, StructArray};
use futures::TryStreamExt;
use iceberg::TableIdent;
use iceberg::expr::{BoundPredicate, Predicate, Reference};
use iceberg::io::FileIO;
use iceberg::scan::FileScanTask;
use iceberg::spec::{Datum, TableMetadata};
use iceberg::table::Table;
use serde::Deserialize;

// ===========================================================================================
// The Java oracle row model — deserialized from java_{files,data_files,delete_files}.json. Every column is
// present EXCEPT the deferred `readable_metrics`. Bound maps are {field_id-as-string: hex-of-bytes}.
// ===========================================================================================

/// One row of Java's `FilesTable` / `DataFilesTable` / `DeleteFilesTable`, keyed by COLUMN NAME (the Java
/// oracle derives name→position from `mt.schema().columns()`).
#[derive(Debug, Clone, PartialEq, Deserialize)]
struct JavaFileRow {
    content: i32,
    file_path: String,
    file_format: String,
    spec_id: i32,
    partition: JavaPartition,
    record_count: i64,
    file_size_in_bytes: i64,
    #[serde(default)]
    column_sizes: Option<HashMap<i32, i64>>,
    #[serde(default)]
    value_counts: Option<HashMap<i32, i64>>,
    #[serde(default)]
    null_value_counts: Option<HashMap<i32, i64>>,
    #[serde(default)]
    nan_value_counts: Option<HashMap<i32, i64>>,
    /// {field_id: hex-of-bytes} — decoded to raw bytes before comparison.
    #[serde(default)]
    lower_bounds: Option<HashMap<i32, String>>,
    #[serde(default)]
    upper_bounds: Option<HashMap<i32, String>>,
    #[serde(default)]
    key_metadata: Option<String>,
    #[serde(default)]
    split_offsets: Option<Vec<i64>>,
    #[serde(default)]
    equality_ids: Option<Vec<i32>>,
    #[serde(default)]
    sort_order_id: Option<i32>,
    #[serde(default)]
    first_row_id: Option<i64>,
    #[serde(default)]
    referenced_data_file: Option<String>,
    #[serde(default)]
    content_offset: Option<i64>,
    #[serde(default)]
    content_size_in_bytes: Option<i64>,
}

/// The identity-`category` partition tuple as a name→value object. Only `category` is present in this
/// fixture; kept as a map so the comparison is robust to sub-field ordering.
#[derive(Debug, Clone, PartialEq, Deserialize)]
struct JavaPartition {
    category: Option<String>,
}

/// A normalized, fully-comparable row — Java + Rust both materialize one of these so equality is a single
/// `==`. Bound maps are raw bytes (Java hex decoded; Rust binary verbatim).
#[derive(Debug, Clone, PartialEq)]
struct FileRow {
    content: i32,
    file_path: String,
    file_format: String,
    spec_id: i32,
    partition_category: Option<String>,
    record_count: i64,
    file_size_in_bytes: i64,
    column_sizes: Option<HashMap<i32, i64>>,
    value_counts: Option<HashMap<i32, i64>>,
    null_value_counts: Option<HashMap<i32, i64>>,
    nan_value_counts: Option<HashMap<i32, i64>>,
    lower_bounds: Option<HashMap<i32, Vec<u8>>>,
    upper_bounds: Option<HashMap<i32, Vec<u8>>>,
    key_metadata: Option<Vec<u8>>,
    split_offsets: Option<Vec<i64>>,
    equality_ids: Option<Vec<i32>>,
    sort_order_id: Option<i32>,
    first_row_id: Option<i64>,
    referenced_data_file: Option<String>,
    content_offset: Option<i64>,
    content_size_in_bytes: Option<i64>,
}

impl JavaFileRow {
    /// Decode the Java row into the comparable [`FileRow`] (hex bound maps → raw bytes).
    fn into_file_row(self) -> FileRow {
        FileRow {
            content: self.content,
            file_path: self.file_path,
            file_format: self.file_format,
            spec_id: self.spec_id,
            partition_category: self.partition.category,
            record_count: self.record_count,
            file_size_in_bytes: self.file_size_in_bytes,
            column_sizes: self.column_sizes,
            value_counts: self.value_counts,
            null_value_counts: self.null_value_counts,
            nan_value_counts: self.nan_value_counts,
            lower_bounds: self.lower_bounds.map(decode_hex_map),
            upper_bounds: self.upper_bounds.map(decode_hex_map),
            key_metadata: self.key_metadata.map(|hex| decode_hex(&hex)),
            split_offsets: self.split_offsets,
            equality_ids: self.equality_ids,
            sort_order_id: self.sort_order_id,
            first_row_id: self.first_row_id,
            referenced_data_file: self.referenced_data_file,
            content_offset: self.content_offset,
            content_size_in_bytes: self.content_size_in_bytes,
        }
    }
}

impl FileRow {
    /// Collapse the ONE KNOWN, content-identical representation divergence (see the module docs) to a
    /// canonical form so the bulk equality compares CONTENT: an absent metric/bound map (`None` on Java,
    /// `Some(empty)` on Rust) normalized to `None`. `file_format` is NOT canonicalized — Rust now upper-cases
    /// the rendered value to match Java's enum name exactly, so it is compared verbatim.
    fn canonical(mut self) -> FileRow {
        self.column_sizes = none_if_empty_long(self.column_sizes);
        self.value_counts = none_if_empty_long(self.value_counts);
        self.null_value_counts = none_if_empty_long(self.null_value_counts);
        self.nan_value_counts = none_if_empty_long(self.nan_value_counts);
        self.lower_bounds = none_if_empty_bytes(self.lower_bounds);
        self.upper_bounds = none_if_empty_bytes(self.upper_bounds);
        self
    }
}

/// `Some(empty)` → `None` (an absent count map; the empty-vs-null divergence).
fn none_if_empty_long(map: Option<HashMap<i32, i64>>) -> Option<HashMap<i32, i64>> {
    map.filter(|m| !m.is_empty())
}

/// `Some(empty)` → `None` (an absent bound map; the empty-vs-null divergence).
fn none_if_empty_bytes(map: Option<HashMap<i32, Vec<u8>>>) -> Option<HashMap<i32, Vec<u8>>> {
    map.filter(|m| !m.is_empty())
}

/// Decode a {field_id: hex} map into {field_id: raw bytes}.
fn decode_hex_map(map: HashMap<i32, String>) -> HashMap<i32, Vec<u8>> {
    map.into_iter().map(|(k, v)| (k, decode_hex(&v))).collect()
}

/// Decode a lowercase hex string into raw bytes (Java emitted `String.format("%02x", b & 0xff)` per byte).
fn decode_hex(hex: &str) -> Vec<u8> {
    assert!(
        hex.len().is_multiple_of(2),
        "hex string must have even length: {hex}"
    );
    (0..hex.len())
        .step_by(2)
        .map(|i| {
            u8::from_str_radix(&hex[i..i + 2], 16)
                .unwrap_or_else(|error| panic!("invalid hex byte in {hex}: {error}"))
        })
        .collect()
}

// ===========================================================================================
// Fixture loading + Table construction.
// ===========================================================================================

/// The temp dir the Java oracle wrote the table + JSON rows into. `None` when the env var is unset.
fn manifest_dir() -> Option<PathBuf> {
    std::env::var_os("ICEBERG_INTEROP_MANIFEST_DIR").map(PathBuf::from)
}

/// Load + parse one Java JSON fixture from the temp dir.
fn read_java_rows(dir: &std::path::Path, file_name: &str) -> Vec<JavaFileRow> {
    let path = dir.join(file_name);
    let json = fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("read {}: {error}", path.display()));
    serde_json::from_str::<Vec<JavaFileRow>>(&json)
        .unwrap_or_else(|error| panic!("parse {}: {error}", path.display()))
}

/// Build a `Table` over the Java-written `final.metadata.json`, using a LOCAL-FILESYSTEM `FileIO` so the
/// absolute on-disk manifest-list + manifest paths the commits wrote resolve directly.
fn load_table(dir: &std::path::Path) -> Table {
    let metadata_path = dir.join("table/metadata/final.metadata.json");
    let json = fs::read_to_string(&metadata_path)
        .unwrap_or_else(|error| panic!("read {}: {error}", metadata_path.display()));
    let metadata: TableMetadata = serde_json::from_str(&json)
        .unwrap_or_else(|error| panic!("parse {}: {error}", metadata_path.display()));

    Table::builder()
        .metadata(metadata)
        .metadata_location(metadata_path.to_string_lossy().to_string())
        .identifier(
            TableIdent::from_strs(["interop", "inspection_manifests"]).expect("valid identifier"),
        )
        .file_io(FileIO::new_with_fs())
        .build()
        .expect("build table from Java-written final.metadata.json")
}

// ===========================================================================================
// Arrow column extraction — a files-table batch into the comparable [`FileRow`]s.
// ===========================================================================================

/// A by-name column source: a `files`-family Arrow batch (the A1 `files` scans) OR the nested `data_file`
/// STRUCT inside an `entries` row (A2). Both `RecordBatch` and `StructArray` expose an identically-typed
/// `column_by_name`, so the [`FileRow`] extraction below works against either — A1's `files` rows are read
/// straight from the batch; A2's `entries.data_file` rows are read from the nested struct WITHOUT
/// duplicating the (large) DataFile-projection extraction.
trait ColumnSource {
    fn column(&self, name: &str) -> Option<&ArrayRef>;
    fn rows(&self) -> usize;
}

impl ColumnSource for RecordBatch {
    fn column(&self, name: &str) -> Option<&ArrayRef> {
        self.column_by_name(name)
    }
    fn rows(&self) -> usize {
        self.num_rows()
    }
}

impl ColumnSource for StructArray {
    fn column(&self, name: &str) -> Option<&ArrayRef> {
        self.column_by_name(name)
    }
    fn rows(&self) -> usize {
        self.len()
    }
}

/// Extract a files-family column source (a `files` batch OR an `entries.data_file` struct) into
/// [`FileRow`]s by COLUMN NAME (never by position), covering every column except the deferred
/// `readable_metrics`.
fn extract_rust_rows(batch: &dyn ColumnSource) -> Vec<FileRow> {
    let content = primitive::<Int32Type>(batch, "content");
    let file_path = string_col(batch, "file_path");
    let file_format = string_col(batch, "file_format");
    let spec_id = primitive::<Int32Type>(batch, "spec_id");
    let record_count = primitive::<Int64Type>(batch, "record_count");
    let file_size = primitive::<Int64Type>(batch, "file_size_in_bytes");
    let sort_order_id = primitive::<Int32Type>(batch, "sort_order_id");
    let first_row_id = primitive::<Int64Type>(batch, "first_row_id");
    let content_offset = primitive::<Int64Type>(batch, "content_offset");
    let content_size = primitive::<Int64Type>(batch, "content_size_in_bytes");
    let referenced_data_file = string_col(batch, "referenced_data_file");

    // The `partition` struct's single `category` (Utf8) sub-field.
    let partition = batch.column("partition").expect("partition").as_struct();
    let partition_category = partition
        .column_by_name("category")
        .map(|c| c.as_string::<i32>());

    (0..batch.rows())
        .map(|i| FileRow {
            content: content.value(i),
            file_path: file_path.value(i).to_string(),
            file_format: file_format.value(i).to_string(),
            spec_id: spec_id.value(i),
            partition_category: partition_category.and_then(|c| {
                if c.is_null(i) {
                    None
                } else {
                    Some(c.value(i).to_string())
                }
            }),
            record_count: record_count.value(i),
            file_size_in_bytes: file_size.value(i),
            column_sizes: long_map(batch, "column_sizes", i),
            value_counts: long_map(batch, "value_counts", i),
            null_value_counts: long_map(batch, "null_value_counts", i),
            nan_value_counts: long_map(batch, "nan_value_counts", i),
            lower_bounds: bytes_map(batch, "lower_bounds", i),
            upper_bounds: bytes_map(batch, "upper_bounds", i),
            key_metadata: opt_binary(batch, "key_metadata", i),
            split_offsets: long_list(batch, "split_offsets", i),
            equality_ids: int_list(batch, "equality_ids", i),
            sort_order_id: opt_i32(sort_order_id, i),
            first_row_id: opt_i64(first_row_id, i),
            referenced_data_file: opt_str(referenced_data_file, i),
            content_offset: opt_i64(content_offset, i),
            content_size_in_bytes: opt_i64(content_size, i),
        })
        .collect()
}

fn primitive<'a, T: arrow_array::types::ArrowPrimitiveType>(
    batch: &'a dyn ColumnSource,
    name: &str,
) -> &'a arrow_array::PrimitiveArray<T> {
    batch
        .column(name)
        .unwrap_or_else(|| panic!("column {name} present"))
        .as_primitive::<T>()
}

fn string_col<'a>(batch: &'a dyn ColumnSource, name: &str) -> &'a StringArray {
    batch
        .column(name)
        .unwrap_or_else(|| panic!("column {name} present"))
        .as_string::<i32>()
}

fn opt_i32(arr: &arrow_array::PrimitiveArray<Int32Type>, i: usize) -> Option<i32> {
    if arr.is_null(i) {
        None
    } else {
        Some(arr.value(i))
    }
}

fn opt_i64(arr: &arrow_array::PrimitiveArray<Int64Type>, i: usize) -> Option<i64> {
    if arr.is_null(i) {
        None
    } else {
        Some(arr.value(i))
    }
}

fn opt_str(arr: &StringArray, i: usize) -> Option<String> {
    if arr.is_null(i) {
        None
    } else {
        Some(arr.value(i).to_string())
    }
}

/// A `map<int, long>` metrics column at row `i` → `Some(HashMap)` when present, `None` when the map cell is
/// NULL (matching Java's `null` for an absent metric map).
fn long_map(batch: &dyn ColumnSource, name: &str, i: usize) -> Option<HashMap<i32, i64>> {
    let map = batch
        .column(name)
        .unwrap_or_else(|| panic!("column {name} present"))
        .as_map();
    if map.is_null(i) {
        return None;
    }
    let entries = map.value(i);
    let keys = entries.column(0).as_primitive::<Int32Type>();
    let values = entries.column(1).as_primitive::<Int64Type>();
    let mut out = HashMap::new();
    for e in 0..entries.len() {
        out.insert(keys.value(e), values.value(e));
    }
    Some(out)
}

/// A `map<int, binary>` bound column at row `i` → `Some(HashMap<field_id, raw bytes>)` or `None`.
fn bytes_map(batch: &dyn ColumnSource, name: &str, i: usize) -> Option<HashMap<i32, Vec<u8>>> {
    let map = batch
        .column(name)
        .unwrap_or_else(|| panic!("column {name} present"))
        .as_map();
    if map.is_null(i) {
        return None;
    }
    let entries = map.value(i);
    let keys = entries.column(0).as_primitive::<Int32Type>();
    // Iceberg `binary` maps to Arrow LargeBinary (`schema_to_arrow_schema`), so the bound-map value column
    // is a `LargeBinaryArray` — read it with the i64 offset width.
    let values = entries.column(1).as_binary::<i64>();
    let mut out = HashMap::new();
    for e in 0..entries.len() {
        out.insert(keys.value(e), values.value(e).to_vec());
    }
    Some(out)
}

/// A `list<long>` column at row `i` → `Some(Vec)` or `None`.
fn long_list(batch: &dyn ColumnSource, name: &str, i: usize) -> Option<Vec<i64>> {
    let list = batch
        .column(name)
        .unwrap_or_else(|| panic!("column {name} present"))
        .as_list::<i32>();
    if list.is_null(i) {
        return None;
    }
    let values = list.value(i);
    let values = values.as_primitive::<Int64Type>();
    Some((0..values.len()).map(|e| values.value(e)).collect())
}

/// A `list<int>` column at row `i` → `Some(Vec)` or `None`.
fn int_list(batch: &dyn ColumnSource, name: &str, i: usize) -> Option<Vec<i32>> {
    let list = batch
        .column(name)
        .unwrap_or_else(|| panic!("column {name} present"))
        .as_list::<i32>();
    if list.is_null(i) {
        return None;
    }
    let values = list.value(i);
    let values = values.as_primitive::<Int32Type>();
    Some((0..values.len()).map(|e| values.value(e)).collect())
}

/// An optional binary column (`key_metadata`) at row `i`. The files table stores it as `LargeBinary`.
fn opt_binary(batch: &dyn ColumnSource, name: &str, i: usize) -> Option<Vec<u8>> {
    let arr = batch
        .column(name)
        .unwrap_or_else(|| panic!("column {name} present"))
        .as_binary::<i64>();
    if arr.is_null(i) {
        None
    } else {
        Some(arr.value(i).to_vec())
    }
}

/// Collect a metadata-table scan into the comparable [`FileRow`]s (the files scans emit one batch).
async fn scan_rows(stream: iceberg::scan::ArrowRecordBatchStream) -> Vec<FileRow> {
    let batches: Vec<RecordBatch> = stream.try_collect().await.expect("collect files scan");
    let mut rows = Vec::new();
    for batch in &batches {
        rows.extend(extract_rust_rows(batch));
    }
    rows
}

/// Canonicalize (collapse the one known representation divergence — absent map vs empty) + sort by
/// `file_path` for an order-independent CONTENT comparison.
fn canonical_sorted(rows: Vec<FileRow>) -> Vec<FileRow> {
    let mut rows: Vec<FileRow> = rows.into_iter().map(FileRow::canonical).collect();
    rows.sort_by(|a, b| a.file_path.cmp(&b.file_path));
    rows
}

// ===========================================================================================
// The single env-gated interop test.
// ===========================================================================================

#[tokio::test]
async fn test_files_tables_match_java_rows_from_real_manifests() {
    let Some(dir) = manifest_dir() else {
        println!(
            "skipping interop_inspection_manifests — set ICEBERG_INTEROP_MANIFEST_DIR \
             (run dev/java-interop/run-inspection-manifests.sh)"
        );
        return;
    };

    let table = load_table(&dir);

    // RAW (un-canonicalized) Rust `files` rows — kept so the file_format case (now matching Java) and the
    // one known representation divergence (empty-map-vs-null) can be PINNED below, not silently masked.
    let rust_files_raw = scan_rows(table.inspect().files().scan().await.expect("files scan")).await;

    // -- files: ALL live entries (2 data + 1 delete). -------------------------------------------------
    let rust_files = canonical_sorted(rust_files_raw.clone());
    let java_files = canonical_sorted(
        read_java_rows(&dir, "java_files.json")
            .into_iter()
            .map(JavaFileRow::into_file_row)
            .collect(),
    );

    assert_eq!(
        rust_files.len(),
        3,
        "the `files` table has 2 data files + 1 position-delete file"
    );
    assert_eq!(
        rust_files, java_files,
        "Rust `files` rows must equal Java's FilesTable rows field-for-field (content, paths, partition, \
         file_format, counts, the metric + bound maps, the list + V2 delete columns) — readable_metrics \
         excluded, the one known representation divergence (absent map vs empty) canonicalized"
    );

    // -- data_files: the 2 DATA files, the delete EXCLUDED. -------------------------------------------
    let rust_data = canonical_sorted(
        scan_rows(
            table
                .inspect()
                .data_files()
                .scan()
                .await
                .expect("data_files scan"),
        )
        .await,
    );
    let java_data = canonical_sorted(
        read_java_rows(&dir, "java_data_files.json")
            .into_iter()
            .map(JavaFileRow::into_file_row)
            .collect(),
    );
    assert_eq!(
        rust_data.len(),
        2,
        "the `data_files` table has the 2 data files"
    );
    assert_eq!(
        rust_data, java_data,
        "Rust `data_files` rows must equal Java's DataFilesTable rows field-for-field"
    );

    // -- delete_files: the 1 DELETE file, the data files EXCLUDED. ------------------------------------
    let rust_deletes = canonical_sorted(
        scan_rows(
            table
                .inspect()
                .delete_files()
                .scan()
                .await
                .expect("delete_files scan"),
        )
        .await,
    );
    let java_deletes = canonical_sorted(
        read_java_rows(&dir, "java_delete_files.json")
            .into_iter()
            .map(JavaFileRow::into_file_row)
            .collect(),
    );
    assert_eq!(
        rust_deletes.len(),
        1,
        "the `delete_files` table has the 1 position-delete file"
    );
    assert_eq!(
        rust_deletes, java_deletes,
        "Rust `delete_files` rows must equal Java's DeleteFilesTable rows field-for-field"
    );

    // ============================================================================================
    // Pin the RAW Rust rows so behavior cannot drift unnoticed:
    //   1. file_format renders UPPERCASE in Rust (matching Java's `FileFormat` enum name) — the inspection
    //      projection upper-cases the lowercase on-disk string. Compared verbatim in the bulk equality.
    //   2. an absent metric map projects to an EMPTY map in Rust (Java emits null) — content-identical; the
    //      ONE remaining divergence, surfaced + reported, NOT masked (the bulk equality canonicalizes it).
    // ============================================================================================
    let raw_data_file = rust_files_raw
        .iter()
        .find(|r| r.content == 0)
        .expect("a raw data-file row");
    assert_eq!(
        raw_data_file.file_format, "PARQUET",
        "Rust renders file_format UPPERCASE, matching Java's `FileFormat` enum name"
    );
    let raw_delete_file = rust_files_raw
        .iter()
        .find(|r| r.content == 1)
        .expect("a raw delete-file row");
    assert_eq!(
        raw_delete_file.column_sizes,
        Some(HashMap::new()),
        "KNOWN DIVERGENCE pin: an absent metric map projects to an EMPTY map in Rust (Java emits null)"
    );

    // ============================================================================================
    // Focused, named assertions — a single regressed column / filter is pinpointed here.
    // ============================================================================================

    // The content FILTER: `data_files` excludes the delete file; `delete_files` excludes the data files.
    let delete_path = &rust_deletes[0].file_path;
    assert!(
        !rust_data.iter().any(|r| &r.file_path == delete_path),
        "`data_files` must NOT include the delete file (content filter)"
    );
    let data_paths: Vec<&String> = rust_data.iter().map(|r| &r.file_path).collect();
    assert!(
        !data_paths
            .iter()
            .any(|p| rust_deletes.iter().any(|d| &&d.file_path == p)),
        "`delete_files` must NOT include any data file (content filter)"
    );

    // The `content` column: 0 for every data file, 1 for the position-delete.
    assert!(
        rust_data.iter().all(|r| r.content == 0),
        "every data file must report content == 0"
    );
    assert_eq!(
        rust_deletes[0].content, 1,
        "the position-delete file must report content == 1"
    );

    // record_count / file_size match the committed DataFiles (a=3 rows / 1100 B, b=2 rows / 900 B,
    // delete=1 row / 150 B), and the partition tuple is the committed `category`.
    let by_leaf: HashMap<String, &FileRow> =
        rust_files.iter().map(|r| (leaf(&r.file_path), r)).collect();
    let file_a = by_leaf["00000-a.parquet"];
    let file_b = by_leaf["00000-b.parquet"];
    let delete_a = by_leaf["00000-a-deletes.parquet"];
    assert_eq!((file_a.record_count, file_a.file_size_in_bytes), (3, 1100));
    assert_eq!((file_b.record_count, file_b.file_size_in_bytes), (2, 900));
    assert_eq!(
        (delete_a.record_count, delete_a.file_size_in_bytes),
        (1, 150)
    );
    assert_eq!(file_a.partition_category.as_deref(), Some("a"));
    assert_eq!(file_b.partition_category.as_deref(), Some("b"));
    assert_eq!(delete_a.partition_category.as_deref(), Some("a"));

    // The category=a data file's lower/upper bound bytes for `id` (field id 1, a long) DECODE to the
    // committed values 1 (lower) and 3 (upper) — 8-byte little-endian. This pins that the on-disk bound
    // bytes survive the round-trip byte-for-byte (the same check the Java hex encodes).
    let lower = file_a
        .lower_bounds
        .as_ref()
        .expect("category=a data file carries lower bounds");
    let upper = file_a
        .upper_bounds
        .as_ref()
        .expect("category=a data file carries upper bounds");
    assert_eq!(
        i64::from_le_bytes(lower[&1].clone().try_into().expect("8-byte long bound")),
        1,
        "category=a id lower bound decodes to the committed long 1"
    );
    assert_eq!(
        i64::from_le_bytes(upper[&1].clone().try_into().expect("8-byte long bound")),
        3,
        "category=a id upper bound decodes to the committed long 3"
    );

    // The delete file carries NO metrics maps (built without `.withMetrics`), so they are all NULL/None.
    assert_eq!(delete_a.column_sizes, None);
    assert_eq!(delete_a.lower_bounds, None);

    println!(
        "interop_inspection_manifests OK — files=3, data_files=2, delete_files=1 rows matched Java \
         field-for-field (readable_metrics deferred)"
    );
}

/// The trailing path segment (e.g. `00000-a.parquet`) of a file path.
fn leaf(path: &str) -> String {
    path.rsplit('/').next().unwrap_or(path).to_string()
}

// ===========================================================================================
// A2 — the `entries` / `manifests` / `partitions` manifest-reading inspection tables.
//
// Builds DIRECTLY on the A1 harness above (reusing `manifest_dir()`, the `FileRow` model + its `canonical`
// representation-divergence collapse, the `ColumnSource`-based DataFile extraction, the hex decode, and
// `FileIO::new_with_fs()`). The Java oracle's `generate-inspection-manifests` mode now ALSO writes a
// richer V2 table to `<dir>/table_a2` (A1's `<dir>/table` is untouched) — partition by identity(category)
// with snapshots {append A,B,C,D; row-delta +pos-delete(cat=a); delete B} — and emits
// `java_entries.json` / `java_manifests.json` / `java_partitions.json` (the rows of Java's REAL
// ManifestEntriesTable / ManifestsTable / PartitionsTable). These three tests load
// `<dir>/table_a2/metadata/final.metadata.json`, run `inspect().entries()/.manifests()/.partitions()`,
// and assert field-for-field equality vs the Java rows, order-independent.
//
// Same env gate as A1 (`ICEBERG_INTEROP_MANIFEST_DIR`): a clean NO-OP when unset. `readable_metrics` (the
// entries table's trailing top-level virtual struct) is DEFERRED exactly as A1.
// ===========================================================================================

/// Build a `Table` over the Java-written A2 `final.metadata.json` (under `<dir>/table_a2`), local-fs FileIO.
fn load_table_a2(dir: &std::path::Path) -> Table {
    let metadata_path = dir.join("table_a2/metadata/final.metadata.json");
    let json = fs::read_to_string(&metadata_path)
        .unwrap_or_else(|error| panic!("read {}: {error}", metadata_path.display()));
    let metadata: TableMetadata = serde_json::from_str(&json)
        .unwrap_or_else(|error| panic!("parse {}: {error}", metadata_path.display()));
    Table::builder()
        .metadata(metadata)
        .metadata_location(metadata_path.to_string_lossy().to_string())
        .identifier(
            TableIdent::from_strs(["interop", "inspection_manifests_a2"])
                .expect("valid identifier"),
        )
        .file_io(FileIO::new_with_fs())
        .build()
        .expect("build A2 table from Java-written final.metadata.json")
}

/// Read + parse one Java JSON fixture into `T` (a typed row vector).
fn read_java<T: serde::de::DeserializeOwned>(dir: &std::path::Path, file_name: &str) -> Vec<T> {
    let path = dir.join(file_name);
    let json = fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("read {}: {error}", path.display()));
    serde_json::from_str::<Vec<T>>(&json)
        .unwrap_or_else(|error| panic!("parse {}: {error}", path.display()))
}

// ---------------------------------------------------------------------------------------------
// `entries` — one row per manifest entry of the current snapshot's manifests, INCLUDING DELETED
// tombstones (status 2). Columns: status, snapshot_id, sequence_number, file_sequence_number, and the
// NESTED `data_file` struct (the SAME DataFile projection A1 flattened — reused via `extract_rust_rows`).
// ---------------------------------------------------------------------------------------------

/// One Java `ManifestEntriesTable` row: the 4 scalar columns + the nested `data_file` (a [`JavaFileRow`]).
#[derive(Debug, Clone, Deserialize)]
struct JavaEntryRow {
    status: i32,
    snapshot_id: Option<i64>,
    sequence_number: Option<i64>,
    file_sequence_number: Option<i64>,
    data_file: JavaFileRow,
}

/// A normalized, fully-comparable `entries` row — the 4 scalars + the canonicalized nested [`FileRow`].
#[derive(Debug, Clone, PartialEq)]
struct EntryRow {
    status: i32,
    snapshot_id: Option<i64>,
    sequence_number: Option<i64>,
    file_sequence_number: Option<i64>,
    data_file: FileRow,
}

impl JavaEntryRow {
    fn into_entry_row(self) -> EntryRow {
        EntryRow {
            status: self.status,
            snapshot_id: self.snapshot_id,
            sequence_number: self.sequence_number,
            file_sequence_number: self.file_sequence_number,
            // Same canonicalization A1 applies to the files rows (absent metric/bound map None≡empty).
            data_file: self.data_file.into_file_row().canonical(),
        }
    }
}

/// Extract the Rust `entries` batch into [`EntryRow`]s: the 4 scalar columns by name, plus the nested
/// `data_file` STRUCT fed through the A1 [`extract_rust_rows`] (reused via the [`ColumnSource`] trait) and
/// canonicalized. `readable_metrics` (a trailing TOP-LEVEL struct, not nested in `data_file`) is ignored.
fn extract_entry_rows(batch: &RecordBatch) -> Vec<EntryRow> {
    let status = primitive::<Int32Type>(batch, "status");
    let snapshot_id = primitive::<Int64Type>(batch, "snapshot_id");
    let sequence_number = primitive::<Int64Type>(batch, "sequence_number");
    let file_sequence_number = primitive::<Int64Type>(batch, "file_sequence_number");
    let data_file_struct = batch
        .column_by_name("data_file")
        .expect("data_file struct column")
        .as_struct();
    let data_file_rows: Vec<FileRow> = extract_rust_rows(data_file_struct)
        .into_iter()
        .map(FileRow::canonical)
        .collect();

    (0..batch.num_rows())
        .map(|i| EntryRow {
            status: status.value(i),
            snapshot_id: opt_i64(snapshot_id, i),
            sequence_number: opt_i64(sequence_number, i),
            file_sequence_number: opt_i64(file_sequence_number, i),
            data_file: data_file_rows[i].clone(),
        })
        .collect()
}

/// Sort `entries` rows order-independently by `(data_file.file_path, status)`.
fn sorted_entries(mut rows: Vec<EntryRow>) -> Vec<EntryRow> {
    rows.sort_by(|a, b| {
        a.data_file
            .file_path
            .cmp(&b.data_file.file_path)
            .then(a.status.cmp(&b.status))
    });
    rows
}

#[tokio::test]
async fn test_entries_table_matches_java_rows() {
    let Some(dir) = manifest_dir() else {
        println!(
            "skipping test_entries_table_matches_java_rows — set ICEBERG_INTEROP_MANIFEST_DIR \
             (run dev/java-interop/run-inspection-manifests.sh)"
        );
        return;
    };

    let table = load_table_a2(&dir);
    let batches: Vec<RecordBatch> = table
        .inspect()
        .entries()
        .scan()
        .await
        .expect("entries scan")
        .try_collect()
        .await
        .expect("collect entries scan");
    let mut rust_rows = Vec::new();
    for batch in &batches {
        rust_rows.extend(extract_entry_rows(batch));
    }
    let rust_rows = sorted_entries(rust_rows);

    let java_rows = sorted_entries(
        read_java::<JavaEntryRow>(&dir, "java_entries.json")
            .into_iter()
            .map(JavaEntryRow::into_entry_row)
            .collect(),
    );

    assert_eq!(
        rust_rows, java_rows,
        "Rust `entries` rows must equal Java's ManifestEntriesTable rows field-for-field (status, \
         snapshot_id, sequence_number, file_sequence_number, and the nested data_file struct) — \
         readable_metrics deferred, the absent-map-vs-empty divergence canonicalized as in A1"
    );

    // Focused: the headline difference from `files` — a DELETED tombstone (status 2). The A2 table deletes
    // data file B in the last commit, so B appears as a status-2 row that `files` would have excluded.
    let tombstones: Vec<&EntryRow> = rust_rows.iter().filter(|r| r.status == 2).collect();
    assert!(
        !tombstones.is_empty(),
        "the `entries` table MUST surface ≥1 DELETED tombstone (status 2) — the difference from `files`"
    );
    assert!(
        tombstones.iter().all(|r| r.data_file.content == 0),
        "the deleted tombstone is the data file B (content == 0)"
    );

    // The position-delete file is the ADDED (status 1) row; its nested data_file reports content == 1.
    let added_delete: Vec<&EntryRow> = rust_rows
        .iter()
        .filter(|r| r.data_file.content == 1)
        .collect();
    assert_eq!(
        added_delete.len(),
        1,
        "exactly one position-delete entry in the current snapshot's manifests"
    );
    assert_eq!(
        added_delete[0].status, 1,
        "the position-delete file was ADDED (status 1) and never rewritten, so it stays status 1"
    );

    // file_format renders UPPERCASE in the nested struct, matching Java's `FileFormat` enum name.
    assert!(
        rust_rows
            .iter()
            .all(|r| r.data_file.file_format == "PARQUET"),
        "the nested data_file.file_format is UPPERCASE (matching Java)"
    );

    println!(
        "test_entries_table_matches_java_rows OK — {} entries matched Java (≥1 status-2 tombstone)",
        rust_rows.len()
    );
}

// ---------------------------------------------------------------------------------------------
// `manifests` — one row per manifest in the CURRENT snapshot's manifest list. Content-gated counts + the
// partition_summaries list (contains_null / contains_nan / lower_bound STRING / upper_bound STRING).
// ---------------------------------------------------------------------------------------------

/// One Java `ManifestsTable` partition-summary struct (lower/upper bounds are STRINGS in Java).
#[derive(Debug, Clone, PartialEq, Deserialize)]
struct JavaPartitionSummary {
    contains_null: bool,
    contains_nan: Option<bool>,
    lower_bound: Option<String>,
    upper_bound: Option<String>,
}

/// One Java `ManifestsTable` row — every column incl. the six content-gated counts + the summaries list.
#[derive(Debug, Clone, PartialEq, Deserialize)]
struct JavaManifestRow {
    content: i32,
    path: String,
    length: i64,
    partition_spec_id: i32,
    added_snapshot_id: i64,
    added_data_files_count: i32,
    existing_data_files_count: i32,
    deleted_data_files_count: i32,
    added_delete_files_count: i32,
    existing_delete_files_count: i32,
    deleted_delete_files_count: i32,
    partition_summaries: Vec<JavaPartitionSummary>,
}

/// A normalized, comparable `manifests` row (Java + Rust both produce one; equality is a single `==`).
#[derive(Debug, Clone, PartialEq)]
struct ManifestRow {
    content: i32,
    path: String,
    length: i64,
    partition_spec_id: i32,
    added_snapshot_id: i64,
    added_data_files_count: i32,
    existing_data_files_count: i32,
    deleted_data_files_count: i32,
    added_delete_files_count: i32,
    existing_delete_files_count: i32,
    deleted_delete_files_count: i32,
    partition_summaries: Vec<PartitionSummary>,
}

#[derive(Debug, Clone, PartialEq)]
struct PartitionSummary {
    contains_null: bool,
    contains_nan: Option<bool>,
    lower_bound: Option<String>,
    upper_bound: Option<String>,
}

impl JavaManifestRow {
    fn into_manifest_row(self) -> ManifestRow {
        ManifestRow {
            content: self.content,
            path: self.path,
            length: self.length,
            partition_spec_id: self.partition_spec_id,
            added_snapshot_id: self.added_snapshot_id,
            added_data_files_count: self.added_data_files_count,
            existing_data_files_count: self.existing_data_files_count,
            deleted_data_files_count: self.deleted_data_files_count,
            added_delete_files_count: self.added_delete_files_count,
            existing_delete_files_count: self.existing_delete_files_count,
            deleted_delete_files_count: self.deleted_delete_files_count,
            partition_summaries: self
                .partition_summaries
                .into_iter()
                .map(|s| PartitionSummary {
                    contains_null: s.contains_null,
                    contains_nan: s.contains_nan,
                    lower_bound: s.lower_bound,
                    upper_bound: s.upper_bound,
                })
                .collect(),
        }
    }
}

/// Extract the Rust `manifests` batch into [`ManifestRow`]s by COLUMN NAME, including the nested
/// `partition_summaries` list<struct>.
fn extract_manifest_rows(batch: &RecordBatch) -> Vec<ManifestRow> {
    let content = primitive::<Int32Type>(batch, "content");
    let path = string_col(batch, "path");
    let length = primitive::<Int64Type>(batch, "length");
    let partition_spec_id = primitive::<Int32Type>(batch, "partition_spec_id");
    let added_snapshot_id = primitive::<Int64Type>(batch, "added_snapshot_id");
    let added_data = primitive::<Int32Type>(batch, "added_data_files_count");
    let existing_data = primitive::<Int32Type>(batch, "existing_data_files_count");
    let deleted_data = primitive::<Int32Type>(batch, "deleted_data_files_count");
    let added_delete = primitive::<Int32Type>(batch, "added_delete_files_count");
    let existing_delete = primitive::<Int32Type>(batch, "existing_delete_files_count");
    let deleted_delete = primitive::<Int32Type>(batch, "deleted_delete_files_count");
    let summaries = batch
        .column_by_name("partition_summaries")
        .expect("partition_summaries")
        .as_list::<i32>();

    (0..batch.num_rows())
        .map(|i| ManifestRow {
            content: content.value(i),
            path: path.value(i).to_string(),
            length: length.value(i),
            partition_spec_id: partition_spec_id.value(i),
            added_snapshot_id: added_snapshot_id.value(i),
            added_data_files_count: added_data.value(i),
            existing_data_files_count: existing_data.value(i),
            deleted_data_files_count: deleted_data.value(i),
            added_delete_files_count: added_delete.value(i),
            existing_delete_files_count: existing_delete.value(i),
            deleted_delete_files_count: deleted_delete.value(i),
            partition_summaries: extract_partition_summaries(&summaries.value(i)),
        })
        .collect()
}

/// Extract one row's `partition_summaries` list element (a struct array) into [`PartitionSummary`]s.
fn extract_partition_summaries(list_values: &ArrayRef) -> Vec<PartitionSummary> {
    let structs = list_values.as_struct();
    let contains_null = structs
        .column_by_name("contains_null")
        .expect("contains_null")
        .as_boolean();
    let contains_nan = structs
        .column_by_name("contains_nan")
        .expect("contains_nan")
        .as_boolean();
    let lower = structs
        .column_by_name("lower_bound")
        .expect("lower_bound")
        .as_string::<i32>();
    let upper = structs
        .column_by_name("upper_bound")
        .expect("upper_bound")
        .as_string::<i32>();
    (0..structs.len())
        .map(|i| PartitionSummary {
            contains_null: contains_null.value(i),
            contains_nan: if contains_nan.is_null(i) {
                None
            } else {
                Some(contains_nan.value(i))
            },
            lower_bound: if lower.is_null(i) {
                None
            } else {
                Some(lower.value(i).to_string())
            },
            upper_bound: if upper.is_null(i) {
                None
            } else {
                Some(upper.value(i).to_string())
            },
        })
        .collect()
}

#[tokio::test]
async fn test_manifests_table_matches_java_rows() {
    let Some(dir) = manifest_dir() else {
        println!(
            "skipping test_manifests_table_matches_java_rows — set ICEBERG_INTEROP_MANIFEST_DIR \
             (run dev/java-interop/run-inspection-manifests.sh)"
        );
        return;
    };

    let table = load_table_a2(&dir);
    let batches: Vec<RecordBatch> = table
        .inspect()
        .manifests()
        .scan()
        .await
        .expect("manifests scan")
        .try_collect()
        .await
        .expect("collect manifests scan");
    let mut rust_rows = Vec::new();
    for batch in &batches {
        rust_rows.extend(extract_manifest_rows(batch));
    }
    rust_rows.sort_by(|a, b| a.path.cmp(&b.path));

    let mut java_rows: Vec<ManifestRow> = read_java::<JavaManifestRow>(&dir, "java_manifests.json")
        .into_iter()
        .map(JavaManifestRow::into_manifest_row)
        .collect();
    java_rows.sort_by(|a, b| a.path.cmp(&b.path));

    assert_eq!(
        rust_rows, java_rows,
        "Rust `manifests` rows must equal Java's ManifestsTable rows field-for-field (content, path, \
         length, spec id, added_snapshot_id, the six content-gated counts, and partition_summaries)"
    );

    // Focused: content gating. A DATA manifest (content == 0) has ZERO delete-file counts; a DELETE
    // manifest (content == 1) has ZERO data-file counts.
    let data_manifests: Vec<&ManifestRow> = rust_rows.iter().filter(|r| r.content == 0).collect();
    let delete_manifests: Vec<&ManifestRow> = rust_rows.iter().filter(|r| r.content == 1).collect();
    assert!(
        !data_manifests.is_empty(),
        "≥1 DATA manifest in the current snapshot's manifest list"
    );
    assert!(
        !delete_manifests.is_empty(),
        "≥1 DELETE manifest in the current snapshot's manifest list"
    );
    for m in &data_manifests {
        assert_eq!(
            (
                m.added_delete_files_count,
                m.existing_delete_files_count,
                m.deleted_delete_files_count
            ),
            (0, 0, 0),
            "a DATA manifest carries ZERO delete-file counts (content gating)"
        );
    }
    for m in &delete_manifests {
        assert_eq!(
            (
                m.added_data_files_count,
                m.existing_data_files_count,
                m.deleted_data_files_count
            ),
            (0, 0, 0),
            "a DELETE manifest carries ZERO data-file counts (content gating)"
        );
    }

    // partition_summaries are non-empty (the spec is partitioned by identity(category)).
    assert!(
        rust_rows.iter().all(|m| !m.partition_summaries.is_empty()),
        "every manifest's partition_summaries is non-empty (partitioned spec)"
    );

    println!(
        "test_manifests_table_matches_java_rows OK — {} manifests matched Java (content-gated counts + \
         non-empty summaries)",
        rust_rows.len()
    );
}

// ---------------------------------------------------------------------------------------------
// `partitions` — one row per partition value over the current snapshot's LIVE entries. The partition
// struct + spec_id + record/file/size rollups + the four delete-count columns + last_updated_at (µs) +
// last_updated_snapshot_id.
// ---------------------------------------------------------------------------------------------

/// One Java `PartitionsTable` row. The `partition` struct reuses the A1 [`JavaPartition`] (single
/// `category`).
#[derive(Debug, Clone, PartialEq, Deserialize)]
struct JavaPartitionRow {
    partition: JavaPartition,
    spec_id: i32,
    record_count: i64,
    file_count: i32,
    total_data_file_size_in_bytes: i64,
    position_delete_record_count: i64,
    position_delete_file_count: i32,
    equality_delete_record_count: i64,
    equality_delete_file_count: i32,
    last_updated_at: Option<i64>,
    last_updated_snapshot_id: Option<i64>,
}

/// A normalized, comparable `partitions` row.
#[derive(Debug, Clone, PartialEq)]
struct PartitionRow {
    partition_category: Option<String>,
    spec_id: i32,
    record_count: i64,
    file_count: i32,
    total_data_file_size_in_bytes: i64,
    position_delete_record_count: i64,
    position_delete_file_count: i32,
    equality_delete_record_count: i64,
    equality_delete_file_count: i32,
    last_updated_at: Option<i64>,
    last_updated_snapshot_id: Option<i64>,
}

impl JavaPartitionRow {
    fn into_partition_row(self) -> PartitionRow {
        PartitionRow {
            partition_category: self.partition.category,
            spec_id: self.spec_id,
            record_count: self.record_count,
            file_count: self.file_count,
            total_data_file_size_in_bytes: self.total_data_file_size_in_bytes,
            position_delete_record_count: self.position_delete_record_count,
            position_delete_file_count: self.position_delete_file_count,
            equality_delete_record_count: self.equality_delete_record_count,
            equality_delete_file_count: self.equality_delete_file_count,
            last_updated_at: self.last_updated_at,
            last_updated_snapshot_id: self.last_updated_snapshot_id,
        }
    }
}

/// Extract the Rust `partitions` batch into [`PartitionRow`]s by COLUMN NAME, incl. the `partition` struct
/// (`category` sub-field) and `last_updated_at` (timestamptz µs).
fn extract_partition_rows(batch: &RecordBatch) -> Vec<PartitionRow> {
    let partition = batch
        .column_by_name("partition")
        .expect("partition")
        .as_struct();
    let partition_category = partition
        .column_by_name("category")
        .map(|c| c.as_string::<i32>());
    let spec_id = primitive::<Int32Type>(batch, "spec_id");
    let record_count = primitive::<Int64Type>(batch, "record_count");
    let file_count = primitive::<Int32Type>(batch, "file_count");
    let total_size = primitive::<Int64Type>(batch, "total_data_file_size_in_bytes");
    let pos_del_records = primitive::<Int64Type>(batch, "position_delete_record_count");
    let pos_del_files = primitive::<Int32Type>(batch, "position_delete_file_count");
    let eq_del_records = primitive::<Int64Type>(batch, "equality_delete_record_count");
    let eq_del_files = primitive::<Int32Type>(batch, "equality_delete_file_count");
    let last_updated_at = primitive::<TimestampMicrosecondType>(batch, "last_updated_at");
    let last_updated_snapshot_id = primitive::<Int64Type>(batch, "last_updated_snapshot_id");

    (0..batch.num_rows())
        .map(|i| PartitionRow {
            partition_category: partition_category.and_then(|c| {
                if c.is_null(i) {
                    None
                } else {
                    Some(c.value(i).to_string())
                }
            }),
            spec_id: spec_id.value(i),
            record_count: record_count.value(i),
            file_count: file_count.value(i),
            total_data_file_size_in_bytes: total_size.value(i),
            position_delete_record_count: pos_del_records.value(i),
            position_delete_file_count: pos_del_files.value(i),
            equality_delete_record_count: eq_del_records.value(i),
            equality_delete_file_count: eq_del_files.value(i),
            last_updated_at: if last_updated_at.is_null(i) {
                None
            } else {
                Some(last_updated_at.value(i))
            },
            last_updated_snapshot_id: opt_i64(last_updated_snapshot_id, i),
        })
        .collect()
}

#[tokio::test]
async fn test_partitions_table_matches_java_rows() {
    let Some(dir) = manifest_dir() else {
        println!(
            "skipping test_partitions_table_matches_java_rows — set ICEBERG_INTEROP_MANIFEST_DIR \
             (run dev/java-interop/run-inspection-manifests.sh)"
        );
        return;
    };

    let table = load_table_a2(&dir);
    let batches: Vec<RecordBatch> = table
        .inspect()
        .partitions()
        .scan()
        .await
        .expect("partitions scan")
        .try_collect()
        .await
        .expect("collect partitions scan");
    let mut rust_rows = Vec::new();
    for batch in &batches {
        rust_rows.extend(extract_partition_rows(batch));
    }
    rust_rows.sort_by(|a, b| a.partition_category.cmp(&b.partition_category));

    let mut java_rows: Vec<PartitionRow> =
        read_java::<JavaPartitionRow>(&dir, "java_partitions.json")
            .into_iter()
            .map(JavaPartitionRow::into_partition_row)
            .collect();
    java_rows.sort_by(|a, b| a.partition_category.cmp(&b.partition_category));

    assert_eq!(
        rust_rows, java_rows,
        "Rust `partitions` rows must equal Java's PartitionsTable rows field-for-field (the partition \
         struct, spec_id, record/file/size rollups, the four delete-count columns, last_updated_at µs, \
         last_updated_snapshot_id)"
    );

    // Focused: ≥2 partition rows; the cat=a partition received a position-delete so its delete counts are
    // non-zero, and its total_data_file_size_in_bytes counts ONLY the data files (not the delete file).
    assert!(
        rust_rows.len() >= 2,
        "≥2 partition rows (the A2 table partitions cat=a and cat=b)"
    );
    let cat_a = rust_rows
        .iter()
        .find(|r| r.partition_category.as_deref() == Some("a"))
        .expect("a cat=a partition row");
    assert!(
        cat_a.position_delete_record_count > 0 && cat_a.position_delete_file_count > 0,
        "the cat=a partition received a position-delete: its position_delete_* counts are non-zero"
    );
    // cat=a has data files A (1100) + C (1300) = 2400; the 150-byte delete file is NOT counted here.
    assert_eq!(
        cat_a.total_data_file_size_in_bytes, 2400,
        "total_data_file_size_in_bytes counts ONLY data-file sizes (A 1100 + C 1300), not the delete file"
    );
    assert_eq!(
        cat_a.file_count, 2,
        "cat=a file_count is the 2 DATA files (A, C); deletes are counted in the delete columns"
    );

    let cat_b = rust_rows
        .iter()
        .find(|r| r.partition_category.as_deref() == Some("b"))
        .expect("a cat=b partition row");
    assert_eq!(
        cat_b.position_delete_record_count, 0,
        "the cat=b partition received no delete (only the surviving data file D)"
    );

    println!(
        "test_partitions_table_matches_java_rows OK — {} partitions matched Java (cat=a delete counts \
         non-zero)",
        rust_rows.len()
    );
}

// ===========================================================================================
// A3 — the FIVE cross-snapshot `all_*` inspection tables: `all_data_files`, `all_delete_files`,
// `all_files`, `all_entries`, `all_manifests`.
//
// Builds DIRECTLY on the A1/A2 harness above (reusing `manifest_dir()`, `load_table_a2()`, the `FileRow`
// model + its `canonical` collapse + `extract_rust_rows`, the `EntryRow` model + `extract_entry_rows`, the
// `ManifestRow` extraction, the hex decode, and `FileIO::new_with_fs()`) over the SAME `<dir>/table_a2`
// that A2 reads — A2's commits already have the right cross-snapshot shape:
//   s1 newAppend(A=cat a, B=cat b, C=cat a, D=cat b) -> a DATA manifest M1.
//   s2 newRowDelta(+pos-delete cat=a)                -> a DELETE manifest MD; M1 is CARRIED into s2's list.
//   s3 newDelete(B)                                  -> a rewritten DATA manifest M1' where B is a DELETED
//                                                       tombstone (status 2); D keeps cat=b alive.
// So the `all_*` semantics ARE exercised:
//   * all_data_files/all_files: the manifest SOURCE is the dedup-by-PATH union of manifests reachable from
//     ALL snapshots (Java `BaseAllMetadataTableScan.reachableManifests`), so they INCLUDE B (live in s1's
//     M1) which the CURRENT `files`/`data_files` table EXCLUDES (current sees only M1' where B is deleted).
//     Manifests are dedup'd by path but the FILES inside are NOT (Java javadoc "may return duplicate rows")
//     — a file present in two distinct reachable manifests (e.g. A in M1 and M1') appears MULTIPLE times.
//   * all_entries: every entry across all reachable manifests, incl. tombstones.
//   * all_manifests: ONE row per (manifest × referencing snapshot), NOT dedup'd — M1 in s1's AND s2's lists
//     -> TWO rows with distinct `reference_snapshot_id` (its `added_snapshot_id` stays s1, so for the
//     s2-referencing carried row reference_snapshot_id != added_snapshot_id). Schema = the regular
//     `manifests` schema PLUS a `reference_snapshot_id` column.
//
// CRITICAL — these tables MAY RETURN DUPLICATE ROWS, so the comparison is an order-independent MULTISET:
// sort BOTH sides by a TOTAL key (the full row's `Debug` repr — a faithful total order over the plain data
// rows, since two rows are `==` iff their `Debug` strings match) and compare element-by-element WITHOUT
// dedup (`assert_eq!` on the equal-length sorted vectors). The same A1/A2 canonical forms apply (absent
// metric/bound map None≡empty; file_format already uppercase). `readable_metrics` is DEFERRED as in A1/A2.
// ===========================================================================================

/// Sort plain data rows by a TOTAL key (their `Debug` repr) for an order-independent MULTISET comparison
/// that PRESERVES duplicates. Two rows compare equal under `==` iff their `Debug` strings are identical for
/// these derive-`Debug` data structs, so sorting by that string then comparing element-by-element (no
/// dedup) is a faithful multiset equality.
fn multiset_sorted<T: std::fmt::Debug>(mut rows: Vec<T>) -> Vec<T> {
    rows.sort_by_key(|row| format!("{row:?}"));
    rows
}

// ---------------------------------------------------------------------------------------------
// all_data_files / all_delete_files / all_files — the same flat `files` schema as A1, so the A1 `FileRow`
// extraction is reused verbatim. The only difference is the manifest SOURCE (cross-snapshot reachable union)
// and that rows may be DUPLICATED — hence the multiset comparison.
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn test_all_data_files_table_matches_java_rows() {
    let Some(dir) = manifest_dir() else {
        println!(
            "skipping test_all_data_files_table_matches_java_rows — set ICEBERG_INTEROP_MANIFEST_DIR \
             (run dev/java-interop/run-inspection-manifests.sh)"
        );
        return;
    };

    let table = load_table_a2(&dir);

    let rust_rows: Vec<FileRow> = multiset_sorted(
        scan_rows(
            table
                .inspect()
                .all_data_files()
                .scan()
                .await
                .expect("all_data_files scan"),
        )
        .await
        .into_iter()
        .map(FileRow::canonical)
        .collect(),
    );
    let java_rows: Vec<FileRow> = multiset_sorted(
        read_java_rows(&dir, "java_all_data_files.json")
            .into_iter()
            .map(|row| row.into_file_row().canonical())
            .collect(),
    );

    assert_eq!(
        rust_rows, java_rows,
        "Rust `all_data_files` rows must equal Java's AllDataFilesTable rows as an order-independent \
         MULTISET (duplicates preserved) — readable_metrics deferred, absent-map-vs-empty canonicalized"
    );

    // Cross-snapshot reach: B (cat=b, deleted at s3) is PRESENT in all_data_files (live in s1's M1) but
    // ABSENT from a fresh current-snapshot `data_files` scan over the same table.
    let b_path = rust_rows
        .iter()
        .map(|r| r.file_path.clone())
        .find(|p| leaf(p) == "00000-b.parquet")
        .expect(
            "all_data_files CONTAINS B (the cat=b file deleted at s3) via cross-snapshot reach",
        );
    let current_data_files = scan_rows(
        table
            .inspect()
            .data_files()
            .scan()
            .await
            .expect("data_files scan"),
    )
    .await;
    assert!(
        !current_data_files.iter().any(|r| r.file_path == b_path),
        "the CURRENT data_files table must NOT contain B (it was deleted at s3) — proving all_data_files \
         reached a non-current snapshot's manifest"
    );

    // Every all_data_files row is DATA content (the delete file is excluded).
    assert!(
        rust_rows.iter().all(|r| r.content == 0),
        "all_data_files contains only DATA-content files (content == 0)"
    );

    println!(
        "test_all_data_files_table_matches_java_rows OK — {} rows matched Java (B present via reach)",
        rust_rows.len()
    );
}

#[tokio::test]
async fn test_all_delete_files_table_matches_java_rows() {
    let Some(dir) = manifest_dir() else {
        println!(
            "skipping test_all_delete_files_table_matches_java_rows — set ICEBERG_INTEROP_MANIFEST_DIR \
             (run dev/java-interop/run-inspection-manifests.sh)"
        );
        return;
    };

    let table = load_table_a2(&dir);

    let rust_rows: Vec<FileRow> = multiset_sorted(
        scan_rows(
            table
                .inspect()
                .all_delete_files()
                .scan()
                .await
                .expect("all_delete_files scan"),
        )
        .await
        .into_iter()
        .map(FileRow::canonical)
        .collect(),
    );
    let java_rows: Vec<FileRow> = multiset_sorted(
        read_java_rows(&dir, "java_all_delete_files.json")
            .into_iter()
            .map(|row| row.into_file_row().canonical())
            .collect(),
    );

    assert_eq!(
        rust_rows, java_rows,
        "Rust `all_delete_files` rows must equal Java's AllDeleteFilesTable rows as an order-independent \
         MULTISET (duplicates preserved)"
    );

    // Every all_delete_files row is DELETE content (the data files are excluded).
    assert!(
        rust_rows.iter().all(|r| r.content == 1),
        "all_delete_files contains only delete-content files (content == 1)"
    );

    println!(
        "test_all_delete_files_table_matches_java_rows OK — {} rows matched Java",
        rust_rows.len()
    );
}

#[tokio::test]
async fn test_all_files_table_matches_java_rows() {
    let Some(dir) = manifest_dir() else {
        println!(
            "skipping test_all_files_table_matches_java_rows — set ICEBERG_INTEROP_MANIFEST_DIR \
             (run dev/java-interop/run-inspection-manifests.sh)"
        );
        return;
    };

    let table = load_table_a2(&dir);

    let rust_raw: Vec<FileRow> = scan_rows(
        table
            .inspect()
            .all_files()
            .scan()
            .await
            .expect("all_files scan"),
    )
    .await
    .into_iter()
    .map(FileRow::canonical)
    .collect();
    let rust_rows = multiset_sorted(rust_raw.clone());
    let java_rows: Vec<FileRow> = multiset_sorted(
        read_java_rows(&dir, "java_all_files.json")
            .into_iter()
            .map(|row| row.into_file_row().canonical())
            .collect(),
    );

    assert_eq!(
        rust_rows, java_rows,
        "Rust `all_files` rows must equal Java's AllFilesTable rows as an order-independent MULTISET \
         (data + delete content; duplicates preserved)"
    );

    // Cross-snapshot reach: B (deleted at s3) is present in all_files but absent from the current `files`.
    let b_path = rust_rows
        .iter()
        .map(|r| r.file_path.clone())
        .find(|p| leaf(p) == "00000-b.parquet")
        .expect("all_files CONTAINS B (the cat=b file deleted at s3) via cross-snapshot reach");
    let current_files = scan_rows(table.inspect().files().scan().await.expect("files scan")).await;
    assert!(
        !current_files.iter().any(|r| r.file_path == b_path),
        "the CURRENT files table must NOT contain B — proving all_files reached a non-current snapshot"
    );

    // DUPLICATE rows: a file present in ≥2 distinct reachable manifests (e.g. A, present in both s1's M1 and
    // s3's rewritten M1') appears MORE THAN ONCE. The total row count therefore EXCEEDS the distinct
    // file-path count, AND at least one specific file_path occurs twice.
    let distinct_paths: std::collections::HashSet<&String> =
        rust_rows.iter().map(|r| &r.file_path).collect();
    assert!(
        rust_rows.len() > distinct_paths.len(),
        "all_files MAY RETURN DUPLICATE ROWS: row count ({}) must exceed the distinct file-path count ({})",
        rust_rows.len(),
        distinct_paths.len()
    );
    let mut path_counts: HashMap<&String, usize> = HashMap::new();
    for row in &rust_rows {
        *path_counts.entry(&row.file_path).or_default() += 1;
    }
    assert!(
        path_counts.values().any(|&count| count >= 2),
        "at least one file_path must appear ≥2 times (a file carried across two reachable manifests)"
    );

    println!(
        "test_all_files_table_matches_java_rows OK — {} rows ({} distinct paths) matched Java (B present, \
         duplicates preserved)",
        rust_rows.len(),
        distinct_paths.len()
    );
}

// ---------------------------------------------------------------------------------------------
// all_entries — every manifest entry across ALL reachable manifests (incl. tombstones). Same nested
// `data_file` schema as A2 `entries`, so `extract_entry_rows` is reused; the multiset comparison preserves
// the duplicate entries a file carried across two reachable manifests produces.
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn test_all_entries_table_matches_java_rows() {
    let Some(dir) = manifest_dir() else {
        println!(
            "skipping test_all_entries_table_matches_java_rows — set ICEBERG_INTEROP_MANIFEST_DIR \
             (run dev/java-interop/run-inspection-manifests.sh)"
        );
        return;
    };

    let table = load_table_a2(&dir);
    let batches: Vec<RecordBatch> = table
        .inspect()
        .all_entries()
        .scan()
        .await
        .expect("all_entries scan")
        .try_collect()
        .await
        .expect("collect all_entries scan");
    let mut rust_rows = Vec::new();
    for batch in &batches {
        rust_rows.extend(extract_entry_rows(batch));
    }
    let rust_rows = multiset_sorted(rust_rows);

    let java_rows = multiset_sorted(
        read_java::<JavaEntryRow>(&dir, "java_all_entries.json")
            .into_iter()
            .map(JavaEntryRow::into_entry_row)
            .collect(),
    );

    assert_eq!(
        rust_rows, java_rows,
        "Rust `all_entries` rows must equal Java's AllEntriesTable rows as an order-independent MULTISET \
         (every entry across all reachable manifests, incl. tombstones; duplicates preserved)"
    );

    // Cross-snapshot reach + tombstone surfacing: B (deleted at s3) appears both LIVE (status 0 Existing /
    // 1 Added, from s1's M1) and as a DELETED tombstone (status 2, from s3's M1'). At minimum ≥1 status-2
    // tombstone is present (as in `entries`), AND B's file_path appears under MORE THAN ONE status.
    assert!(
        rust_rows.iter().any(|r| r.status == 2),
        "all_entries MUST surface ≥1 DELETED tombstone (status 2)"
    );
    let b_statuses: std::collections::HashSet<i32> = rust_rows
        .iter()
        .filter(|r| leaf(&r.data_file.file_path) == "00000-b.parquet")
        .map(|r| r.status)
        .collect();
    assert!(
        b_statuses.len() >= 2,
        "B (deleted at s3) must appear under ≥2 distinct statuses across reachable manifests (live + \
         tombstone), got {b_statuses:?}"
    );

    println!(
        "test_all_entries_table_matches_java_rows OK — {} entries matched Java (B live + tombstone)",
        rust_rows.len()
    );
}

// ---------------------------------------------------------------------------------------------
// all_manifests — ONE row per (manifest × referencing snapshot), NOT dedup'd. Schema = the regular
// `manifests` schema (reused via `extract_manifest_rows`) PLUS a `reference_snapshot_id` column.
// ---------------------------------------------------------------------------------------------

/// One Java `AllManifestsTable` row — the regular [`JavaManifestRow`] columns PLUS `reference_snapshot_id`.
#[derive(Debug, Clone, PartialEq, Deserialize)]
struct JavaAllManifestRow {
    content: i32,
    path: String,
    length: i64,
    partition_spec_id: i32,
    added_snapshot_id: i64,
    added_data_files_count: i32,
    existing_data_files_count: i32,
    deleted_data_files_count: i32,
    added_delete_files_count: i32,
    existing_delete_files_count: i32,
    deleted_delete_files_count: i32,
    partition_summaries: Vec<JavaPartitionSummary>,
    reference_snapshot_id: i64,
}

/// A normalized, comparable `all_manifests` row — the [`ManifestRow`] fields PLUS `reference_snapshot_id`.
#[derive(Debug, Clone, PartialEq)]
struct AllManifestRow {
    manifest: ManifestRow,
    reference_snapshot_id: i64,
}

impl JavaAllManifestRow {
    fn into_all_manifest_row(self) -> AllManifestRow {
        AllManifestRow {
            manifest: ManifestRow {
                content: self.content,
                path: self.path,
                length: self.length,
                partition_spec_id: self.partition_spec_id,
                added_snapshot_id: self.added_snapshot_id,
                added_data_files_count: self.added_data_files_count,
                existing_data_files_count: self.existing_data_files_count,
                deleted_data_files_count: self.deleted_data_files_count,
                added_delete_files_count: self.added_delete_files_count,
                existing_delete_files_count: self.existing_delete_files_count,
                deleted_delete_files_count: self.deleted_delete_files_count,
                partition_summaries: self
                    .partition_summaries
                    .into_iter()
                    .map(|s| PartitionSummary {
                        contains_null: s.contains_null,
                        contains_nan: s.contains_nan,
                        lower_bound: s.lower_bound,
                        upper_bound: s.upper_bound,
                    })
                    .collect(),
            },
            reference_snapshot_id: self.reference_snapshot_id,
        }
    }
}

/// Extract the Rust `all_manifests` batch into [`AllManifestRow`]s: the regular manifest columns (via the
/// A2 [`extract_manifest_rows`]) plus the `reference_snapshot_id` column.
fn extract_all_manifest_rows(batch: &RecordBatch) -> Vec<AllManifestRow> {
    let manifests = extract_manifest_rows(batch);
    let reference_snapshot_id = primitive::<Int64Type>(batch, "reference_snapshot_id");
    manifests
        .into_iter()
        .enumerate()
        .map(|(i, manifest)| AllManifestRow {
            manifest,
            reference_snapshot_id: reference_snapshot_id.value(i),
        })
        .collect()
}

#[tokio::test]
async fn test_all_manifests_table_matches_java_rows() {
    let Some(dir) = manifest_dir() else {
        println!(
            "skipping test_all_manifests_table_matches_java_rows — set ICEBERG_INTEROP_MANIFEST_DIR \
             (run dev/java-interop/run-inspection-manifests.sh)"
        );
        return;
    };

    let table = load_table_a2(&dir);
    let batches: Vec<RecordBatch> = table
        .inspect()
        .all_manifests()
        .scan()
        .await
        .expect("all_manifests scan")
        .try_collect()
        .await
        .expect("collect all_manifests scan");
    let mut rust_rows = Vec::new();
    for batch in &batches {
        rust_rows.extend(extract_all_manifest_rows(batch));
    }
    let rust_rows = multiset_sorted(rust_rows);

    let java_rows = multiset_sorted(
        read_java::<JavaAllManifestRow>(&dir, "java_all_manifests.json")
            .into_iter()
            .map(JavaAllManifestRow::into_all_manifest_row)
            .collect(),
    );

    assert_eq!(
        rust_rows, java_rows,
        "Rust `all_manifests` rows must equal Java's AllManifestsTable rows as an order-independent \
         MULTISET (one row per manifest × referencing snapshot, NOT dedup'd; the regular manifests columns \
         PLUS reference_snapshot_id)"
    );

    // A manifest appears in TWO rows with DIFFERENT reference_snapshot_id (M1 is carried from s1 into s2's
    // list). For the carried row, reference_snapshot_id != added_snapshot_id (added stays s1).
    let mut by_path: HashMap<&String, Vec<&AllManifestRow>> = HashMap::new();
    for row in &rust_rows {
        by_path.entry(&row.manifest.path).or_default().push(row);
    }
    let shared = by_path
        .values()
        .find(|rows| {
            let refs: std::collections::HashSet<i64> =
                rows.iter().map(|r| r.reference_snapshot_id).collect();
            refs.len() >= 2
        })
        .expect(
            "≥1 manifest referenced by TWO snapshots (M1 carried from s1 into s2's manifest list)",
        );
    let carried = shared
        .iter()
        .find(|r| r.reference_snapshot_id != r.manifest.added_snapshot_id)
        .expect("the carried row has reference_snapshot_id != added_snapshot_id");
    assert_ne!(
        carried.reference_snapshot_id, carried.manifest.added_snapshot_id,
        "the carried manifest row's reference_snapshot_id (the LISTING snapshot) differs from its \
         added_snapshot_id (the snapshot that first wrote it)"
    );

    println!(
        "test_all_manifests_table_matches_java_rows OK — {} rows matched Java (shared manifest has 2 \
         distinct reference_snapshot_id; carried ref != added)",
        rust_rows.len()
    );
}

// ===========================================================================================
// A4 — SCAN PLANNING interop: does Rust plan the SAME data files Java does for a given filter?
//
// Builds on the SAME table-writing harness as A1-A3, but proves the SCAN PLANNER instead of the metadata
// TABLES. Scan planning reads the AVRO manifests + applies a filter to PRUNE data files via (a) partition
// predicates and (b) column-metric (lower/upper bound) ranges, ASSOCIATES delete files with the surviving
// data files, and computes the per-file RESIDUAL. It does NOT read parquet — so the SAME env-gated,
// no-parquet methodology as A1-A3 (the referenced .parquet paths need not exist; planning reads manifests).
//
// The Java oracle's `generate-inspection-manifests` mode now ALSO writes a dedicated V2 table to
// `<dir>/table_a4` (A1's `<dir>/table` + A2's `<dir>/table_a2` untouched) — partition by identity(category),
// schema {1 id long, 2 category string, 3 value double}, three DATA files with DISTINCT id metric bounds
// (F1 cat=a id[1,10], F2 cat=b id[11,20], F3 cat=a id[21,30]) + a position-delete for F1 — and, for each
// named filter scenario, emits `java_scan_<name>.json` via Java's REAL `table.newScan().filter(expr)
// .planFiles()`. This test loads `<dir>/table_a4/metadata/final.metadata.json`, runs `table.scan()
// .with_filter(pred).build()?.plan_files()` with the SAME filter built via the Rust predicate API, and
// asserts the {data_file_path SET, per-file sorted delete paths, per-file residual_always_true} EXACTLY
// equals the Java JSON, order-independent by data_file_path.
//
// THE SCENARIOS (a stable ordered list shared with Java BY NAME):
//   s0 "no_filter"       : no filter             -> plans F1,F2,F3 ; F1 carries the delete ; residual TRUE
//   s1 "partition_a"     : category = 'a'         -> plans F1,F3 (partition prune drops the cat=b F2) ;
//                                                    residual always-true (the filter IS the partition)
//   s2 "metric_id_gt_15" : id > 15                -> plans F2,F3 (F1 upper=10 < 15 pruned by METRICS) ;
//                                                    residual NOT always-true (id is not a partition col)
//   s3 "combined"        : category='a' AND id>25 -> plans F3 only (partition drops F2 ; metrics drop F1) ;
//                                                    residual NOT always-true
//
// Same env gate as A1-A3 (`ICEBERG_INTEROP_MANIFEST_DIR`): a clean NO-OP when unset.
//
// RESIDUAL SCOPE (deferral, mirroring the Java oracle). A4 compares only a BOOLEAN "residual is fully
// covered by partitioning" per planned file — Java: `residual().op() == Operation.TRUE`; Rust: the task
// predicate is `None` (no filter / no residual evaluator) OR `Some(BoundPredicate::AlwaysTrue)` (the filter
// was entirely partition-implied for this file's partition tuple). The full residual-EXPRESSION string
// comparison is OUT OF SCOPE (it needs a cross-language expression-normalization design; Rust residuals are
// already unit-tested in `scan/mod.rs`). This proves the partition-filter-removal SPLIT matches WITHOUT a
// fragile cross-language expression-string comparison.
// ===========================================================================================

/// One Java scan-plan row from `java_scan_<name>.json`: a planned data file, its applicable (sorted) delete
/// paths, and whether its residual is fully covered by partitioning.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
struct JavaScanRow {
    data_file_path: String,
    delete_file_paths: Vec<String>,
    residual_always_true: bool,
}

/// A normalized, fully-comparable planned-file row — Java + Rust both produce one of these so the
/// per-scenario comparison is a single sorted-vector `==`. `delete_file_paths` is sorted; the whole vector
/// is sorted by `data_file_path` for an order-independent (by data file) comparison.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ScanRow {
    data_file_path: String,
    delete_file_paths: Vec<String>,
    residual_always_true: bool,
}

impl From<JavaScanRow> for ScanRow {
    fn from(java: JavaScanRow) -> Self {
        let mut delete_file_paths = java.delete_file_paths;
        delete_file_paths.sort();
        ScanRow {
            data_file_path: java.data_file_path,
            delete_file_paths,
            residual_always_true: java.residual_always_true,
        }
    }
}

/// Whether a planned task's residual is fully covered by partitioning. The Rust planner stores the
/// partition-reduced residual in `task.predicate`: `None` when the scan has no filter (no residual
/// evaluator), and `Some(BoundPredicate::AlwaysTrue)` when the filter was entirely partition-implied for
/// this file's partition tuple — both mean "no per-row filtering needed", i.e. residual ⟺ Java's
/// `residual().op() == Operation.TRUE`. Any other `Some(predicate)` is a non-trivial residual.
fn residual_always_true(task: &FileScanTask) -> bool {
    matches!(task.predicate(), None | Some(BoundPredicate::AlwaysTrue))
}

/// Project one Rust [`FileScanTask`] into the comparable [`ScanRow`] (delete paths sorted).
fn task_to_scan_row(task: &FileScanTask) -> ScanRow {
    let mut delete_file_paths: Vec<String> = task
        .deletes
        .iter()
        .map(|delete| delete.file_path.clone())
        .collect();
    delete_file_paths.sort();
    ScanRow {
        data_file_path: task.data_file_path().to_string(),
        delete_file_paths,
        residual_always_true: residual_always_true(task),
    }
}

/// Sort planned rows by `data_file_path` for an order-independent (by data file) comparison.
fn sorted_scan_rows(mut rows: Vec<ScanRow>) -> Vec<ScanRow> {
    rows.sort_by(|a, b| a.data_file_path.cmp(&b.data_file_path));
    rows
}

/// Build a `Table` over the Java-written A4 `final.metadata.json` (under `<dir>/table_a4`), local-fs FileIO.
fn load_table_a4(dir: &std::path::Path) -> Table {
    let metadata_path = dir.join("table_a4/metadata/final.metadata.json");
    let json = fs::read_to_string(&metadata_path)
        .unwrap_or_else(|error| panic!("read {}: {error}", metadata_path.display()));
    let metadata: TableMetadata = serde_json::from_str(&json)
        .unwrap_or_else(|error| panic!("parse {}: {error}", metadata_path.display()));
    Table::builder()
        .metadata(metadata)
        .metadata_location(metadata_path.to_string_lossy().to_string())
        .identifier(
            TableIdent::from_strs(["interop", "inspection_scan_a4"]).expect("valid identifier"),
        )
        .file_io(FileIO::new_with_fs())
        .build()
        .expect("build A4 table from Java-written final.metadata.json")
}

/// Plan a scan with `filter` (or no filter when `None`) via Rust's REAL `table.scan().with_filter(pred)
/// .build()?.plan_files()` and project the planned tasks into the comparable, sorted [`ScanRow`]s.
async fn plan_scan_rows(table: &Table, filter: Option<Predicate>) -> Vec<ScanRow> {
    let mut builder = table.scan();
    if let Some(predicate) = filter {
        builder = builder.with_filter(predicate);
    }
    let scan = builder.build().expect("build table scan");
    let tasks: Vec<FileScanTask> = scan
        .plan_files()
        .await
        .expect("plan_files")
        .try_collect()
        .await
        .expect("collect plan_files");
    sorted_scan_rows(tasks.iter().map(task_to_scan_row).collect())
}

/// Load + sort the Java scan-plan rows for `<name>` from `java_scan_<name>.json`.
fn java_scan_rows(dir: &std::path::Path, name: &str) -> Vec<ScanRow> {
    sorted_scan_rows(
        read_java::<JavaScanRow>(dir, &format!("java_scan_{name}.json"))
            .into_iter()
            .map(ScanRow::from)
            .collect(),
    )
}

/// The leaf (trailing path segment) of every planned data file, sorted — a compact view for focused asserts.
fn leaf_data_paths(rows: &[ScanRow]) -> Vec<String> {
    let mut leaves: Vec<String> = rows.iter().map(|r| leaf(&r.data_file_path)).collect();
    leaves.sort();
    leaves
}

#[tokio::test]
async fn test_scan_planning_matches_java_plans() {
    let Some(dir) = manifest_dir() else {
        println!(
            "skipping test_scan_planning_matches_java_plans — set ICEBERG_INTEROP_MANIFEST_DIR \
             (run dev/java-interop/run-inspection-manifests.sh)"
        );
        return;
    };

    let table = load_table_a4(&dir);

    // The SAME filters Java built, by name, via the Rust predicate API. `category` is the identity-partition
    // column (partition pruning); `id` is a non-partition column whose lower/upper bound metrics drive
    // METRIC pruning.
    let s1_partition_a = Reference::new("category").equal_to(Datum::string("a"));
    let s2_metric_id_gt_15 = Reference::new("id").greater_than(Datum::long(15));
    let s3_combined = Reference::new("category")
        .equal_to(Datum::string("a"))
        .and(Reference::new("id").greater_than(Datum::long(25)));

    // -- s0 no_filter: plans F1, F2, F3; F1 carries the delete; residual always-true for every file. -------
    let rust_s0 = plan_scan_rows(&table, None).await;
    let java_s0 = java_scan_rows(&dir, "no_filter");
    assert_eq!(
        rust_s0, java_s0,
        "s0 no_filter: Rust plan_files() must equal Java's planFiles() (data-file SET, per-file sorted \
         delete paths, per-file residual_always_true)"
    );
    assert_eq!(
        leaf_data_paths(&rust_s0),
        vec![
            "00000-f1.parquet".to_string(),
            "00000-f2.parquet".to_string(),
            "00001-f3.parquet".to_string(),
        ],
        "s0 plans all three data files F1, F2, F3"
    );
    assert!(
        rust_s0.iter().all(|r| r.residual_always_true),
        "s0 has no filter, so every task's residual is trivially always-true"
    );

    // -- s1 partition_a (category='a'): plans F1, F3 (partition prune drops the cat=b F2). ----------------
    let rust_s1 = plan_scan_rows(&table, Some(s1_partition_a)).await;
    let java_s1 = java_scan_rows(&dir, "partition_a");
    assert_eq!(
        rust_s1, java_s1,
        "s1 partition_a: Rust plan_files() must equal Java's planFiles()"
    );
    assert_eq!(
        leaf_data_paths(&rust_s1),
        vec![
            "00000-f1.parquet".to_string(),
            "00001-f3.parquet".to_string()
        ],
        "s1 plans exactly {{F1, F3}} — PARTITION pruning drops the cat=b F2"
    );
    assert!(
        rust_s1.iter().all(|r| r.residual_always_true),
        "s1's filter category='a' IS the identity partition, so the residual reduces to always-true"
    );

    // -- s2 metric_id_gt_15 (id > 15): plans F2, F3 (F1 upper=10 < 15 pruned by METRICS — the subtle one). -
    let rust_s2 = plan_scan_rows(&table, Some(s2_metric_id_gt_15)).await;
    let java_s2 = java_scan_rows(&dir, "metric_id_gt_15");
    assert_eq!(
        rust_s2, java_s2,
        "s2 metric_id_gt_15: Rust plan_files() must equal Java's planFiles()"
    );
    assert_eq!(
        leaf_data_paths(&rust_s2),
        vec![
            "00000-f2.parquet".to_string(),
            "00001-f3.parquet".to_string()
        ],
        "s2 plans exactly {{F2, F3}} — METRIC pruning drops F1 (its id upper bound 10 < 15)"
    );
    assert!(
        rust_s2.iter().all(|r| !r.residual_always_true),
        "s2's filter id>15 is NOT a partition column, so the residual is NOT always-true"
    );

    // -- s3 combined (category='a' AND id>25): plans F3 only (partition drops F2; metrics drop F1). --------
    let rust_s3 = plan_scan_rows(&table, Some(s3_combined)).await;
    let java_s3 = java_scan_rows(&dir, "combined");
    assert_eq!(
        rust_s3, java_s3,
        "s3 combined: Rust plan_files() must equal Java's planFiles()"
    );
    assert_eq!(
        leaf_data_paths(&rust_s3),
        vec!["00001-f3.parquet".to_string()],
        "s3 plans exactly {{F3}} — partition drops the cat=b F2, metrics drop F1 (id upper 10 < 25)"
    );
    assert!(
        rust_s3.iter().all(|r| !r.residual_always_true),
        "s3's filter still carries the non-partition id>25 leaf, so the residual is NOT always-true on F3"
    );

    // -- Delete association: F1's planned task carries the position-delete in s0 AND s1 (same cat=a partition,
    //    sequence number after F1's append). Pinned on F1 specifically (unambiguous); the bulk comparisons
    //    above already proved the FULL per-file delete sets match Java for every scenario. --------------
    for (label, rows) in [("s0", &rust_s0), ("s1", &rust_s1)] {
        let f1 = rows
            .iter()
            .find(|r| leaf(&r.data_file_path) == "00000-f1.parquet")
            .unwrap_or_else(|| panic!("{label}: F1 is planned"));
        assert_eq!(
            f1.delete_file_paths.len(),
            1,
            "{label}: F1 carries exactly one applicable delete file"
        );
        assert_eq!(
            leaf(&f1.delete_file_paths[0]),
            "00000-f1-deletes.parquet",
            "{label}: F1's associated delete is the position-delete written for category=a"
        );
    }

    println!(
        "test_scan_planning_matches_java_plans OK — s0={}, s1={}, s2={}, s3={} planned files matched Java \
         (partition-prune s1 drops F2, metric-prune s2 drops F1, combined s3 = F3, F1 delete associated)",
        rust_s0.len(),
        rust_s1.len(),
        rust_s2.len(),
        rust_s3.len()
    );
}

// ===========================================================================================
// readable_metrics — the typed-decode interop proof for the `files` table's trailing virtual
// `readable_metrics` STRUCT, the LAST inspection-table surface A1-A4 explicitly DEFER.
//
// `readable_metrics` (Java MetricsUtil.readableMetricsStruct) is a STRUCT with ONE sub-field per LEAF column
// of the data schema, each itself a struct of the six metrics {column_size, value_count, null_value_count,
// nan_value_count, lower_bound, upper_bound}. The four counts come from the file's metric maps; the bounds
// are the file's stored bound bytes DECODED to the COLUMN's OWN type (Java
// Conversions.fromByteBuffer(field.type(), buffer); Rust `Datum::try_from_bytes`). A1 round-trips the same
// metric/bound MAPS but compares only the bound BYTES — it never reaches the TYPED decode, notably a STRING
// bound. This test proves that decode across THREE type classes: Int64 for id, Utf8 for name, Float64 for
// score.
//
// The Java oracle's `generate-inspection-manifests` mode writes a dedicated UNPARTITIONED V2 table to
// `<dir>/table_rm` (A1's `<dir>/table` + A2's `<dir>/table_a2` + A4's `<dir>/table_a4` UNTOUCHED) — schema
// {1 id long, 2 name string, 3 score double}, one data file with RICH, DISTINCT per-column metrics — and
// emits `java_rm_files.json` keyed by leaf column NAME -> the six metric NAMES -> the typed scalar value.
// This test loads `<dir>/table_rm`, runs `inspect().files().scan().to_arrow()`, extracts the trailing
// `readable_metrics` struct, and asserts BY COLUMN NAME and BY METRIC NAME (order-independent) that every
// metric equals Java's: counts exact; long/string bounds exact; the double bound compared by `f64::to_bits`.
//
// Same env gate as A1-A4 (`ICEBERG_INTEROP_MANIFEST_DIR`): a clean NO-OP when unset.
//
// COMPARE BY NAME, NOT FIELD ID. Java's readable_metrics sub-field ids come from a HashMap-order counter; the
// Rust port assigns ascending ids (a documented divergence — see `readable_metrics.rs` module docs). The
// sub-field NAMES match, so everything is keyed by name; the id divergence is OUT OF SCOPE here (the row
// stays 🟡 — the GAP_MATRIX `ids` divergence note stands).
//
// DEFERRED — the PROMOTED-TYPE bound (an int-encoded bound on a column promoted to long). It needs schema
// evolution / an old-schema data file and is OUT OF SCOPE for this increment; long/string/double is rich
// enough to pin the typed decode. Noted as deferred.
// ===========================================================================================

/// The decoded `readable_metrics` of ONE leaf column: the four counts (optional i64) plus the lower/upper
/// bound read as the column's own typed Arrow value. Exactly one of the three typed-bound `Option`s is
/// populated per column (Int64 for id, Utf8 for name, Float64 for score) — the other two stay `None`.
#[derive(Debug, Clone, PartialEq)]
struct ColumnReadableMetrics {
    column_size: Option<i64>,
    value_count: Option<i64>,
    null_value_count: Option<i64>,
    nan_value_count: Option<i64>,
    lower_bound_long: Option<i64>,
    upper_bound_long: Option<i64>,
    lower_bound_string: Option<String>,
    upper_bound_string: Option<String>,
    lower_bound_double: Option<f64>,
    upper_bound_double: Option<f64>,
}

/// Build a `Table` over the Java-written `table_rm` `final.metadata.json`, local-fs FileIO.
fn load_table_rm(dir: &std::path::Path) -> Table {
    let metadata_path = dir.join("table_rm/metadata/final.metadata.json");
    let json = fs::read_to_string(&metadata_path)
        .unwrap_or_else(|error| panic!("read {}: {error}", metadata_path.display()));
    let metadata: TableMetadata = serde_json::from_str(&json)
        .unwrap_or_else(|error| panic!("parse {}: {error}", metadata_path.display()));
    Table::builder()
        .metadata(metadata)
        .metadata_location(metadata_path.to_string_lossy().to_string())
        .identifier(
            TableIdent::from_strs(["interop", "inspection_readable_metrics"])
                .expect("valid identifier"),
        )
        .file_io(FileIO::new_with_fs())
        .build()
        .expect("build readable_metrics table from Java-written final.metadata.json")
}

/// An optional i64 count sub-field of a per-column metric struct, read by metric NAME.
fn rm_opt_i64(metrics: &StructArray, name: &str) -> Option<i64> {
    let array = metrics
        .column_by_name(name)
        .unwrap_or_else(|| panic!("readable_metrics metric {name} present"))
        .as_primitive::<Int64Type>();
    if array.is_null(0) {
        None
    } else {
        Some(array.value(0))
    }
}

/// Extract the SINGLE-row Rust `readable_metrics` struct into a map keyed by leaf column NAME. Each leaf
/// column's sub-struct is read for the four counts and the THREE possible typed bound shapes; only the shape
/// matching the column's Arrow type is populated (the others stay `None`), so the test can assert the right
/// type decoded without hardcoding column order.
fn extract_rust_readable_metrics(batch: &RecordBatch) -> HashMap<String, ColumnReadableMetrics> {
    assert_eq!(
        batch.num_rows(),
        1,
        "table_rm has exactly one data file, so the files table has exactly one row"
    );
    let readable_metrics = batch
        .column_by_name("readable_metrics")
        .expect("the files table has a trailing readable_metrics struct column")
        .as_struct();

    let mut out = HashMap::new();
    for column_index in 0..readable_metrics.num_columns() {
        let column_name = readable_metrics.column_names()[column_index].to_string();
        let metrics = readable_metrics.column(column_index).as_struct();

        // The two bound sub-fields carry the COLUMN's own Arrow type. Read whichever of Int64 / Utf8 /
        // Float64 the sub-field actually is; the other two stay None. A wrong builder type in the production
        // path would make BOTH the expected reader fail here AND leave the typed value unmatched below.
        let bound_field = |name: &str| -> &ArrayRef {
            metrics.column_by_name(name).unwrap_or_else(|| {
                panic!("readable_metrics bound {name} present for {column_name}")
            })
        };
        let lower = bound_field("lower_bound");
        let upper = bound_field("upper_bound");

        let read_long = |array: &ArrayRef| -> Option<i64> {
            let array = array.as_primitive::<Int64Type>();
            if array.is_null(0) {
                None
            } else {
                Some(array.value(0))
            }
        };
        let read_string = |array: &ArrayRef| -> Option<String> {
            let array = array.as_string::<i32>();
            if array.is_null(0) {
                None
            } else {
                Some(array.value(0).to_string())
            }
        };
        let read_double = |array: &ArrayRef| -> Option<f64> {
            let array = array.as_primitive::<Float64Type>();
            if array.is_null(0) {
                None
            } else {
                Some(array.value(0))
            }
        };

        let is_long = matches!(lower.data_type(), arrow_schema::DataType::Int64);
        let is_string = matches!(lower.data_type(), arrow_schema::DataType::Utf8);
        let is_double = matches!(lower.data_type(), arrow_schema::DataType::Float64);

        out.insert(column_name.clone(), ColumnReadableMetrics {
            column_size: rm_opt_i64(metrics, "column_size"),
            value_count: rm_opt_i64(metrics, "value_count"),
            null_value_count: rm_opt_i64(metrics, "null_value_count"),
            nan_value_count: rm_opt_i64(metrics, "nan_value_count"),
            lower_bound_long: if is_long { read_long(lower) } else { None },
            upper_bound_long: if is_long { read_long(upper) } else { None },
            lower_bound_string: if is_string { read_string(lower) } else { None },
            upper_bound_string: if is_string { read_string(upper) } else { None },
            lower_bound_double: if is_double { read_double(lower) } else { None },
            upper_bound_double: if is_double { read_double(upper) } else { None },
        });
    }
    out
}

/// Read one metric of a Java leaf column's object as an optional i64 (a count or a long bound). A JSON null
/// is `None`; a number is `Some`. Panics if the metric is missing or not an integer.
fn java_opt_i64(column: &serde_json::Value, column_name: &str, metric: &str) -> Option<i64> {
    let value = column
        .get(metric)
        .unwrap_or_else(|| panic!("Java {column_name}.{metric} present"));
    if value.is_null() {
        None
    } else {
        Some(
            value
                .as_i64()
                .unwrap_or_else(|| panic!("Java {column_name}.{metric} is an integer")),
        )
    }
}

#[tokio::test]
async fn test_readable_metrics_table_matches_java_rows() {
    let Some(dir) = manifest_dir() else {
        println!(
            "skipping test_readable_metrics_table_matches_java_rows — set ICEBERG_INTEROP_MANIFEST_DIR \
             (run dev/java-interop/run-inspection-manifests.sh)"
        );
        return;
    };

    let table = load_table_rm(&dir);
    let batches: Vec<RecordBatch> = table
        .inspect()
        .files()
        .scan()
        .await
        .expect("files scan")
        .try_collect()
        .await
        .expect("collect files scan");
    assert_eq!(
        batches.len(),
        1,
        "table_rm's single data file yields one files-table batch"
    );
    let rust_metrics = extract_rust_readable_metrics(&batches[0]);

    // Parse Java's `java_rm_files.json`: an object keyed by leaf column NAME -> the six metric names.
    let java_path = dir.join("java_rm_files.json");
    let java_json = fs::read_to_string(&java_path)
        .unwrap_or_else(|error| panic!("read {}: {error}", java_path.display()));
    let java_columns: HashMap<String, serde_json::Value> = serde_json::from_str(&java_json)
        .unwrap_or_else(|error| panic!("parse {}: {error}", java_path.display()));

    // RISK (missing/extra leaf column): the SET of leaf column names must match Java exactly. A leaf-detection
    // bug (e.g. dropping a primitive, or emitting a non-leaf container) would change this set.
    let rust_names: std::collections::BTreeSet<&String> = rust_metrics.keys().collect();
    let java_names: std::collections::BTreeSet<&String> = java_columns.keys().collect();
    assert_eq!(
        rust_names, java_names,
        "the SET of readable_metrics leaf column names must match Java (no missing/extra column)"
    );
    assert_eq!(
        rust_names.into_iter().collect::<Vec<_>>(),
        vec!["id", "name", "score"],
        "table_rm's three leaf columns are id, name, score (sorted by name)"
    );

    // -- Per-column, per-metric comparison BY NAME (order-independent). --------------------------------
    for (column_name, rust) in &rust_metrics {
        let java = java_columns
            .get(column_name)
            .unwrap_or_else(|| panic!("Java has the {column_name} readable_metrics column"));

        // RISK (count cross-wiring / wrong source): each of the four counts must equal Java's EXACTLY. Java's
        // metrics are distinct per metric source (column_size != value_count != null_value_count != ...), so a
        // swap between two count sub-fields is observable. nan_value_count is non-null ONLY for score (the
        // double column); null_value_count is non-zero for name — both pin the absent-vs-present count path.
        assert_eq!(
            rust.column_size,
            java_opt_i64(java, column_name, "column_size"),
            "{column_name}.column_size must equal Java"
        );
        assert_eq!(
            rust.value_count,
            java_opt_i64(java, column_name, "value_count"),
            "{column_name}.value_count must equal Java"
        );
        assert_eq!(
            rust.null_value_count,
            java_opt_i64(java, column_name, "null_value_count"),
            "{column_name}.null_value_count must equal Java (non-zero for name)"
        );
        assert_eq!(
            rust.nan_value_count,
            java_opt_i64(java, column_name, "nan_value_count"),
            "{column_name}.nan_value_count must equal Java (present only for the double score)"
        );

        // RISK (wrong typed decode of the bound): the lower/upper bound must decode to the COLUMN's own type
        // and value. For id (long) and name (string), an exact compare; for score (double), a `f64::to_bits`
        // compare so float bit-level drift in the decode cannot hide. The Java bound is the already-decoded
        // scalar; the Rust bound is the Arrow typed value. Comparing by NAME catches a typed-decode bug that a
        // raw-bytes compare (A1) cannot — most pointedly the STRING bound.
        let lower_value = java
            .get("lower_bound")
            .unwrap_or_else(|| panic!("Java {column_name}.lower_bound present"));
        let upper_value = java
            .get("upper_bound")
            .unwrap_or_else(|| panic!("Java {column_name}.upper_bound present"));

        match column_name.as_str() {
            "id" => {
                let java_lower = lower_value.as_i64().expect("id lower_bound is a long");
                let java_upper = upper_value.as_i64().expect("id upper_bound is a long");
                assert_eq!(
                    rust.lower_bound_long,
                    Some(java_lower),
                    "id.lower_bound decodes to the column's LONG type and Java's value"
                );
                assert_eq!(
                    rust.upper_bound_long,
                    Some(java_upper),
                    "id.upper_bound decodes to the column's LONG type and Java's value"
                );
            }
            "name" => {
                let java_lower = lower_value.as_str().expect("name lower_bound is a string");
                let java_upper = upper_value.as_str().expect("name upper_bound is a string");
                assert_eq!(
                    rust.lower_bound_string.as_deref(),
                    Some(java_lower),
                    "name.lower_bound decodes to the column's STRING type and Java's value (the case A1's \
                     raw-bytes compare cannot reach)"
                );
                assert_eq!(
                    rust.upper_bound_string.as_deref(),
                    Some(java_upper),
                    "name.upper_bound decodes to the column's STRING type and Java's value"
                );
            }
            "score" => {
                let java_lower = lower_value.as_f64().expect("score lower_bound is a double");
                let java_upper = upper_value.as_f64().expect("score upper_bound is a double");
                let rust_lower = rust
                    .lower_bound_double
                    .expect("score.lower_bound decodes to the column's DOUBLE type");
                let rust_upper = rust
                    .upper_bound_double
                    .expect("score.upper_bound decodes to the column's DOUBLE type");
                // RISK (float bit-level drift in the bound decode): compare by `f64::to_bits`, not `==`, so a
                // NaN-vs-NaN or a 1-ulp drift in the double-bound round-trip cannot pass unnoticed.
                assert_eq!(
                    rust_lower.to_bits(),
                    java_lower.to_bits(),
                    "score.lower_bound decodes to Java's double bit-for-bit"
                );
                assert_eq!(
                    rust_upper.to_bits(),
                    java_upper.to_bits(),
                    "score.upper_bound decodes to Java's double bit-for-bit"
                );
            }
            other => panic!("unexpected leaf column {other} in table_rm readable_metrics"),
        }
    }

    println!(
        "test_readable_metrics_table_matches_java_rows OK — {} leaf columns (id long, name string, score \
         double) matched Java's readable_metrics struct field-for-field (counts + typed bounds)",
        rust_metrics.len()
    );
}
