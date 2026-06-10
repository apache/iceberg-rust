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

//! METADATA-LEVEL row-delta interop (sprint increment E1) — the snapshot/manifest SEMANTICS proof
//! that complements the data-level scan-execution interop (`interop_scan_exec.rs`, which proved the
//! ROWS match both directions).
//!
//! Both sides build the same CANONICAL "snapshot metadata view" of an on-disk table — the Java side
//! via `InteropOracle` mode `emit-snapshot-meta` (`SnapshotMetaOracle`), the Rust side via
//! [`snapshot_meta_view`] here. The canonicalization (mirrored EXACTLY in the Java emitter):
//!
//! - snapshots ordered by SEQUENCE NUMBER; snapshot ids replaced by ORDINALS (parent too);
//! - `operation` as its own field (the re-parsed summary splits it out of the map on both sides);
//! - summary filtered to the COUNT-key allowlist — `*-files-size` keys are EXCLUDED because parquet
//!   byte sizes legitimately differ between writers;
//! - manifests sorted by `(content, sequence_number, min_sequence_number)`; paths excluded;
//!   `added_snapshot_id` emitted as an ordinal;
//! - entries sorted by the tuple `(status, content, record_count, sequence_number, equality_ids,
//!   partition)`; the `sequence_number` is the POST-INHERITANCE data sequence number (the
//!   merge-on-read applicability input); `file_sequence_number` is NOT emitted (no public Rust
//!   accessor — tracked);
//! - `partition` is `null` for an unpartitioned spec, else the spec's SINGLE-VALUE JSON
//!   serialization (Rust `Literal::try_into_json` ↔ Java `SingleValueParser.toJson` — the one
//!   cross-language-canonical rendering of a partition tuple).
//!
//! THE THREE COMPARISONS (per fixture; driven by `dev/java-interop/run-interop-rowdelta-meta.sh`):
//! 1. Rust's view of the JAVA-written table  == `java_meta.json` (Java's view of its own table) —
//!    READ parity: sequence-number inheritance, summary parsing, operation classification.
//! 2. Rust's view of the RUST-written table  == `java_meta.json` — WRITE parity: Rust's
//!    `fast_append` + `row_delta` commits produce metadata canonically indistinguishable from
//!    Java's `newAppend` + `newRowDelta` for the same logical operations.
//! 3. (script-side) Java's view of the RUST-written table == `java_meta.json`, diffed byte-for-byte
//!    by the run script — Java itself judging Rust's written metadata.
//!
//! GATED on `ICEBERG_INTEROP_META_DIR` (the run script's temp dir containing the per-fixture
//! subdirectories). Unset ⇒ clean no-op, so the offline `cargo test` gate is unaffected.
//!
//! LIMIT (reviewer-flagged): the ordinal scheme assumes DISTINCT sequence numbers. Every V1
//! snapshot has sequence number 0, so a multi-snapshot V1 table would produce
//! iteration-order-dependent ordinals — do NOT extend this oracle to V1 tables without adding a
//! tiebreaker (timestamp, then snapshot id). The three fixtures are V2 row-delta chains, where
//! every commit bumps the sequence number.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use iceberg::io::FileIO;
use iceberg::spec::{
    DataContentType, Literal, ManifestContentType, ManifestStatus, TableMetadata, Type,
};
use serde_json::{Map as JsonMap, Value as JsonValue, json};

/// The COUNT-only summary allowlist — must stay IDENTICAL to the Java
/// `SnapshotMetaOracle.SUMMARY_COUNT_KEYS` (every count key, no byte-size keys).
const SUMMARY_COUNT_KEYS: &[&str] = &[
    "added-data-files",
    "deleted-data-files",
    "total-data-files",
    "added-delete-files",
    "added-equality-delete-files",
    "removed-equality-delete-files",
    "added-position-delete-files",
    "removed-position-delete-files",
    "added-dvs",
    "removed-dvs",
    "removed-delete-files",
    "total-delete-files",
    "added-records",
    "deleted-records",
    "total-records",
    "added-position-deletes",
    "removed-position-deletes",
    "total-position-deletes",
    "added-equality-deletes",
    "removed-equality-deletes",
    "total-equality-deletes",
    "changed-partition-count",
];

// ===========================================================================================
// The canonical view builder — the Rust mirror of Java's `SnapshotMetaOracle.emit`.
// ===========================================================================================

/// Build the canonical snapshot-metadata view of the table whose CURRENT metadata file is
/// `metadata_json_path`, reading manifests through a local-filesystem `FileIO` (the fixtures write
/// bare absolute paths). Risk this pins: a divergence anywhere in operation classification, summary
/// counts, manifest-list structure, or post-inheritance sequence numbers surfaces as a JSON diff.
async fn snapshot_meta_view(metadata_json_path: &Path) -> JsonValue {
    let raw = fs::read_to_string(metadata_json_path)
        .unwrap_or_else(|error| panic!("read {}: {error}", metadata_json_path.display()));
    let metadata: TableMetadata = serde_json::from_str(&raw)
        .unwrap_or_else(|error| panic!("parse {}: {error}", metadata_json_path.display()));
    let file_io = FileIO::new_with_fs();

    // Snapshots ordered by sequence number; ordinal = index in that order.
    let mut snapshots: Vec<_> = metadata.snapshots().cloned().collect();
    snapshots.sort_by_key(|snapshot| snapshot.sequence_number());
    let ordinals: BTreeMap<i64, usize> = snapshots
        .iter()
        .enumerate()
        .map(|(ordinal, snapshot)| (snapshot.snapshot_id(), ordinal))
        .collect();

    let mut snapshot_views = Vec::with_capacity(snapshots.len());
    for snapshot in &snapshots {
        let summary = snapshot.summary();
        let mut summary_view = JsonMap::new();
        for key in SUMMARY_COUNT_KEYS {
            if let Some(value) = summary.additional_properties.get(*key) {
                summary_view.insert((*key).to_string(), JsonValue::String(value.clone()));
            }
        }

        let manifest_list = snapshot
            .load_manifest_list(&file_io, &metadata)
            .await
            .unwrap_or_else(|error| {
                panic!(
                    "load manifest list of snapshot {}: {error}",
                    snapshot.snapshot_id()
                )
            });
        let mut manifests: Vec<_> = manifest_list.entries().to_vec();
        manifests.sort_by_key(|manifest| {
            (
                content_rank(&manifest.content),
                manifest.sequence_number,
                manifest.min_sequence_number,
            )
        });

        let mut manifest_views = Vec::with_capacity(manifests.len());
        for manifest_file in &manifests {
            let manifest = manifest_file
                .load_manifest(&file_io)
                .await
                .unwrap_or_else(|error| panic!("load manifest: {error}"));

            // The manifest's OWN spec + schema (all its entries share them) drive the partition
            // rendering — the file's-own-spec rule, not the table default.
            let manifest_meta = manifest.metadata();
            let unpartitioned = manifest_meta.partition_spec.fields().is_empty();
            let partition_type = manifest_meta
                .partition_spec
                .partition_type(&manifest_meta.schema)
                .expect("partition type of the manifest's own spec");

            let mut entry_views: Vec<(EntrySortKey, JsonValue)> = manifest
                .entries()
                .iter()
                .map(|entry| {
                    let data_file = entry.data_file();
                    let mut equality_ids = data_file.equality_ids();
                    if let Some(ids) = equality_ids.as_mut() {
                        ids.sort_unstable();
                    }
                    let partition_json = if unpartitioned {
                        JsonValue::Null
                    } else {
                        Literal::Struct(data_file.partition().clone())
                            .try_into_json(&Type::Struct(partition_type.clone()))
                            .expect("partition tuple renders to single-value JSON")
                    };
                    let equality_ids_key: String = equality_ids
                        .iter()
                        .flatten()
                        .map(|id| format!("{id},"))
                        .collect();
                    let key = EntrySortKey {
                        status: status_id(entry.status()),
                        content: content_name(data_file.content_type()).to_string(),
                        record_count: data_file.record_count(),
                        sequence_number: entry.sequence_number().unwrap_or(i64::MIN),
                        equality_ids_key,
                        partition_key: partition_json_sort_key(&partition_json),
                    };
                    let view = json!({
                        "status": status_id(entry.status()),
                        "content": content_name(data_file.content_type()),
                        "record_count": data_file.record_count(),
                        "sequence_number": entry.sequence_number(),
                        "equality_ids": equality_ids,
                        "partition": partition_json,
                    });
                    (key, view)
                })
                .collect();
            entry_views.sort_by(|a, b| a.0.cmp(&b.0));

            manifest_views.push(json!({
                "content": match manifest_file.content {
                    ManifestContentType::Data => "data",
                    ManifestContentType::Deletes => "deletes",
                },
                "sequence_number": manifest_file.sequence_number,
                "min_sequence_number": manifest_file.min_sequence_number,
                "added_snapshot_ordinal": ordinals.get(&manifest_file.added_snapshot_id),
                "added_files_count": manifest_file.added_files_count,
                "existing_files_count": manifest_file.existing_files_count,
                "deleted_files_count": manifest_file.deleted_files_count,
                "added_rows_count": manifest_file.added_rows_count,
                "existing_rows_count": manifest_file.existing_rows_count,
                "deleted_rows_count": manifest_file.deleted_rows_count,
                "entries": entry_views.into_iter().map(|(_, view)| view).collect::<Vec<_>>(),
            }));
        }

        snapshot_views.push(json!({
            "ordinal": ordinals[&snapshot.snapshot_id()],
            "parent_ordinal": snapshot
                .parent_snapshot_id()
                .map(|parent_id| ordinals[&parent_id]),
            "sequence_number": snapshot.sequence_number(),
            "operation": summary.operation.as_str(),
            "summary": JsonValue::Object(summary_view),
            "manifests": manifest_views,
        }));
    }

    json!({ "snapshots": snapshot_views })
}

/// The explicit cross-language entry sort tuple — must order identically to the Java comparator in
/// `SnapshotMetaOracle.entryJsons`.
#[derive(PartialEq, Eq, PartialOrd, Ord)]
struct EntrySortKey {
    status: i32,
    content: String,
    record_count: u64,
    sequence_number: i64,
    equality_ids_key: String,
    partition_key: String,
}

fn status_id(status: ManifestStatus) -> i32 {
    match status {
        ManifestStatus::Existing => 0,
        ManifestStatus::Added => 1,
        ManifestStatus::Deleted => 2,
    }
}

fn content_name(content: DataContentType) -> &'static str {
    match content {
        DataContentType::Data => "data",
        DataContentType::PositionDeletes => "position_deletes",
        DataContentType::EqualityDeletes => "equality_deletes",
    }
}

fn content_rank(content: &ManifestContentType) -> i32 {
    match content {
        ManifestContentType::Data => 0,
        ManifestContentType::Deletes => 1,
    }
}

/// The partition component of the entry sort key. Java sorts by its rendered partition JSON string
/// (null ⇒ ""); rendering a `serde_json::Value` compactly is NOT guaranteed byte-identical to
/// Jackson, but within ONE side the key is total and consistent — and the fixtures never contain
/// two entries that tie on every preceding tuple component AND differ only in partition rendering,
/// so the cross-language order is identical where it matters. (Guarded by the comparisons
/// themselves: a wrong ordering surfaces as a JSON diff.)
fn partition_json_sort_key(partition: &JsonValue) -> String {
    match partition {
        JsonValue::Null => String::new(),
        other => other.to_string(),
    }
}

// ===========================================================================================
// The env-gated tests.
// ===========================================================================================

fn meta_dir() -> Option<PathBuf> {
    std::env::var_os("ICEBERG_INTEROP_META_DIR")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
}

/// The fixture subdirectories the run script generates. `part_scan` exercises the partitioned
/// rendering; the other two are unpartitioned position- and equality-delete row-delta chains.
const FIXTURES: &[&str] = &["scan_exec", "eq_delete", "part_scan"];

fn load_json(path: &Path) -> JsonValue {
    let raw =
        fs::read_to_string(path).unwrap_or_else(|error| panic!("read {}: {error}", path.display()));
    serde_json::from_str(&raw).unwrap_or_else(|error| panic!("parse {}: {error}", path.display()))
}

/// Direction 1 (READ parity): Rust's canonical view of the JAVA-written table equals Java's own
/// view of it. Pins: sequence-number inheritance on read, operation + summary parsing, the
/// manifest-list structure as Rust sees it.
#[tokio::test]
async fn test_rust_view_of_java_table_matches_java_view() {
    let Some(dir) = meta_dir() else {
        println!(
            "skipping interop_rowdelta_meta — set ICEBERG_INTEROP_META_DIR \
             (run dev/java-interop/run-interop-rowdelta-meta.sh)"
        );
        return;
    };

    for fixture in FIXTURES {
        let fixture_dir = dir.join(fixture);
        let java_view = load_json(&fixture_dir.join("java_meta.json"));
        let rust_view =
            snapshot_meta_view(&fixture_dir.join("table/metadata/final.metadata.json")).await;
        assert_eq!(
            rust_view, java_view,
            "{fixture}: Rust's view of the JAVA-written table diverges from Java's own view"
        );
        println!("{fixture}: Rust view of Java table == Java view OK");
    }
}

/// Direction 2 (WRITE parity — the E1 crown jewel): Rust's canonical view of the RUST-written
/// table ALSO equals Java's view of the JAVA-written table for the same logical operations. With
/// the script-side check (Java's view of the Rust table diffed against `java_meta.json`), this
/// pins that Rust's `fast_append` + `row_delta` emit Java-identical snapshot semantics: operation
/// classification, summary counts, manifest split, and the sequence-number/inheritance chain.
#[tokio::test]
async fn test_rust_written_table_metadata_matches_java_semantics() {
    let Some(dir) = meta_dir() else {
        println!(
            "skipping interop_rowdelta_meta — set ICEBERG_INTEROP_META_DIR \
             (run dev/java-interop/run-interop-rowdelta-meta.sh)"
        );
        return;
    };

    for fixture in FIXTURES {
        let fixture_dir = dir.join(fixture);
        let rust_table_metadata = fixture_dir.join("rust_table/metadata/final.metadata.json");
        assert!(
            rust_table_metadata.exists(),
            "{fixture}: missing {} — run the Rust GEN step of the run script first",
            rust_table_metadata.display()
        );
        let java_view = load_json(&fixture_dir.join("java_meta.json"));
        let rust_view = snapshot_meta_view(&rust_table_metadata).await;
        assert_eq!(
            rust_view, java_view,
            "{fixture}: the RUST-written table's canonical metadata diverges from Java's \
             semantics for the same logical operations"
        );
        println!("{fixture}: Rust-written table metadata == Java semantics OK");
    }
}
