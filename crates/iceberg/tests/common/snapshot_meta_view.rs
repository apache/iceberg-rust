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

//! The CANONICAL "snapshot metadata view" builder shared by the metadata-level interop tests
//! (`interop_rowdelta_meta.rs`, `interop_write_actions_meta.rs`) — the Rust mirror of the Java
//! `InteropOracle.SnapshotMetaOracle` emitter (mode `emit-snapshot-meta`). The canonicalization
//! contract (incl. the V1 sequence-number-tie ordinal limit) is documented on the E1 test module
//! and the Java emitter; the two sides' allowlists + sort tuples must stay IDENTICAL.

use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

use iceberg::io::FileIO;
use iceberg::spec::{
    DataContentType, Literal, ManifestContentType, ManifestStatus, TableMetadata, Type,
};
use serde_json::{Map as JsonMap, Value as JsonValue, json};

/// The COUNT-only summary allowlist — must stay IDENTICAL to the Java
/// `SnapshotMetaOracle.SUMMARY_COUNT_KEYS` (every count key, no byte-size keys).
pub const SUMMARY_COUNT_KEYS: &[&str] = &[
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
    // Not a count, but a cross-language-comparable semantic marker (Java
    // `BaseReplacePartitions` sets `replace-partitions=true`; the Rust action mirrors it).
    "replace-partitions",
];

// ===========================================================================================
// The canonical view builder — the Rust mirror of Java's `SnapshotMetaOracle.emit`.
// ===========================================================================================

/// Build the canonical snapshot-metadata view of the table whose CURRENT metadata file is
/// `metadata_json_path`, reading manifests through a local-filesystem `FileIO` (the fixtures write
/// bare absolute paths). Risk this pins: a divergence anywhere in operation classification, summary
/// counts, manifest-list structure, or post-inheritance sequence numbers surfaces as a JSON diff.
pub async fn snapshot_meta_view(metadata_json_path: &Path) -> JsonValue {
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
        // The full tuple, INCLUDING the count fields: within one commit a rewritten
        // (tombstone-carrying) manifest and an added manifest share (content, seq, min_seq), and a
        // tie would fall back to each table's manifest-LIST file order — writer-dependent, not a
        // spec contract. The counts disambiguate deterministically (mirrored by the Java
        // SnapshotMetaOracle comparator).
        manifests.sort_by_key(|manifest| {
            (
                content_rank(&manifest.content),
                manifest.sequence_number,
                manifest.min_sequence_number,
                manifest.added_files_count.map_or(-1, i64::from),
                manifest.existing_files_count.map_or(-1, i64::from),
                manifest.deleted_files_count.map_or(-1, i64::from),
                manifest.added_rows_count.map_or(-1, |count| count as i64),
                manifest
                    .existing_rows_count
                    .map_or(-1, |count| count as i64),
                manifest.deleted_rows_count.map_or(-1, |count| count as i64),
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
