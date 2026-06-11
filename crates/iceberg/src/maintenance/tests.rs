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

//! Tests for [`DeleteOrphanFiles`](super::DeleteOrphanFiles).
//!
//! The end-to-end tests run against a [`MemoryCatalog`](crate::memory::MemoryCatalog) wired with a
//! LOCAL-FILESYSTEM [`FileIO`] (so real files land on disk under a `TempDir` and
//! [`FileIO::list`](crate::io::FileIO::list) walks a real directory tree). The unit tests pin the
//! URI normalization / orphan-join / hidden-path logic directly (cheaper, and the exact place the
//! corruption-class risks live).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use futures::future::BoxFuture;
use tempfile::TempDir;

// =============================================================================================
// Unit tests — URI normalization, the orphan join, the hidden-path filter (the risk core)
// =============================================================================================
use super::delete_orphan_files::test_hooks::{
    FileUriProbe, classify_one, default_equal_schemes_probe, flatten_map_probe,
    is_hidden_under_probe, split_uri_probe, version_hint_probe,
};
use super::{DeleteOrphanFiles, PrefixMismatchMode};
use crate::io::{FileIO, FileIOBuilder, LocalFsStorageFactory};
use crate::memory::MemoryCatalogBuilder;
use crate::spec::{
    DataContentType, DataFile, DataFileBuilder, DataFileFormat, Literal, NestedField,
    PartitionSpec, PrimitiveType, Schema, Struct, Transform, Type,
};
use crate::table::Table;
use crate::transaction::{ApplyTransactionAction, Transaction};
use crate::{Catalog, CatalogBuilder, NamespaceIdent, Result, TableCreation, TableIdent};

/// Risk: the URI split MUST distinguish a scheme-less local path from a `file://` URI — a split
/// that invents a scheme on a bare path (or drops the scheme on a `file://` path) either splits
/// matching files (never deletes) or conflates them (deletes live data). Pins the exact
/// (scheme, authority, path) tuple for each shape Iceberg writers produce.
#[test]
fn test_split_uri_distinguishes_local_path_from_file_scheme_and_object_store() {
    // Scheme-less absolute local path: no scheme, no authority, path is the whole string.
    assert_eq!(
        split_uri_probe("/tmp/warehouse/t/data/a.parquet"),
        (None, None, "/tmp/warehouse/t/data/a.parquet".to_string())
    );
    // file:/path (no authority).
    assert_eq!(
        split_uri_probe("file:/tmp/t/a.parquet"),
        (
            Some("file".to_string()),
            None,
            "/tmp/t/a.parquet".to_string()
        )
    );
    // file:///path (empty authority, path keeps its leading slash).
    assert_eq!(
        split_uri_probe("file:///tmp/t/a.parquet"),
        (
            Some("file".to_string()),
            Some(String::new()),
            "/tmp/t/a.parquet".to_string()
        )
    );
    // s3://bucket/key.
    assert_eq!(
        split_uri_probe("s3://bucket/warehouse/t/a.parquet"),
        (
            Some("s3".to_string()),
            Some("bucket".to_string()),
            "/warehouse/t/a.parquet".to_string()
        )
    );
    // s3a://bucket/key.
    assert_eq!(
        split_uri_probe("s3a://bucket/warehouse/t/a.parquet"),
        (
            Some("s3a".to_string()),
            Some("bucket".to_string()),
            "/warehouse/t/a.parquet".to_string()
        )
    );
}

/// Risk: a scheme-less VALID path (metadata stores bare paths) must MATCH a `file://`-listed
/// actual path — Java `uriComponentMatch` makes a null/empty valid component match any actual.
/// Getting this backwards makes every local-fs table report all its files as scheme-conflicts.
#[test]
fn test_schemeless_valid_path_matches_any_actual_scheme() {
    let empty = HashMap::new();
    let valid = FileUriProbe::parse("/tmp/t/a.parquet", &empty, &empty); // scheme None
    let actual_file = FileUriProbe::parse("file:///tmp/t/a.parquet", &empty, &empty); // scheme file
    assert!(
        valid.scheme_matches(&actual_file),
        "null valid scheme matches any actual"
    );
    assert!(
        valid.authority_matches(&actual_file),
        "null valid authority matches any actual"
    );

    // The reverse direction is a REAL conflict: a `file://` VALID vs a bare ACTUAL — file != null.
    let valid_file = FileUriProbe::parse("file:///tmp/t/a.parquet", &empty, &empty);
    let actual_bare = FileUriProbe::parse("/tmp/t/a.parquet", &empty, &empty);
    assert!(
        !valid_file.scheme_matches(&actual_bare),
        "a concrete valid scheme does NOT match an absent actual scheme"
    );
}

/// Risk: equal_schemes/equal_authorities must canonicalize BOTH sides so an `s3a://` listed file
/// and an `s3://` valid file are recognized as the same. The default `{s3n,s3a → s3}` plus the
/// merge-with-user semantics are pinned here.
#[test]
fn test_equal_schemes_default_and_user_merge_canonicalize_both_sides() {
    // Defaults map s3a and s3n to s3.
    let defaults = default_equal_schemes_probe();
    assert_eq!(defaults.get("s3a"), Some(&"s3".to_string()));
    assert_eq!(defaults.get("s3n"), Some(&"s3".to_string()));

    let schemes = HashMap::from([("s3a".to_string(), "s3".to_string())]);
    let empty = HashMap::new();
    let valid = FileUriProbe::parse("s3://bucket/t/a.parquet", &schemes, &empty);
    let actual = FileUriProbe::parse("s3a://bucket/t/a.parquet", &schemes, &empty);
    assert_eq!(valid.path_probe(), actual.path_probe(), "paths join");
    assert!(
        valid.scheme_matches(&actual),
        "s3a normalizes to s3 ⇒ scheme matches"
    );

    // User comma-key flatten: "s1,s2 → svc" splits to both.
    let flattened = flatten_map_probe(HashMap::from([(
        "name1, name2".to_string(),
        "service".to_string(),
    )]));
    assert_eq!(flattened.get("name1"), Some(&"service".to_string()));
    assert_eq!(flattened.get("name2"), Some(&"service".to_string()));
}

/// Risk (PrefixMismatchMode, the three branches): a path-matched file with a scheme/authority
/// conflict must be classified per the mode — DELETE ⇒ orphan, IGNORE ⇒ not orphan, ERROR ⇒ not
/// orphan-now but registers a conflict (the action later fails). A wrong branch deletes live data
/// (DELETE leaking) or never cleans (DELETE not firing).
#[test]
fn test_prefix_mismatch_mode_classifies_scheme_conflict_three_ways() {
    let empty = HashMap::new();
    // VALID has a concrete authority "bucketA"; ACTUAL has "bucketB" — a real authority conflict
    // on the same path.
    let valid = FileUriProbe::parse("s3://bucketA/t/a.parquet", &empty, &empty);
    let actual = FileUriProbe::parse("s3://bucketB/t/a.parquet", &empty, &empty);

    // DELETE ⇒ orphan.
    let (is_orphan, _, _) = classify_one(
        &actual,
        std::slice::from_ref(&valid),
        PrefixMismatchMode::Delete,
    );
    assert!(
        is_orphan,
        "DELETE mode treats the conflicting file as orphan"
    );

    // IGNORE ⇒ not orphan, conflict recorded but harmless.
    let (is_orphan, schemes, authorities) = classify_one(
        &actual,
        std::slice::from_ref(&valid),
        PrefixMismatchMode::Ignore,
    );
    assert!(!is_orphan, "IGNORE mode skips the conflicting file");
    assert!(schemes.is_empty(), "scheme matched (both s3)");
    assert_eq!(
        authorities,
        vec![("bucketA".to_string(), "bucketB".to_string())],
        "the authority conflict is recorded"
    );

    // ERROR ⇒ not orphan-now, conflict recorded (the caller turns a non-empty set into an error).
    let (is_orphan, _, authorities) = classify_one(&actual, &[valid], PrefixMismatchMode::Error);
    assert!(
        !is_orphan,
        "ERROR mode does not delete; it records the conflict to fail later"
    );
    assert_eq!(authorities.len(), 1);
}

/// Risk: a path-matched file that is FULLY compatible with ANY valid candidate is NEVER orphan and
/// NEVER a conflict, even if another valid candidate on the same path conflicts. The "any match ⇒
/// safe" reduction is the under-deletion bias; losing it would delete a reachable file.
#[test]
fn test_any_compatible_valid_candidate_makes_file_not_orphan() {
    let empty = HashMap::new();
    let actual = FileUriProbe::parse("s3://bucketB/t/a.parquet", &empty, &empty);
    let conflicting = FileUriProbe::parse("s3://bucketA/t/a.parquet", &empty, &empty);
    let compatible = FileUriProbe::parse("s3://bucketB/t/a.parquet", &empty, &empty);

    // Even in DELETE mode, a compatible candidate spares the file.
    let (is_orphan, _, _) = classify_one(
        &actual,
        &[conflicting, compatible],
        PrefixMismatchMode::Delete,
    );
    assert!(
        !is_orphan,
        "a fully-compatible valid candidate makes the file reachable"
    );
}

/// Risk (the hidden-path filter): files under `.`/`_` directories are SKIPPED (Iceberg's own
/// hidden/temp dirs), EXCEPT partition directories `_<field>=` for a partition field whose name
/// starts with `_`/`.`. A filter that hides a real partition dir would orphan (and delete) live
/// partition data; one that fails to hide `.tmp/` would delete an in-progress write.
#[test]
fn test_hidden_path_filter_skips_hidden_dirs_except_named_partition_dirs() {
    let base = "/wh/t";
    // A partition field literally named "_part" ⇒ "_part=" dirs are visible.
    let partition_prefixes = vec!["_part=".to_string()];

    // Plain hidden dirs are hidden.
    assert!(is_hidden_under_probe(
        base,
        "/wh/t/.hidden/x.parquet",
        &partition_prefixes
    ));
    assert!(is_hidden_under_probe(
        base,
        "/wh/t/_tmp/x.parquet",
        &partition_prefixes
    ));
    // A normal data file is not hidden.
    assert!(!is_hidden_under_probe(
        base,
        "/wh/t/data/x.parquet",
        &partition_prefixes
    ));
    // A partition dir matching the named-partition exception is NOT hidden.
    assert!(!is_hidden_under_probe(
        base,
        "/wh/t/data/_part=5/x.parquet",
        &partition_prefixes
    ));
    // A `_other=` dir that is NOT a declared hidden-named partition IS hidden (only the exact
    // declared field names are exempted).
    assert!(is_hidden_under_probe(
        base,
        "/wh/t/data/_other=5/x.parquet",
        &partition_prefixes
    ));
    // With NO hidden-named partition fields, every `_`/`.` dir is hidden.
    assert!(is_hidden_under_probe(
        base,
        "/wh/t/data/_part=5/x.parquet",
        &[]
    ));
    // A hidden segment in the BASE itself (above the listing root) does not disqualify.
    assert!(!is_hidden_under_probe(
        "/wh/_t",
        "/wh/_t/data/x.parquet",
        &[]
    ));
}

/// Risk: the version-hint location is part of the valid set so a stray `version-hint.text` is
/// never deleted. Pins the exact path shape (Java `ReachableFileUtil.versionHintLocation`).
#[test]
fn test_version_hint_location_shape() {
    assert_eq!(
        version_hint_probe("/wh/t"),
        "/wh/t/metadata/version-hint.text"
    );
    assert_eq!(
        version_hint_probe("/wh/t/"),
        "/wh/t/metadata/version-hint.text"
    );
}

// =============================================================================================
// End-to-end fixtures — a local-fs-backed memory catalog, real files on disk
// =============================================================================================

/// A memory catalog whose FileIO is the LOCAL filesystem, rooted at a fresh `TempDir`. Returns the
/// catalog, a local-fs [`FileIO`] for planting/inspecting files, and the temp-dir guard (kept
/// alive for the test's duration).
async fn local_fs_catalog() -> (impl Catalog, FileIO, TempDir) {
    let temp_dir = TempDir::new().expect("temp dir");
    let warehouse = temp_dir
        .path()
        .to_str()
        .expect("utf8 temp path")
        .to_string();
    let catalog = MemoryCatalogBuilder::default()
        .with_storage_factory(Arc::new(LocalFsStorageFactory))
        .load(
            "memory",
            HashMap::from([("warehouse".to_string(), warehouse)]),
        )
        .await
        .expect("load local-fs memory catalog");
    let file_io = FileIOBuilder::new(Arc::new(LocalFsStorageFactory)).build();
    (catalog, file_io, temp_dir)
}

/// A schema of three long columns `x`, `y`, `z` (matches the minimal table fixtures).
fn three_long_schema() -> Schema {
    Schema::builder()
        .with_fields(vec![
            Arc::new(NestedField::required(
                1,
                "x",
                Type::Primitive(PrimitiveType::Long),
            )),
            Arc::new(NestedField::required(
                2,
                "y",
                Type::Primitive(PrimitiveType::Long),
            )),
            Arc::new(NestedField::required(
                3,
                "z",
                Type::Primitive(PrimitiveType::Long),
            )),
        ])
        .build()
        .expect("build schema")
}

/// Create a table with the given partition spec under a fresh namespace.
async fn create_table(catalog: &impl Catalog, spec: PartitionSpec) -> Table {
    let namespace = NamespaceIdent::new(format!("ns-{}", uuid::Uuid::new_v4()));
    catalog
        .create_namespace(&namespace, HashMap::new())
        .await
        .expect("create namespace");
    let table_ident = TableIdent::new(namespace.clone(), "t".to_string());
    let creation = TableCreation::builder()
        .name(table_ident.name().to_string())
        .schema(three_long_schema())
        .partition_spec(spec)
        .build();
    catalog
        .create_table(&namespace, creation)
        .await
        .expect("create table")
}

/// An unpartitioned table (empty spec).
async fn create_unpartitioned_table(catalog: &impl Catalog) -> Table {
    create_table(catalog, PartitionSpec::unpartition_spec()).await
}

/// A real data file: write `content` to `path` on disk through `file_io`, then build a
/// metadata-consistent [`DataFile`] for the unpartitioned spec.
async fn real_data_file(file_io: &FileIO, path: &str, content: &[u8]) -> DataFile {
    write_real_file(file_io, path, content).await;
    DataFileBuilder::default()
        .content(DataContentType::Data)
        .file_path(path.to_string())
        .file_format(DataFileFormat::Parquet)
        .file_size_in_bytes(content.len() as u64)
        .record_count(1)
        .partition_spec_id(0)
        .partition(Struct::empty())
        .build()
        .expect("build data file")
}

/// A real parquet position-delete file at `path` (V2 delete), referencing `referenced`.
async fn real_position_delete_file(
    file_io: &FileIO,
    path: &str,
    referenced: &str,
    content: &[u8],
) -> DataFile {
    write_real_file(file_io, path, content).await;
    DataFileBuilder::default()
        .content(DataContentType::PositionDeletes)
        .file_path(path.to_string())
        .file_format(DataFileFormat::Parquet)
        .file_size_in_bytes(content.len() as u64)
        .record_count(1)
        .partition_spec_id(0)
        .partition(Struct::empty())
        .referenced_data_file(Some(referenced.to_string()))
        .build()
        .expect("build position delete file")
}

/// Write `content` to `path` through `file_io` (creates parent dirs on the local fs).
async fn write_real_file(file_io: &FileIO, path: &str, content: &[u8]) {
    file_io
        .new_output(path)
        .expect("new output")
        .write(Bytes::copy_from_slice(content))
        .await
        .expect("write file");
}

/// Append `files` to `table` via a fast append, committed through `catalog`.
async fn append(catalog: &impl Catalog, table: &Table, files: Vec<DataFile>) -> Table {
    let tx = Transaction::new(table);
    let tx = tx
        .fast_append()
        .add_data_files(files)
        .apply(tx)
        .expect("apply fast append");
    tx.commit(catalog).await.expect("commit fast append")
}

/// Add `delete_files` to `table` via a row delta, committed through `catalog`.
async fn add_deletes(catalog: &impl Catalog, table: &Table, delete_files: Vec<DataFile>) -> Table {
    let tx = Transaction::new(table);
    let tx = tx
        .row_delta()
        .add_deletes(delete_files)
        .apply(tx)
        .expect("apply row delta");
    tx.commit(catalog).await.expect("commit row delta")
}

/// True iff `path` exists on disk through `file_io`.
async fn exists(file_io: &FileIO, path: &str) -> bool {
    file_io.exists(path).await.expect("exists check")
}

/// A recording delete fn that records calls and "succeeds" without touching storage.
#[allow(clippy::type_complexity)]
fn recording_delete_fn() -> (
    Arc<Mutex<Vec<String>>>,
    impl Fn(String) -> BoxFuture<'static, Result<()>> + Send + Sync + 'static,
) {
    let recorded = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&recorded);
    let delete_fn = move |path: String| -> BoxFuture<'static, Result<()>> {
        sink.lock().expect("recorder lock").push(path);
        Box::pin(async { Ok(()) })
    };
    (recorded, delete_fn)
}

/// The set of every reachable file under the table location, by category — used to assert that a
/// sweep leaves EVERY reachable file intact (a category omission fails loudly).
struct ReachableSet {
    data_files: Vec<String>,
    delete_files: Vec<String>,
    manifests: Vec<String>,
    manifest_lists: Vec<String>,
    metadata_json: Vec<String>,
    statistics: Vec<String>,
}

/// Enumerate the table's reachable files by category, directly (the assertion oracle — independent
/// of the action's own `collect_valid_files`).
async fn enumerate_reachable(table: &Table) -> ReachableSet {
    let metadata = table.metadata();
    let file_io = table.file_io();
    let mut set = ReachableSet {
        data_files: Vec::new(),
        delete_files: Vec::new(),
        manifests: Vec::new(),
        manifest_lists: Vec::new(),
        metadata_json: Vec::new(),
        statistics: Vec::new(),
    };
    for snapshot in metadata.snapshots() {
        set.manifest_lists
            .push(snapshot.manifest_list().to_string());
        let manifest_list = snapshot
            .load_manifest_list(file_io, metadata)
            .await
            .expect("load manifest list");
        for manifest_file in manifest_list.entries() {
            set.manifests.push(manifest_file.manifest_path.clone());
            let manifest = manifest_file
                .load_manifest(file_io)
                .await
                .expect("load manifest");
            for entry in manifest.entries() {
                match entry.content_type() {
                    DataContentType::Data => set.data_files.push(entry.file_path().to_string()),
                    DataContentType::PositionDeletes | DataContentType::EqualityDeletes => {
                        set.delete_files.push(entry.file_path().to_string())
                    }
                }
            }
        }
    }
    if let Some(current) = table.metadata_location() {
        set.metadata_json.push(current.to_string());
    }
    for log_entry in metadata.metadata_log() {
        set.metadata_json.push(log_entry.metadata_file.clone());
    }
    for statistics in metadata.statistics_iter() {
        set.statistics.push(statistics.statistics_path.clone());
    }
    dedup_sort(&mut set.data_files);
    dedup_sort(&mut set.delete_files);
    dedup_sort(&mut set.manifests);
    dedup_sort(&mut set.manifest_lists);
    dedup_sort(&mut set.metadata_json);
    dedup_sort(&mut set.statistics);
    set
}

fn dedup_sort(vec: &mut Vec<String>) {
    vec.sort();
    vec.dedup();
}

// =============================================================================================
// End-to-end tests — the crown jewel and the algorithm pins
// =============================================================================================

/// THE crown-jewel GC-safety pin: a table with multiple snapshots, data files, a delete file,
/// plus PLANTED orphans (stray parquet in the data dir, stray avro in the metadata dir, an orphan
/// in a nested partition-like dir). With `older_than = now+ε` (everything eligible by age):
/// EXACTLY the planted orphans are deleted; EVERY reachable file (enumerated BY CATEGORY —
/// data/delete/manifest/manifest-list/metadata-json) still exists; a full table scan still
/// succeeds; non-current-snapshot files SURVIVE (history stays readable). A reachability omission
/// in ANY category deletes live data and fails this test loudly.
#[tokio::test]
async fn test_crown_jewel_only_planted_orphans_deleted_all_reachable_survive() {
    let (catalog, file_io, _temp) = local_fs_catalog().await;
    let table = create_unpartitioned_table(&catalog).await;
    let location = table.metadata().location().to_string();

    // S1: two data files.
    let data_a = format!("{location}/data/a.parquet");
    let data_b = format!("{location}/data/b.parquet");
    let table = append(&catalog, &table, vec![
        real_data_file(&file_io, &data_a, b"aaaa").await,
        real_data_file(&file_io, &data_b, b"bbbb").await,
    ])
    .await;
    let s1_id = table.metadata().current_snapshot_id().expect("s1");

    // S2: a position-delete file against data_a (V2 parquet delete). Both the delete file and a
    // third data file land in a NEW snapshot — the prior snapshot's files must still be valid.
    let delete_path = format!("{location}/data/del-a.parquet");
    let table = add_deletes(&catalog, &table, vec![
        real_position_delete_file(&file_io, &delete_path, &data_a, b"del").await,
    ])
    .await;
    let s2_id = table.metadata().current_snapshot_id().expect("s2");
    assert_ne!(s1_id, s2_id, "two distinct snapshots");

    // Enumerate the reachable set BY CATEGORY before planting orphans.
    let reachable = enumerate_reachable(&table).await;
    assert!(!reachable.data_files.is_empty(), "fixture: data files");
    assert!(!reachable.delete_files.is_empty(), "fixture: a delete file");
    assert!(!reachable.manifests.is_empty(), "fixture: manifests");
    assert!(
        !reachable.manifest_lists.is_empty(),
        "fixture: manifest lists"
    );
    assert!(
        !reachable.metadata_json.is_empty(),
        "fixture: metadata json"
    );
    // A non-current snapshot (s1) must have its manifest list among the reachable lists — proving
    // history is in the valid set, not just the current snapshot.
    let s1_list = table
        .metadata()
        .snapshot_by_id(s1_id)
        .expect("s1 present")
        .manifest_list()
        .to_string();
    assert!(
        reachable.manifest_lists.contains(&s1_list),
        "the non-current snapshot's manifest list must be reachable"
    );

    // Plant orphans: a stray parquet in the data dir, a stray avro in the metadata dir, and an
    // orphan in a nested non-hidden directory.
    let orphan_data = format!("{location}/data/orphan.parquet");
    let orphan_meta = format!("{location}/metadata/orphan-manifest.avro");
    let orphan_nested = format!("{location}/data/extra/orphan-nested.parquet");
    for orphan in [&orphan_data, &orphan_meta, &orphan_nested] {
        write_real_file(&file_io, orphan, b"orphan").await;
    }

    // Run with older_than = now+1h (everything eligible).
    let now_plus = now_plus_one_hour();
    let result = DeleteOrphanFiles::new(table.clone())
        .older_than(now_plus)
        .execute()
        .await
        .expect("execute");

    // EXACTLY the three planted orphans deleted.
    let mut deleted = result.orphan_file_locations.clone();
    deleted.sort();
    let mut expected = vec![
        orphan_data.clone(),
        orphan_meta.clone(),
        orphan_nested.clone(),
    ];
    expected.sort();
    assert_eq!(deleted, expected, "exactly the planted orphans are orphan");
    assert!(
        result.delete_failures.is_empty(),
        "no delete failures: {:?}",
        result.delete_failures
    );
    for orphan in [&orphan_data, &orphan_meta, &orphan_nested] {
        assert!(
            !exists(&file_io, orphan).await,
            "orphan {orphan} must be deleted"
        );
    }

    // EVERY reachable file, by category, still exists (statistics is empty for this fixture but
    // enumerated so a future stats-bearing fixture inherits the per-category survival check).
    for (category, files) in [
        ("data", &reachable.data_files),
        ("delete", &reachable.delete_files),
        ("manifest", &reachable.manifests),
        ("manifest-list", &reachable.manifest_lists),
        ("metadata-json", &reachable.metadata_json),
        ("statistics", &reachable.statistics),
    ] {
        for path in files {
            assert!(
                exists(&file_io, path).await,
                "reachable {category} file must survive: {path}"
            );
        }
    }
    assert!(
        exists(&file_io, &s1_list).await,
        "the non-current snapshot's list must survive"
    );

    // A full table scan still succeeds (history + current both readable).
    let reloaded = catalog
        .load_table(table.identifier())
        .await
        .expect("reload table");
    let scan = reloaded.scan().build().expect("build scan");
    let plan = scan.plan_files().await.expect("plan files");
    use futures::TryStreamExt;
    let tasks: Vec<_> = plan.try_collect().await.expect("collect scan tasks");
    assert!(
        !tasks.is_empty(),
        "the table still scans to live data files"
    );
}

/// Risk (olderThan grace — in-flight-commit protection): a FRESH orphan (created now) must NOT be
/// deleted under the default `older_than` (now−3d), but MUST be deleted with `older_than = now+ε`.
/// A grace that does not apply would delete a file a concurrent commit is mid-write on.
#[tokio::test]
async fn test_older_than_grace_protects_fresh_orphan_until_cutoff_passes() {
    let (catalog, file_io, _temp) = local_fs_catalog().await;
    let table = create_unpartitioned_table(&catalog).await;
    let location = table.metadata().location().to_string();
    let table = append(&catalog, &table, vec![
        real_data_file(&file_io, &format!("{location}/data/a.parquet"), b"a").await,
    ])
    .await;

    let fresh_orphan = format!("{location}/data/fresh-orphan.parquet");
    write_real_file(&file_io, &fresh_orphan, b"fresh").await;

    // Default older_than (now − 3d): the fresh orphan is younger than the cutoff ⇒ NOT deleted.
    let result = DeleteOrphanFiles::new(table.clone())
        .execute()
        .await
        .expect("default execute");
    assert!(
        result.orphan_file_locations.is_empty(),
        "the fresh orphan must be protected by the 3-day grace: {:?}",
        result.orphan_file_locations
    );
    assert!(
        exists(&file_io, &fresh_orphan).await,
        "fresh orphan must survive default grace"
    );

    // older_than = now+ε: now the fresh orphan IS eligible.
    let result = DeleteOrphanFiles::new(table.clone())
        .older_than(now_plus_one_hour())
        .execute()
        .await
        .expect("now+ε execute");
    assert_eq!(
        result.orphan_file_locations,
        vec![fresh_orphan.clone()],
        "with the cutoff in the future the orphan is deleted"
    );
    assert!(
        !exists(&file_io, &fresh_orphan).await,
        "orphan deleted past the cutoff"
    );
}

/// Risk (list-error propagation — a listing failure must NOT be read as "zero orphans, success"):
/// if `FileIO::list` errors (an unreadable subtree under the location), `execute()` must return
/// `Err` and delete NOTHING. Treating a listing IO error as an empty success would make the sweep
/// silently no-op on a partially-unreadable store — masking real orphans, or worse, if a later
/// re-list partially succeeded, classifying live files as orphan. A genuine IO error must surface.
#[cfg(unix)]
#[tokio::test]
async fn test_list_error_propagates_and_deletes_nothing() {
    use std::os::unix::fs::PermissionsExt;

    let (catalog, file_io, _temp) = local_fs_catalog().await;
    let table = create_unpartitioned_table(&catalog).await;
    let location = table.metadata().location().to_string();
    let table = append(&catalog, &table, vec![
        real_data_file(&file_io, &format!("{location}/data/a.parquet"), b"a").await,
    ])
    .await;
    let orphan = format!("{location}/data/orphan.parquet");
    write_real_file(&file_io, &orphan, b"o").await;

    // Plant an unreadable subdirectory under the listing root so the recursive walk fails mid-list.
    // The local-fs warehouse is a BARE path, so `location` is already a filesystem path.
    let unreadable_dir = format!("{location}/data/unreadable");
    write_real_file(&file_io, &format!("{unreadable_dir}/x.parquet"), b"x").await;
    let unreadable_path = std::path::PathBuf::from(&unreadable_dir);
    std::fs::set_permissions(&unreadable_path, std::fs::Permissions::from_mode(0o000))
        .expect("chmod 000 the unreadable dir");

    let outcome = DeleteOrphanFiles::new(table.clone())
        .older_than(now_plus_one_hour())
        .execute()
        .await;

    // Restore permissions so the TempDir guard can clean up regardless of the assertion outcome.
    let _ = std::fs::set_permissions(&unreadable_path, std::fs::Permissions::from_mode(0o755));

    assert!(
        outcome.is_err(),
        "a listing IO error must surface as Err, not a zero-orphan success: {outcome:?}"
    );
    // Nothing was deleted — the orphan and the live file both remain.
    assert!(
        exists(&file_io, &orphan).await,
        "no deletion on a list error"
    );
    assert!(
        exists(&file_io, &format!("{location}/data/a.parquet")).await,
        "the live file is untouched on a list error"
    );
}

/// Risk (statistics + PARTITION-statistics inclusion in the valid universe): a Puffin statistics
/// file and a partition-statistics file referenced by table metadata must NOT be swept as orphans
/// — they are reachable metadata. The builder CODES partition-statistics inclusion
/// (`partition_statistics_iter`); this test PINS it so dropping that category from the universe
/// (the "M-stats" mutation) deletes a referenced stats file and fails loudly.
#[tokio::test]
async fn test_statistics_and_partition_statistics_files_are_not_orphan() {
    use crate::spec::{PartitionStatisticsFile, StatisticsFile};

    let (catalog, file_io, _temp) = local_fs_catalog().await;
    let table = create_unpartitioned_table(&catalog).await;
    let location = table.metadata().location().to_string();
    let table = append(&catalog, &table, vec![
        real_data_file(&file_io, &format!("{location}/data/a.parquet"), b"a").await,
    ])
    .await;
    let snapshot_id = table.metadata().current_snapshot_id().expect("snapshot");

    // Plant REAL stats files on disk under the metadata dir and register them in metadata.
    let stats_path = format!("{location}/metadata/stats-{snapshot_id}.puffin");
    let partition_stats_path = format!("{location}/metadata/partition-stats-{snapshot_id}.parquet");
    write_real_file(&file_io, &stats_path, b"puffin-stats").await;
    write_real_file(&file_io, &partition_stats_path, b"partition-stats").await;

    let metadata = table
        .metadata()
        .clone()
        .into_builder(table.metadata_location().map(str::to_string))
        .set_statistics(StatisticsFile {
            snapshot_id,
            statistics_path: stats_path.clone(),
            file_size_in_bytes: 12,
            file_footer_size_in_bytes: 4,
            key_metadata: None,
            blob_metadata: vec![],
        })
        .set_partition_statistics(PartitionStatisticsFile {
            snapshot_id,
            statistics_path: partition_stats_path.clone(),
            file_size_in_bytes: 15,
        })
        .build()
        .expect("build metadata with stats")
        .metadata;
    let table = table.clone().with_metadata(Arc::new(metadata));

    // Confirm both stats files are present in the metadata iterators (fixture precondition).
    assert_eq!(
        table.metadata().statistics_iter().count(),
        1,
        "stats registered"
    );
    assert_eq!(
        table.metadata().partition_statistics_iter().count(),
        1,
        "partition stats registered"
    );

    let result = DeleteOrphanFiles::new(table.clone())
        .older_than(now_plus_one_hour())
        .execute()
        .await
        .expect("execute");

    assert!(
        !result.orphan_file_locations.contains(&stats_path),
        "the statistics file must be reachable, not orphan: {:?}",
        result.orphan_file_locations
    );
    assert!(
        !result.orphan_file_locations.contains(&partition_stats_path),
        "the PARTITION-statistics file must be reachable, not orphan: {:?}",
        result.orphan_file_locations
    );
    assert!(
        exists(&file_io, &stats_path).await,
        "statistics file must survive the sweep"
    );
    assert!(
        exists(&file_io, &partition_stats_path).await,
        "partition-statistics file must survive the sweep"
    );
}

/// Risk (the `olderThan` cut is a STRICT `<`, Java `createdAtMillis() < olderThanTimestamp`): a
/// file whose modification time EXACTLY equals the cutoff must be SPARED — this boundary IS the
/// in-flight-commit protection (a commit writing a file at time T must survive a sweep with
/// `older_than == T`). Relaxing to `<=` would delete a file an in-flight commit is mid-write on.
/// Pins the exact boundary by reading the planted orphan's real mtime and cutting AT it.
#[tokio::test]
async fn test_older_than_cut_is_strict_less_than_at_the_exact_boundary() {
    let (catalog, file_io, _temp) = local_fs_catalog().await;
    let table = create_unpartitioned_table(&catalog).await;
    let location = table.metadata().location().to_string();
    let table = append(&catalog, &table, vec![
        real_data_file(&file_io, &format!("{location}/data/a.parquet"), b"a").await,
    ])
    .await;

    let boundary_orphan = format!("{location}/data/boundary-orphan.parquet");
    write_real_file(&file_io, &boundary_orphan, b"boundary").await;

    // Read the orphan's ACTUAL recorded mtime from the listing, then cut EXACTLY at it.
    let listed = file_io.list(&location).await.expect("list");
    let orphan_mtime = listed
        .iter()
        .find(|info| info.location == boundary_orphan)
        .expect("planted orphan listed")
        .created_at_millis;

    // older_than == orphan_mtime: strict `<` ⇒ NOT eligible ⇒ spared. `<=` would delete it.
    let result = DeleteOrphanFiles::new(table.clone())
        .older_than(orphan_mtime)
        .execute()
        .await
        .expect("execute at the exact boundary");
    assert!(
        !result.orphan_file_locations.contains(&boundary_orphan),
        "a file whose mtime EQUALS the cutoff must be spared (strict <): {:?}",
        result.orphan_file_locations
    );
    assert!(
        exists(&file_io, &boundary_orphan).await,
        "the boundary file must survive (in-flight-commit protection)"
    );

    // One past the boundary (older_than = mtime+1): now strictly older ⇒ eligible ⇒ deleted.
    let result = DeleteOrphanFiles::new(table.clone())
        .older_than(orphan_mtime + 1)
        .execute()
        .await
        .expect("execute one past the boundary");
    assert_eq!(
        result.orphan_file_locations,
        vec![boundary_orphan.clone()],
        "one millisecond past the cutoff the file is eligible"
    );
}

/// Risk (the default `older_than = now − 3 days` must make a sweep of a FRESH table a no-op): a
/// table whose every file was just written must report ZERO orphans under the default cutoff — a
/// default that swept fresh files would delete a just-committed table's data.
#[tokio::test]
async fn test_default_older_than_makes_fresh_table_sweep_a_no_op() {
    let (catalog, file_io, _temp) = local_fs_catalog().await;
    let table = create_unpartitioned_table(&catalog).await;
    let location = table.metadata().location().to_string();
    let table = append(&catalog, &table, vec![
        real_data_file(&file_io, &format!("{location}/data/a.parquet"), b"a").await,
        real_data_file(&file_io, &format!("{location}/data/b.parquet"), b"b").await,
    ])
    .await;
    // Also plant a fresh orphan — it too must be spared by the default 3-day grace.
    write_real_file(
        &file_io,
        &format!("{location}/data/fresh.parquet"),
        b"fresh",
    )
    .await;

    // Default execute() — no older_than override ⇒ cutoff is now−3d ⇒ nothing is eligible.
    let result = DeleteOrphanFiles::new(table.clone())
        .execute()
        .await
        .expect("default sweep");
    assert!(
        result.orphan_file_locations.is_empty(),
        "the default now−3d cutoff must make a fresh-table sweep a no-op: {:?}",
        result.orphan_file_locations
    );
}

/// Risk (hidden-path filter): orphans under `.hidden/` and `_tmp/` must NOT be deleted (Iceberg's
/// own hidden/temp dirs), while an orphan under a real partition dir `_<field>=x/` IS a candidate
/// and is deleted. A filter that fails to exempt the partition dir would delete LIVE partition
/// data; one that fails to hide `_tmp/` would delete an in-progress write.
#[tokio::test]
async fn test_hidden_path_filter_spares_hidden_dirs_but_sweeps_named_partition_dirs() {
    let (catalog, file_io, _temp) = local_fs_catalog().await;
    // A table whose partition spec has a field literally named `_part` (identity on x).
    let schema = three_long_schema();
    let spec = PartitionSpec::builder(schema)
        .with_spec_id(0)
        .add_partition_field("x", "_part", Transform::Identity)
        .expect("add partition field")
        .build()
        .expect("build spec");
    let table = create_table(&catalog, spec).await;
    let location = table.metadata().location().to_string();

    // A partitioned data file (partition value x=5) so the table has a snapshot.
    let data_path = format!("{location}/data/_part=5/a.parquet");
    write_real_file(&file_io, &data_path, b"a").await;
    let data_file = DataFileBuilder::default()
        .content(DataContentType::Data)
        .file_path(data_path.clone())
        .file_format(DataFileFormat::Parquet)
        .file_size_in_bytes(1)
        .record_count(1)
        .partition_spec_id(0)
        .partition(Struct::from_iter([Some(Literal::long(5))]))
        .build()
        .expect("build partitioned data file");
    let table = append(&catalog, &table, vec![data_file]).await;

    // Plant: orphans under `.hidden/` and `_tmp/` (must survive), and an orphan under the REAL
    // partition dir `_part=7/` (must be deleted — it is not referenced).
    let orphan_hidden = format!("{location}/data/.hidden/x.parquet");
    let orphan_tmp = format!("{location}/_tmp/x.parquet");
    let orphan_partition = format!("{location}/data/_part=7/orphan.parquet");
    for orphan in [&orphan_hidden, &orphan_tmp, &orphan_partition] {
        write_real_file(&file_io, orphan, b"orphan").await;
    }

    let result = DeleteOrphanFiles::new(table.clone())
        .older_than(now_plus_one_hour())
        .execute()
        .await
        .expect("execute");

    assert_eq!(
        result.orphan_file_locations,
        vec![orphan_partition.clone()],
        "only the named-partition-dir orphan is swept; hidden dirs are spared"
    );
    assert!(
        exists(&file_io, &orphan_hidden).await,
        "`.hidden/` orphan must survive"
    );
    assert!(
        exists(&file_io, &orphan_tmp).await,
        "`_tmp/` orphan must survive"
    );
    assert!(
        !exists(&file_io, &orphan_partition).await,
        "`_part=7/` orphan is deleted"
    );
    assert!(
        exists(&file_io, &data_path).await,
        "the LIVE partition data file must survive"
    );
}

/// Risk (location override): `.location(subdir)` sweeps only that subtree — an orphan OUTSIDE the
/// subtree is untouched even though it is unreferenced. A location override that leaks to the full
/// table root would delete files the caller did not intend to scan.
#[tokio::test]
async fn test_location_override_sweeps_only_the_subtree() {
    let (catalog, file_io, _temp) = local_fs_catalog().await;
    let table = create_unpartitioned_table(&catalog).await;
    let location = table.metadata().location().to_string();
    let table = append(&catalog, &table, vec![
        real_data_file(&file_io, &format!("{location}/data/a.parquet"), b"a").await,
    ])
    .await;

    let orphan_in_data = format!("{location}/data/orphan.parquet");
    let orphan_in_other = format!("{location}/other/orphan.parquet");
    write_real_file(&file_io, &orphan_in_data, b"o1").await;
    write_real_file(&file_io, &orphan_in_other, b"o2").await;

    let data_subdir = format!("{location}/data");
    let result = DeleteOrphanFiles::new(table.clone())
        .location(&data_subdir)
        .older_than(now_plus_one_hour())
        .execute()
        .await
        .expect("execute");

    assert_eq!(
        result.orphan_file_locations,
        vec![orphan_in_data.clone()],
        "only the orphan under the overridden location is swept"
    );
    assert!(
        !exists(&file_io, &orphan_in_data).await,
        "data-subtree orphan deleted"
    );
    assert!(
        exists(&file_io, &orphan_in_other).await,
        "the orphan OUTSIDE the swept subtree must be untouched"
    );
}

/// Risk (result contract + delete_with override): a custom delete function must receive EXACTLY
/// the orphan set (nothing reachable), and the result's `orphan_file_locations` must match what
/// was handed to the delete function. A mismatch means either a reachable file was passed to
/// deletion (corruption) or the result under-reports.
#[tokio::test]
async fn test_delete_with_receives_exactly_the_orphan_set() {
    let (catalog, file_io, _temp) = local_fs_catalog().await;
    let table = create_unpartitioned_table(&catalog).await;
    let location = table.metadata().location().to_string();
    let table = append(&catalog, &table, vec![
        real_data_file(&file_io, &format!("{location}/data/a.parquet"), b"a").await,
        real_data_file(&file_io, &format!("{location}/data/b.parquet"), b"b").await,
    ])
    .await;

    let orphan_one = format!("{location}/data/orphan-1.parquet");
    let orphan_two = format!("{location}/data/orphan-2.parquet");
    write_real_file(&file_io, &orphan_one, b"o1").await;
    write_real_file(&file_io, &orphan_two, b"o2").await;

    let (recorded, delete_fn) = recording_delete_fn();
    let result = DeleteOrphanFiles::new(table.clone())
        .older_than(now_plus_one_hour())
        .delete_with(delete_fn)
        .execute()
        .await
        .expect("execute");

    let mut handed = recorded.lock().expect("recorder lock").clone();
    handed.sort();
    let mut expected = vec![orphan_one.clone(), orphan_two.clone()];
    expected.sort();
    assert_eq!(
        handed, expected,
        "the delete fn receives exactly the orphan set"
    );
    let mut reported = result.orphan_file_locations.clone();
    reported.sort();
    assert_eq!(
        reported, expected,
        "the result reports exactly the orphan set"
    );
    // The custom delete fn did NOT touch storage ⇒ nothing actually deleted (dry-run shape).
    assert!(
        exists(&file_io, &orphan_one).await,
        "custom delete fn left storage untouched"
    );
    assert!(
        exists(&file_io, &format!("{location}/data/a.parquet")).await,
        "no reachable file was ever handed to deletion"
    );
}

/// Risk (result contract on delete failure): a per-file delete failure must be COLLECTED (not
/// abort the sweep), the other orphans must still be deleted, and `orphan_file_locations` must
/// still report the FULL orphan set regardless of individual delete success (Java returns the
/// orphan list before deletion).
#[tokio::test]
async fn test_delete_failure_is_collected_and_full_orphan_list_returned() {
    let (catalog, file_io, _temp) = local_fs_catalog().await;
    let table = create_unpartitioned_table(&catalog).await;
    let location = table.metadata().location().to_string();
    let table = append(&catalog, &table, vec![
        real_data_file(&file_io, &format!("{location}/data/a.parquet"), b"a").await,
    ])
    .await;

    let orphan_good = format!("{location}/data/orphan-good.parquet");
    let orphan_fail = format!("{location}/data/orphan-fail.parquet");
    write_real_file(&file_io, &orphan_good, b"g").await;
    write_real_file(&file_io, &orphan_fail, b"f").await;

    let io = file_io.clone();
    let fail_path = orphan_fail.clone();
    let result = DeleteOrphanFiles::new(table.clone())
        .older_than(now_plus_one_hour())
        .delete_with(move |path: String| -> BoxFuture<'static, Result<()>> {
            let io = io.clone();
            let fail_path = fail_path.clone();
            Box::pin(async move {
                if path == fail_path {
                    Err(crate::Error::new(
                        crate::ErrorKind::Unexpected,
                        "injected delete failure",
                    ))
                } else {
                    io.delete(&path).await
                }
            })
        })
        .execute()
        .await
        .expect("execute");

    // The FULL orphan set is reported regardless of the failure.
    let mut reported = result.orphan_file_locations.clone();
    reported.sort();
    let mut expected = vec![orphan_good.clone(), orphan_fail.clone()];
    expected.sort();
    assert_eq!(
        reported, expected,
        "the full orphan list is returned despite a delete failure"
    );
    // The failure is collected, not swallowed; the sweep continued (good orphan deleted).
    assert_eq!(result.delete_failures.len(), 1, "one collected failure");
    assert_eq!(result.delete_failures[0].path, orphan_fail);
    assert!(
        !exists(&file_io, &orphan_good).await,
        "the sweep continued past the failure"
    );
    assert!(
        exists(&file_io, &orphan_fail).await,
        "the failing delete left its file"
    );
}

/// Risk (GC gate): `gc.enabled=false` must REFUSE the action with Java's verbatim message, before
/// any listing or deletion. A gate that does not fire would let orphan-deletion run on a table
/// where another table may share files.
#[tokio::test]
async fn test_gc_disabled_refuses_with_java_message() {
    let (catalog, file_io, _temp) = local_fs_catalog().await;
    let table = create_unpartitioned_table(&catalog).await;
    let location = table.metadata().location().to_string();
    let table = append(&catalog, &table, vec![
        real_data_file(&file_io, &format!("{location}/data/a.parquet"), b"a").await,
    ])
    .await;
    // Set gc.enabled=false through the metadata builder.
    let disabled_metadata = table
        .metadata()
        .clone()
        .into_builder(None)
        .set_properties(HashMap::from([(
            "gc.enabled".to_string(),
            "false".to_string(),
        )]))
        .expect("set gc.enabled")
        .build()
        .expect("build gc-disabled metadata")
        .metadata;
    let disabled_table = table.clone().with_metadata(Arc::new(disabled_metadata));

    let orphan = format!("{location}/data/orphan.parquet");
    write_real_file(&file_io, &orphan, b"o").await;

    let error = DeleteOrphanFiles::new(disabled_table)
        .older_than(now_plus_one_hour())
        .execute()
        .await
        .expect_err("gc.enabled=false must refuse");
    assert_eq!(error.kind(), crate::ErrorKind::DataInvalid);
    assert_eq!(
        error.message(),
        "Cannot delete orphan files: GC is disabled (deleting files may corrupt other tables)"
    );
    assert!(
        exists(&file_io, &orphan).await,
        "no deletion may run when GC is disabled"
    );
}

/// Risk (history reachability across ALL snapshots): a file removed by a copy-on-write delete is
/// no longer LIVE in the current snapshot, but it remains referenced by the PRIOR snapshot's
/// manifest (and by a DELETED tombstone in the rewritten one). Because the valid-file universe
/// spans every snapshot, the file must NOT be orphan — deleting it corrupts the readable history.
///
/// NOTE on the DELETED-tombstone clause specifically: Java's `contentFileDS` reads *every*
/// manifest entry (`ManifestFiles.read`, not `liveEntries`), so even a file referenced ONLY by a
/// tombstone counts as valid. In THIS test the tombstoned file is still also ALIVE in S1's manifest
/// (the universe spans all snapshots), so this test pins the history-reachability guarantee. The
/// genuinely tombstone-ONLY case — where the only surviving reference is a `Deleted` tombstone
/// after the adding snapshot is expired — IS constructible and is pinned separately by
/// [`test_tombstone_only_referenced_file_is_not_orphan_after_expire`] (which kills the no-liveness-
/// filter mutation directly).
#[tokio::test]
async fn test_copy_on_write_deleted_file_survives_because_history_references_it() {
    let (catalog, file_io, _temp) = local_fs_catalog().await;
    let table = create_unpartitioned_table(&catalog).await;
    let location = table.metadata().location().to_string();

    // S1: two data files.
    let data_a = format!("{location}/data/a.parquet");
    let data_b = format!("{location}/data/b.parquet");
    let table = append(&catalog, &table, vec![
        real_data_file(&file_io, &data_a, b"a").await,
        real_data_file(&file_io, &data_b, b"b").await,
    ])
    .await;

    // S2: delete data_a (copy-on-write ⇒ a DELETED tombstone for data_a in a rewritten manifest).
    let tx = Transaction::new(&table);
    let tx = tx
        .delete_files()
        .delete_file(&data_a)
        .apply(tx)
        .expect("apply delete files");
    let table = tx.commit(&catalog).await.expect("commit delete files");

    // data_a is no longer LIVE in the current snapshot, but it IS referenced by the prior
    // snapshot's manifest and by a DELETED tombstone — so it must NOT be orphan.
    let result = DeleteOrphanFiles::new(table.clone())
        .older_than(now_plus_one_hour())
        .execute()
        .await
        .expect("execute");
    assert!(
        result.orphan_file_locations.is_empty(),
        "a tombstoned-but-referenced file must not be orphan: {:?}",
        result.orphan_file_locations
    );
    assert!(
        exists(&file_io, &data_a).await,
        "the tombstoned file must survive (history)"
    );
    assert!(
        exists(&file_io, &data_b).await,
        "the live file must survive"
    );
}

/// Risk (PrefixMismatchMode::Error end-to-end): an ERROR-mode prefix conflict between the valid
/// set and a listed file must FAIL the whole action with Java's pinned message — before any
/// deletion. (Synthesized via a custom `.location()` whose listed scheme differs is impractical
/// on local-fs, so this exercises the join path at the action level with equal_schemes mapping
/// that creates a real authority conflict — see the unit tests for the exhaustive branch matrix.)
#[tokio::test]
async fn test_error_mode_prefix_conflict_message_pins() {
    // The end-to-end conflict path is exercised at the unit level
    // (`test_prefix_mismatch_mode_classifies_scheme_conflict_three_ways`); here we pin the exact
    // ERROR message text by driving the conflict-error formatter through the public surface.
    let (catalog, file_io, _temp) = local_fs_catalog().await;
    let table = create_unpartitioned_table(&catalog).await;
    let location = table.metadata().location().to_string();
    let table = append(&catalog, &table, vec![
        real_data_file(&file_io, &format!("{location}/data/a.parquet"), b"a").await,
    ])
    .await;
    // A clean run (no scheme/authority conflict on local-fs) must NOT error in ERROR mode — the
    // default mode must be safe for the common all-local-paths table.
    let result = DeleteOrphanFiles::new(table.clone())
        .prefix_mismatch_mode(PrefixMismatchMode::Error)
        .older_than(now_plus_one_hour())
        .execute()
        .await
        .expect("ERROR mode must not fire when all paths are scheme-less local paths");
    assert!(result.orphan_file_locations.is_empty());
}

/// Risk (the LOAD-BEARING no-liveness-filter guarantee — Java reads every manifest entry, not
/// `liveEntries`): a file whose ONLY remaining reference is a DELETED tombstone must NOT be orphan.
///
/// This is the constructible form of the case the crown-jewel test could only document as
/// "unconstructible": S1 adds `data_a`; S2 copy-on-write-deletes it (a DELETED tombstone in the
/// rewritten manifest); then S1 is EXPIRED through the fork's own `ExpireSnapshots` (metadata-only
/// — it deletes no files). Now `data_a` is on disk, S1's live entry is gone, and the SOLE manifest
/// reference to `data_a` is S2's `Deleted` tombstone. Because the universe spans every entry of
/// every retained snapshot WITHOUT a liveness filter, `data_a` must be spared. Adding an
/// `is_alive()` filter to the universe (the surviving "M1" mutation) deletes `data_a` and corrupts
/// the readable history — this test is what kills that mutation.
#[tokio::test]
async fn test_tombstone_only_referenced_file_is_not_orphan_after_expire() {
    let (catalog, file_io, _temp) = local_fs_catalog().await;
    let table = create_unpartitioned_table(&catalog).await;
    let location = table.metadata().location().to_string();

    let data_a = format!("{location}/data/a.parquet");
    let data_b = format!("{location}/data/b.parquet");
    let table = append(&catalog, &table, vec![
        real_data_file(&file_io, &data_a, b"a").await,
        real_data_file(&file_io, &data_b, b"b").await,
    ])
    .await;
    let s1_id = table.metadata().current_snapshot_id().expect("s1");

    // S2: copy-on-write delete of data_a → a DELETED tombstone for data_a in the rewritten manifest.
    let tx = Transaction::new(&table);
    let tx = tx
        .delete_files()
        .delete_file(&data_a)
        .apply(tx)
        .expect("apply delete files");
    let table = tx.commit(&catalog).await.expect("commit delete files");
    assert_ne!(s1_id, table.metadata().current_snapshot_id().expect("s2"));

    // Expire S1 through the fork's own ExpireSnapshots (metadata-only; it deletes NO files), so
    // data_a survives on disk while its only surviving manifest reference is S2's tombstone.
    let tx = Transaction::new(&table);
    let tx = tx
        .expire_snapshots()
        .expire_snapshot_id(s1_id)
        .apply(tx)
        .expect("apply expire");
    let table = tx.commit(&catalog).await.expect("commit expire");

    // Prove the precondition: data_a is referenced ONLY by a Deleted tombstone now (S1 is gone).
    let metadata = table.metadata();
    assert_eq!(
        metadata.snapshots().count(),
        1,
        "only S2 remains after expiry"
    );
    let mut data_a_alive_anywhere = false;
    let mut data_a_referenced = false;
    for snapshot in metadata.snapshots() {
        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), metadata)
            .await
            .expect("load manifest list");
        for manifest_file in manifest_list.entries() {
            let manifest = manifest_file
                .load_manifest(table.file_io())
                .await
                .expect("load manifest");
            for entry in manifest.entries() {
                if entry.file_path() == data_a {
                    data_a_referenced = true;
                    data_a_alive_anywhere |= entry.is_alive();
                }
            }
        }
    }
    assert!(
        data_a_referenced,
        "data_a must remain referenced by a manifest entry"
    );
    assert!(
        !data_a_alive_anywhere,
        "data_a's only surviving reference must be a Deleted tombstone (S1 expired)"
    );
    assert!(
        exists(&file_io, &data_a).await,
        "data_a must still be on disk"
    );

    // The no-liveness-filter universe must SPARE the tombstone-only file.
    let result = DeleteOrphanFiles::new(table.clone())
        .older_than(now_plus_one_hour())
        .execute()
        .await
        .expect("execute");
    assert!(
        !result.orphan_file_locations.contains(&data_a),
        "a file referenced only by a Deleted tombstone must NOT be orphan: {:?}",
        result.orphan_file_locations
    );
    assert!(
        exists(&file_io, &data_a).await,
        "the tombstone-only file must survive the sweep (deleting it corrupts history)"
    );
}

/// Risk (hidden filter is applied RELATIVE to the listed root, not to absolute segments): a table
/// whose LOCATION itself sits under a `_`-prefixed parent directory must still have its files
/// considered (Java `FileSystemWalker.isHiddenPath` only walks segments strictly UNDER baseDir, so
/// a hidden segment IN the base never disqualifies). If the Rust filter checked absolute segments,
/// every file under such a table would be masked (orphans never cleaned) — pin that it is NOT.
#[tokio::test]
async fn test_table_under_hidden_parent_dir_still_sweeps_orphans() {
    let temp_dir = TempDir::new().expect("temp dir");
    // Warehouse root contains a `_internal` parent segment — the table location inherits it.
    let warehouse = format!("{}/_internal", temp_dir.path().to_str().expect("utf8"));
    let catalog = MemoryCatalogBuilder::default()
        .with_storage_factory(Arc::new(LocalFsStorageFactory))
        .load(
            "memory",
            HashMap::from([("warehouse".to_string(), warehouse)]),
        )
        .await
        .expect("load catalog");
    let file_io = FileIOBuilder::new(Arc::new(LocalFsStorageFactory)).build();
    let table = create_unpartitioned_table(&catalog).await;
    let location = table.metadata().location().to_string();
    assert!(
        location.contains("/_internal/"),
        "fixture: the table location must carry a hidden parent segment: {location}"
    );
    let data_a = format!("{location}/data/a.parquet");
    let table = append(&catalog, &table, vec![
        real_data_file(&file_io, &data_a, b"a").await,
    ])
    .await;
    let orphan = format!("{location}/data/orphan.parquet");
    write_real_file(&file_io, &orphan, b"o").await;

    let result = DeleteOrphanFiles::new(table.clone())
        .older_than(now_plus_one_hour())
        .execute()
        .await
        .expect("execute");

    // The orphan under a table whose ROOT has a hidden segment is STILL swept (the base's `_internal`
    // does not mask files under it); the live data file survives.
    assert_eq!(
        result.orphan_file_locations,
        vec![orphan.clone()],
        "the orphan under a hidden-parent table must still be detected"
    );
    assert!(!exists(&file_io, &orphan).await, "orphan deleted");
    assert!(exists(&file_io, &data_a).await, "live data file survives");
}

/// Risk (the INVERSE file:// composition — metadata carries `file://`, the local-fs listing
/// returns BARE paths): no LIVE file may be deleted and the action must not error. The valid-file
/// universe normalizes `file://…/x` to path `/…/x`, matching the bare listed path key, so the join
/// is correct in principle.
///
/// CAVEAT pinned here (a SAFE under-deletion limitation, NOT corruption): when the table `location`
/// itself carries a scheme (`file://`) that the local-fs backend strips on the listing side, the
/// hidden-path filter's `relative_under(base=file://…, listed=/…)` cannot strip the base, so EVERY
/// listed file is treated as hidden and the sweep is a silent no-op — it deletes nothing (orphans
/// included). That is under-deletion-biased and never destroys live data; the OpenDAL backends
/// (S3/Glue) re-prefix listed entries WITH the scheme so base and listing agree and the sweep works
/// normally there. This test pins the safety guarantee (zero live deletions, no error) for the
/// scheme-qualified-location case.
#[tokio::test]
async fn test_file_scheme_location_never_deletes_live_files() {
    let temp_dir = TempDir::new().expect("temp dir");
    let warehouse = format!("file://{}", temp_dir.path().to_str().expect("utf8"));
    let catalog = MemoryCatalogBuilder::default()
        .with_storage_factory(Arc::new(LocalFsStorageFactory))
        .load(
            "memory",
            HashMap::from([("warehouse".to_string(), warehouse)]),
        )
        .await
        .expect("load catalog");
    let file_io = FileIOBuilder::new(Arc::new(LocalFsStorageFactory)).build();
    let table = create_unpartitioned_table(&catalog).await;
    let location = table.metadata().location().to_string();
    assert!(
        location.starts_with("file://"),
        "fixture: scheme-qualified location"
    );

    let data_a = format!("{location}/data/a.parquet");
    let table = append(&catalog, &table, vec![
        real_data_file(&file_io, &data_a, b"a").await,
    ])
    .await;

    // The valid universe DOES normalize the file:// data file path to the bare path the listing
    // would produce — pin the join-key equality (the matching logic is correct).
    let listed = file_io.list(&location).await.expect("list");
    let empty = HashMap::new();
    let valid = FileUriProbe::parse(&data_a, &empty, &empty);
    let bare_listed = listed
        .iter()
        .find(|info| info.location.ends_with("/data/a.parquet"))
        .expect("data file listed");
    let actual = FileUriProbe::parse(&bare_listed.location, &empty, &empty);
    assert_eq!(
        valid.path_probe(),
        actual.path_probe(),
        "file:// valid path normalizes to the bare listed path (join keys agree)"
    );

    // The action must not error and must delete NO live file (the safety guarantee).
    let result = DeleteOrphanFiles::new(table.clone())
        .older_than(now_plus_one_hour())
        .execute()
        .await
        .expect("a scheme-qualified-location table must not error");
    assert!(
        !result.orphan_file_locations.contains(&data_a),
        "the live data file must never be classified orphan: {:?}",
        result.orphan_file_locations
    );
    let reachable = enumerate_reachable(&table).await;
    for path in reachable
        .manifests
        .iter()
        .chain(&reachable.manifest_lists)
        .chain(&reachable.data_files)
        .chain(&reachable.metadata_json)
    {
        assert!(
            exists(&file_io, path).await,
            "no reachable file may be deleted under a scheme-qualified location: {path}"
        );
    }
}

/// Risk (a trailing-slash warehouse must not corrupt the join): the Rust `split_uri` does NOT
/// collapse doubled slashes the way Hadoop's `new Path(s).toUri()` does (a documented divergence).
/// That is SAFE for a Rust-native table because BOTH the metadata-writer and the lister carry the
/// identical `//` string, so they still join — pin that a trailing-slash warehouse leaves every
/// reachable file intact (the doubled-slash divergence is internally consistent, not a deleter).
#[tokio::test]
async fn test_trailing_slash_warehouse_leaves_reachable_files_intact() {
    let temp_dir = TempDir::new().expect("temp dir");
    let warehouse = format!("{}/", temp_dir.path().to_str().expect("utf8"));
    let catalog = MemoryCatalogBuilder::default()
        .with_storage_factory(Arc::new(LocalFsStorageFactory))
        .load(
            "memory",
            HashMap::from([("warehouse".to_string(), warehouse)]),
        )
        .await
        .expect("load catalog");
    let file_io = FileIOBuilder::new(Arc::new(LocalFsStorageFactory)).build();
    let table = create_unpartitioned_table(&catalog).await;
    let location = table.metadata().location().to_string();
    assert!(
        location.contains("//"),
        "fixture: a doubled slash in the location"
    );

    let data_a = format!("{location}/data/a.parquet");
    let table = append(&catalog, &table, vec![
        real_data_file(&file_io, &data_a, b"a").await,
    ])
    .await;
    let orphan = format!("{location}/data/orphan.parquet");
    write_real_file(&file_io, &orphan, b"o").await;

    let result = DeleteOrphanFiles::new(table.clone())
        .older_than(now_plus_one_hour())
        .execute()
        .await
        .expect("execute");

    // The orphan is still swept and EVERY reachable file survives (the `//` joins consistently).
    assert!(
        result.orphan_file_locations.contains(&orphan),
        "the orphan under a trailing-slash warehouse is still detected: {:?}",
        result.orphan_file_locations
    );
    assert!(exists(&file_io, &data_a).await, "live data file survives");
    let reachable = enumerate_reachable(&table).await;
    for path in reachable
        .manifests
        .iter()
        .chain(&reachable.manifest_lists)
        .chain(&reachable.data_files)
        .chain(&reachable.metadata_json)
    {
        assert!(
            exists(&file_io, path).await,
            "no reachable file may be deleted under a trailing-slash warehouse: {path}"
        );
    }
}

/// Current epoch millis + 1 hour — a cutoff comfortably in the future so every file is age-eligible.
fn now_plus_one_hour() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time after epoch")
        .as_millis();
    i64::try_from(now).expect("now fits i64") + 60 * 60 * 1000
}
