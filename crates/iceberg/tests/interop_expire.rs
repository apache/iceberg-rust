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

//! Java interop for `ExpireSnapshots` (increment A3) — proving the Rust `ExpireSnapshotsAction`
//! (B1 retention) + `ExpireSnapshotsCleanup` (B2 `ReachableFileCleanup` file GC) agree with Java
//! 1.10.0 `RemoveSnapshots` + `cleanExpiredFiles(true)` on the SAME fixtures, judged by Java where
//! possible. Driven by `dev/java-interop/run-interop-expire.sh`.
//!
//! # The two directions (over five fixtures: linear / tag_protected / stats / deletes / rewrite)
//!
//! **Direction 1 — Rust acts, Java judges.** [`test_expire_gen_rust_expires_the_fixtures`] performs
//! the SAME commit chain as the Java `ExpireOracle` on a real-FS `MemoryCatalog` (so the manifests +
//! manifest lists are REAL on disk), adds the surviving tag (and statistics, for the `stats`
//! fixture) through the production catalog path, runs the production [`ExpireSnapshotsAction`] to
//! compute the expiry and the production [`ExpireSnapshotsCleanup`] (via `commit_and_clean`) with a
//! COLLECTING deleter (collect-only — NO physical delete, so Java can still read the table), lands
//! the post-expire `final.metadata.json` at `<fixture>/rust_table/metadata/` and writes
//! `rust_deleted.json` (the path-independent `<funnel>@ord<N>` deleted descriptor). The run script
//! then has Java emit its canonical snapshot-metadata view of the Rust-expired table and byte-diff
//! it against Java's own, AND run `verify-interop-expire` (Java asserts the surviving-snapshot count
//! + ref names match and the deleted descriptors are equal).
//!
//! **Direction 2 — Java acts, Rust verifies.** [`test_rust_view_of_java_expire_matches_java_view`]
//! asserts Rust's canonical view of the JAVA-expired table equals Java's own view (the surviving
//! snapshot-id / ref / statistics structure, read through Rust's production metadata reader). The
//! deleted-set agreement in this direction is covered by the descriptor comparison above (the
//! cleanup candidate set the Rust side computes equals Java's) — emitted as `rust_deleted.json` and
//! diffed against `java_deleted.json` by both the Java verify AND
//! [`test_rust_deleted_descriptor_matches_java`] here.
//!
//! # Deterministic-by-OUTCOME cut (not by raw timestamp)
//!
//! The Java oracle re-stamps its snapshots to fixed timestamps so its cut is wall-clock-independent.
//! The Rust side cannot re-stamp through the public catalog path, but it does not need to: the
//! canonical snapshot-metadata view excludes timestamps (it keys on ORDINALS), and the deleted
//! descriptor keys on ordinals too — so the COMPARISON is timestamp-agnostic, and only the expiry
//! OUTCOME (which snapshots expire) must match Java's. This test commits the fixture through a real
//! catalog, reads each snapshot's ACTUAL timestamp, and uses `expire_older_than(head_timestamp)` +
//! `retain_last(1)` — which expires every snapshot strictly older than the head (Java's
//! `timestampMillis() >= cut` keeps `ts == head_ts`; the head is the unique newest) while the
//! surviving TAG protects its target (the ref-protection element). The B1 retention is the REAL
//! [`ExpireSnapshotsAction`]; the B2 file GC is the REAL [`ExpireSnapshotsCleanup`] driven through
//! its [`ExpireSnapshotsCleanup::commit_and_clean`] catalog seam with a COLLECTING (collect-only)
//! deleter, so Java can still read the table.
//!
//! # The env gate
//!
//! All three tests are clean NO-OPS (runtime early-return, not `#[ignore]`) unless their env var is
//! set non-empty, so the offline `cargo test` gate needs no Java/Maven. `ICEBERG_INTEROP_EXPIRE_GEN_DIR`
//! drives the Rust GEN (Direction 1); `ICEBERG_INTEROP_EXPIRE_DIR` drives the comparison tests
//! (Direction 2 + the descriptor check), which REQUIRE the Java fixtures + `java_deleted.json` and
//! fail loudly otherwise.

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use futures::future::BoxFuture;
use iceberg::io::{FileIO, LocalFsStorageFactory};
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::spec::{
    DataContentType, DataFile, DataFileBuilder, DataFileFormat, FormatVersion, Literal,
    ManifestContentType, ManifestStatus, NestedField, PrimitiveType, Schema, Snapshot, SortOrder,
    StatisticsFile, Struct, TableMetadata, Transform, Type, UnboundPartitionSpec,
};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, ExpireSnapshotsCleanup, Transaction};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation};
use serde_json::{Map as JsonMap, Value as JsonValue, json};

/// The COUNT-only summary allowlist — MUST stay IDENTICAL to the Java
/// `SnapshotMetaOracle.SUMMARY_COUNT_KEYS` and `common::snapshot_meta_view::SUMMARY_COUNT_KEYS`
/// (inlined here rather than imported so this binary does not pull in the unused
/// `common::snapshot_meta_view` view builder — which would warn as dead code, and which this
/// increment replaces with the expired-parent-aware `expire_meta_view` below).
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
    "replace-partitions",
];

/// The five fixtures, identical names + chains to the Java `ExpireOracle.FIXTURES`.
const FIXTURES: &[&str] = &["linear", "tag_protected", "stats", "deletes", "rewrite"];

fn gen_dir() -> Option<PathBuf> {
    std::env::var_os("ICEBERG_INTEROP_EXPIRE_GEN_DIR")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
}

fn compare_dir() -> Option<PathBuf> {
    std::env::var_os("ICEBERG_INTEROP_EXPIRE_DIR")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
}

// ===========================================================================================
// The fixture schema / spec / file builders (identical logical constants to the Java oracle).
// ===========================================================================================

/// `{1 id long required, 2 category string required}` — the Java fixture's schema.
fn fixture_schema() -> Schema {
    Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::required(2, "category", Type::Primitive(PrimitiveType::String)).into(),
        ])
        .build()
        .expect("build the {id, category} schema")
}

/// identity(category), spec id 0 — the Java fixture's spec.
fn fixture_spec() -> UnboundPartitionSpec {
    UnboundPartitionSpec::builder()
        .with_spec_id(0)
        .add_partition_field(2, "category".to_string(), Transform::Identity)
        .expect("add identity(category) partition field")
        .build()
}

/// A metadata-only `DataFile` (the path need not exist — the cleanup walk deletes by path through
/// the collecting deleter, and the canonical view only reads manifests).
fn data_file(table_location: &str, name: &str, category: &str, record_count: u64) -> DataFile {
    DataFileBuilder::default()
        .content(DataContentType::Data)
        .file_path(format!("{table_location}/data/{name}.parquet"))
        .file_format(DataFileFormat::Parquet)
        .file_size_in_bytes(record_count * 100)
        .record_count(record_count)
        .partition_spec_id(0)
        .partition(Struct::from_iter([Some(Literal::string(category))]))
        .build()
        .expect("build metadata-only data file")
}

/// A metadata-only POSITION-delete `DataFile` (the `deletes` fixture's delete manifest content).
fn position_delete_file(table_location: &str, name: &str, category: &str) -> DataFile {
    DataFileBuilder::default()
        .content(DataContentType::PositionDeletes)
        .file_path(format!("{table_location}/data/{name}.parquet"))
        .file_format(DataFileFormat::Parquet)
        .file_size_in_bytes(100)
        .record_count(1)
        .partition_spec_id(0)
        .partition(Struct::from_iter([Some(Literal::string(category))]))
        .build()
        .expect("build metadata-only position-delete file")
}

// ===========================================================================================
// Direction 1 — the Rust GEN: perform each fixture's chain, re-stamp, expire+clean (collect-only).
// ===========================================================================================

/// Build one fixture's table through real appends (real manifests + lists on disk), returning the
/// committed table. The chain MIRRORS the Java `ExpireOracle.buildAndExpire` switch exactly.
async fn build_fixture(fixture: &str, catalog: &impl Catalog, table_location: &str) -> Table {
    let namespace = NamespaceIdent::new("interop".to_string());
    catalog
        .create_namespace(&namespace, HashMap::new())
        .await
        .expect("create namespace");
    let creation = TableCreation::builder()
        .name("rust_table".to_string())
        .location(table_location.to_string())
        .schema(fixture_schema())
        .partition_spec(fixture_spec())
        .sort_order(SortOrder::unsorted_order())
        .format_version(FormatVersion::V2)
        .build();
    let mut table = catalog
        .create_table(&namespace, creation)
        .await
        .expect("create rust_table");

    match fixture {
        "linear" | "tag_protected" => {
            for index in 1..=4 {
                table = append(catalog, &table, vec![data_file(
                    table_location,
                    &format!("f{index}"),
                    "a",
                    10,
                )])
                .await;
            }
        }
        "stats" => {
            for index in 1..=3 {
                table = append(catalog, &table, vec![data_file(
                    table_location,
                    &format!("f{index}"),
                    "a",
                    10,
                )])
                .await;
            }
        }
        "deletes" => {
            table = append(catalog, &table, vec![
                data_file(table_location, "a1", "a", 10),
                data_file(table_location, "b1", "b", 20),
            ])
            .await;
            // s2 row_delta adding a position-delete on the cat=a file (a DELETE manifest).
            let tx = Transaction::new(&table);
            let tx = tx
                .row_delta()
                .add_deletes(vec![position_delete_file(
                    table_location,
                    "a1-deletes",
                    "a",
                )])
                .apply(tx)
                .expect("apply row_delta");
            table = tx.commit(catalog).await.expect("commit row_delta");
            table = append(catalog, &table, vec![data_file(
                table_location,
                "c1",
                "a",
                30,
            )])
            .await;
        }
        "rewrite" => {
            // s1 append {d1, d2} in ONE manifest, s2 append d3, s3 delete d1 (rewrites s1's manifest
            // into 1-existing(d2) + 1-deleted(d1) — NOT all-tombstone, carried forward by both
            // engines), s4 append d4. Pins the content + manifest deletion funnels: s1's ORIGINAL
            // 2-file manifest + d1's data file die; the rewritten manifest (d2 existing) survives.
            // (See the Java oracle's note: an EMPTYING delete would trip a separate FastAppend
            // carry-forward divergence — avoided here by leaving d2 existing.)
            table = append(catalog, &table, vec![
                data_file(table_location, "d1", "a", 10),
                data_file(table_location, "d2", "a", 20),
            ])
            .await;
            table = append(catalog, &table, vec![data_file(
                table_location,
                "d3",
                "a",
                30,
            )])
            .await;
            let tx = Transaction::new(&table);
            let tx = tx
                .delete_files()
                .delete_file(format!("{table_location}/data/d1.parquet"))
                .apply(tx)
                .expect("apply delete_files");
            table = tx.commit(catalog).await.expect("commit delete_files");
            table = append(catalog, &table, vec![data_file(
                table_location,
                "d4",
                "a",
                40,
            )])
            .await;
        }
        other => panic!("unknown fixture: {other}"),
    }
    table
}

async fn append(catalog: &impl Catalog, table: &Table, files: Vec<DataFile>) -> Table {
    let tx = Transaction::new(table);
    let tx = tx
        .fast_append()
        .add_data_files(files)
        .apply(tx)
        .expect("apply fast append");
    tx.commit(catalog).await.expect("commit fast append")
}

/// The snapshot id at the given ORDINAL (sequence-number order), 0-based.
fn snapshot_id_at_ordinal(metadata: &TableMetadata, ordinal: usize) -> i64 {
    let mut ordered: Vec<Arc<Snapshot>> = metadata.snapshots().cloned().collect();
    ordered.sort_by_key(|snapshot| snapshot.sequence_number());
    ordered[ordinal].snapshot_id()
}

/// The maximum snapshot timestamp = the head's timestamp (the cut: `expire_older_than(head_ts)`
/// keeps `ts >= head_ts`, i.e. only the unique-newest head, and expires every strictly-older
/// snapshot — the same OUTCOME as Java's fixed-timestamp cut, comparison-equivalent because the
/// canonical view + the deleted descriptor both key on ordinals, never raw timestamps).
fn head_timestamp_ms(metadata: &TableMetadata) -> i64 {
    metadata
        .snapshots()
        .map(|snapshot| snapshot.timestamp_ms())
        .max()
        .expect("at least one snapshot")
}

/// Add the fixture's surviving TAG via the production `manage_snapshots().create_tag` — the
/// ref-protection element AND (Java-side) the ReachableFileCleanup forcing condition. tag_protected
/// pins the SECOND snapshot (ordinal 1, expirable by the cut but kept by the tag); every other
/// fixture pins the head.
async fn add_keep_tag(catalog: &impl Catalog, table: &Table, fixture: &str) -> Table {
    let tag_target = if fixture == "tag_protected" {
        snapshot_id_at_ordinal(table.metadata(), 1)
    } else {
        table
            .metadata()
            .current_snapshot_id()
            .expect("a current head")
    };
    let tx = Transaction::new(table);
    let tx = tx
        .manage_snapshots()
        .create_tag("keep", tag_target)
        .apply(tx)
        .expect("apply create_tag");
    tx.commit(catalog).await.expect("commit create_tag")
}

/// The `stats` fixture: attach a real statistics file to the SECOND snapshot (expired by the cut)
/// and one to the head (survives) — the before-minus-after location diff, both directions. The
/// puffin bytes are never read by cleanup; only the LOCATIONS are deleted.
async fn add_statistics(catalog: &impl Catalog, table: &Table) -> Table {
    let head_id = table.metadata().current_snapshot_id().expect("head");
    let second_id = snapshot_id_at_ordinal(table.metadata(), 1);
    let mut table = table.clone();
    for (snapshot_id, name) in [(second_id, "stats-expired"), (head_id, "stats-head")] {
        let path = format!("{}/metadata/{name}.puffin", table.metadata().location());
        table
            .file_io()
            .new_output(&path)
            .expect("stats output")
            .write(Bytes::from_static(b"interop-expire stats fixture"))
            .await
            .expect("write stats fixture");
        let tx = Transaction::new(&table);
        let tx = tx
            .update_statistics()
            .set_statistics(StatisticsFile {
                snapshot_id,
                statistics_path: path,
                file_size_in_bytes: 18,
                file_footer_size_in_bytes: 4,
                key_metadata: None,
                blob_metadata: vec![],
            })
            .apply(tx)
            .expect("apply set_statistics");
        table = tx.commit(catalog).await.expect("commit set_statistics");
    }
    table
}

#[tokio::test]
async fn test_expire_gen_rust_expires_the_fixtures() {
    let Some(gen_dir) = gen_dir() else {
        println!(
            "skipping interop_expire GEN — set ICEBERG_INTEROP_EXPIRE_GEN_DIR \
             (run dev/java-interop/run-interop-expire.sh)"
        );
        return;
    };

    for fixture in FIXTURES {
        let fixture_dir = gen_dir.join(fixture);
        let warehouse = fixture_dir.to_string_lossy().to_string();
        let table_location = format!("{warehouse}/rust_table");
        let catalog = MemoryCatalogBuilder::default()
            .with_storage_factory(Arc::new(LocalFsStorageFactory))
            .load(
                &format!("interop_expire_{fixture}"),
                HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), warehouse.clone())]),
            )
            .await
            .expect("build MemoryCatalog over local FS");

        // 1. Build the fixture (real manifests + lists on disk), then add the statistics (stats
        //    fixture only) and the surviving tag — all through the production catalog path.
        let mut table = build_fixture(fixture, &catalog, &table_location).await;
        if *fixture == "stats" {
            table = add_statistics(&catalog, &table).await;
        }
        table = add_keep_tag(&catalog, &table, fixture).await;

        // The pre-expire metadata is captured BEFORE the expire transaction commits (it owns the
        // real on-disk manifests/lists that the descriptor normalization reads). The cut keeps only
        // the head; the tag protects its target.
        let before = (*table.metadata_ref()).clone();
        let cut = head_timestamp_ms(&before);

        // 2. REAL ExpireSnapshotsAction (B1) + REAL ExpireSnapshotsCleanup (B2) via the
        //    commit_and_clean catalog seam with a COLLECTING (collect-only) deleter — Java must
        //    still read the table, so nothing is physically removed.
        let recorded: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::clone(&recorded);
        let cleanup = ExpireSnapshotsCleanup::new(table.file_io().clone()).delete_with(
            move |path: String| -> BoxFuture<'static, iceberg::Result<()>> {
                sink.lock().expect("recorder lock").push(path);
                Box::pin(async { Ok(()) })
            },
        );
        let expire_tx = {
            let tx = Transaction::new(&table);
            tx.expire_snapshots()
                .expire_older_than(cut)
                .retain_last(1)
                .apply(tx)
                .expect("apply expire")
        };
        let (expired_table, report) = cleanup
            .commit_and_clean(expire_tx, &catalog)
            .await
            .expect("commit and clean");
        let after = (*expired_table.metadata_ref()).clone();
        assert!(
            report.failures.is_empty(),
            "{fixture}: cleanup must not fail: {:?}",
            report.failures
        );

        // 3. Land the post-expire metadata at the known path for the Java emitter + comparison,
        //    plus the path-independent deleted descriptor.
        let final_metadata_path = format!("{table_location}/metadata/final.metadata.json");
        after
            .clone()
            .write_to(expired_table.file_io(), &final_metadata_path)
            .await
            .expect("write final.metadata.json");

        let descriptor = normalize_deleted(&before, &report).await;
        let rust_deleted_path = fixture_dir.join("rust_deleted.json");
        write_descriptor(&rust_deleted_path, &descriptor);

        println!("interop_expire GEN {fixture}: deleted descriptor {descriptor:?}");
    }
}

// ===========================================================================================
// The path-independent deleted descriptor — the SAME `<funnel>@ord<N>` multiset Java emits.
// ===========================================================================================

/// Normalize the cleanup report's deleted PATHS to a path-independent `<funnel>@ord<N>` multiset,
/// keyed by the owning snapshot's ordinal (sequence-number order in `before`). MUST agree
/// token-for-token with the Java `ExpireOracle.normalizeDeleted`:
/// - manifest list → the ordinal of the snapshot whose `manifest_list` equals the path;
/// - manifest → the ordinal of its `added_snapshot_id`;
/// - content file → the ordinal of its manifest's `added_snapshot_id`;
/// - statistics file → the ordinal of its `snapshot_id`.
async fn normalize_deleted(
    before: &TableMetadata,
    report: &iceberg::transaction::CleanupReport,
) -> Vec<String> {
    use iceberg::io::FileIO;

    let mut ordered: Vec<Arc<Snapshot>> = before.snapshots().cloned().collect();
    ordered.sort_by_key(|snapshot| snapshot.sequence_number());
    let ordinal_by_id: HashMap<i64, usize> = ordered
        .iter()
        .enumerate()
        .map(|(ordinal, snapshot)| (snapshot.snapshot_id(), ordinal))
        .collect();

    let file_io = FileIO::new_with_fs();
    // path -> (owning-snapshot ordinal, per-FILE discriminator). The discriminator is a
    // cross-language-stable fingerprint that makes the descriptor INJECTIVE within a fixture: two
    // DIFFERENT files added by the SAME snapshot (e.g. a 2-file append) share an ordinal, so an
    // ordinal-only token would let a Rust-deletes-X / Java-deletes-Y swap pass set-equality. The
    // discriminators below distinguish such siblings (record count for content; the added/existing/
    // deleted file counts for manifests; file size for statistics) — see `descriptor_injectivity`.
    let mut list_owner: HashMap<String, usize> = HashMap::new();
    let mut manifest_owner: HashMap<String, (usize, String)> = HashMap::new();
    let mut content_owner: HashMap<String, (usize, String)> = HashMap::new();
    for snapshot in &ordered {
        let snapshot_ordinal = ordinal_by_id[&snapshot.snapshot_id()];
        list_owner.insert(snapshot.manifest_list().to_string(), snapshot_ordinal);
        let manifest_list = snapshot
            .load_manifest_list(&file_io, before)
            .await
            .expect("load manifest list");
        for manifest_file in manifest_list.entries() {
            let manifest_ordinal = *ordinal_by_id
                .get(&manifest_file.added_snapshot_id)
                .unwrap_or(&snapshot_ordinal);
            let manifest_discriminator = format!(
                "a{}e{}d{}",
                manifest_file.added_files_count.unwrap_or(0),
                manifest_file.existing_files_count.unwrap_or(0),
                manifest_file.deleted_files_count.unwrap_or(0),
            );
            manifest_owner
                .entry(manifest_file.manifest_path.clone())
                .or_insert((manifest_ordinal, manifest_discriminator));
            let manifest = manifest_file
                .load_manifest(&file_io)
                .await
                .expect("load manifest");
            for entry in manifest.entries() {
                let content_discriminator = format!("rc{}", entry.data_file().record_count());
                content_owner
                    .entry(entry.file_path().to_string())
                    .or_insert((manifest_ordinal, content_discriminator));
            }
        }
    }
    let mut stats_owner: HashMap<String, (usize, String)> = HashMap::new();
    for stats in before.statistics_iter() {
        if let Some(ordinal) = ordinal_by_id.get(&stats.snapshot_id) {
            stats_owner.insert(
                stats.statistics_path.clone(),
                (*ordinal, format!("sz{}", stats.file_size_in_bytes)),
            );
        }
    }
    for stats in before.partition_statistics_iter() {
        if let Some(ordinal) = ordinal_by_id.get(&stats.snapshot_id) {
            stats_owner.insert(
                stats.statistics_path.clone(),
                (*ordinal, format!("sz{}", stats.file_size_in_bytes)),
            );
        }
    }

    let mut descriptor = Vec::new();
    for path in &report.deleted_content_files {
        descriptor.push(token("content", content_owner.get(path), path));
    }
    for path in &report.deleted_manifests {
        descriptor.push(token("manifest", manifest_owner.get(path), path));
    }
    for path in &report.deleted_manifest_lists {
        descriptor.push(token(
            "manifest_list",
            list_owner
                .get(path)
                .map(|ordinal| (*ordinal, String::new()))
                .as_ref(),
            path,
        ));
    }
    for path in &report.deleted_statistics_files {
        descriptor.push(token("statistics", stats_owner.get(path), path));
    }
    descriptor.sort();
    descriptor
}

/// `<funnel>@ord<N>` (optionally `#<discriminator>` when non-empty), or a loud `UNCLASSIFIED:` token
/// (so a misclassified path fails the comparison rather than silently dropping out — matching the
/// Java side's posture). The discriminator is what makes the multiset INJECTIVE over sibling files
/// added by one snapshot (the swap-masking hole the bare ordinal had); a manifest LIST needs none
/// (one list per snapshot ⇒ the ordinal is already unique).
fn token(funnel: &str, owner: Option<&(usize, String)>, path: &str) -> String {
    match owner {
        Some((ordinal, discriminator)) if discriminator.is_empty() => {
            format!("{funnel}@ord{ordinal}")
        }
        Some((ordinal, discriminator)) => format!("{funnel}@ord{ordinal}#{discriminator}"),
        None => format!("UNCLASSIFIED:{path}"),
    }
}

/// INJECTIVITY PIN (offline, no env) — the descriptor multiset must distinguish two DIFFERENT
/// content files added by the SAME snapshot, or a Rust-deletes-X / Java-deletes-Y SWAP would pass
/// set-equality vacuously. Risk pinned: a coarse `<funnel>@ord<N>` token (ordinal only) collapses
/// every sibling file of one commit (e.g. a 2-file append `{d1 rc=10, d2 rc=20}`) onto the SAME
/// token, so deleting the WRONG one of a pair is invisible. The per-file discriminator (`rc<count>`
/// for content) closes the hole: the two siblings get distinct tokens, so the swapped multisets
/// differ. (Mirrors the Java `ExpireOracle.normalizeDeleted` discriminator scheme.)
#[test]
fn test_descriptor_token_is_injective_over_sibling_files_of_one_snapshot() {
    // Two distinct files added by snapshot ordinal 0, differing only in record count.
    let d1 = (0usize, "rc10".to_string());
    let d2 = (0usize, "rc20".to_string());
    let token_d1 = token("content", Some(&d1), "/abs/a/d1.parquet");
    let token_d2 = token("content", Some(&d2), "/abs/b/d2.parquet");

    // The discriminator must make the two siblings DISTINCT (else a swap is invisible).
    assert_eq!(token_d1, "content@ord0#rc10");
    assert_eq!(token_d2, "content@ord0#rc20");
    assert_ne!(
        token_d1, token_d2,
        "two distinct same-snapshot files must NOT share a descriptor token"
    );

    // A SWAP of which sibling each side deletes yields DIFFERENT multisets — caught by the compare.
    // (Rust deletes d1, Java deletes d2, both also deleting some shared file f.)
    let shared = token("manifest_list", Some(&(1, String::new())), "/list");
    let mut rust_side = vec![token_d1.clone(), shared.clone()];
    let mut java_side = vec![token_d2.clone(), shared];
    rust_side.sort();
    java_side.sort();
    assert_ne!(
        rust_side, java_side,
        "a swapped sibling-deletion must produce DIFFERENT descriptor multisets (the swap is caught)"
    );

    // A manifest LIST carries no discriminator (one list per snapshot ⇒ ordinal already unique).
    assert_eq!(
        token("manifest_list", Some(&(2, String::new())), "/x"),
        "manifest_list@ord2"
    );
    // An unclassified path surfaces loudly (a misattribution fails the comparison, never drops out).
    assert_eq!(token("content", None, "/lost"), "UNCLASSIFIED:/lost");
}

fn write_descriptor(path: &Path, descriptor: &[String]) {
    let json = JsonValue::Array(
        descriptor
            .iter()
            .map(|token| JsonValue::String(token.clone()))
            .collect(),
    );
    fs::write(
        path,
        serde_json::to_string(&json).expect("serialize descriptor"),
    )
    .unwrap_or_else(|error| panic!("write {}: {error}", path.display()));
}

fn read_descriptor(path: &Path) -> Vec<String> {
    let raw =
        fs::read_to_string(path).unwrap_or_else(|error| panic!("read {}: {error}", path.display()));
    let value: JsonValue = serde_json::from_str(&raw)
        .unwrap_or_else(|error| panic!("parse {}: {error}", path.display()));
    let mut tokens: Vec<String> = value
        .as_array()
        .expect("descriptor is a JSON array")
        .iter()
        .map(|token| {
            token
                .as_str()
                .expect("descriptor token is a string")
                .to_string()
        })
        .collect();
    tokens.sort();
    tokens
}

// ===========================================================================================
// Direction 2 + the descriptor check — the comparison tests.
// ===========================================================================================

fn load_json(path: &Path) -> JsonValue {
    let raw =
        fs::read_to_string(path).unwrap_or_else(|error| panic!("read {}: {error}", path.display()));
    serde_json::from_str(&raw).unwrap_or_else(|error| panic!("parse {}: {error}", path.display()))
}

/// Direction 2 (Java acts, Rust verifies): Rust's canonical view of the JAVA-expired table equals
/// Java's own canonical view — the surviving snapshot / ref / statistics structure as Rust reads it.
#[tokio::test]
async fn test_rust_view_of_java_expire_matches_java_view() {
    let Some(dir) = compare_dir() else {
        println!(
            "skipping interop_expire compare — set ICEBERG_INTEROP_EXPIRE_DIR \
             (run dev/java-interop/run-interop-expire.sh)"
        );
        return;
    };

    for fixture in FIXTURES {
        let fixture_dir = dir.join(fixture);
        let java_view = load_json(&fixture_dir.join("java_meta.json"));
        let rust_view =
            expire_meta_view(&fixture_dir.join("table/metadata/final.metadata.json")).await;
        assert_eq!(
            rust_view, java_view,
            "{fixture}: Rust's view of the JAVA-expired table diverges from Java's own view"
        );
        println!("expire/{fixture}: Rust view of Java table == Java view OK");
    }
}

// ===========================================================================================
// The canonical snapshot-metadata view — a LOCAL copy of `common/snapshot_meta_view.rs` with the
// EXPIRED-PARENT fix: a surviving snapshot whose parent was expired out of the table emits a NULL
// `parent_ordinal` (the shared helper indexes `ordinals[&parent_id]` and would PANIC; the Java
// `SnapshotMetaOracle.emit` carries the matching null-on-absent-parent rule). The expire fixtures
// are the first interop fixtures where a retained snapshot's parent is removed, so the shared helper
// cannot be reused unchanged here (and modifying it is out of this increment's scope).
// ===========================================================================================

/// Build the canonical snapshot-metadata view of the table at `metadata_json_path`, byte-comparable
/// against the Java `SnapshotMetaOracle.emit` output — with `parent_ordinal` = null when the parent
/// is no longer in-table (the expire case). Mirrors `common::snapshot_meta_view::snapshot_meta_view`
/// in every other respect (the SAME `SUMMARY_COUNT_KEYS`, manifest sort tuple, and entry sort tuple).
async fn expire_meta_view(metadata_json_path: &Path) -> JsonValue {
    let raw = fs::read_to_string(metadata_json_path)
        .unwrap_or_else(|error| panic!("read {}: {error}", metadata_json_path.display()));
    let metadata: TableMetadata = serde_json::from_str(&raw)
        .unwrap_or_else(|error| panic!("parse {}: {error}", metadata_json_path.display()));
    let file_io = FileIO::new_with_fs();

    let mut snapshots: Vec<_> = metadata.snapshots().cloned().collect();
    snapshots.sort_by_key(|snapshot| snapshot.sequence_number());
    let ordinals: HashMap<i64, usize> = snapshots
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
                panic!("load manifest list of {}: {error}", snapshot.snapshot_id())
            });
        let mut manifests: Vec<_> = manifest_list.entries().to_vec();
        // W3 (2026-06-11): `partition_spec_id` is the FINAL tiebreaker (position 10, Option B),
        // mirroring common/snapshot_meta_view.rs and the Java SnapshotMetaOracle comparator. The
        // sort exists to erase writer-dependent manifest-list ordering; its only contract is
        // cross-language determinism, which the final-tiebreaker position provides for future
        // multi-spec fixtures with zero risk to existing ordering (spec_id is constant 0 in every
        // single-spec fixture, so the key is byte-invisible here).
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
                manifest.partition_spec_id,
            )
        });

        let mut manifest_views = Vec::with_capacity(manifests.len());
        for manifest_file in &manifests {
            let manifest = manifest_file
                .load_manifest(&file_io)
                .await
                .unwrap_or_else(|error| panic!("load manifest: {error}"));
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
                "partition_spec_id": manifest_file.partition_spec_id,
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

        // THE EXPIRE FIX: parent_ordinal is null when the parent has no in-table ordinal (it was
        // expired) — matching the Java `SnapshotMetaOracle.emit` null-on-absent-parent rule.
        let parent_ordinal = snapshot
            .parent_snapshot_id()
            .and_then(|parent_id| ordinals.get(&parent_id).copied());
        snapshot_views.push(json!({
            "ordinal": ordinals[&snapshot.snapshot_id()],
            "parent_ordinal": parent_ordinal,
            "sequence_number": snapshot.sequence_number(),
            "operation": summary.operation.as_str(),
            "summary": JsonValue::Object(summary_view),
            "manifests": manifest_views,
        }));
    }

    json!({ "snapshots": snapshot_views })
}

/// The cross-language entry sort tuple — identical to `common::snapshot_meta_view`'s.
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

fn partition_json_sort_key(partition: &JsonValue) -> String {
    match partition {
        JsonValue::Null => String::new(),
        other => other.to_string(),
    }
}

/// The deleted-set agreement (both directions of the cleanup set): the Rust-collected deleted
/// descriptor (`rust_deleted.json`) equals Java's (`java_deleted.json`) — proving Rust's cleanup
/// candidate computation on the pre-expire fixture matches Java's actual `deleteWith` set.
#[tokio::test]
async fn test_rust_deleted_descriptor_matches_java() {
    let Some(dir) = compare_dir() else {
        println!(
            "skipping interop_expire descriptor — set ICEBERG_INTEROP_EXPIRE_DIR \
             (run dev/java-interop/run-interop-expire.sh)"
        );
        return;
    };

    for fixture in FIXTURES {
        let fixture_dir = dir.join(fixture);
        let java_descriptor = read_descriptor(&fixture_dir.join("java_deleted.json"));
        let rust_deleted_path = fixture_dir.join("rust_deleted.json");
        assert!(
            rust_deleted_path.exists(),
            "{fixture}: missing rust_deleted.json — run the GEN step of the run script first"
        );
        let rust_descriptor = read_descriptor(&rust_deleted_path);
        assert_eq!(
            rust_descriptor, java_descriptor,
            "{fixture}: the Rust deleted descriptor diverges from Java's"
        );
        println!("expire/{fixture}: deleted descriptor Rust == Java OK ({rust_descriptor:?})");
    }
}
