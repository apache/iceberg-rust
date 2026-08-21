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

use std::collections::{HashMap, HashSet};

use futures::{StreamExt, TryStreamExt};

use crate::Result;
use crate::spec::{DataContentType, ManifestFile, ManifestReader, SnapshotRef, TableMetadata};
use crate::table::Table;

/// Bound on concurrent manifest-list / manifest loads, matching `CatalogUtil`'s delete concurrency.
const LOAD_CONCURRENCY: usize = 10;

/// Files reachable only from expired snapshots, grouped by kind, that are therefore safe to delete.
///
/// Paths are absolute and de-duplicated. A file referenced by any retained snapshot is never
/// included, even if an expired snapshot also references it.
#[derive(Debug)]
pub struct UnreferencedFiles {
    /// Manifest-list (snapshot) files.
    pub manifest_lists: HashSet<String>,
    /// Manifest files.
    pub manifests: HashSet<String>,
    /// Data files (`DataContentType::Data`).
    pub data_files: HashSet<String>,
    /// Delete files (positional or equality deletes).
    pub delete_files: HashSet<String>,
    /// Table statistics (Puffin) files.
    pub statistics_files: HashSet<String>,
    /// Partition statistics files.
    pub partition_statistics_files: HashSet<String>,
}

impl UnreferencedFiles {
    /// Total number of files across every kind.
    pub fn len(&self) -> usize {
        self.manifest_lists.len()
            + self.manifests.len()
            + self.data_files.len()
            + self.delete_files.len()
            + self.statistics_files.len()
            + self.partition_statistics_files.len()
    }

    /// Whether there is nothing to delete.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Iterates every file path across all kinds.
    pub fn all_paths(&self) -> impl Iterator<Item = &String> {
        self.manifest_lists
            .iter()
            .chain(&self.manifests)
            .chain(&self.data_files)
            .chain(&self.delete_files)
            .chain(&self.statistics_files)
            .chain(&self.partition_statistics_files)
    }
}

/// Computes the files reachable only from the expired snapshots of `table`.
///
/// `table` must be the metadata *before* expiry (still carrying the expired snapshots), and
/// `expired_snapshot_ids` the ids about to be removed. The result is the reference-count difference
/// `files(expired) - files(retained)`, mirroring Java `ReachableFileCleanup`: a file kept alive by
/// any surviving snapshot is excluded.
///
/// Data and delete files are only collected when the `gc.enabled` table property is `true`, matching
/// [`crate::catalog::utils::drop_table_data`] — they may be shared with other tables (e.g. via
/// shallow clones), whereas manifests, manifest lists, and statistics files are table-private.
///
/// Reachability is resolved by reading manifests, so any I/O error aborts the whole call: a
/// manifest list or manifest that cannot be read — for a retained or an expired snapshot alike —
/// leaves a reachable set incomplete, and proceeding could delete a file that is still live.
///
/// This only computes paths; it does not delete anything.
pub async fn unreferenced_files(
    table: &Table,
    expired_snapshot_ids: &HashSet<i64>,
) -> Result<UnreferencedFiles> {
    let metadata = table.metadata();
    let gc_enabled = metadata.table_properties()?.gc_enabled;

    let (expired, retained): (Vec<&SnapshotRef>, Vec<&SnapshotRef>) = metadata
        .snapshots()
        .partition(|snapshot| expired_snapshot_ids.contains(&snapshot.snapshot_id()));

    let retained_reachable = collect_reachable(table, &retained, gc_enabled).await?;
    let expired_reachable = collect_reachable(table, &expired, gc_enabled).await?;

    let (retained_stats, retained_partition_stats) = stats_paths(metadata, &retained);
    let (expired_stats, expired_partition_stats) = stats_paths(metadata, &expired);

    Ok(UnreferencedFiles {
        manifest_lists: difference(
            expired_reachable.manifest_lists,
            &retained_reachable.manifest_lists,
        ),
        manifests: difference(expired_reachable.manifests, &retained_reachable.manifests),
        data_files: difference(expired_reachable.data_files, &retained_reachable.data_files),
        delete_files: difference(
            expired_reachable.delete_files,
            &retained_reachable.delete_files,
        ),
        statistics_files: difference(expired_stats, &retained_stats),
        partition_statistics_files: difference(expired_partition_stats, &retained_partition_stats),
    })
}

/// File paths reachable from a set of snapshots, before any anti-join against the retained set.
#[derive(Default)]
struct Reachable {
    manifest_lists: HashSet<String>,
    manifests: HashSet<String>,
    data_files: HashSet<String>,
    delete_files: HashSet<String>,
}

async fn collect_reachable(
    table: &Table,
    snapshots: &[&SnapshotRef],
    gc_enabled: bool,
) -> Result<Reachable> {
    let mut reachable = Reachable::default();

    // Load each snapshot's manifest list concurrently; any failure aborts (see the fn docs).
    let manifest_lists = futures::stream::iter(snapshots.iter().copied())
        .map(|snapshot| async move {
            let manifest_list = table.manifest_list_reader(snapshot).load().await?;
            Ok::<_, crate::Error>((snapshot.manifest_list().to_string(), manifest_list))
        })
        .buffer_unordered(LOAD_CONCURRENCY)
        .try_collect::<Vec<_>>()
        .await?;

    // Dedup manifests by path so one shared across snapshots in this set is only read once.
    let mut manifests_by_path: HashMap<String, ManifestFile> = HashMap::new();
    for (location, manifest_list) in manifest_lists {
        if !location.is_empty() {
            reachable.manifest_lists.insert(location);
        }
        for manifest_file in manifest_list.entries() {
            reachable
                .manifests
                .insert(manifest_file.manifest_path.clone());
            manifests_by_path.insert(manifest_file.manifest_path.clone(), manifest_file.clone());
        }
    }

    // Data/delete files are only relevant under gc.enabled (see the function-level docs).
    if gc_enabled {
        let io = table.file_io();
        // `ManifestReader` (unlike a raw read) transparently decrypts an encrypted manifest.
        let manifests = futures::stream::iter(manifests_by_path.into_values())
            .map(|manifest_file| async move {
                ManifestReader::new(io.clone()).read(&manifest_file).await
            })
            .buffer_unordered(LOAD_CONCURRENCY)
            .try_collect::<Vec<_>>()
            .await?;

        for manifest in manifests {
            for entry in manifest.entries() {
                // Only live (added/existing) entries reference a file; a Deleted entry means the
                // snapshot dropped it, so it must not keep that file alive (matches Java's
                // `liveEntries()`), otherwise a file deleted by a retained snapshot would never be
                // reported even once every snapshot that still held it live has expired.
                if !entry.is_alive() {
                    continue;
                }
                let path = entry.file_path().to_string();
                match entry.data_file().content_type() {
                    DataContentType::Data => reachable.data_files.insert(path),
                    DataContentType::PositionDeletes | DataContentType::EqualityDeletes => {
                        reachable.delete_files.insert(path)
                    }
                };
            }
        }
    }

    Ok(reachable)
}

/// Statistics and partition-statistics file paths for the given snapshots (keyed by snapshot id).
fn stats_paths(
    metadata: &TableMetadata,
    snapshots: &[&SnapshotRef],
) -> (HashSet<String>, HashSet<String>) {
    let mut statistics = HashSet::new();
    let mut partition_statistics = HashSet::new();
    for snapshot in snapshots {
        if let Some(file) = metadata.statistics_for_snapshot(snapshot.snapshot_id()) {
            statistics.insert(file.statistics_path.clone());
        }
        if let Some(file) = metadata.partition_statistics_for_snapshot(snapshot.snapshot_id()) {
            partition_statistics.insert(file.statistics_path.clone());
        }
    }
    (statistics, partition_statistics)
}

/// `from - remove`, consuming `from`.
fn difference(mut from: HashSet<String>, remove: &HashSet<String>) -> HashSet<String> {
    from.retain(|path| !remove.contains(path));
    from
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::sync::Arc;

    use tempfile::TempDir;
    use uuid::Uuid;

    use super::*;
    use crate::TableIdent;
    use crate::io::FileIO;
    use crate::spec::{
        DataFile, DataFileBuilder, DataFileFormat, FormatVersion, ManifestListWriter,
        ManifestWriterBuilder, NestedField, Operation, PartitionSpec, PartitionStatisticsFile,
        PrimitiveType, Schema, SchemaRef, Snapshot, SnapshotReference, SnapshotRetention,
        SortOrder, StatisticsFile, Struct, Summary, TableMetadataBuilder, Type,
    };

    // Oldest to newest; S3 is only used by the multi-snapshot test.
    const S1: i64 = 1;
    const S2: i64 = 2;
    const S3: i64 = 3;

    fn schema() -> SchemaRef {
        Arc::new(
            Schema::builder()
                .with_schema_id(0)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                ])
                .build()
                .unwrap(),
        )
    }

    fn unpartitioned() -> PartitionSpec {
        PartitionSpec::unpartition_spec()
    }

    fn data_file(path: &str, content: DataContentType) -> DataFile {
        DataFileBuilder::default()
            .partition_spec_id(0)
            .content(content)
            .file_path(path.to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(10)
            .record_count(1)
            .partition(Struct::empty())
            .key_metadata(None)
            .build()
            .unwrap()
    }

    /// Writes a manifest holding `files` (all of `content` kind) and returns it.
    async fn write_manifest(
        file_io: &FileIO,
        loc: &str,
        snapshot_id: i64,
        sequence_number: i64,
        content: DataContentType,
        files: &[&str],
    ) -> ManifestFile {
        let output = file_io
            .new_output(format!(
                "{loc}/metadata/manifest-{snapshot_id}-{}.avro",
                Uuid::new_v4()
            ))
            .unwrap();
        let builder =
            ManifestWriterBuilder::new(output, Some(snapshot_id), schema(), unpartitioned());
        let mut writer = match content {
            DataContentType::Data => builder.build_v2_data(),
            _ => builder.build_v2_deletes(),
        };
        for path in files {
            writer
                .add_file(data_file(path, content), sequence_number)
                .unwrap();
        }
        writer.write_manifest_file().await.unwrap()
    }

    /// Writes a data manifest whose single entry marks `path` as `Deleted`, and returns it.
    async fn write_deleted_data_manifest(
        file_io: &FileIO,
        loc: &str,
        snapshot_id: i64,
        file_sequence_number: i64,
        path: &str,
    ) -> ManifestFile {
        let output = file_io
            .new_output(format!(
                "{loc}/metadata/manifest-{snapshot_id}-{}.avro",
                Uuid::new_v4()
            ))
            .unwrap();
        let mut writer =
            ManifestWriterBuilder::new(output, Some(snapshot_id), schema(), unpartitioned())
                .build_v2_data();
        writer
            .add_delete_file(
                data_file(path, DataContentType::Data),
                file_sequence_number,
                Some(file_sequence_number),
            )
            .unwrap();
        writer.write_manifest_file().await.unwrap()
    }

    /// Writes a snapshot's manifest list from the given `manifests` and returns its location.
    async fn write_list(
        file_io: &FileIO,
        loc: &str,
        snapshot_id: i64,
        parent_snapshot_id: Option<i64>,
        sequence_number: i64,
        manifests: Vec<ManifestFile>,
    ) -> String {
        let location = format!("{loc}/metadata/snap-{snapshot_id}.avro");
        let output = file_io.new_output(&location).unwrap();
        let mut writer = ManifestListWriter::v2(
            output.writer().await.unwrap(),
            snapshot_id,
            parent_snapshot_id,
            sequence_number,
        );
        writer.add_manifests(manifests.into_iter()).unwrap();
        writer.close().await.unwrap();
        location
    }

    /// Convenience over [`write_list`]: builds one data manifest and/or one delete manifest for the
    /// snapshot, then writes the manifest list referencing them.
    async fn write_manifest_list(
        file_io: &FileIO,
        loc: &str,
        snapshot_id: i64,
        parent_snapshot_id: Option<i64>,
        sequence_number: i64,
        data_files: &[&str],
        delete_files: &[&str],
    ) -> String {
        let mut manifests: Vec<ManifestFile> = vec![];
        if !data_files.is_empty() {
            manifests.push(
                write_manifest(
                    file_io,
                    loc,
                    snapshot_id,
                    sequence_number,
                    DataContentType::Data,
                    data_files,
                )
                .await,
            );
        }
        if !delete_files.is_empty() {
            manifests.push(
                write_manifest(
                    file_io,
                    loc,
                    snapshot_id,
                    sequence_number,
                    DataContentType::PositionDeletes,
                    delete_files,
                )
                .await,
            );
        }
        write_list(
            file_io,
            loc,
            snapshot_id,
            parent_snapshot_id,
            sequence_number,
            manifests,
        )
        .await
    }

    fn snapshot(
        id: i64,
        parent: Option<i64>,
        sequence_number: i64,
        timestamp_ms: i64,
        manifest_list: String,
    ) -> Snapshot {
        Snapshot::builder()
            .with_snapshot_id(id)
            .with_parent_snapshot_id(parent)
            .with_sequence_number(sequence_number)
            .with_timestamp_ms(timestamp_ms)
            .with_schema_id(0)
            .with_manifest_list(manifest_list)
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: HashMap::new(),
            })
            .build()
    }

    fn stats_file(snapshot_id: i64, path: &str) -> StatisticsFile {
        StatisticsFile {
            snapshot_id,
            statistics_path: path.to_string(),
            file_size_in_bytes: 1,
            file_footer_size_in_bytes: 1,
            key_metadata: None,
            blob_metadata: vec![],
        }
    }

    fn partition_stats_file(snapshot_id: i64, path: &str) -> PartitionStatisticsFile {
        PartitionStatisticsFile {
            snapshot_id,
            statistics_path: path.to_string(),
            file_size_in_bytes: 1,
        }
    }

    /// Builds a table from pre-built `snapshots`, with `main_head` as the main branch head.
    fn build_table(
        tmp: &TempDir,
        file_io: FileIO,
        snapshots: Vec<Snapshot>,
        main_head: i64,
        properties: HashMap<String, String>,
        statistics: Vec<StatisticsFile>,
        partition_statistics: Vec<PartitionStatisticsFile>,
    ) -> Table {
        let location = tmp.path().join("table").to_str().unwrap().to_string();
        let mut builder = TableMetadataBuilder::new(
            (*schema()).clone(),
            unpartitioned(),
            SortOrder::unsorted_order(),
            location,
            FormatVersion::V2,
            properties,
        )
        .unwrap();
        for snap in snapshots {
            builder = builder.add_snapshot(snap).unwrap();
        }
        builder = builder
            .set_ref("main", SnapshotReference {
                snapshot_id: main_head,
                retention: SnapshotRetention::Branch {
                    min_snapshots_to_keep: None,
                    max_snapshot_age_ms: None,
                    max_ref_age_ms: None,
                },
            })
            .unwrap();
        for stats in statistics {
            builder = builder.set_statistics(stats);
        }
        for partition_stats in partition_statistics {
            builder = builder.set_partition_statistics(partition_stats);
        }
        let metadata = builder.build().unwrap().metadata;

        Table::builder()
            .metadata(metadata)
            .identifier(TableIdent::from_strs(["db", "t"]).unwrap())
            .file_io(file_io)
            .metadata_location(
                tmp.path()
                    .join("metadata/v1.json")
                    .to_str()
                    .unwrap()
                    .to_string(),
            )
            .runtime(crate::test_utils::test_runtime())
            .build()
            .unwrap()
    }

    #[tokio::test]
    async fn returns_only_files_reachable_solely_from_expired_snapshot() {
        let tmp = TempDir::new().unwrap();
        let file_io = FileIO::new_with_fs();
        let loc = tmp.path().join("table").to_str().unwrap().to_string();

        // S1 shares `shared.parquet` with S2; each also has a private data file.
        let s1_list = write_manifest_list(
            &file_io,
            &loc,
            S1,
            None,
            1,
            &["/shared.parquet", "/s1.parquet"],
            &[],
        )
        .await;
        let s2_list = write_manifest_list(
            &file_io,
            &loc,
            S2,
            Some(S1),
            2,
            &["/shared.parquet", "/s2.parquet"],
            &[],
        )
        .await;

        let table = build_table(
            &tmp,
            file_io,
            vec![
                snapshot(S1, None, 1, 1000, s1_list.clone()),
                snapshot(S2, Some(S1), 2, 2000, s2_list),
            ],
            S2,
            HashMap::new(),
            vec![],
            vec![],
        );

        let files = unreferenced_files(&table, &HashSet::from([S1]))
            .await
            .unwrap();

        assert_eq!(files.manifest_lists, HashSet::from([s1_list]));
        assert_eq!(files.data_files, HashSet::from(["/s1.parquet".to_string()]));
        assert!(files.delete_files.is_empty());
        // S1 has exactly one (data) manifest, distinct from S2's.
        assert_eq!(files.manifests.len(), 1);
    }

    #[tokio::test]
    async fn returns_delete_files_of_expired_snapshot() {
        let tmp = TempDir::new().unwrap();
        let file_io = FileIO::new_with_fs();
        let loc = tmp.path().join("table").to_str().unwrap().to_string();

        let s1_list = write_manifest_list(&file_io, &loc, S1, None, 1, &["/s1.parquet"], &[
            "/s1-delete.parquet",
        ])
        .await;
        let s2_list =
            write_manifest_list(&file_io, &loc, S2, Some(S1), 2, &["/s2.parquet"], &[]).await;

        let table = build_table(
            &tmp,
            file_io,
            vec![
                snapshot(S1, None, 1, 1000, s1_list),
                snapshot(S2, Some(S1), 2, 2000, s2_list),
            ],
            S2,
            HashMap::new(),
            vec![],
            vec![],
        );

        let files = unreferenced_files(&table, &HashSet::from([S1]))
            .await
            .unwrap();
        assert_eq!(files.data_files, HashSet::from(["/s1.parquet".to_string()]));
        assert_eq!(
            files.delete_files,
            HashSet::from(["/s1-delete.parquet".to_string()])
        );
    }

    #[tokio::test]
    async fn shared_delete_file_is_excluded() {
        let tmp = TempDir::new().unwrap();
        let file_io = FileIO::new_with_fs();
        let loc = tmp.path().join("table").to_str().unwrap().to_string();

        // Both snapshots reference `/shared-delete.parquet`; only S1 also has a private delete file.
        let s1_list = write_manifest_list(&file_io, &loc, S1, None, 1, &["/s1.parquet"], &[
            "/shared-delete.parquet",
            "/s1-delete.parquet",
        ])
        .await;
        let s2_list = write_manifest_list(&file_io, &loc, S2, Some(S1), 2, &["/s2.parquet"], &[
            "/shared-delete.parquet",
        ])
        .await;

        let table = build_table(
            &tmp,
            file_io,
            vec![
                snapshot(S1, None, 1, 1000, s1_list),
                snapshot(S2, Some(S1), 2, 2000, s2_list),
            ],
            S2,
            HashMap::new(),
            vec![],
            vec![],
        );

        let files = unreferenced_files(&table, &HashSet::from([S1]))
            .await
            .unwrap();
        assert_eq!(
            files.delete_files,
            HashSet::from(["/s1-delete.parquet".to_string()])
        );
    }

    #[tokio::test]
    async fn file_deleted_by_retained_snapshot_is_returned() {
        let tmp = TempDir::new().unwrap();
        let file_io = FileIO::new_with_fs();
        let loc = tmp.path().join("table").to_str().unwrap().to_string();

        // S1 (expired) adds F as a live entry; S2 (retained) carries F only as a Deleted entry. Once
        // S1 expires, nothing holds F live, so a deleted entry must not keep it alive — F is orphaned
        // and must be reported for deletion.
        let s1_manifest = write_manifest(&file_io, &loc, S1, 1, DataContentType::Data, &[
            "/f.parquet",
        ])
        .await;
        let s2_manifest = write_deleted_data_manifest(&file_io, &loc, S2, 1, "/f.parquet").await;
        let s1_list = write_list(&file_io, &loc, S1, None, 1, vec![s1_manifest]).await;
        let s2_list = write_list(&file_io, &loc, S2, Some(S1), 2, vec![s2_manifest]).await;

        let table = build_table(
            &tmp,
            file_io,
            vec![
                snapshot(S1, None, 1, 1000, s1_list),
                snapshot(S2, Some(S1), 2, 2000, s2_list),
            ],
            S2,
            HashMap::new(),
            vec![],
            vec![],
        );

        let files = unreferenced_files(&table, &HashSet::from([S1]))
            .await
            .unwrap();
        assert_eq!(files.data_files, HashSet::from(["/f.parquet".to_string()]));
    }

    #[tokio::test]
    async fn returns_statistics_files_of_expired_snapshot() {
        let tmp = TempDir::new().unwrap();
        let file_io = FileIO::new_with_fs();
        let loc = tmp.path().join("table").to_str().unwrap().to_string();

        let s1_list = write_manifest_list(&file_io, &loc, S1, None, 1, &["/s1.parquet"], &[]).await;
        let s2_list =
            write_manifest_list(&file_io, &loc, S2, Some(S1), 2, &["/s2.parquet"], &[]).await;

        let table = build_table(
            &tmp,
            file_io,
            vec![
                snapshot(S1, None, 1, 1000, s1_list),
                snapshot(S2, Some(S1), 2, 2000, s2_list),
            ],
            S2,
            HashMap::new(),
            vec![
                stats_file(S1, "/s1-stats.puffin"),
                stats_file(S2, "/s2-stats.puffin"),
            ],
            vec![],
        );

        let files = unreferenced_files(&table, &HashSet::from([S1]))
            .await
            .unwrap();
        // Only the expired snapshot's statistics file is returned; the retained one is kept.
        assert_eq!(
            files.statistics_files,
            HashSet::from(["/s1-stats.puffin".to_string()])
        );
    }

    #[tokio::test]
    async fn returns_partition_statistics_files_of_expired_snapshot() {
        let tmp = TempDir::new().unwrap();
        let file_io = FileIO::new_with_fs();
        let loc = tmp.path().join("table").to_str().unwrap().to_string();

        let s1_list = write_manifest_list(&file_io, &loc, S1, None, 1, &["/s1.parquet"], &[]).await;
        let s2_list =
            write_manifest_list(&file_io, &loc, S2, Some(S1), 2, &["/s2.parquet"], &[]).await;

        let table = build_table(
            &tmp,
            file_io,
            vec![
                snapshot(S1, None, 1, 1000, s1_list),
                snapshot(S2, Some(S1), 2, 2000, s2_list),
            ],
            S2,
            HashMap::new(),
            vec![],
            vec![
                partition_stats_file(S1, "/s1-pstats.puffin"),
                partition_stats_file(S2, "/s2-pstats.puffin"),
            ],
        );

        let files = unreferenced_files(&table, &HashSet::from([S1]))
            .await
            .unwrap();
        assert_eq!(
            files.partition_statistics_files,
            HashSet::from(["/s1-pstats.puffin".to_string()])
        );
        assert!(files.statistics_files.is_empty());
    }

    #[tokio::test]
    async fn gc_disabled_excludes_content_files_but_keeps_metadata() {
        let tmp = TempDir::new().unwrap();
        let file_io = FileIO::new_with_fs();
        let loc = tmp.path().join("table").to_str().unwrap().to_string();

        let s1_list = write_manifest_list(&file_io, &loc, S1, None, 1, &["/s1.parquet"], &[]).await;
        let s2_list =
            write_manifest_list(&file_io, &loc, S2, Some(S1), 2, &["/s2.parquet"], &[]).await;

        let table = build_table(
            &tmp,
            file_io,
            vec![
                snapshot(S1, None, 1, 1000, s1_list.clone()),
                snapshot(S2, Some(S1), 2, 2000, s2_list),
            ],
            S2,
            HashMap::from([("gc.enabled".to_string(), "false".to_string())]),
            vec![],
            vec![],
        );

        let files = unreferenced_files(&table, &HashSet::from([S1]))
            .await
            .unwrap();
        // Data files are not collected with gc disabled, but the table-private metadata still is.
        assert!(files.data_files.is_empty());
        assert_eq!(files.manifest_lists, HashSet::from([s1_list]));
        assert_eq!(files.manifests.len(), 1);
    }

    #[tokio::test]
    async fn shared_manifest_carried_forward_is_excluded() {
        let tmp = TempDir::new().unwrap();
        let file_io = FileIO::new_with_fs();
        let loc = tmp.path().join("table").to_str().unwrap().to_string();

        // A single manifest is carried forward from S1 into S2; each snapshot also has a private one.
        let shared = write_manifest(&file_io, &loc, S1, 1, DataContentType::Data, &[
            "/shared.parquet",
        ])
        .await;
        let s1_only = write_manifest(&file_io, &loc, S1, 1, DataContentType::Data, &[
            "/s1.parquet",
        ])
        .await;
        let s2_only = write_manifest(&file_io, &loc, S2, 2, DataContentType::Data, &[
            "/s2.parquet",
        ])
        .await;
        let s1_list = write_list(&file_io, &loc, S1, None, 1, vec![
            shared.clone(),
            s1_only.clone(),
        ])
        .await;
        // A carried-forward manifest keeps the sequence number it was first committed with, as a
        // real fast-append would when reading it back from S1's manifest list.
        let mut shared_in_s2 = shared.clone();
        shared_in_s2.sequence_number = 1;
        shared_in_s2.min_sequence_number = 1;
        let s2_list =
            write_list(&file_io, &loc, S2, Some(S1), 2, vec![shared_in_s2, s2_only]).await;

        let table = build_table(
            &tmp,
            file_io,
            vec![
                snapshot(S1, None, 1, 1000, s1_list.clone()),
                snapshot(S2, Some(S1), 2, 2000, s2_list),
            ],
            S2,
            HashMap::new(),
            vec![],
            vec![],
        );

        let files = unreferenced_files(&table, &HashSet::from([S1]))
            .await
            .unwrap();
        // S1's manifest list is orphaned; the carried-forward manifest is kept (still in S2), while
        // S1's private manifest is not.
        assert_eq!(files.manifest_lists, HashSet::from([s1_list]));
        assert!(files.manifests.contains(&s1_only.manifest_path));
        assert!(!files.manifests.contains(&shared.manifest_path));
        assert_eq!(files.manifests.len(), 1);
        // The shared data file is kept; only S1's private data file is unreferenced.
        assert_eq!(files.data_files, HashSet::from(["/s1.parquet".to_string()]));
    }

    #[tokio::test]
    async fn expires_multiple_snapshots() {
        let tmp = TempDir::new().unwrap();
        let file_io = FileIO::new_with_fs();
        let loc = tmp.path().join("table").to_str().unwrap().to_string();

        let l1 = write_manifest_list(&file_io, &loc, S1, None, 1, &["/f1.parquet"], &[]).await;
        let l2 = write_manifest_list(&file_io, &loc, S2, Some(S1), 2, &["/f2.parquet"], &[]).await;
        let l3 = write_manifest_list(&file_io, &loc, S3, Some(S2), 3, &["/f3.parquet"], &[]).await;

        let table = build_table(
            &tmp,
            file_io,
            vec![
                snapshot(S1, None, 1, 1000, l1.clone()),
                snapshot(S2, Some(S1), 2, 2000, l2.clone()),
                snapshot(S3, Some(S2), 3, 3000, l3),
            ],
            S3,
            HashMap::new(),
            vec![],
            vec![],
        );

        let files = unreferenced_files(&table, &HashSet::from([S1, S2]))
            .await
            .unwrap();
        assert_eq!(files.manifest_lists, HashSet::from([l1, l2]));
        assert_eq!(
            files.data_files,
            HashSet::from(["/f1.parquet".to_string(), "/f2.parquet".to_string()])
        );
    }

    #[tokio::test]
    async fn empty_expired_set_returns_nothing() {
        let tmp = TempDir::new().unwrap();
        let file_io = FileIO::new_with_fs();
        let loc = tmp.path().join("table").to_str().unwrap().to_string();

        let s1_list = write_manifest_list(&file_io, &loc, S1, None, 1, &["/s1.parquet"], &[]).await;
        let s2_list =
            write_manifest_list(&file_io, &loc, S2, Some(S1), 2, &["/s2.parquet"], &[]).await;

        let table = build_table(
            &tmp,
            file_io,
            vec![
                snapshot(S1, None, 1, 1000, s1_list),
                snapshot(S2, Some(S1), 2, 2000, s2_list),
            ],
            S2,
            HashMap::new(),
            vec![],
            vec![],
        );

        let files = unreferenced_files(&table, &HashSet::new()).await.unwrap();
        assert!(files.is_empty());
    }

    #[tokio::test]
    async fn retained_snapshot_manifest_list_load_error_fails() {
        let tmp = TempDir::new().unwrap();
        let file_io = FileIO::new_with_fs();
        let loc = tmp.path().join("table").to_str().unwrap().to_string();

        // S2 (retained) points at a manifest list that was never written.
        let s1_list = write_manifest_list(&file_io, &loc, S1, None, 1, &["/s1.parquet"], &[]).await;
        let missing = format!("{loc}/metadata/snap-missing.avro");

        let table = build_table(
            &tmp,
            file_io,
            vec![
                snapshot(S1, None, 1, 1000, s1_list),
                snapshot(S2, Some(S1), 2, 2000, missing),
            ],
            S2,
            HashMap::new(),
            vec![],
            vec![],
        );

        assert!(
            unreferenced_files(&table, &HashSet::from([S1]))
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn retained_snapshot_manifest_read_error_fails() {
        let tmp = TempDir::new().unwrap();
        let file_io = FileIO::new_with_fs();
        let loc = tmp.path().join("table").to_str().unwrap().to_string();

        // S2 (retained): its manifest list loads, but the manifest it references is removed from
        // storage, so reading manifest contents under gc.enabled fails.
        let s2_manifest = write_manifest(&file_io, &loc, S2, 2, DataContentType::Data, &[
            "/s2.parquet",
        ])
        .await;
        let s2_list = write_list(&file_io, &loc, S2, Some(S1), 2, vec![s2_manifest.clone()]).await;
        let s1_list = write_manifest_list(&file_io, &loc, S1, None, 1, &["/s1.parquet"], &[]).await;
        std::fs::remove_file(&s2_manifest.manifest_path).unwrap();

        let table = build_table(
            &tmp,
            file_io,
            vec![
                snapshot(S1, None, 1, 1000, s1_list),
                snapshot(S2, Some(S1), 2, 2000, s2_list),
            ],
            S2,
            HashMap::new(),
            vec![],
            vec![],
        );

        assert!(
            unreferenced_files(&table, &HashSet::from([S1]))
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn expired_snapshot_load_error_fails() {
        let tmp = TempDir::new().unwrap();
        let file_io = FileIO::new_with_fs();
        let loc = tmp.path().join("table").to_str().unwrap().to_string();

        // S1 (expired) points at a manifest list that was never written; S2 is intact.
        let missing = format!("{loc}/metadata/snap-missing.avro");
        let s2_list =
            write_manifest_list(&file_io, &loc, S2, Some(S1), 2, &["/s2.parquet"], &[]).await;

        let table = build_table(
            &tmp,
            file_io,
            vec![
                snapshot(S1, None, 1, 1000, missing),
                snapshot(S2, Some(S1), 2, 2000, s2_list),
            ],
            S2,
            HashMap::new(),
            vec![],
            vec![],
        );

        // Strict: an unreadable expired snapshot aborts the call (matching Java), not skipped.
        assert!(
            unreferenced_files(&table, &HashSet::from([S1]))
                .await
                .is_err()
        );
    }
}
