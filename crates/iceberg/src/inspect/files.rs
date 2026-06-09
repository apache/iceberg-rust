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

//! The `files` family of metadata tables: `files`, `data_files`, `delete_files` (current snapshot) and
//! their cross-snapshot siblings `all_files`, `all_data_files`, `all_delete_files`.
//!
//! Each exposes data/delete files as rows, with the data-file column set (content, file path/format,
//! partition, record/size counts, the metrics maps, and the V3 deletion-vector fields). All six tables
//! share one schema, one read, and one row builder and differ along TWO orthogonal axes — mirroring Java
//! `BaseFilesTable`:
//!
//! - **content kind** ([`FilesTableKind`]): which manifests by content —
//!   - `All`     → all manifests          (Java `FilesTable` / `snapshot().allManifests()`)
//!   - `Data`    → DATA-content manifests  (Java `DataFilesTable` / `snapshot().dataManifests()`)
//!   - `Deletes` → DELETE-content manifests (Java `DeleteFilesTable` / `snapshot().deleteManifests()`)
//! - **snapshot scope** ([`MetadataScope`]): which snapshots' manifests —
//!   - `CurrentSnapshot` → the current snapshot only (`files` / `data_files` / `delete_files`)
//!   - `AllSnapshots`    → the deduplicated union of manifests reachable from ALL snapshots
//!     (`all_files` / `all_data_files` / `all_delete_files`, Java `AllFilesTable` /
//!     `AllDataFilesTable` / `AllDeleteFilesTable`, "valid file = readable from ANY snapshot currently
//!     tracked by the table"). Manifests are deduplicated, but the FILES inside them are NOT — Java's
//!     javadoc: "may return duplicate rows".
//!
//! Within a selected manifest only LIVE entries (Added/Existing, [`ManifestEntry::is_alive`]) are rows.
//! The manifest source (current-snapshot vs reachable-union) is the shared
//! [`crate::inspect::manifest_source`] helper, so this table and `entries` cannot drift.
//!
//! The data-file column set (schema + row builder) is the shared [`crate::inspect::data_file`] projection
//! — the `files` family flattens it to top-level columns, the `entries` table nests it under a `data_file`
//! struct. See that module (Rule of Three).
//!
//! References:
//! - <https://github.com/apache/iceberg/blob/main/core/src/main/java/org/apache/iceberg/BaseFilesTable.java>
//! - <https://github.com/apache/iceberg/blob/main/core/src/main/java/org/apache/iceberg/AllFilesTable.java>
//! - <https://github.com/apache/iceberg/blob/main/api/src/main/java/org/apache/iceberg/DataFile.java>
//!
//! The `readable_metrics` virtual STRUCT column (Java `MetricsUtil.readableMetricsStruct` — one sub-field
//! per leaf DATA column, each a struct of human-readable min/max/counts) is APPENDED last, alongside the
//! raw `data_file` columns. See [`crate::inspect::readable_metrics`].

use std::sync::Arc;

use arrow_array::RecordBatch;
use futures::{StreamExt, stream};

use super::data_file::{DataFileStructBuilder, data_file_fields};
use super::manifest_source::{MetadataScope, collect_manifest_files};
use super::readable_metrics::{
    ReadableMetricsBuilder, readable_metrics_field, readable_metrics_struct_fields,
};
use crate::Result;
use crate::arrow::schema_to_arrow_schema;
use crate::scan::ArrowRecordBatchStream;
use crate::spec::{ManifestContentType, Schema};
use crate::table::Table;

/// Which files a [`FilesTable`] exposes — the only thing that differs across the three tables.
///
/// Mirrors the Java `BaseFilesTableScan.manifests()` override on each concrete table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FilesTableKind {
    /// All manifests (Java `FilesTable`).
    All,
    /// DATA-content manifests only (Java `DataFilesTable`).
    Data,
    /// DELETE-content manifests only (Java `DeleteFilesTable`).
    Deletes,
}

impl FilesTableKind {
    /// Returns whether a manifest of the given content type should be read for this table.
    fn includes_manifest(&self, content: ManifestContentType) -> bool {
        match self {
            FilesTableKind::All => true,
            FilesTableKind::Data => content == ManifestContentType::Data,
            FilesTableKind::Deletes => content == ManifestContentType::Deletes,
        }
    }
}

/// The shared base for the `files` / `data_files` / `delete_files` metadata tables and their
/// cross-snapshot siblings `all_files` / `all_data_files` / `all_delete_files` (Java `BaseFilesTable`).
/// Each concrete table wraps this with a fixed ([`FilesTableKind`], [`MetadataScope`]) pair.
pub struct FilesTable<'a> {
    table: &'a Table,
    kind: FilesTableKind,
    scope: MetadataScope,
}

impl<'a> FilesTable<'a> {
    fn new(table: &'a Table, kind: FilesTableKind, scope: MetadataScope) -> Self {
        Self { table, kind, scope }
    }

    /// Create a `files` table (all data + delete files in the current snapshot).
    pub fn all(table: &'a Table) -> Self {
        Self::new(table, FilesTableKind::All, MetadataScope::CurrentSnapshot)
    }

    /// Create a `data_files` table (only DATA-content files in the current snapshot).
    pub fn data(table: &'a Table) -> Self {
        Self::new(table, FilesTableKind::Data, MetadataScope::CurrentSnapshot)
    }

    /// Create a `delete_files` table (only position/equality delete files in the current snapshot).
    pub fn deletes(table: &'a Table) -> Self {
        Self::new(
            table,
            FilesTableKind::Deletes,
            MetadataScope::CurrentSnapshot,
        )
    }

    /// Create an `all_files` table (all data + delete files reachable from ANY snapshot, Java
    /// `AllFilesTable`).
    pub fn all_files(table: &'a Table) -> Self {
        Self::new(table, FilesTableKind::All, MetadataScope::AllSnapshots)
    }

    /// Create an `all_data_files` table (only DATA-content files reachable from ANY snapshot, Java
    /// `AllDataFilesTable`).
    pub fn all_data_files(table: &'a Table) -> Self {
        Self::new(table, FilesTableKind::Data, MetadataScope::AllSnapshots)
    }

    /// Create an `all_delete_files` table (only delete-content files reachable from ANY snapshot, Java
    /// `AllDeleteFilesTable`).
    pub fn all_delete_files(table: &'a Table) -> Self {
        Self::new(table, FilesTableKind::Deletes, MetadataScope::AllSnapshots)
    }

    /// Returns the iceberg schema of the files metadata table.
    ///
    /// Mirrors Java `DataFile.getType(partitionType).fields()` — the field ids are the canonical
    /// `DataFile` ids from `api/DataFile.java`, built from the shared [`data_file_fields`] projection (the
    /// `files` family exposes them FLAT as the table's top-level columns). The partition column carries the
    /// table's DEFAULT partition type. The `readable_metrics` virtual STRUCT column is APPENDED last (Java
    /// `BaseFilesTable` joins it via `TypeUtil.join(schema, readableMetricsSchema(...))`) — one sub-field per
    /// leaf column of the DATA table, each a struct of human-readable per-column metrics.
    pub fn schema(&self) -> Schema {
        let partition_type = self.table.metadata().default_partition_type();
        let mut fields = data_file_fields(partition_type);

        // Append `readable_metrics`, its id counter seeded at the data_file projection's highest field id
        // (Java `metadataTableSchema.highestFieldId()`), over the DATA table's current schema.
        let data_file_schema = Schema::builder()
            .with_fields(fields.clone())
            .build()
            .expect("files metadata table data_file schema is statically valid");
        fields.push(readable_metrics_field(
            self.table.metadata().current_schema(),
            data_file_schema.highest_field_id(),
        ));

        Schema::builder()
            .with_fields(fields)
            .build()
            .expect("files metadata table schema is statically valid")
    }

    /// Scans the files metadata table.
    ///
    /// Resolves the manifest source for this table's [`MetadataScope`] (the current snapshot's
    /// manifests, or the deduplicated reachable union over ALL snapshots) via the shared
    /// [`collect_manifest_files`] helper, selects the manifests whose content passes this table's
    /// [`FilesTableKind`] filter, and emits one row per LIVE manifest entry built from its
    /// [`crate::spec::DataFile`]. An empty table (no current snapshot / no snapshots) yields a single
    /// empty batch.
    pub async fn scan(&self) -> Result<ArrowRecordBatchStream> {
        // The flattened files-table Arrow schema is the `data_file` struct's child fields (top-level)
        // FOLLOWED BY the appended `readable_metrics` struct column. The same `DataFileStructBuilder` that
        // builds the `entries` nested column builds the data_file rows (we then split its `StructArray`
        // into the top-level columns); `readable_metrics` is built alongside and appended as the last
        // column.
        let arrow_schema = Arc::new(schema_to_arrow_schema(&self.schema())?);
        let partition_type = self.table.metadata().default_partition_type().clone();
        let data_schema = self.table.metadata().current_schema().clone();

        // The leading top-level columns are the data_file projection; the trailing one is readable_metrics.
        let column_count = arrow_schema.fields().len();
        let data_file_arrow_fields: arrow_schema::Fields =
            arrow_schema.fields()[..column_count - 1].into();
        let readable_metrics_arrow_fields = readable_metrics_struct_fields(&arrow_schema)?;

        let mut data_file_builder =
            DataFileStructBuilder::new(&data_file_arrow_fields, &partition_type);
        let mut readable_metrics_builder =
            ReadableMetricsBuilder::try_new(&readable_metrics_arrow_fields, &data_schema)?;

        let manifest_files = collect_manifest_files(self.table, self.scope).await?;
        for manifest_file in &manifest_files {
            if !self.kind.includes_manifest(manifest_file.content) {
                continue;
            }
            let manifest = manifest_file.load_manifest(self.table.file_io()).await?;
            for entry in manifest.entries() {
                if entry.is_alive() {
                    data_file_builder.append(entry.data_file())?;
                    readable_metrics_builder.append(entry.data_file())?;
                }
            }
        }

        let data_file_struct = data_file_builder.finish();
        let mut columns = data_file_struct.columns().to_vec();
        columns.push(Arc::new(readable_metrics_builder.finish()));
        let batch = RecordBatch::try_new(arrow_schema, columns)?;
        Ok(stream::iter(vec![Ok(batch)]).boxed())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow_array::cast::AsArray;
    use arrow_array::{Array, StructArray};
    use futures::TryStreamExt;

    use crate::scan::tests::TableTestFixture;
    use crate::spec::{
        DataContentType, DataFileBuilder, DataFileFormat, Datum, Literal, ManifestContentType,
        ManifestEntry, ManifestListWriter, ManifestStatus, ManifestWriterBuilder, Operation,
        Snapshot, SnapshotReference, SnapshotRetention, Struct, Summary,
    };

    /// A known, fixed file size used for every file in the fixtures (the metadata table reads only the
    /// manifest metadata, so no real parquet data file is needed).
    const FILE_SIZE: u64 = 1024;

    /// Builds the current snapshot's manifest list with one DATA manifest (3 data files:
    /// Added/Deleted/Existing across partitions 100/200/300) AND one DELETE manifest (1 Added
    /// position-delete file in partition 100). Returns nothing — the fixture's current snapshot is wired.
    ///
    /// This drives only public crate APIs (`ManifestWriterBuilder`, `ManifestListWriter`, the fixture's
    /// public `table`/`table_location`), so it does not depend on the scan fixture's private helpers.
    async fn setup_data_and_delete_manifests(fixture: &TableTestFixture) {
        let metadata = fixture.table.metadata().clone();
        let current_snapshot = metadata.current_snapshot().unwrap();
        let parent_snapshot = current_snapshot.parent_snapshot(&metadata).unwrap();
        let current_schema = current_snapshot.schema(&metadata).unwrap();
        let current_partition_spec = metadata.default_partition_spec();

        let manifest_output = |fixture: &TableTestFixture| {
            fixture
                .table
                .file_io()
                .new_output(format!(
                    "{}/metadata/manifest_{}.avro",
                    fixture.table_location,
                    uuid::Uuid::new_v4()
                ))
                .unwrap()
        };

        // DATA manifest.
        let mut data_writer = ManifestWriterBuilder::new(
            manifest_output(fixture),
            Some(current_snapshot.snapshot_id()),
            None,
            current_schema.clone(),
            current_partition_spec.as_ref().clone(),
        )
        .build_v2_data();
        data_writer
            .add_entry(
                ManifestEntry::builder()
                    .status(ManifestStatus::Added)
                    .data_file(
                        DataFileBuilder::default()
                            .partition_spec_id(0)
                            .content(DataContentType::Data)
                            .file_path(format!("{}/1.parquet", &fixture.table_location))
                            .file_format(DataFileFormat::Parquet)
                            .file_size_in_bytes(FILE_SIZE)
                            .record_count(1)
                            .partition(Struct::from_iter([Some(Literal::long(100))]))
                            .column_sizes(HashMap::from([(1, 42u64)]))
                            .lower_bounds(HashMap::from([(1, Datum::long(1))]))
                            .key_metadata(None)
                            .build()
                            .unwrap(),
                    )
                    .build(),
            )
            .unwrap();
        data_writer
            .add_delete_entry(
                ManifestEntry::builder()
                    .status(ManifestStatus::Deleted)
                    .snapshot_id(parent_snapshot.snapshot_id())
                    .sequence_number(parent_snapshot.sequence_number())
                    .file_sequence_number(parent_snapshot.sequence_number())
                    .data_file(
                        DataFileBuilder::default()
                            .partition_spec_id(0)
                            .content(DataContentType::Data)
                            .file_path(format!("{}/2.parquet", &fixture.table_location))
                            .file_format(DataFileFormat::Parquet)
                            .file_size_in_bytes(FILE_SIZE)
                            .record_count(1)
                            .partition(Struct::from_iter([Some(Literal::long(200))]))
                            .build()
                            .unwrap(),
                    )
                    .build(),
            )
            .unwrap();
        data_writer
            .add_existing_entry(
                ManifestEntry::builder()
                    .status(ManifestStatus::Existing)
                    .snapshot_id(parent_snapshot.snapshot_id())
                    .sequence_number(parent_snapshot.sequence_number())
                    .file_sequence_number(parent_snapshot.sequence_number())
                    .data_file(
                        DataFileBuilder::default()
                            .partition_spec_id(0)
                            .content(DataContentType::Data)
                            .file_path(format!("{}/3.parquet", &fixture.table_location))
                            .file_format(DataFileFormat::Parquet)
                            .file_size_in_bytes(FILE_SIZE)
                            .record_count(1)
                            .partition(Struct::from_iter([Some(Literal::long(300))]))
                            .build()
                            .unwrap(),
                    )
                    .build(),
            )
            .unwrap();
        let data_manifest = data_writer.write_manifest_file().await.unwrap();

        // DELETE manifest: one Added position-delete file in partition 100.
        let mut delete_writer = ManifestWriterBuilder::new(
            manifest_output(fixture),
            Some(current_snapshot.snapshot_id()),
            None,
            current_schema.clone(),
            current_partition_spec.as_ref().clone(),
        )
        .build_v2_deletes();
        delete_writer
            .add_entry(
                ManifestEntry::builder()
                    .status(ManifestStatus::Added)
                    .data_file(
                        DataFileBuilder::default()
                            .partition_spec_id(0)
                            .content(DataContentType::PositionDeletes)
                            .file_path(format!("{}/delete-1.parquet", &fixture.table_location))
                            .file_format(DataFileFormat::Parquet)
                            .file_size_in_bytes(FILE_SIZE)
                            .record_count(1)
                            .partition(Struct::from_iter([Some(Literal::long(100))]))
                            .build()
                            .unwrap(),
                    )
                    .build(),
            )
            .unwrap();
        let delete_manifest = delete_writer.write_manifest_file().await.unwrap();

        let mut manifest_list_write = ManifestListWriter::v2(
            fixture
                .table
                .file_io()
                .new_output(current_snapshot.manifest_list())
                .unwrap(),
            current_snapshot.snapshot_id(),
            current_snapshot.parent_snapshot_id(),
            current_snapshot.sequence_number(),
        );
        manifest_list_write
            .add_manifests(vec![data_manifest, delete_manifest].into_iter())
            .unwrap();
        manifest_list_write.close().await.unwrap();

        // Sanity: the manifest list now carries exactly one DATA and one DELETE manifest.
        let manifest_list = current_snapshot
            .load_manifest_list(fixture.table.file_io(), &metadata)
            .await
            .unwrap();
        let contents: Vec<ManifestContentType> =
            manifest_list.entries().iter().map(|m| m.content).collect();
        assert!(contents.contains(&ManifestContentType::Data));
        assert!(contents.contains(&ManifestContentType::Deletes));
    }

    /// Writes a manifest list for `snapshot` referencing the given manifests, at the snapshot's own
    /// `manifest_list()` location. Used by the multi-snapshot fixture so BOTH the parent and current
    /// snapshots' manifest lists exist on disk (the current-snapshot tables only ever read the current
    /// list, so the existing fixtures leave the parent list unwritten — the `all_*` tables read both).
    async fn write_manifest_list(
        fixture: &TableTestFixture,
        snapshot: &crate::spec::Snapshot,
        manifests: Vec<crate::spec::ManifestFile>,
    ) {
        let mut writer = ManifestListWriter::v2(
            fixture
                .table
                .file_io()
                .new_output(snapshot.manifest_list())
                .unwrap(),
            snapshot.snapshot_id(),
            snapshot.parent_snapshot_id(),
            snapshot.sequence_number(),
        );
        writer.add_manifests(manifests.into_iter()).unwrap();
        writer.close().await.unwrap();
    }

    /// Builds an Added DATA manifest entry for a single data file at the given partition value.
    fn added_data_entry(table_location: &str, name: &str, partition: i64) -> ManifestEntry {
        ManifestEntry::builder()
            .status(ManifestStatus::Added)
            .data_file(
                DataFileBuilder::default()
                    .partition_spec_id(0)
                    .content(DataContentType::Data)
                    .file_path(format!("{table_location}/{name}"))
                    .file_format(DataFileFormat::Parquet)
                    .file_size_in_bytes(FILE_SIZE)
                    .record_count(1)
                    .partition(Struct::from_iter([Some(Literal::long(partition))]))
                    .build()
                    .unwrap(),
            )
            .build()
    }

    /// Builds a MULTI-SNAPSHOT fixture exercising the cross-snapshot (`all_*`) semantics.
    ///
    /// PARENT snapshot (`3051…`) manifest list:
    /// - DATA manifest `old_data` → `old-1.parquet` (Added) — a file present ONLY in the OLD snapshot.
    /// - SHARED DATA manifest `shared_data` → `shared-1.parquet` (Added) — referenced by BOTH snapshots.
    ///
    /// CURRENT snapshot (`3055…`) manifest list:
    /// - DATA manifest `cur_data` → `cur-1.parquet` (Added), `cur-del.parquet` (Deleted tombstone).
    /// - DELETE manifest `cur_delete` → `delete-1.parquet` (Added position-delete).
    /// - the SAME SHARED DATA manifest `shared_data` (same `manifest_path`).
    ///
    /// This pins: cross-snapshot inclusion (`old-1`), manifest dedup (`shared-1` read once across
    /// snapshots), content filters across snapshots, and `all_entries` tombstones (`cur-del`).
    async fn setup_multi_snapshot(fixture: &TableTestFixture) {
        let metadata = fixture.table.metadata().clone();
        let current_snapshot = metadata.current_snapshot().unwrap();
        let parent_snapshot = current_snapshot.parent_snapshot(&metadata).unwrap();
        let current_schema = current_snapshot.schema(&metadata).unwrap();
        let current_partition_spec = metadata.default_partition_spec();

        let new_writer = |file_name: &str| {
            let output = fixture
                .table
                .file_io()
                .new_output(format!("{}/metadata/{file_name}", fixture.table_location))
                .unwrap();
            ManifestWriterBuilder::new(
                output,
                Some(parent_snapshot.snapshot_id()),
                None,
                current_schema.clone(),
                current_partition_spec.as_ref().clone(),
            )
        };

        // SHARED DATA manifest (written ONCE, referenced by both snapshots' lists by the same path).
        // It was committed by the PARENT snapshot, so its sequence number is the parent's (0). Stamp it
        // explicitly: a manifest list only ASSIGNS a sequence number to a manifest it ADDED (one whose
        // `added_snapshot_id == list.snapshot_id`); a manifest carried forward into a LATER snapshot's
        // list (here the current snapshot's) must already carry an assigned seq, exactly as a real
        // commit would have stamped it. Without this the current list write fails "Found unassigned
        // sequence number".
        let mut shared_writer = new_writer("shared_data.avro").build_v2_data();
        shared_writer
            .add_entry(added_data_entry(
                &fixture.table_location,
                "shared-1.parquet",
                700,
            ))
            .unwrap();
        let mut shared_manifest = shared_writer.write_manifest_file().await.unwrap();
        shared_manifest.sequence_number = parent_snapshot.sequence_number();
        shared_manifest.min_sequence_number = parent_snapshot.sequence_number();

        // PARENT-only DATA manifest: `old-1.parquet`.
        let mut old_writer = new_writer("old_data.avro").build_v2_data();
        old_writer
            .add_entry(added_data_entry(
                &fixture.table_location,
                "old-1.parquet",
                800,
            ))
            .unwrap();
        let old_manifest = old_writer.write_manifest_file().await.unwrap();

        write_manifest_list(fixture, &parent_snapshot, vec![
            old_manifest,
            shared_manifest.clone(),
        ])
        .await;

        // CURRENT DATA manifest: `cur-1.parquet` (Added) + `cur-del.parquet` (Deleted tombstone).
        let mut cur_writer = ManifestWriterBuilder::new(
            fixture
                .table
                .file_io()
                .new_output(format!("{}/metadata/cur_data.avro", fixture.table_location))
                .unwrap(),
            Some(current_snapshot.snapshot_id()),
            None,
            current_schema.clone(),
            current_partition_spec.as_ref().clone(),
        )
        .build_v2_data();
        cur_writer
            .add_entry(added_data_entry(
                &fixture.table_location,
                "cur-1.parquet",
                100,
            ))
            .unwrap();
        cur_writer
            .add_delete_entry(
                ManifestEntry::builder()
                    .status(ManifestStatus::Deleted)
                    .snapshot_id(parent_snapshot.snapshot_id())
                    .sequence_number(parent_snapshot.sequence_number())
                    .file_sequence_number(parent_snapshot.sequence_number())
                    .data_file(
                        DataFileBuilder::default()
                            .partition_spec_id(0)
                            .content(DataContentType::Data)
                            .file_path(format!("{}/cur-del.parquet", &fixture.table_location))
                            .file_format(DataFileFormat::Parquet)
                            .file_size_in_bytes(FILE_SIZE)
                            .record_count(1)
                            .partition(Struct::from_iter([Some(Literal::long(200))]))
                            .build()
                            .unwrap(),
                    )
                    .build(),
            )
            .unwrap();
        let cur_data_manifest = cur_writer.write_manifest_file().await.unwrap();

        // CURRENT DELETE manifest: one Added position-delete file.
        let mut cur_delete_writer = ManifestWriterBuilder::new(
            fixture
                .table
                .file_io()
                .new_output(format!(
                    "{}/metadata/cur_delete.avro",
                    fixture.table_location
                ))
                .unwrap(),
            Some(current_snapshot.snapshot_id()),
            None,
            current_schema.clone(),
            current_partition_spec.as_ref().clone(),
        )
        .build_v2_deletes();
        cur_delete_writer
            .add_entry(
                ManifestEntry::builder()
                    .status(ManifestStatus::Added)
                    .data_file(
                        DataFileBuilder::default()
                            .partition_spec_id(0)
                            .content(DataContentType::PositionDeletes)
                            .file_path(format!("{}/delete-1.parquet", &fixture.table_location))
                            .file_format(DataFileFormat::Parquet)
                            .file_size_in_bytes(FILE_SIZE)
                            .record_count(1)
                            .partition(Struct::from_iter([Some(Literal::long(100))]))
                            .build()
                            .unwrap(),
                    )
                    .build(),
            )
            .unwrap();
        let cur_delete_manifest = cur_delete_writer.write_manifest_file().await.unwrap();

        write_manifest_list(fixture, current_snapshot, vec![
            cur_data_manifest,
            cur_delete_manifest,
            shared_manifest,
        ])
        .await;
    }

    /// Collects the sorted `file_path` set of a files-table scan.
    async fn scan_paths(stream: crate::scan::ArrowRecordBatchStream) -> Vec<String> {
        let batches: Vec<_> = stream.try_collect().await.unwrap();
        let mut paths = Vec::new();
        for batch in &batches {
            let column = batch
                .column_by_name("file_path")
                .unwrap()
                .as_string::<i32>();
            for index in 0..column.len() {
                paths.push(column.value(index).to_string());
            }
        }
        paths.sort();
        paths
    }

    /// Concatenates a files-table scan into a single batch.
    async fn scan_single_batch(
        stream: crate::scan::ArrowRecordBatchStream,
    ) -> arrow_array::RecordBatch {
        let batches: Vec<_> = stream.try_collect().await.unwrap();
        arrow_select::concat::concat_batches(&batches[0].schema(), &batches).unwrap()
    }

    #[tokio::test]
    async fn test_files_table_lists_live_data_and_delete_files() {
        // RISK: wrong file set — `files` must list every LIVE data + delete file (Added/Existing),
        // never the Deleted tombstone (2.parquet).
        let fixture = TableTestFixture::new();
        setup_data_and_delete_manifests(&fixture).await;

        let stream = fixture.table.inspect().files().scan().await.unwrap();
        let paths = scan_paths(stream).await;

        assert_eq!(paths, vec![
            format!("{}/1.parquet", fixture.table_location),
            format!("{}/3.parquet", fixture.table_location),
            format!("{}/delete-1.parquet", fixture.table_location),
        ]);
    }

    #[tokio::test]
    async fn test_data_files_table_excludes_delete_files() {
        // RISK: wrong content filter — `data_files` reads DATA manifests only, so the position-delete
        // file must NOT appear, and the Deleted 2.parquet stays excluded as a tombstone.
        let fixture = TableTestFixture::new();
        setup_data_and_delete_manifests(&fixture).await;

        let stream = fixture.table.inspect().data_files().scan().await.unwrap();
        let paths = scan_paths(stream).await;

        assert_eq!(paths, vec![
            format!("{}/1.parquet", fixture.table_location),
            format!("{}/3.parquet", fixture.table_location),
        ]);
    }

    #[tokio::test]
    async fn test_delete_files_table_lists_only_delete_files() {
        // RISK: wrong content filter — `delete_files` reads DELETE manifests only; exactly the one
        // position-delete file, none of the data files.
        let fixture = TableTestFixture::new();
        setup_data_and_delete_manifests(&fixture).await;

        let stream = fixture.table.inspect().delete_files().scan().await.unwrap();
        let paths = scan_paths(stream).await;

        assert_eq!(paths, vec![format!(
            "{}/delete-1.parquet",
            fixture.table_location
        )]);
    }

    #[tokio::test]
    async fn test_files_table_content_column_distinguishes_data_and_deletes() {
        // RISK: wrong `content` value — DATA files must report content 0, the position-delete file 1.
        let fixture = TableTestFixture::new();
        setup_data_and_delete_manifests(&fixture).await;

        let batch = scan_single_batch(fixture.table.inspect().files().scan().await.unwrap()).await;

        let paths = batch
            .column_by_name("file_path")
            .unwrap()
            .as_string::<i32>();
        let content = batch
            .column_by_name("content")
            .unwrap()
            .as_primitive::<arrow_array::types::Int32Type>();
        let mut content_by_suffix = HashMap::new();
        for index in 0..paths.len() {
            let suffix = paths.value(index).rsplit('/').next().unwrap().to_string();
            content_by_suffix.insert(suffix, content.value(index));
        }
        assert_eq!(content_by_suffix["1.parquet"], 0);
        assert_eq!(content_by_suffix["3.parquet"], 0);
        assert_eq!(content_by_suffix["delete-1.parquet"], 1);
    }

    #[tokio::test]
    async fn test_files_table_record_count_and_size_match_committed_metadata() {
        // RISK: wrong column mapping — record_count / file_size_in_bytes must reflect the committed
        // DataFile values (record_count == 1; file_size == FILE_SIZE).
        let fixture = TableTestFixture::new();
        setup_data_and_delete_manifests(&fixture).await;

        let batch = scan_single_batch(fixture.table.inspect().files().scan().await.unwrap()).await;

        let record_count = batch
            .column_by_name("record_count")
            .unwrap()
            .as_primitive::<arrow_array::types::Int64Type>();
        let file_size = batch
            .column_by_name("file_size_in_bytes")
            .unwrap()
            .as_primitive::<arrow_array::types::Int64Type>();
        assert_eq!(record_count.len(), 3);
        for index in 0..record_count.len() {
            assert_eq!(record_count.value(index), 1);
            assert_eq!(file_size.value(index), FILE_SIZE as i64);
        }
    }

    #[tokio::test]
    async fn test_files_table_partition_struct_and_metrics_map_present() {
        // RISK: wrong column — the partition column must be the partition struct (long `x`), and the
        // metrics maps must be populated for the Added file (column_sizes {1: 42}).
        let fixture = TableTestFixture::new();
        setup_data_and_delete_manifests(&fixture).await;

        let batch = scan_single_batch(fixture.table.inspect().files().scan().await.unwrap()).await;

        let partition = batch.column_by_name("partition").unwrap().as_struct();
        assert_eq!(partition.num_columns(), 1);
        let partition_values = partition
            .column(0)
            .as_primitive::<arrow_array::types::Int64Type>();
        let mut partitions: Vec<i64> = (0..partition_values.len())
            .map(|index| partition_values.value(index))
            .collect();
        partitions.sort();
        assert_eq!(partitions, vec![100, 100, 300]);

        let column_sizes = batch.column_by_name("column_sizes").unwrap().as_map();
        let mut found_added_metrics = false;
        for index in 0..column_sizes.len() {
            let entries = column_sizes.value(index);
            let keys = entries
                .column(0)
                .as_primitive::<arrow_array::types::Int32Type>();
            let values = entries
                .column(1)
                .as_primitive::<arrow_array::types::Int64Type>();
            if keys.len() == 1 && keys.value(0) == 1 && values.value(0) == 42 {
                found_added_metrics = true;
            }
        }
        assert!(
            found_added_metrics,
            "expected column_sizes {{1: 42}} on the Added file"
        );
    }

    #[tokio::test]
    async fn test_files_table_arrow_schema_columns_and_types() {
        // RISK: wrong column set / type — assert the Arrow schema is the DataFile column set with the
        // expected leading types (content Int32, file_path Utf8, partition Struct, the metrics Maps).
        let fixture = TableTestFixture::new();
        let schema = fixture.table.inspect().files().schema();
        let arrow = crate::arrow::schema_to_arrow_schema(&schema).unwrap();

        let names: Vec<&str> = arrow.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, vec![
            "content",
            "file_path",
            "file_format",
            "spec_id",
            "partition",
            "record_count",
            "file_size_in_bytes",
            "column_sizes",
            "value_counts",
            "null_value_counts",
            "nan_value_counts",
            "lower_bounds",
            "upper_bounds",
            "key_metadata",
            "split_offsets",
            "equality_ids",
            "sort_order_id",
            "first_row_id",
            "referenced_data_file",
            "content_offset",
            "content_size_in_bytes",
            "readable_metrics",
        ]);

        use arrow_schema::DataType;
        assert_eq!(
            arrow.field_with_name("content").unwrap().data_type(),
            &DataType::Int32
        );
        // `readable_metrics` is the appended virtual STRUCT column (one sub-field per leaf data column).
        assert!(matches!(
            arrow
                .field_with_name("readable_metrics")
                .unwrap()
                .data_type(),
            DataType::Struct(_)
        ));
        assert_eq!(
            arrow.field_with_name("file_path").unwrap().data_type(),
            &DataType::Utf8
        );
        assert_eq!(
            arrow.field_with_name("record_count").unwrap().data_type(),
            &DataType::Int64
        );
        assert!(matches!(
            arrow.field_with_name("partition").unwrap().data_type(),
            DataType::Struct(_)
        ));
        assert!(matches!(
            arrow.field_with_name("column_sizes").unwrap().data_type(),
            DataType::Map(_, _)
        ));
        assert!(matches!(
            arrow.field_with_name("lower_bounds").unwrap().data_type(),
            DataType::Map(_, _)
        ));
    }

    #[tokio::test]
    async fn test_files_table_empty_table_yields_empty_batch() {
        // RISK: panic / non-empty on an empty table — no current snapshot must yield zero rows.
        let fixture = TableTestFixture::new_empty();
        let batches: Vec<_> = fixture
            .table
            .inspect()
            .files()
            .scan()
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 0);
    }

    #[tokio::test]
    async fn test_files_table_unpartitioned_keeps_empty_partition_struct_known_divergence() {
        // RISK / KNOWN DIVERGENCE from Java: for an UNPARTITIONED table Java `BaseFilesTable.schema()`
        // DROPS the `partition` field entirely ("avoid returning an empty struct, which is not always
        // supported. instead, drop the partition field" — `TypeUtil.selectNot(schema, PARTITION_ID)`).
        // The Rust port currently KEEPS a `partition` column typed as an empty struct (`Struct([])`).
        // This is non-corrupting (the file rows + every other column are correct, the row count is
        // right) but is a schema-shape divergence that matters for eventual Java interop — tracked in
        // GAP_MATRIX/todo as a deferral, NOT silently wrong. This test PINS the current behavior so the
        // divergence cannot change unnoticed; when the Java drop-empty-partition rule is implemented,
        // this test flips to assert the `partition` column is ABSENT.
        let fixture = TableTestFixture::new_unpartitioned();
        let metadata = fixture.table.metadata().clone();
        let current_snapshot = metadata.current_snapshot().unwrap();
        let current_schema = current_snapshot.schema(&metadata).unwrap();
        let current_partition_spec = metadata.default_partition_spec();

        let output = fixture
            .table
            .file_io()
            .new_output(format!(
                "{}/metadata/manifest_unp_{}.avro",
                fixture.table_location,
                uuid::Uuid::new_v4()
            ))
            .unwrap();
        let mut data_writer = ManifestWriterBuilder::new(
            output,
            Some(current_snapshot.snapshot_id()),
            None,
            current_schema.clone(),
            current_partition_spec.as_ref().clone(),
        )
        .build_v2_data();
        data_writer
            .add_entry(
                ManifestEntry::builder()
                    .status(ManifestStatus::Added)
                    .data_file(
                        DataFileBuilder::default()
                            .partition_spec_id(0)
                            .content(DataContentType::Data)
                            .file_path(format!("{}/u1.parquet", &fixture.table_location))
                            .file_format(DataFileFormat::Parquet)
                            .file_size_in_bytes(FILE_SIZE)
                            .record_count(1)
                            .partition(Struct::empty())
                            .build()
                            .unwrap(),
                    )
                    .build(),
            )
            .unwrap();
        let data_manifest = data_writer.write_manifest_file().await.unwrap();

        let mut manifest_list_write = ManifestListWriter::v2(
            fixture
                .table
                .file_io()
                .new_output(current_snapshot.manifest_list())
                .unwrap(),
            current_snapshot.snapshot_id(),
            current_snapshot.parent_snapshot_id(),
            current_snapshot.sequence_number(),
        );
        manifest_list_write
            .add_manifests(vec![data_manifest].into_iter())
            .unwrap();
        manifest_list_write.close().await.unwrap();

        let batch = scan_single_batch(fixture.table.inspect().files().scan().await.unwrap()).await;

        // Does not panic; the single data file is listed.
        assert_eq!(batch.num_rows(), 1);
        // CURRENT (divergent) behavior: the partition column is present as an empty struct.
        let partition = batch.column_by_name("partition").unwrap().as_struct();
        assert_eq!(
            partition.num_columns(),
            0,
            "unpartitioned files table currently keeps an empty-struct partition column \
             (Java drops it) — see the GAP_MATRIX deferral"
        );
    }

    #[tokio::test]
    async fn test_all_files_includes_file_only_in_old_snapshot() {
        // RISK (the core all-vs-current behavior): a live data file that exists ONLY in an OLDER
        // snapshot's manifest must appear in `all_files`/`all_data_files` but NOT in the
        // current-snapshot `files`/`data_files`. Mutation-pin: if `AllSnapshots` read only the current
        // snapshot, `old-1.parquet` would be absent from `all_files`.
        let fixture = TableTestFixture::new();
        setup_multi_snapshot(&fixture).await;

        let current = scan_paths(fixture.table.inspect().files().scan().await.unwrap()).await;
        let all = scan_paths(fixture.table.inspect().all_files().scan().await.unwrap()).await;

        let old = format!("{}/old-1.parquet", fixture.table_location);
        assert!(
            !current.contains(&old),
            "current-snapshot `files` must NOT include the old-only file"
        );
        assert!(
            all.contains(&old),
            "`all_files` must include the file reachable only from the old snapshot"
        );

        // `all_files` is the full reachable union: current data + delete + the old + the shared file.
        assert_eq!(all, vec![
            format!("{}/cur-1.parquet", fixture.table_location),
            format!("{}/delete-1.parquet", fixture.table_location),
            format!("{}/old-1.parquet", fixture.table_location),
            format!("{}/shared-1.parquet", fixture.table_location),
        ]);
    }

    #[tokio::test]
    async fn test_all_data_files_excludes_delete_files_across_snapshots() {
        // RISK: the content filter must still hold under `AllSnapshots` — `all_data_files` reads DATA
        // manifests only, so the position-delete file never appears even though it is reachable.
        let fixture = TableTestFixture::new();
        setup_multi_snapshot(&fixture).await;

        let paths = scan_paths(
            fixture
                .table
                .inspect()
                .all_data_files()
                .scan()
                .await
                .unwrap(),
        )
        .await;

        assert_eq!(paths, vec![
            format!("{}/cur-1.parquet", fixture.table_location),
            format!("{}/old-1.parquet", fixture.table_location),
            format!("{}/shared-1.parquet", fixture.table_location),
        ]);
        assert!(
            !paths.contains(&format!("{}/delete-1.parquet", fixture.table_location)),
            "`all_data_files` must exclude delete files across all snapshots"
        );
    }

    #[tokio::test]
    async fn test_all_delete_files_excludes_data_files_across_snapshots() {
        // RISK: the content filter must still hold under `AllSnapshots` — `all_delete_files` reads
        // DELETE manifests only, so NONE of the reachable data files appear, only the delete file.
        let fixture = TableTestFixture::new();
        setup_multi_snapshot(&fixture).await;

        let paths = scan_paths(
            fixture
                .table
                .inspect()
                .all_delete_files()
                .scan()
                .await
                .unwrap(),
        )
        .await;

        assert_eq!(paths, vec![format!(
            "{}/delete-1.parquet",
            fixture.table_location
        )]);
    }

    #[tokio::test]
    async fn test_all_files_deduplicates_shared_manifest() {
        // RISK: a manifest referenced by TWO snapshots' manifest lists must be read ONCE — the shared
        // data file `shared-1.parquet` must appear exactly once in `all_files`, not twice. Mutation-pin:
        // dropping the dedup seen-set makes this a count of 2.
        let fixture = TableTestFixture::new();
        setup_multi_snapshot(&fixture).await;

        let paths = scan_paths(fixture.table.inspect().all_files().scan().await.unwrap()).await;
        let shared = format!("{}/shared-1.parquet", fixture.table_location);
        let occurrences = paths.iter().filter(|p| **p == shared).count();
        assert_eq!(
            occurrences, 1,
            "the file from the manifest shared by both snapshots must appear exactly once"
        );
    }

    #[tokio::test]
    async fn test_all_files_schema_equals_files_schema() {
        // RISK: schema drift — `all_files` must have the IDENTICAL Arrow schema (columns, field ids,
        // types) as its non-all counterpart `files`.
        let fixture = TableTestFixture::new();
        let files_schema =
            crate::arrow::schema_to_arrow_schema(&fixture.table.inspect().files().schema())
                .unwrap();
        let all_files_schema =
            crate::arrow::schema_to_arrow_schema(&fixture.table.inspect().all_files().schema())
                .unwrap();
        assert_eq!(files_schema, all_files_schema);

        // And the same for the data/delete content variants.
        assert_eq!(
            crate::arrow::schema_to_arrow_schema(&fixture.table.inspect().data_files().schema())
                .unwrap(),
            crate::arrow::schema_to_arrow_schema(
                &fixture.table.inspect().all_data_files().schema()
            )
            .unwrap()
        );
        assert_eq!(
            crate::arrow::schema_to_arrow_schema(&fixture.table.inspect().delete_files().schema())
                .unwrap(),
            crate::arrow::schema_to_arrow_schema(
                &fixture.table.inspect().all_delete_files().schema()
            )
            .unwrap()
        );
    }

    #[tokio::test]
    async fn test_all_files_empty_table_yields_empty_batch() {
        // RISK: panic / non-empty on an empty table — no snapshots must yield zero rows for `all_files`.
        let fixture = TableTestFixture::new_empty();
        let batches: Vec<_> = fixture
            .table
            .inspect()
            .all_files()
            .scan()
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 0);
    }

    /// Grafts a THIRD snapshot that is a FORK off the parent (a sibling of the current snapshot, NOT in
    /// the current snapshot's ancestry) onto the multi-snapshot fixture, and writes its manifest list (one
    /// DATA manifest holding `fork-1.parquet`) to disk. The `main` ref stays at the CURRENT snapshot, so
    /// the fork is a tracked-but-non-ancestor snapshot — exactly the shape Java's `table().snapshots()`
    /// (ALL tracked snapshots) includes but a current-snapshot-ancestry walk would miss.
    ///
    /// Returns the file_path leaf of the fork-only file (`fork-1.parquet`).
    async fn graft_forked_snapshot(fixture: &mut TableTestFixture) -> String {
        const FORK_SNAPSHOT_ID: i64 = 3060729675574597004;
        // Must exceed the metadata's `last_sequence_number` (34) so `add_snapshot` accepts it; matches the
        // sequence number the history.rs forked fixture uses.
        const FORK_SEQUENCE_NUMBER: i64 = 35;
        // After the metadata's `last_updated_ms` (1602638573590 ≈ 2020) so `add_snapshot`'s
        // monotonic-timestamp check passes.
        const FORK_TIMESTAMP_MS: i64 = 1700000000000;

        let metadata = fixture.table.metadata().clone();
        let parent_snapshot = metadata.current_snapshot().unwrap().clone();
        let parent_snapshot = parent_snapshot.parent_snapshot(&metadata).unwrap();
        let current_snapshot_id = metadata.current_snapshot().unwrap().snapshot_id();
        let current_schema = metadata
            .current_snapshot()
            .unwrap()
            .schema(&metadata)
            .unwrap();
        let current_partition_spec = metadata.default_partition_spec();

        // The fork's DATA manifest, holding a file present ONLY on the fork branch.
        let mut fork_writer = ManifestWriterBuilder::new(
            fixture
                .table
                .file_io()
                .new_output(format!(
                    "{}/metadata/fork_data.avro",
                    fixture.table_location
                ))
                .unwrap(),
            Some(FORK_SNAPSHOT_ID),
            None,
            current_schema.clone(),
            current_partition_spec.as_ref().clone(),
        )
        .build_v2_data();
        fork_writer
            .add_entry(added_data_entry(
                &fixture.table_location,
                "fork-1.parquet",
                950,
            ))
            .unwrap();
        let fork_manifest = fork_writer.write_manifest_file().await.unwrap();

        let fork_manifest_list_location = format!(
            "{}/metadata/fork_manifest_list.avro",
            fixture.table_location
        );
        let mut fork_list = ManifestListWriter::v2(
            fixture
                .table
                .file_io()
                .new_output(fork_manifest_list_location.clone())
                .unwrap(),
            FORK_SNAPSHOT_ID,
            Some(parent_snapshot.snapshot_id()),
            FORK_SEQUENCE_NUMBER,
        );
        fork_list
            .add_manifests(vec![fork_manifest].into_iter())
            .unwrap();
        fork_list.close().await.unwrap();

        // Graft the fork snapshot into the metadata but keep `main` pointing at the CURRENT snapshot, so
        // the fork is tracked yet NOT a current ancestor.
        let fork_snapshot = Snapshot::builder()
            .with_snapshot_id(FORK_SNAPSHOT_ID)
            .with_parent_snapshot_id(Some(parent_snapshot.snapshot_id()))
            .with_sequence_number(FORK_SEQUENCE_NUMBER)
            .with_timestamp_ms(FORK_TIMESTAMP_MS)
            .with_manifest_list(fork_manifest_list_location)
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: HashMap::new(),
            })
            .with_schema_id(current_schema.schema_id())
            .build();

        let forked_metadata = metadata
            .into_builder(None)
            .add_snapshot(fork_snapshot)
            .expect("add fork snapshot")
            .set_ref(
                "main",
                SnapshotReference::new(
                    current_snapshot_id,
                    SnapshotRetention::branch(None, None, None),
                ),
            )
            .expect("keep main at the current snapshot")
            .build()
            .expect("build forked metadata")
            .metadata;

        fixture.table = fixture
            .table
            .clone()
            .with_metadata(Arc::new(forked_metadata));
        "fork-1.parquet".to_string()
    }

    #[tokio::test]
    async fn test_all_files_includes_file_from_non_ancestor_snapshot() {
        // RISK (mutation-pin for "all snapshots" vs a current-ancestry walk): Java `reachableManifests`
        // unions over `table().snapshots()` = EVERY tracked snapshot, not just the current snapshot's
        // parent chain. A FORK snapshot (a sibling of the current snapshot, not in its ancestry) holds
        // `fork-1.parquet`; `all_files` MUST include it. An implementation that walked only the current
        // snapshot's ancestry (current + parent…) would silently drop it — the 2-snapshot inclusion tests
        // cannot catch that because their parent IS the only other snapshot and it is an ancestor.
        let mut fixture = TableTestFixture::new();
        setup_multi_snapshot(&fixture).await;
        let fork_file = graft_forked_snapshot(&mut fixture).await;

        let current = scan_paths(fixture.table.inspect().files().scan().await.unwrap()).await;
        let all = scan_paths(fixture.table.inspect().all_files().scan().await.unwrap()).await;

        let fork_path = format!("{}/{fork_file}", fixture.table_location);
        assert!(
            !current.contains(&fork_path),
            "current-snapshot `files` must NOT include the fork-only file (it is not in the current \
             snapshot's manifests)"
        );
        assert!(
            all.contains(&fork_path),
            "`all_files` must include a file reachable only from a tracked NON-ANCESTOR (forked) snapshot \
             — Java unions over ALL `table().snapshots()`, not just the current ancestry"
        );
    }

    /// Looks up the per-column metric sub-struct for `column` within the `readable_metrics` struct column,
    /// for the row whose `file_path` ends in `file_suffix`. Returns the `(row_index, column_struct)`.
    fn readable_metrics_column(
        batch: &arrow_array::RecordBatch,
        file_suffix: &str,
        column: &str,
    ) -> (usize, StructArray) {
        let file_path = batch
            .column_by_name("file_path")
            .unwrap()
            .as_string::<i32>();
        let row = (0..file_path.len())
            .find(|index| {
                file_path
                    .value(*index)
                    .rsplit('/')
                    .next()
                    .map(|leaf| leaf == file_suffix)
                    .unwrap_or(false)
            })
            .unwrap_or_else(|| panic!("row for {file_suffix} not found"));
        let readable_metrics = batch
            .column_by_name("readable_metrics")
            .unwrap()
            .as_struct();
        let column_struct = readable_metrics
            .column_by_name(column)
            .unwrap_or_else(|| panic!("readable_metrics has no column {column}"))
            .as_struct()
            .clone();
        (row, column_struct)
    }

    #[tokio::test]
    async fn test_files_readable_metrics_schema_present_with_one_struct_per_leaf_column() {
        // RISK: the `readable_metrics` virtual column must exist on `files` with one sub-field per LEAF
        // data column (the fixture schema has 8 primitive columns: a, bool, dbl, i32, i64, x, y, z), each
        // a 6-field metric struct, and the lower/upper bounds carry the COLUMN's type (NOT binary).
        use arrow_schema::DataType;
        let fixture = TableTestFixture::new();
        let arrow = crate::arrow::schema_to_arrow_schema(&fixture.table.inspect().files().schema())
            .unwrap();

        let readable_metrics = arrow.field_with_name("readable_metrics").unwrap();
        let DataType::Struct(columns) = readable_metrics.data_type() else {
            panic!("readable_metrics must be a struct");
        };
        let column_names: Vec<&str> = columns.iter().map(|f| f.name().as_str()).collect();
        // Sorted by name (Java sorts the per-column sub-fields by name).
        assert_eq!(column_names, vec![
            "a", "bool", "dbl", "i32", "i64", "x", "y", "z",
        ]);

        // Each per-column sub-field is a 6-field metric struct.
        let x_field = columns.iter().find(|f| f.name() == "x").unwrap();
        let DataType::Struct(x_metrics) = x_field.data_type() else {
            panic!("per-column metric must be a struct");
        };
        let metric_names: Vec<&str> = x_metrics.iter().map(|f| f.name().as_str()).collect();
        assert_eq!(metric_names, vec![
            "column_size",
            "value_count",
            "null_value_count",
            "nan_value_count",
            "lower_bound",
            "upper_bound",
        ]);

        // `x` is a LONG column → its bound sub-fields are Int64 (the COLUMN type), the counts are Int64.
        let lower = x_metrics
            .iter()
            .find(|f| f.name() == "lower_bound")
            .unwrap();
        assert_eq!(lower.data_type(), &DataType::Int64);
        // `a` is a STRING column → its bound sub-fields are Utf8 (proves the bound carries the column type,
        // not a single binary type for every column).
        let a_field = columns.iter().find(|f| f.name() == "a").unwrap();
        let DataType::Struct(a_metrics) = a_field.data_type() else {
            panic!("struct");
        };
        let a_lower = a_metrics
            .iter()
            .find(|f| f.name() == "lower_bound")
            .unwrap();
        assert_eq!(a_lower.data_type(), &DataType::Utf8);
    }

    #[tokio::test]
    async fn test_files_readable_metrics_reports_counts_and_decoded_typed_bounds() {
        // RISK (the load-bearing value test): for the Added file (1.parquet) the fixture sets
        // column_sizes {1:42} and lower_bounds {1: Datum::long(1)} on column id 1 = `x` (long). So x's
        // readable_metrics must report column_size=42 and lower_bound=1 DECODED to a typed long (not raw
        // bytes), while value_count / null_value_count / nan_value_count / upper_bound are NULL (the file
        // carries none), and a DIFFERENT column (`y`, id 2) has ALL metrics NULL.
        use arrow_array::types::{Int32Type, Int64Type};
        let fixture = TableTestFixture::new();
        setup_data_and_delete_manifests(&fixture).await;
        let batch = scan_single_batch(fixture.table.inspect().files().scan().await.unwrap()).await;

        // Column `x` (the column the fixture populated) on the Added file 1.parquet.
        let (row, x_metrics) = readable_metrics_column(&batch, "1.parquet", "x");
        let column_size = x_metrics
            .column_by_name("column_size")
            .unwrap()
            .as_primitive::<Int64Type>();
        assert!(!column_size.is_null(row), "x.column_size must be present");
        assert_eq!(column_size.value(row), 42, "x.column_size == 42");

        // lower_bound DECODED to a typed LONG value (== the integer 1), NOT raw bytes.
        let lower = x_metrics
            .column_by_name("lower_bound")
            .unwrap()
            .as_primitive::<Int64Type>();
        assert!(!lower.is_null(row), "x.lower_bound must be present");
        assert_eq!(lower.value(row), 1, "x.lower_bound decoded to the long 1");

        // The metrics the file does NOT carry are NULL.
        for absent in ["value_count", "null_value_count", "nan_value_count"] {
            let array = x_metrics
                .column_by_name(absent)
                .unwrap()
                .as_primitive::<Int64Type>();
            assert!(
                array.is_null(row),
                "x.{absent} must be NULL (file carries none)"
            );
        }
        let upper = x_metrics
            .column_by_name("upper_bound")
            .unwrap()
            .as_primitive::<Int64Type>();
        assert!(
            upper.is_null(row),
            "x.upper_bound must be NULL (file carries none)"
        );

        // A column the file carries NO metric for (`y`, id 2) → every metric NULL.
        let (row_y, y_metrics) = readable_metrics_column(&batch, "1.parquet", "y");
        let y_column_size = y_metrics
            .column_by_name("column_size")
            .unwrap()
            .as_primitive::<Int64Type>();
        assert!(y_column_size.is_null(row_y), "y.column_size must be NULL");
        let y_lower = y_metrics
            .column_by_name("lower_bound")
            .unwrap()
            .as_primitive::<Int64Type>();
        assert!(y_lower.is_null(row_y), "y.lower_bound must be NULL");

        // The integer column `i32` (id 6) carries no metric here → its (Int32-typed) bound is NULL, which
        // also pins that the bound sub-field is the column's OWN type (Int32), not Int64.
        let (row_i32, i32_metrics) = readable_metrics_column(&batch, "1.parquet", "i32");
        let i32_lower = i32_metrics
            .column_by_name("lower_bound")
            .unwrap()
            .as_primitive::<Int32Type>();
        assert!(
            i32_lower.is_null(row_i32),
            "i32.lower_bound must be NULL and Int32-typed"
        );
    }

    #[tokio::test]
    async fn test_files_raw_bound_and_count_maps_unchanged_alongside_readable_metrics() {
        // RISK (backward-compat regression guard): adding `readable_metrics` must NOT alter the existing
        // raw columns. The raw `lower_bounds`/`upper_bounds`/count-map columns must still be present as
        // map<int,*> and still carry the committed values (column_sizes {1: 42}).
        use arrow_schema::DataType;
        let fixture = TableTestFixture::new();
        setup_data_and_delete_manifests(&fixture).await;
        let batch = scan_single_batch(fixture.table.inspect().files().scan().await.unwrap()).await;

        // The raw map columns are still present and still Map-typed (unchanged from before).
        for raw in [
            "column_sizes",
            "value_counts",
            "null_value_counts",
            "nan_value_counts",
            "lower_bounds",
            "upper_bounds",
        ] {
            assert!(
                matches!(
                    batch.column_by_name(raw).unwrap().data_type(),
                    DataType::Map(_, _)
                ),
                "raw column {raw} must still be a Map"
            );
        }

        // The raw `column_sizes` map still carries the committed {1: 42} on the Added file.
        let column_sizes = batch.column_by_name("column_sizes").unwrap().as_map();
        let mut found = false;
        for index in 0..column_sizes.len() {
            let entries = column_sizes.value(index);
            let keys = entries
                .column(0)
                .as_primitive::<arrow_array::types::Int32Type>();
            let values = entries
                .column(1)
                .as_primitive::<arrow_array::types::Int64Type>();
            if keys.len() == 1 && keys.value(0) == 1 && values.value(0) == 42 {
                found = true;
            }
        }
        assert!(found, "the raw column_sizes map must still carry {{1: 42}}");
    }

    /// Writes a single DATA manifest with one Added file carrying DISTINCT lower/upper bounds on column
    /// `x` (id 1): lower=10, upper=99 — so the lower↔upper decode is observable (the swap mutation fails).
    async fn setup_distinct_bounds_manifest(fixture: &TableTestFixture) {
        let metadata = fixture.table.metadata().clone();
        let current_snapshot = metadata.current_snapshot().unwrap();
        let current_schema = current_snapshot.schema(&metadata).unwrap();
        let current_partition_spec = metadata.default_partition_spec();

        let output = fixture
            .table
            .file_io()
            .new_output(format!(
                "{}/metadata/manifest_bounds_{}.avro",
                fixture.table_location,
                uuid::Uuid::new_v4()
            ))
            .unwrap();
        let mut data_writer = ManifestWriterBuilder::new(
            output,
            Some(current_snapshot.snapshot_id()),
            None,
            current_schema.clone(),
            current_partition_spec.as_ref().clone(),
        )
        .build_v2_data();
        data_writer
            .add_entry(
                ManifestEntry::builder()
                    .status(ManifestStatus::Added)
                    .data_file(
                        DataFileBuilder::default()
                            .partition_spec_id(0)
                            .content(DataContentType::Data)
                            .file_path(format!("{}/bounds.parquet", &fixture.table_location))
                            .file_format(DataFileFormat::Parquet)
                            .file_size_in_bytes(FILE_SIZE)
                            .record_count(1)
                            .partition(Struct::from_iter([Some(Literal::long(100))]))
                            .lower_bounds(HashMap::from([(1, Datum::long(10))]))
                            .upper_bounds(HashMap::from([(1, Datum::long(99))]))
                            .build()
                            .unwrap(),
                    )
                    .build(),
            )
            .unwrap();
        let data_manifest = data_writer.write_manifest_file().await.unwrap();

        let mut manifest_list_write = ManifestListWriter::v2(
            fixture
                .table
                .file_io()
                .new_output(current_snapshot.manifest_list())
                .unwrap(),
            current_snapshot.snapshot_id(),
            current_snapshot.parent_snapshot_id(),
            current_snapshot.sequence_number(),
        );
        manifest_list_write
            .add_manifests(vec![data_manifest].into_iter())
            .unwrap();
        manifest_list_write.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_files_readable_metrics_lower_and_upper_bound_are_distinct_and_typed() {
        // RISK (mutation-pin for swapping lower↔upper, AND for emitting raw bytes): with distinct bounds
        // lower=10, upper=99 on column `x` (long), readable_metrics must report x.lower_bound==10 and
        // x.upper_bound==99 as TYPED longs. A lower↔upper swap fails this; emitting the raw `to_bytes`
        // value (8 LE bytes) instead of the decoded long would not even be Int64-typed (the cast panics or
        // the value differs).
        use arrow_array::types::Int64Type;
        let fixture = TableTestFixture::new();
        setup_distinct_bounds_manifest(&fixture).await;
        let batch = scan_single_batch(fixture.table.inspect().files().scan().await.unwrap()).await;

        let (row, x_metrics) = readable_metrics_column(&batch, "bounds.parquet", "x");
        let lower = x_metrics
            .column_by_name("lower_bound")
            .unwrap()
            .as_primitive::<Int64Type>();
        let upper = x_metrics
            .column_by_name("upper_bound")
            .unwrap()
            .as_primitive::<Int64Type>();
        assert_eq!(lower.value(row), 10, "x.lower_bound must decode to 10");
        assert_eq!(upper.value(row), 99, "x.upper_bound must decode to 99");
        assert_ne!(
            lower.value(row),
            upper.value(row),
            "lower and upper bounds must be distinct (pins the lower↔upper swap mutation)"
        );
    }
}
