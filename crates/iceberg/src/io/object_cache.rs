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

use std::collections::HashMap;
use std::mem::size_of;
use std::sync::Arc;

use crate::io::FileIO;
use crate::spec::{
    DataFile, FieldSummary, FormatVersion, Manifest, ManifestEntry, ManifestFile, ManifestList,
    SchemaId, SnapshotRef, TableMetadataRef,
};
use crate::{Error, ErrorKind, Result};

const DEFAULT_CACHE_SIZE_BYTES: u64 = 32 * 1024 * 1024; // 32MB

/// Hashbrown group width used for control byte padding in the bucket array.
/// SSE2 uses 16; this is the common default on x86_64.
const HASHMAP_GROUP_WIDTH: usize = 16;

/// Overhead for an `Arc` heap allocation: strong count + weak count (2 × usize).
const ARC_INNER_OVERHEAD: usize = size_of::<usize>() * 2;

#[derive(Clone, Debug)]
pub(crate) enum CachedItem {
    ManifestList(Arc<ManifestList>),
    Manifest(Arc<Manifest>),
}

#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub(crate) enum CachedObjectKey {
    ManifestList((String, FormatVersion, SchemaId)),
    Manifest(String),
}

// ---------------------------------------------------------------------------
// Heap size estimation helpers
// ---------------------------------------------------------------------------
//
// These functions estimate the actual heap memory consumed by cached objects.
// (as opposed to `size_of_val()`, which only returns the shallow struct size
// (~100–200 bytes) regardless of heap allocations).
//
// See: https://github.com/apache/iceberg-rust/issues/1720

/// Estimates the heap allocation of a [`HashMap<K, V>`].
///
/// Hashbrown (backing `std::collections::HashMap`) allocates:
///   `[ctrl bytes: capacity + GROUP_WIDTH] [slots: capacity × size_of::<(K, V)>()]`
fn estimated_hashmap_heap<K, V>(map: &HashMap<K, V>) -> usize {
    let cap = map.capacity();
    if cap == 0 {
        return 0;
    }
    let ctrl_bytes = cap + HASHMAP_GROUP_WIDTH;
    let slot_bytes = cap * size_of::<(K, V)>();
    ctrl_bytes + slot_bytes
}

/// Estimates total memory for a cached [`ManifestList`], including the struct
/// itself and all heap allocations within its [`ManifestFile`] entries.
fn estimated_manifest_list_size(list: &ManifestList) -> usize {
    let entries = list.entries();
    let mut size = size_of::<ManifestList>();

    // Vec<ManifestFile> heap: one ManifestFile inline per entry.
    // We use len() as an approximation for capacity (we only have a slice).
    size += std::mem::size_of_val(entries);

    // Per-entry string and vec heap allocations
    for entry in entries {
        size += estimated_manifest_file_heap(entry);
    }

    size
}

/// Estimates heap allocations within a single [`ManifestFile`] beyond its
/// inline struct size (already counted by the parent Vec allocation).
fn estimated_manifest_file_heap(file: &ManifestFile) -> usize {
    let mut size = 0;

    // String: manifest_path
    size += file.manifest_path.capacity();

    // Option<Vec<FieldSummary>>: partitions
    if let Some(ref partitions) = file.partitions {
        size += partitions.len() * size_of::<FieldSummary>();
        for summary in partitions {
            if let Some(ref lb) = summary.lower_bound {
                size += lb.len();
            }
            if let Some(ref ub) = summary.upper_bound {
                size += ub.len();
            }
        }
    }

    // Option<Vec<u8>>: key_metadata
    if let Some(ref metadata) = file.key_metadata {
        size += metadata.capacity();
    }

    size
}

/// Estimates total memory for a cached [`Manifest`], including the struct,
/// `Arc` overhead per entry, and all heap allocations within each [`DataFile`].
///
/// Note: Skips `Arc<Schema>` in [`ManifestMetadata`] because schemas are
/// shared across manifests (also held by `TableMetadata`). Schema size is
/// constant per table and does not scale with file count.
fn estimated_manifest_size(manifest: &Manifest) -> usize {
    let entries = manifest.entries();
    let mut size = size_of::<Manifest>();

    // Vec<Arc<ManifestEntry>>: one Arc pointer per entry
    size += std::mem::size_of_val(entries);

    for entry in entries {
        // Each Arc points to a heap allocation: ArcInner overhead + ManifestEntry
        size += ARC_INNER_OVERHEAD + size_of::<ManifestEntry>();
        // Heap allocations within the DataFile
        size += estimated_data_file_heap(&entry.data_file);
    }

    size
}

/// Estimates heap allocations within a single [`DataFile`].
///
/// This is where most of the memory lives: six `HashMap`s of per-column
/// statistics that scale with the number of columns in the table schema.
fn estimated_data_file_heap(df: &DataFile) -> usize {
    let mut size = 0;

    // String: file_path
    size += df.file_path.capacity();

    // Struct (partition values): Vec<Option<Literal>> heap
    size += std::mem::size_of_val(df.partition.fields());

    // 4× HashMap<i32, u64>
    size += estimated_hashmap_heap(&df.column_sizes);
    size += estimated_hashmap_heap(&df.value_counts);
    size += estimated_hashmap_heap(&df.null_value_counts);
    size += estimated_hashmap_heap(&df.nan_value_counts);

    // 2× HashMap<i32, Datum>: uses flat per-Datum estimate (does not traverse
    // into String/Binary Datum variants for heap beyond the inline enum size).
    size += estimated_hashmap_heap(&df.lower_bounds);
    size += estimated_hashmap_heap(&df.upper_bounds);

    // Option<Vec<u8>>: key_metadata
    if let Some(ref km) = df.key_metadata {
        size += km.capacity();
    }

    // Option<Vec<i64>>: split_offsets
    if let Some(ref so) = df.split_offsets {
        size += so.capacity() * size_of::<i64>();
    }

    // Option<Vec<i32>>: equality_ids
    if let Some(ref ei) = df.equality_ids {
        size += ei.capacity() * size_of::<i32>();
    }

    // Option<String>: referenced_data_file
    if let Some(ref rdf) = df.referenced_data_file {
        size += rdf.capacity();
    }

    size
}

/// Caches metadata objects deserialized from immutable files
#[derive(Clone, Debug)]
pub struct ObjectCache {
    cache: moka::future::Cache<CachedObjectKey, CachedItem>,
    file_io: FileIO,
    cache_disabled: bool,
}

impl ObjectCache {
    /// Creates a new [`ObjectCache`]
    /// with the default cache size
    pub(crate) fn new(file_io: FileIO) -> Self {
        Self::new_with_capacity(file_io, DEFAULT_CACHE_SIZE_BYTES)
    }

    /// Creates a new [`ObjectCache`]
    /// with a specific cache size
    pub(crate) fn new_with_capacity(file_io: FileIO, cache_size_bytes: u64) -> Self {
        if cache_size_bytes == 0 {
            Self::with_disabled_cache(file_io)
        } else {
            Self {
                cache: moka::future::Cache::builder()
                    .weigher(|_, val: &CachedItem| {
                        let size = match val {
                            CachedItem::ManifestList(item) => estimated_manifest_list_size(item),
                            CachedItem::Manifest(item) => estimated_manifest_size(item),
                        };
                        // moka weigher returns u32; clamp to u32::MAX (~4 GiB) for safety
                        size.min(u32::MAX as usize) as u32
                    })
                    .max_capacity(cache_size_bytes)
                    .build(),
                file_io,
                cache_disabled: false,
            }
        }
    }

    /// Creates a new [`ObjectCache`]
    /// with caching disabled
    pub(crate) fn with_disabled_cache(file_io: FileIO) -> Self {
        Self {
            cache: moka::future::Cache::new(0),
            file_io,
            cache_disabled: true,
        }
    }

    /// Retrieves an Arc [`Manifest`] from the cache
    /// or retrieves one from FileIO and parses it if not present
    pub(crate) async fn get_manifest(&self, manifest_file: &ManifestFile) -> Result<Arc<Manifest>> {
        if self.cache_disabled {
            return manifest_file
                .load_manifest(&self.file_io)
                .await
                .map(Arc::new);
        }

        let key = CachedObjectKey::Manifest(manifest_file.manifest_path.clone());

        let cache_entry = self
            .cache
            .entry_by_ref(&key)
            .or_try_insert_with(self.fetch_and_parse_manifest(manifest_file))
            .await
            .map_err(|err| {
                Error::new(
                    ErrorKind::Unexpected,
                    format!("Failed to load manifest {}", manifest_file.manifest_path),
                )
                .with_source(err)
            })?
            .into_value();

        match cache_entry {
            CachedItem::Manifest(arc_manifest) => Ok(arc_manifest),
            _ => Err(Error::new(
                ErrorKind::Unexpected,
                format!("cached object for key '{key:?}' is not a Manifest"),
            )),
        }
    }

    /// Retrieves an Arc [`ManifestList`] from the cache
    /// or retrieves one from FileIO and parses it if not present
    pub async fn get_manifest_list(
        &self,
        snapshot: &SnapshotRef,
        table_metadata: &TableMetadataRef,
    ) -> Result<Arc<ManifestList>> {
        if self.cache_disabled {
            return snapshot
                .load_manifest_list(&self.file_io, table_metadata)
                .await
                .map(Arc::new);
        }

        let key = CachedObjectKey::ManifestList((
            snapshot.manifest_list().to_string(),
            table_metadata.format_version,
            snapshot
                .schema_id()
                .unwrap_or_else(|| table_metadata.current_schema_id()),
        ));
        let cache_entry = self
            .cache
            .entry_by_ref(&key)
            .or_try_insert_with(self.fetch_and_parse_manifest_list(snapshot, table_metadata))
            .await
            .map_err(|err| {
                Arc::try_unwrap(err).unwrap_or_else(|err| {
                    Error::new(
                        ErrorKind::Unexpected,
                        "Failed to load manifest list in cache",
                    )
                    .with_source(err)
                })
            })?
            .into_value();

        match cache_entry {
            CachedItem::ManifestList(arc_manifest_list) => Ok(arc_manifest_list),
            _ => Err(Error::new(
                ErrorKind::Unexpected,
                format!("cached object for path '{key:?}' is not a manifest list"),
            )),
        }
    }

    async fn fetch_and_parse_manifest(&self, manifest_file: &ManifestFile) -> Result<CachedItem> {
        let manifest = manifest_file.load_manifest(&self.file_io).await?;

        Ok(CachedItem::Manifest(Arc::new(manifest)))
    }

    async fn fetch_and_parse_manifest_list(
        &self,
        snapshot: &SnapshotRef,
        table_metadata: &TableMetadataRef,
    ) -> Result<CachedItem> {
        let manifest_list = snapshot
            .load_manifest_list(&self.file_io, table_metadata)
            .await?;

        Ok(CachedItem::ManifestList(Arc::new(manifest_list)))
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use minijinja::value::Value;
    use minijinja::{AutoEscape, Environment, context};
    use tempfile::TempDir;
    use uuid::Uuid;

    use super::*;
    use crate::TableIdent;
    use crate::io::{FileIO, OutputFile};
    use crate::spec::{
        DataContentType, DataFileBuilder, DataFileFormat, Literal, ManifestEntry,
        ManifestListWriter, ManifestStatus, ManifestWriterBuilder, Struct, TableMetadata,
    };
    use crate::table::Table;

    fn render_template(template: &str, ctx: Value) -> String {
        let mut env = Environment::new();
        env.set_auto_escape_callback(|_| AutoEscape::None);
        env.render_str(template, ctx).unwrap()
    }

    struct TableTestFixture {
        table_location: String,
        table: Table,
    }

    impl TableTestFixture {
        fn new() -> Self {
            let tmp_dir = TempDir::new().unwrap();
            let table_location = tmp_dir.path().join("table1");
            let manifest_list1_location = table_location.join("metadata/manifests_list_1.avro");
            let manifest_list2_location = table_location.join("metadata/manifests_list_2.avro");
            let table_metadata1_location = table_location.join("metadata/v1.json");

            let file_io = FileIO::from_path(table_location.as_os_str().to_str().unwrap())
                .unwrap()
                .build()
                .unwrap();

            let table_metadata = {
                let template_json_str = fs::read_to_string(format!(
                    "{}/testdata/example_table_metadata_v2.json",
                    env!("CARGO_MANIFEST_DIR")
                ))
                .unwrap();
                let metadata_json = render_template(&template_json_str, context! {
                    table_location => &table_location,
                    manifest_list_1_location => &manifest_list1_location,
                    manifest_list_2_location => &manifest_list2_location,
                    table_metadata_1_location => &table_metadata1_location,
                });
                serde_json::from_str::<TableMetadata>(&metadata_json).unwrap()
            };

            let table = Table::builder()
                .metadata(table_metadata)
                .identifier(TableIdent::from_strs(["db", "table1"]).unwrap())
                .file_io(file_io.clone())
                .metadata_location(table_metadata1_location.as_os_str().to_str().unwrap())
                .build()
                .unwrap();

            Self {
                table_location: table_location.to_str().unwrap().to_string(),
                table,
            }
        }

        fn next_manifest_file(&self) -> OutputFile {
            self.table
                .file_io()
                .new_output(format!(
                    "{}/metadata/manifest_{}.avro",
                    self.table_location,
                    Uuid::new_v4()
                ))
                .unwrap()
        }

        async fn setup_manifest_files(&mut self) {
            let current_snapshot = self.table.metadata().current_snapshot().unwrap();
            let current_schema = current_snapshot.schema(self.table.metadata()).unwrap();
            let current_partition_spec = self.table.metadata().default_partition_spec();

            // Write data files
            let mut writer = ManifestWriterBuilder::new(
                self.next_manifest_file(),
                Some(current_snapshot.snapshot_id()),
                None,
                current_schema.clone(),
                current_partition_spec.as_ref().clone(),
            )
            .build_v2_data();
            writer
                .add_entry(
                    ManifestEntry::builder()
                        .status(ManifestStatus::Added)
                        .data_file(
                            DataFileBuilder::default()
                                .partition_spec_id(0)
                                .content(DataContentType::Data)
                                .file_path(format!("{}/1.parquet", &self.table_location))
                                .file_format(DataFileFormat::Parquet)
                                .file_size_in_bytes(100)
                                .record_count(1)
                                .partition(Struct::from_iter([Some(Literal::long(100))]))
                                .build()
                                .unwrap(),
                        )
                        .build(),
                )
                .unwrap();
            let data_file_manifest = writer.write_manifest_file().await.unwrap();

            // Write to manifest list
            let mut manifest_list_write = ManifestListWriter::v2(
                self.table
                    .file_io()
                    .new_output(current_snapshot.manifest_list())
                    .unwrap(),
                current_snapshot.snapshot_id(),
                current_snapshot.parent_snapshot_id(),
                current_snapshot.sequence_number(),
            );
            manifest_list_write
                .add_manifests(vec![data_file_manifest].into_iter())
                .unwrap();
            manifest_list_write.close().await.unwrap();
        }
    }

    #[tokio::test]
    async fn test_get_manifest_list_and_manifest_from_disabled_cache() {
        let mut fixture = TableTestFixture::new();
        fixture.setup_manifest_files().await;

        let object_cache = ObjectCache::with_disabled_cache(fixture.table.file_io().clone());

        let result_manifest_list = object_cache
            .get_manifest_list(
                fixture.table.metadata().current_snapshot().unwrap(),
                &fixture.table.metadata_ref(),
            )
            .await
            .unwrap();

        assert_eq!(result_manifest_list.entries().len(), 1);

        let manifest_file = result_manifest_list.entries().first().unwrap();
        let result_manifest = object_cache.get_manifest(manifest_file).await.unwrap();

        assert_eq!(
            result_manifest
                .entries()
                .first()
                .unwrap()
                .file_path()
                .split("/")
                .last()
                .unwrap(),
            "1.parquet"
        );
    }

    #[tokio::test]
    async fn test_get_manifest_list_and_manifest_from_default_cache() {
        let mut fixture = TableTestFixture::new();
        fixture.setup_manifest_files().await;

        let object_cache = ObjectCache::new(fixture.table.file_io().clone());

        // not in cache
        let result_manifest_list = object_cache
            .get_manifest_list(
                fixture.table.metadata().current_snapshot().unwrap(),
                &fixture.table.metadata_ref(),
            )
            .await
            .unwrap();

        assert_eq!(result_manifest_list.entries().len(), 1);

        // retrieve cached version
        let result_manifest_list = object_cache
            .get_manifest_list(
                fixture.table.metadata().current_snapshot().unwrap(),
                &fixture.table.metadata_ref(),
            )
            .await
            .unwrap();

        assert_eq!(result_manifest_list.entries().len(), 1);

        let manifest_file = result_manifest_list.entries().first().unwrap();

        // not in cache
        let result_manifest = object_cache.get_manifest(manifest_file).await.unwrap();

        assert_eq!(
            result_manifest
                .entries()
                .first()
                .unwrap()
                .file_path()
                .split("/")
                .last()
                .unwrap(),
            "1.parquet"
        );

        // retrieve cached version
        let result_manifest = object_cache.get_manifest(manifest_file).await.unwrap();

        assert_eq!(
            result_manifest
                .entries()
                .first()
                .unwrap()
                .file_path()
                .split("/")
                .last()
                .unwrap(),
            "1.parquet"
        );
    }

    /// Regression test: ObjectCache::get_manifest_list must not panic when
    /// the snapshot has no schema_id (Iceberg v1 format tables).
    #[tokio::test]
    async fn test_get_manifest_list_v1_snapshot_without_schema_id() {
        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().join("table1");
        let manifest_list_location = table_location.join("metadata/manifests_list_1.avro");
        let table_metadata_location = table_location.join("metadata/v1.json");

        let file_io = FileIO::from_path(table_location.as_os_str().to_str().unwrap())
            .unwrap()
            .build()
            .unwrap();

        let template_json_str = fs::read_to_string(format!(
            "{}/testdata/example_table_metadata_v1.json",
            env!("CARGO_MANIFEST_DIR")
        ))
        .unwrap();
        let metadata_json = render_template(&template_json_str, context! {
            table_location => &table_location,
            manifest_list_location => &manifest_list_location,
            table_metadata_location => &table_metadata_location,
        });
        let table_metadata: TableMetadata = serde_json::from_str(&metadata_json).unwrap();

        let table = Table::builder()
            .metadata(table_metadata)
            .identifier(TableIdent::from_strs(["db", "table1"]).unwrap())
            .file_io(file_io.clone())
            .metadata_location(table_metadata_location.as_os_str().to_str().unwrap())
            .build()
            .unwrap();

        let current_snapshot = table.metadata().current_snapshot().unwrap();

        // Verify the snapshot has no schema_id (the condition that caused the panic)
        assert!(current_snapshot.schema_id().is_none());

        let current_schema = current_snapshot.schema(table.metadata()).unwrap();
        let current_partition_spec = table.metadata().default_partition_spec();

        // Write a manifest file
        let manifest_output = table
            .file_io()
            .new_output(format!(
                "{}/metadata/manifest_{}.avro",
                table_location.to_str().unwrap(),
                Uuid::new_v4()
            ))
            .unwrap();

        let mut writer = ManifestWriterBuilder::new(
            manifest_output,
            Some(current_snapshot.snapshot_id()),
            None,
            current_schema.clone(),
            current_partition_spec.as_ref().clone(),
        )
        .build_v1();
        writer
            .add_entry(
                ManifestEntry::builder()
                    .status(ManifestStatus::Added)
                    .data_file(
                        DataFileBuilder::default()
                            .partition_spec_id(0)
                            .content(DataContentType::Data)
                            .file_path(format!("{}/1.parquet", table_location.to_str().unwrap()))
                            .file_format(DataFileFormat::Parquet)
                            .file_size_in_bytes(100)
                            .record_count(1)
                            .partition(Struct::from_iter([Some(Literal::long(100))]))
                            .build()
                            .unwrap(),
                    )
                    .build(),
            )
            .unwrap();
        let data_file_manifest = writer.write_manifest_file().await.unwrap();

        // Write manifest list
        let mut manifest_list_write = ManifestListWriter::v1(
            table
                .file_io()
                .new_output(current_snapshot.manifest_list())
                .unwrap(),
            current_snapshot.snapshot_id(),
            current_snapshot.parent_snapshot_id(),
        );
        manifest_list_write
            .add_manifests(vec![data_file_manifest].into_iter())
            .unwrap();
        manifest_list_write.close().await.unwrap();

        // This used to panic with: called `Option::unwrap()` on a `None` value
        let object_cache = ObjectCache::new(table.file_io().clone());
        let result = object_cache
            .get_manifest_list(current_snapshot, &table.metadata_ref())
            .await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap().entries().len(), 1);
    }

    #[test]
    fn test_estimated_data_file_heap_scales_with_columns() {
        use std::collections::HashMap;

        use crate::spec::Datum;

        let make_data_file = |num_columns: i32| -> DataFile {
            let mut column_sizes = HashMap::new();
            let mut value_counts = HashMap::new();
            let mut null_value_counts = HashMap::new();
            let mut nan_value_counts = HashMap::new();
            let mut lower_bounds = HashMap::new();
            let mut upper_bounds = HashMap::new();

            for col_id in 0..num_columns {
                column_sizes.insert(col_id, 1024);
                value_counts.insert(col_id, 1000);
                null_value_counts.insert(col_id, 5);
                nan_value_counts.insert(col_id, 0);
                lower_bounds.insert(col_id, Datum::long(0));
                upper_bounds.insert(col_id, Datum::long(i64::MAX));
            }

            DataFileBuilder::default()
                .content(DataContentType::Data)
                .file_path("s3://bucket/table/data/file_0001.parquet".to_string())
                .file_format(DataFileFormat::Parquet)
                .file_size_in_bytes(100 * 1024 * 1024)
                .record_count(1_000_000)
                .partition(Struct::empty())
                .partition_spec_id(0)
                .column_sizes(column_sizes)
                .value_counts(value_counts)
                .null_value_counts(null_value_counts)
                .nan_value_counts(nan_value_counts)
                .lower_bounds(lower_bounds)
                .upper_bounds(upper_bounds)
                .build()
                .unwrap()
        };

        let df_20_cols = make_data_file(20);
        let df_50_cols = make_data_file(50);

        let heap_20 = estimated_data_file_heap(&df_20_cols);
        let heap_50 = estimated_data_file_heap(&df_50_cols);

        // The estimate must be far larger than the shallow struct size, which
        // is what the old broken weigher returned.
        let shallow = std::mem::size_of_val(&df_20_cols);
        assert!(
            heap_20 > shallow * 3,
            "20-col estimate ({heap_20}) should be much larger than shallow size ({shallow})"
        );

        // More columns → larger estimate (roughly proportional).
        assert!(
            heap_50 > heap_20,
            "50-col estimate ({heap_50}) should exceed 20-col estimate ({heap_20})"
        );

        // Sanity: 20 columns × 6 HashMaps × ~20 entries × ~15–50 bytes/bucket > 1500 bytes.
        assert!(
            heap_20 > 1500,
            "20-col estimate ({heap_20}) should be at least 1500 bytes"
        );
    }

    #[test]
    fn test_estimated_manifest_file_heap_accounts_for_string_and_partitions() {
        use serde_bytes::ByteBuf;

        use crate::spec::ManifestContentType;

        let file = ManifestFile {
            manifest_path:
                "s3://my-very-long-bucket-name/warehouse/db/table/metadata/manifest_abc123.avro"
                    .to_string(),
            manifest_length: 4096,
            partition_spec_id: 0,
            content: ManifestContentType::Data,
            sequence_number: 1,
            min_sequence_number: 1,
            added_snapshot_id: 100,
            added_files_count: Some(10),
            existing_files_count: Some(0),
            deleted_files_count: Some(0),
            added_rows_count: Some(1000),
            existing_rows_count: Some(0),
            deleted_rows_count: Some(0),
            partitions: Some(vec![
                FieldSummary {
                    contains_null: false,
                    contains_nan: None,
                    lower_bound: Some(ByteBuf::from(vec![0u8; 8])),
                    upper_bound: Some(ByteBuf::from(vec![0xFF; 8])),
                },
                FieldSummary {
                    contains_null: true,
                    contains_nan: Some(false),
                    lower_bound: Some(ByteBuf::from(vec![0u8; 16])),
                    upper_bound: Some(ByteBuf::from(vec![0xFF; 16])),
                },
            ]),
            key_metadata: None,
            first_row_id: None,
        };

        let heap = estimated_manifest_file_heap(&file);

        // Should at least include the manifest_path capacity
        assert!(
            heap >= file.manifest_path.len(),
            "heap estimate ({heap}) should include manifest_path length ({})",
            file.manifest_path.len()
        );

        // Should include partition field summary bounds: 2 summaries × (8 + 8) + (16 + 16) = 48 bytes
        assert!(
            heap > file.manifest_path.len() + 40,
            "heap estimate ({heap}) should include FieldSummary bound data"
        );
    }

    #[test]
    fn test_estimated_data_file_heap_empty_stats() {
        // A DataFile with no column stats should still account for the file_path
        let df = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("s3://bucket/file.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(1024)
            .record_count(10)
            .partition(Struct::empty())
            .partition_spec_id(0)
            .build()
            .unwrap();

        let heap = estimated_data_file_heap(&df);

        // At minimum, the file_path string capacity
        assert!(
            heap >= "s3://bucket/file.parquet".len(),
            "heap estimate ({heap}) should include file_path"
        );

        // With no column stats, the HashMaps should be empty → 0 HashMap heap
        // So the estimate should be relatively small
        assert!(
            heap < 500,
            "empty-stats DataFile heap ({heap}) should be small"
        );
    }
}
