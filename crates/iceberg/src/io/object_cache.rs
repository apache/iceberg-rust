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
use std::hash::Hash;
use std::sync::Arc;

use crate::encryption::EncryptionManager;
use crate::io::FileIO;
use crate::spec::{
    DataFile, Datum, FieldSummary, Literal, Manifest, ManifestEntry, ManifestEntryRef,
    ManifestFile, ManifestList, ManifestListReader, ManifestMetadata, ManifestReader, NestedField,
    NestedFieldRef, PartitionField, PrimitiveLiteral, Schema, SnapshotRef, Struct, StructType,
    TableMetadataRef, Type,
};
use crate::{Error, ErrorKind, Result};

const DEFAULT_CACHE_SIZE_BYTES: u64 = 32 * 1024 * 1024; // 32MB

#[derive(Clone, Debug)]
pub(crate) enum CachedItem {
    ManifestList(Arc<ManifestList>),
    Manifest(Arc<Manifest>),
}

impl CachedItem {
    /// Approximate bytes this item keeps resident, inline and on the heap.
    ///
    /// The estimate counts every owned allocation that scales with the number
    /// of entries. It skips per-allocation overhead and hash-map control bytes
    /// beyond the bucket slots, so it slightly undercounts a large item.
    fn weight(&self) -> u32 {
        let bytes = match self {
            CachedItem::ManifestList(item) => manifest_list_size(item),
            CachedItem::Manifest(item) => manifest_size(item),
        };
        u32::try_from(bytes).unwrap_or(u32::MAX)
    }
}

fn manifest_list_size(list: &ManifestList) -> usize {
    size_of::<ManifestList>()
        + list
            .entries()
            .iter()
            .map(|file| size_of::<ManifestFile>() + manifest_file_heap_size(file))
            .sum::<usize>()
}

fn manifest_file_heap_size(file: &ManifestFile) -> usize {
    file.manifest_path.capacity()
        + file.partitions.as_ref().map_or(0, |summaries| {
            summaries
                .iter()
                .map(|summary| size_of::<FieldSummary>() + field_summary_heap_size(summary))
                .sum()
        })
        + file.key_metadata.as_ref().map_or(0, Vec::capacity)
}

fn field_summary_heap_size(summary: &FieldSummary) -> usize {
    summary.lower_bound.as_ref().map_or(0, |b| b.capacity())
        + summary.upper_bound.as_ref().map_or(0, |b| b.capacity())
}

fn manifest_size(manifest: &Manifest) -> usize {
    size_of::<Manifest>()
        + manifest_metadata_heap_size(manifest.metadata())
        + manifest
            .entries()
            .iter()
            .map(|entry| {
                size_of::<ManifestEntryRef>()
                    + size_of::<ManifestEntry>()
                    + data_file_heap_size(&entry.data_file)
            })
            .sum::<usize>()
}

// Each parsed manifest owns its own `Schema`, deserialized from the Avro
// user metadata, so the schema is not shared with the table metadata.
fn manifest_metadata_heap_size(metadata: &ManifestMetadata) -> usize {
    schema_size(&metadata.schema)
        + metadata
            .partition_spec
            .fields()
            .iter()
            .map(|field| size_of::<PartitionField>() + field.name.capacity())
            .sum::<usize>()
}

fn schema_size(schema: &Schema) -> usize {
    size_of::<Schema>() + struct_type_heap_size(schema.as_struct())
}

fn struct_type_heap_size(struct_type: &StructType) -> usize {
    struct_type.fields().iter().map(nested_field_size).sum()
}

// `Schema` indexes every field by id and by name in several lookup maps, which
// holds a few more copies of the name alongside the field itself.
fn nested_field_size(field: &NestedFieldRef) -> usize {
    size_of::<NestedFieldRef>()
        + size_of::<NestedField>()
        + 4 * field.name.capacity()
        + field.doc.as_ref().map_or(0, String::capacity)
        + type_size(&field.field_type)
        + field.initial_default.as_ref().map_or(0, literal_heap_size)
        + field.write_default.as_ref().map_or(0, literal_heap_size)
}

fn type_size(ty: &Type) -> usize {
    size_of::<Type>()
        + match ty {
            Type::Primitive(_) | Type::Variant(_) => 0,
            Type::Struct(struct_type) => struct_type_heap_size(struct_type),
            Type::List(list) => nested_field_size(&list.element_field),
            Type::Map(map) => {
                nested_field_size(&map.key_field) + nested_field_size(&map.value_field)
            }
        }
}

fn data_file_heap_size(file: &DataFile) -> usize {
    file.file_path.capacity()
        + struct_heap_size(&file.partition)
        + hash_map_heap_size(&file.column_sizes)
        + hash_map_heap_size(&file.value_counts)
        + hash_map_heap_size(&file.null_value_counts)
        + hash_map_heap_size(&file.nan_value_counts)
        + datum_map_size(&file.lower_bounds)
        + datum_map_size(&file.upper_bounds)
        + file.key_metadata.as_ref().map_or(0, Vec::capacity)
        + file
            .split_offsets
            .as_ref()
            .map_or(0, |v| v.capacity() * size_of::<i64>())
        + file
            .equality_ids
            .as_ref()
            .map_or(0, |v| v.capacity() * size_of::<i32>())
        + file
            .referenced_data_file
            .as_ref()
            .map_or(0, String::capacity)
}

fn datum_map_size(map: &HashMap<i32, Datum>) -> usize {
    hash_map_heap_size(map)
        + map
            .values()
            .map(|datum| primitive_literal_heap_size(datum.literal()))
            .sum::<usize>()
}

// hashbrown stores one control byte per bucket alongside the `(K, V)` slots.
fn hash_map_heap_size<K: Eq + Hash, V>(map: &HashMap<K, V>) -> usize {
    map.capacity() * (size_of::<(K, V)>() + 1)
}

fn struct_heap_size(value: &Struct) -> usize {
    optional_literals_size(value.fields())
}

fn optional_literals_size(literals: &[Option<Literal>]) -> usize {
    literals
        .iter()
        .map(|literal| size_of::<Option<Literal>>() + literal.as_ref().map_or(0, literal_heap_size))
        .sum()
}

fn literal_heap_size(literal: &Literal) -> usize {
    match literal {
        Literal::Primitive(primitive) => primitive_literal_heap_size(primitive),
        Literal::Struct(value) => struct_heap_size(value),
        Literal::List(items) => optional_literals_size(items),
        // `Map` exposes no iterator, so the keys' and values' own heap is
        // not counted.
        Literal::Map(map) => {
            map.len()
                * (size_of::<(Literal, Option<Literal>)>() + size_of::<(Literal, usize)>() + 1)
        }
    }
}

fn primitive_literal_heap_size(literal: &PrimitiveLiteral) -> usize {
    match literal {
        PrimitiveLiteral::String(s) => s.capacity(),
        PrimitiveLiteral::Binary(b) => b.capacity(),
        _ => 0,
    }
}

#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub(crate) enum CachedObjectKey {
    // Keyed by location alone. A location holds one set of immutable bytes, and
    // a cache is bound to one table, so the first access parses those bytes
    // under the table's then-current format version and every later access
    // shares that entry. The version can still shift under a live cache across
    // an in-place upgrade (`Table::with_metadata`), which is why the key omits it.
    ManifestList(String),
    // Keyed by the path plus every manifest-list value that `ManifestReader`
    // inherits onto the parsed entries. The same physical manifest can be
    // referenced under different values: `first_row_id` differs across
    // snapshots and branches, and `sequence_number` differs when a transaction
    // retry reuses a manifest it wrote under an earlier attempt's manifest
    // list. A key missing any of these would serve one reference's inherited
    // values for another.
    Manifest(ManifestKey),
}

#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub(crate) struct ManifestKey {
    path: String,
    added_snapshot_id: i64,
    sequence_number: i64,
    first_row_id: Option<u64>,
}

impl ManifestKey {
    fn new(manifest_file: &ManifestFile) -> Self {
        Self {
            path: manifest_file.manifest_path.clone(),
            added_snapshot_id: manifest_file.added_snapshot_id,
            sequence_number: manifest_file.sequence_number,
            first_row_id: manifest_file.first_row_id,
        }
    }
}

/// Caches metadata objects deserialized from immutable files
#[derive(Clone, Debug)]
pub struct ObjectCache {
    cache: moka::future::Cache<CachedObjectKey, CachedItem>,
    file_io: FileIO,
    cache_disabled: bool,
    encryption_manager: Option<Arc<EncryptionManager>>,
}

impl ObjectCache {
    /// Creates a new [`ObjectCache`]
    /// with the default cache size
    pub(crate) fn new(file_io: FileIO, encryption_manager: Option<Arc<EncryptionManager>>) -> Self {
        Self::new_with_capacity(file_io, DEFAULT_CACHE_SIZE_BYTES, encryption_manager)
    }

    /// Creates a new [`ObjectCache`]
    /// with a specific cache size
    pub(crate) fn new_with_capacity(
        file_io: FileIO,
        cache_size_bytes: u64,
        encryption_manager: Option<Arc<EncryptionManager>>,
    ) -> Self {
        if cache_size_bytes == 0 {
            Self::with_disabled_cache(file_io, encryption_manager)
        } else {
            Self {
                cache: moka::future::Cache::builder()
                    .weigher(|_, val: &CachedItem| val.weight())
                    .max_capacity(cache_size_bytes)
                    .build(),
                file_io,
                cache_disabled: false,
                encryption_manager,
            }
        }
    }

    /// Creates a new [`ObjectCache`]
    /// with caching disabled
    pub(crate) fn with_disabled_cache(
        file_io: FileIO,
        encryption_manager: Option<Arc<EncryptionManager>>,
    ) -> Self {
        Self {
            cache: moka::future::Cache::new(0),
            file_io,
            cache_disabled: true,
            encryption_manager,
        }
    }

    /// Retrieves an Arc [`Manifest`] from the cache
    /// or retrieves one from FileIO and parses it if not present
    pub(crate) async fn get_manifest(&self, manifest_file: &ManifestFile) -> Result<Arc<Manifest>> {
        if self.cache_disabled {
            return ManifestReader::new(self.file_io.clone())
                .read(manifest_file)
                .await
                .map(Arc::new);
        }

        let key = CachedObjectKey::Manifest(ManifestKey::new(manifest_file));

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
    pub(crate) async fn get_manifest_list(
        &self,
        snapshot: &SnapshotRef,
        table_metadata: &TableMetadataRef,
    ) -> Result<Arc<ManifestList>> {
        if self.cache_disabled {
            return ManifestListReader::new(
                snapshot.clone(),
                self.file_io.clone(),
                table_metadata.clone(),
                self.encryption_manager.clone(),
            )
            .load()
            .await
            .map(Arc::new);
        }

        let key = CachedObjectKey::ManifestList(snapshot.manifest_list().to_string());
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
        let manifest = ManifestReader::new(self.file_io.clone())
            .read(manifest_file)
            .await?;

        Ok(CachedItem::Manifest(Arc::new(manifest)))
    }

    async fn fetch_and_parse_manifest_list(
        &self,
        snapshot: &SnapshotRef,
        table_metadata: &TableMetadataRef,
    ) -> Result<CachedItem> {
        let manifest_list = ManifestListReader::new(
            snapshot.clone(),
            self.file_io.clone(),
            table_metadata.clone(),
            self.encryption_manager.clone(),
        )
        .load()
        .await?;

        Ok(CachedItem::ManifestList(Arc::new(manifest_list)))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fs;

    use minijinja::value::Value;
    use minijinja::{AutoEscape, Environment, context};
    use tempfile::TempDir;
    use uuid::Uuid;

    use super::*;
    use crate::TableIdent;
    use crate::io::{FileIO, OutputFile};
    use crate::spec::{
        DataContentType, DataFileBuilder, DataFileFormat, FormatVersion, Literal,
        ManifestContentType, ManifestEntry, ManifestListWriter, ManifestStatus,
        ManifestWriterBuilder, Operation, Snapshot, Struct, Summary, TableMetadata,
    };
    use crate::table::Table;
    use crate::test_utils::test_runtime;

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

            let file_io = FileIO::new_with_fs();

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
                .runtime(test_runtime())
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
            let manifest_list_writer = self
                .table
                .file_io()
                .new_output(current_snapshot.manifest_list())
                .unwrap()
                .writer()
                .await
                .unwrap();
            let mut manifest_list_write = ManifestListWriter::v2(
                manifest_list_writer,
                current_snapshot.snapshot_id(),
                current_snapshot.parent_snapshot_id(),
                current_snapshot.sequence_number(),
            );
            manifest_list_write
                .add_manifests(vec![data_file_manifest].into_iter())
                .unwrap();
            manifest_list_write.close().await.unwrap();
        }

        async fn setup_v1_manifest_files(&mut self) {
            let current_snapshot = self.table.metadata().current_snapshot().unwrap();
            let current_schema = current_snapshot.schema(self.table.metadata()).unwrap();
            let current_partition_spec = self.table.metadata().default_partition_spec();

            // Write data files
            let mut writer = ManifestWriterBuilder::new(
                self.next_manifest_file(),
                Some(current_snapshot.snapshot_id()),
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
            let manifest_list_writer = self
                .table
                .file_io()
                .new_output(current_snapshot.manifest_list())
                .unwrap()
                .writer()
                .await
                .unwrap();
            let mut manifest_list_write = ManifestListWriter::v1(
                manifest_list_writer,
                current_snapshot.snapshot_id(),
                current_snapshot.parent_snapshot_id(),
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

        let object_cache = ObjectCache::with_disabled_cache(fixture.table.file_io().clone(), None);

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

        let object_cache = ObjectCache::new(fixture.table.file_io().clone(), None);

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

    fn manifest_with_entries(table: &Table, entry_count: usize, file_path: &str) -> Manifest {
        let snapshot = table.metadata().current_snapshot().unwrap();
        let metadata = ManifestMetadata::builder()
            .schema(snapshot.schema(table.metadata()).unwrap())
            .schema_id(0)
            .partition_spec(table.metadata().default_partition_spec().as_ref().clone())
            .format_version(FormatVersion::V2)
            .content(ManifestContentType::Data)
            .build();
        let entries = (0..entry_count)
            .map(|_| {
                ManifestEntry::builder()
                    .status(ManifestStatus::Added)
                    .data_file(
                        DataFileBuilder::default()
                            .partition_spec_id(0)
                            .content(DataContentType::Data)
                            .file_path(file_path.to_string())
                            .file_format(DataFileFormat::Parquet)
                            .file_size_in_bytes(100)
                            .record_count(1)
                            .partition(Struct::from_iter([Some(Literal::long(100))]))
                            .build()
                            .unwrap(),
                    )
                    .build()
            })
            .collect();
        Manifest::new(metadata, entries)
    }

    fn manifest_weight(manifest: Manifest) -> usize {
        CachedItem::Manifest(Arc::new(manifest)).weight() as usize
    }

    #[test]
    fn test_manifest_weight_counts_each_entry_and_its_heap() {
        let fixture = TableTestFixture::new();
        let file_path = format!("{}/1.parquet", fixture.table_location);

        let empty = manifest_weight(manifest_with_entries(&fixture.table, 0, &file_path));
        let one = manifest_weight(manifest_with_entries(&fixture.table, 1, &file_path));
        let many = manifest_weight(manifest_with_entries(&fixture.table, 1000, &file_path));

        let entry_weight = size_of::<ManifestEntryRef>()
            + size_of::<ManifestEntry>()
            + file_path.len()
            + size_of::<Option<Literal>>();
        assert_eq!(one - empty, entry_weight);
        assert_eq!(many - empty, 1000 * entry_weight);
    }

    #[test]
    fn test_manifest_weight_counts_file_path_bytes() {
        let fixture = TableTestFixture::new();
        let short_path = "s3://b/1.parquet";
        let long_path = format!("s3://b/{}.parquet", "x".repeat(64 * 1024));

        let short = manifest_weight(manifest_with_entries(&fixture.table, 1, short_path));
        let long = manifest_weight(manifest_with_entries(&fixture.table, 1, &long_path));

        assert_eq!(long - short, long_path.len() - short_path.len());
    }

    #[tokio::test]
    async fn test_cache_weighted_size_is_the_estimate_of_its_items() {
        let mut fixture = TableTestFixture::new();
        fixture.setup_manifest_files().await;

        let object_cache = ObjectCache::new(fixture.table.file_io().clone(), None);
        let manifest_list = object_cache
            .get_manifest_list(
                fixture.table.metadata().current_snapshot().unwrap(),
                &fixture.table.metadata_ref(),
            )
            .await
            .unwrap();
        let manifest = object_cache
            .get_manifest(manifest_list.entries().first().unwrap())
            .await
            .unwrap();
        object_cache.cache.run_pending_tasks().await;

        let expected = CachedItem::ManifestList(manifest_list).weight() as u64
            + CachedItem::Manifest(manifest).weight() as u64;
        assert_eq!(object_cache.cache.weighted_size(), expected);
    }

    #[tokio::test]
    async fn test_cache_evicts_when_items_outweigh_capacity() {
        let mut fixture = TableTestFixture::new();
        fixture.setup_manifest_files().await;

        let unbounded = ObjectCache::new(fixture.table.file_io().clone(), None);
        let manifest_list = unbounded
            .get_manifest_list(
                fixture.table.metadata().current_snapshot().unwrap(),
                &fixture.table.metadata_ref(),
            )
            .await
            .unwrap();
        let manifest_file = manifest_list.entries().first().unwrap().clone();
        let manifest = unbounded.get_manifest(&manifest_file).await.unwrap();
        let capacity = CachedItem::ManifestList(manifest_list).weight() as u64
            + CachedItem::Manifest(manifest).weight() as u64
            - 1;

        let object_cache =
            ObjectCache::new_with_capacity(fixture.table.file_io().clone(), capacity, None);
        object_cache
            .get_manifest_list(
                fixture.table.metadata().current_snapshot().unwrap(),
                &fixture.table.metadata_ref(),
            )
            .await
            .unwrap();
        object_cache.get_manifest(&manifest_file).await.unwrap();
        object_cache.cache.run_pending_tasks().await;

        assert!(object_cache.cache.weighted_size() <= capacity);
        assert_eq!(object_cache.cache.entry_count(), 1);
    }

    #[tokio::test]
    async fn test_get_manifest_list_with_no_schema_id() {
        let mut fixture = TableTestFixture::new();
        fixture.setup_manifest_files().await;

        let current_snapshot = fixture.table.metadata().current_snapshot().unwrap();

        // The spec marks `schema-id` optional in every version (v1-v3), so a
        // snapshot may omit it; fetching its manifest list must not depend on the
        // schema-id being present.
        let snapshot_without_schema_id: SnapshotRef = Snapshot::builder()
            .with_snapshot_id(current_snapshot.snapshot_id())
            .with_sequence_number(current_snapshot.sequence_number())
            .with_timestamp_ms(current_snapshot.timestamp_ms())
            .with_manifest_list(current_snapshot.manifest_list())
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: HashMap::new(),
            })
            .build()
            .into();
        assert!(snapshot_without_schema_id.schema_id().is_none());
        assert!(current_snapshot.schema_id().is_some());

        let object_cache = ObjectCache::new(fixture.table.file_io().clone(), None);

        // Cold miss: the schema-id-less snapshot populates the cache.
        let inserted = object_cache
            .get_manifest_list(&snapshot_without_schema_id, &fixture.table.metadata_ref())
            .await
            .unwrap();
        assert_eq!(inserted.entries().len(), 1);

        // Warm hit: the original snapshot carries a schema-id but points at the same
        // manifest-list location, so it returns the same cached entry.
        let cached = object_cache
            .get_manifest_list(current_snapshot, &fixture.table.metadata_ref())
            .await
            .unwrap();
        assert!(
            Arc::ptr_eq(&inserted, &cached),
            "snapshots with and without schema-id at one location must share a cache entry"
        );
    }

    #[tokio::test]
    async fn test_get_manifest_list_serves_cached_v1_parse_after_upgrade() {
        let mut fixture = TableTestFixture::new();
        fixture.setup_v1_manifest_files().await;

        let current_snapshot = fixture.table.metadata().current_snapshot().unwrap();

        // The manifest list on disk is v1. The fixture table is v2, so clone its
        // metadata as v1 to model the state before an in-place upgrade; the real
        // metadata_ref models the state after it. Both drive lookups at the same
        // location through one cache.
        let metadata_v2 = fixture.table.metadata_ref();
        let mut metadata_v1 = fixture.table.metadata().clone();
        metadata_v1.format_version = FormatVersion::V1;
        let metadata_v1: TableMetadataRef = Arc::new(metadata_v1);
        assert_ne!(
            metadata_v2.format_version, metadata_v1.format_version,
            "setup invariant: the two metadata refs must differ in format version"
        );

        let object_cache = ObjectCache::new(fixture.table.file_io().clone(), None);

        // Cold miss under v1 parses the v1 bytes as v1: content defaults to Data and
        // the sequence number to 0.
        let cached_under_v1 = object_cache
            .get_manifest_list(current_snapshot, &metadata_v1)
            .await
            .unwrap();
        let entry = cached_under_v1.entries().first().unwrap();
        assert_eq!(entry.content, ManifestContentType::Data);
        assert_eq!(entry.sequence_number, 0);

        // Warm hit under v2 at the same location returns the cached v1 parse rather
        // than reparsing the v1 bytes as v2, so the upgraded caller sees the same
        // correct content.
        let cached_under_v2 = object_cache
            .get_manifest_list(current_snapshot, &metadata_v2)
            .await
            .unwrap();
        assert!(
            Arc::ptr_eq(&cached_under_v1, &cached_under_v2),
            "lookups at one location must share a cache entry across format versions"
        );
        let entry = cached_under_v2.entries().first().unwrap();
        assert_eq!(entry.content, ManifestContentType::Data);
        assert_eq!(entry.sequence_number, 0);
    }

    #[tokio::test]
    async fn test_get_manifest_keys_on_first_row_id() {
        use crate::spec::{NestedField, PartitionSpec, PrimitiveType, Schema, SchemaRef, Type};

        let io = FileIO::new_with_memory();
        let path = "memory:///metadata/first_row_id_manifest.avro";

        let schema: SchemaRef = Schema::builder()
            .with_fields(vec![
                NestedField::optional(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            ])
            .build()
            .unwrap()
            .into();
        let partition_spec = PartitionSpec::builder(schema.clone())
            .with_spec_id(0)
            .build()
            .unwrap();

        let mut writer = ManifestWriterBuilder::new(
            io.new_output(path).unwrap(),
            Some(1),
            schema,
            partition_spec,
        )
        .build_v3_data();
        writer
            .add_entry(
                ManifestEntry::builder()
                    .status(ManifestStatus::Added)
                    .data_file(
                        DataFileBuilder::default()
                            .content(DataContentType::Data)
                            .file_path(path.to_string())
                            .file_format(DataFileFormat::Parquet)
                            .file_size_in_bytes(100)
                            .record_count(1)
                            .build()
                            .unwrap(),
                    )
                    .build(),
            )
            .unwrap();
        let manifest_file = writer.write_manifest_file().await.unwrap();

        // Two manifest-list references to the same physical manifest carrying
        // different offsets, as time travel or two branches would produce.
        let mut manifest_file_a = manifest_file.clone();
        manifest_file_a.first_row_id = Some(1000);
        let mut manifest_file_b = manifest_file;
        manifest_file_b.first_row_id = Some(2000);

        let object_cache = ObjectCache::new(io, None);

        let manifest_a = object_cache.get_manifest(&manifest_file_a).await.unwrap();
        let manifest_b = object_cache.get_manifest(&manifest_file_b).await.unwrap();

        // A cache keyed on path alone would serve manifest_a's inherited ids for
        // the second read; keying on first_row_id keeps them distinct.
        assert_eq!(
            manifest_a.entries()[0].data_file().first_row_id(),
            Some(1000)
        );
        assert_eq!(
            manifest_b.entries()[0].data_file().first_row_id(),
            Some(2000)
        );
    }

    /// A transaction retry reuses the manifest it wrote during an earlier
    /// attempt, but the retry's manifest list assigns it a newer sequence
    /// number. Entries inherit that number on read, so a cache that already
    /// holds the earlier attempt's parse must not serve it for the retry.
    #[tokio::test]
    async fn test_get_manifest_keys_on_sequence_number() {
        use crate::spec::{NestedField, PartitionSpec, PrimitiveType, Schema, SchemaRef, Type};

        let io = FileIO::new_with_memory();
        let path = "memory:///metadata/retried_manifest.avro";

        let schema: SchemaRef = Schema::builder()
            .with_fields(vec![
                NestedField::optional(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            ])
            .build()
            .unwrap()
            .into();
        let partition_spec = PartitionSpec::builder(schema.clone())
            .with_spec_id(0)
            .build()
            .unwrap();

        // A v2 manifest: entries carry no sequence number of their own and
        // `first_row_id` stays `None`, so only the sequence number distinguishes
        // the two references below.
        let mut writer = ManifestWriterBuilder::new(
            io.new_output(path).unwrap(),
            Some(1),
            schema,
            partition_spec,
        )
        .build_v2_data();
        writer
            .add_entry(
                ManifestEntry::builder()
                    .status(ManifestStatus::Added)
                    .data_file(
                        DataFileBuilder::default()
                            .content(DataContentType::Data)
                            .file_path(path.to_string())
                            .file_format(DataFileFormat::Parquet)
                            .file_size_in_bytes(100)
                            .record_count(1)
                            .build()
                            .unwrap(),
                    )
                    .build(),
            )
            .unwrap();
        let manifest_file = writer.write_manifest_file().await.unwrap();
        assert_eq!(manifest_file.first_row_id, None);

        // First attempt's manifest list assigns 35; after a rebase the retry's
        // manifest list assigns 36 to the same physical manifest.
        let mut first_attempt = manifest_file.clone();
        first_attempt.sequence_number = 35;
        let mut retry = manifest_file;
        retry.sequence_number = 36;

        let object_cache = ObjectCache::new(io, None);

        let first = object_cache.get_manifest(&first_attempt).await.unwrap();
        let second = object_cache.get_manifest(&retry).await.unwrap();

        assert_eq!(first.entries()[0].sequence_number(), Some(35));
        assert_eq!(second.entries()[0].sequence_number(), Some(36));
    }
}
