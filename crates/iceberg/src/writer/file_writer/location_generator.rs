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

//! This module contains the location generator and file name generator for generating path of data file.

use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use crate::Result;
use crate::spec::{DataFileFormat, PartitionKey, TableMetadata};

/// `LocationGenerator` used to generate the location of data file.
pub trait LocationGenerator: Clone + Send + Sync + 'static {
    /// Generate an absolute path for the given file name that includes the partition path.
    ///
    /// # Arguments
    ///
    /// * `partition_key` - The partition key of the file. If None, generate a non-partitioned path.
    /// * `file_name` - The name of the file
    ///
    /// # Returns
    ///
    /// An absolute path that includes the partition path, e.g.,
    /// "/table/data/id=1/name=alice/part-00000.parquet"
    /// or non-partitioned path:
    /// "/table/data/part-00000.parquet"
    fn generate_location(&self, partition_key: Option<&PartitionKey>, file_name: &str) -> String;
}

const WRITE_DATA_LOCATION: &str = "write.data.path";
const WRITE_FOLDER_STORAGE_LOCATION: &str = "write.folder-storage.path";
const DEFAULT_DATA_DIR: &str = "/data";

/// Deprecated object storage path property, kept as a fallback for compatibility.
const WRITE_OBJECT_STORAGE_LOCATION: &str = "write.object-storage.path";
/// Property controlling whether partition values are included in object storage paths.
const WRITE_OBJECT_STORAGE_PARTITIONED_PATHS: &str = "write.object-storage.partitioned-paths";
const WRITE_OBJECT_STORAGE_PARTITIONED_PATHS_DEFAULT: bool = true;

/// Number of trailing hash bits used to build the entropy directories.
const HASH_BINARY_STRING_BITS: usize = 20;
/// Length of each entropy directory.
const ENTROPY_DIR_LENGTH: usize = 4;
/// Number of entropy directories generated from the hash.
const ENTROPY_DIR_DEPTH: usize = 3;

#[derive(Clone, Debug)]
/// `DefaultLocationGenerator` used to generate the data dir location of data file.
/// The location is generated based on the table location and the data location in table properties.
pub struct DefaultLocationGenerator {
    data_location: String,
}

impl DefaultLocationGenerator {
    /// Create a new `DefaultLocationGenerator`.
    pub fn new(table_metadata: &TableMetadata) -> Result<Self> {
        let table_location = table_metadata.location();
        let prop = table_metadata.properties();
        let configured_data_location = prop
            .get(WRITE_DATA_LOCATION)
            .or(prop.get(WRITE_FOLDER_STORAGE_LOCATION));
        let data_location = if let Some(data_location) = configured_data_location {
            data_location.clone()
        } else {
            format!("{table_location}{DEFAULT_DATA_DIR}")
        };
        Ok(Self { data_location })
    }

    /// Create a new `DefaultLocationGenerator` with a specified data location.
    ///
    /// # Arguments
    ///
    /// * `data_location` - The data location to use for generating file locations.
    pub fn with_data_location(data_location: String) -> Self {
        Self { data_location }
    }
}

impl LocationGenerator for DefaultLocationGenerator {
    fn generate_location(&self, partition_key: Option<&PartitionKey>, file_name: &str) -> String {
        if PartitionKey::is_effectively_none(partition_key) {
            format!("{}/{}", self.data_location, file_name)
        } else {
            format!(
                "{}/{}/{}",
                self.data_location,
                partition_key.unwrap().to_path(),
                file_name
            )
        }
    }
}

/// `ObjectStorageLocationGenerator` injects hash entropy into generated file locations so that
/// files are spread across many object-store prefixes.
///
/// Object stores such as S3 shard request throughput by key prefix, so writing every file under a
/// common `.../data/` prefix creates a throughput hotspot. This generator prepends a
/// deterministic, hashed directory tree (derived from the file name) to each location, mirroring
/// Java Iceberg's `ObjectStoreLocationProvider`.
///
/// The behavior is controlled by these table properties:
/// * `write.data.path` / `write.object-storage.path` / `write.folder-storage.path` - the base data
///   location (checked in that order), defaulting to `{table_location}/data`.
/// * `write.object-storage.partitioned-paths` - whether partition values are included in the path
///   (defaults to `true`).
#[derive(Clone, Debug)]
pub struct ObjectStorageLocationGenerator {
    storage_location: String,
    /// Database/table context, only set when the storage location is outside the table location.
    context: Option<String>,
    include_partition_paths: bool,
}

impl ObjectStorageLocationGenerator {
    /// Create a new `ObjectStorageLocationGenerator` from table metadata.
    pub fn new(table_metadata: &TableMetadata) -> Result<Self> {
        let table_location = strip_trailing_slash(table_metadata.location());
        let prop = table_metadata.properties();
        let storage_location =
            strip_trailing_slash(&resolve_data_location(prop, table_location)).to_string();

        // If the storage location is within the table prefix, files are already scoped to this
        // table so there is no need to add database/table context to avoid collisions.
        let context = if storage_location.starts_with(table_location) {
            None
        } else {
            Some(path_context(table_location))
        };

        let include_partition_paths = prop
            .get(WRITE_OBJECT_STORAGE_PARTITIONED_PATHS)
            .and_then(|value| value.parse::<bool>().ok())
            .unwrap_or(WRITE_OBJECT_STORAGE_PARTITIONED_PATHS_DEFAULT);

        Ok(Self {
            storage_location,
            context,
            include_partition_paths,
        })
    }

    /// Build the final location for a fully-formed data file name (which may already include a
    /// partition path).
    fn new_data_location(&self, name: &str) -> String {
        let hash = self.compute_hash(name);
        if let Some(context) = &self.context {
            format!("{}/{}/{}/{}", self.storage_location, hash, context, name)
        } else if self.include_partition_paths {
            format!("{}/{}/{}", self.storage_location, hash, name)
        } else {
            // When partition paths are excluded, join the entropy to the file name with `-` so the
            // file still lives directly under the storage location.
            format!("{}/{}-{}", self.storage_location, hash, name)
        }
    }

    /// Compute the entropy directory tree for the given name, e.g. `0101/0110/1001/10110010`.
    fn compute_hash(&self, name: &str) -> String {
        let mut bytes = name.as_bytes();
        let hash_code = murmur3::murmur3_32(&mut bytes, 0).unwrap();

        // Force the top bit so the binary string always has 32 characters (Rust, like Java's
        // Integer.toBinaryString, drops leading zeros otherwise), then keep the trailing bits.
        let binary = format!("{:032b}", hash_code | 0x8000_0000);
        let hash = &binary[binary.len() - HASH_BINARY_STRING_BITS..];
        dirs_from_hash(hash)
    }
}

impl LocationGenerator for ObjectStorageLocationGenerator {
    fn generate_location(&self, partition_key: Option<&PartitionKey>, file_name: &str) -> String {
        let name =
            if self.include_partition_paths && !PartitionKey::is_effectively_none(partition_key) {
                format!("{}/{}", partition_key.unwrap().to_path(), file_name)
            } else {
                file_name.to_string()
            };
        self.new_data_location(&name)
    }
}

/// Strip a single trailing slash from a location, if present.
fn strip_trailing_slash(location: &str) -> &str {
    location.strip_suffix('/').unwrap_or(location)
}

/// Derive the `{parent}/{name}` context from a table location, mirroring Hadoop's
/// `Path.getParent().getName()` / `Path.getName()`.
fn path_context(table_location: &str) -> String {
    let mut segments = table_location.rsplit('/').filter(|s| !s.is_empty());
    let name = segments.next().unwrap_or("");
    match segments.next() {
        Some(parent) => format!("{parent}/{name}"),
        None => name.to_string(),
    }
}

/// Divide a binary hash string into directories for optimized listing/orphan removal.
///
/// With `ENTROPY_DIR_DEPTH = 3` and `ENTROPY_DIR_LENGTH = 4`, the 20-bit hash
/// `10011001100110011001` becomes `1001/1001/1001/10011001`.
fn dirs_from_hash(hash: &str) -> String {
    let mut result = String::new();

    let mut i = 0;
    while i < ENTROPY_DIR_DEPTH * ENTROPY_DIR_LENGTH {
        if i > 0 {
            result.push('/');
        }
        let end = (i + ENTROPY_DIR_LENGTH).min(hash.len());
        result.push_str(&hash[i..end]);
        i += ENTROPY_DIR_LENGTH;
    }

    if hash.len() > ENTROPY_DIR_DEPTH * ENTROPY_DIR_LENGTH {
        result.push('/');
        result.push_str(&hash[ENTROPY_DIR_DEPTH * ENTROPY_DIR_LENGTH..]);
    }

    result
}

/// Resolve the base data location from table properties, falling back to `{table_location}/data`.
///
/// Precedence follows Java Iceberg: `write.data.path` first, then the deprecated
/// `write.object-storage.path` and `write.folder-storage.path` properties, and finally the default
/// `{table_location}/data`.
fn resolve_data_location(
    properties: &std::collections::HashMap<String, String>,
    table_location: &str,
) -> String {
    properties
        .get(WRITE_DATA_LOCATION)
        .or_else(|| properties.get(WRITE_OBJECT_STORAGE_LOCATION))
        .or_else(|| properties.get(WRITE_FOLDER_STORAGE_LOCATION))
        .cloned()
        .unwrap_or_else(|| format!("{table_location}{DEFAULT_DATA_DIR}"))
}

/// `FileNameGeneratorTrait` used to generate file name for data file. The file name can be passed to `LocationGenerator` to generate the location of the file.
pub trait FileNameGenerator: Clone + Send + Sync + 'static {
    /// Generate a file name.
    fn generate_file_name(&self) -> String;
}

/// `DefaultFileNameGenerator` used to generate file name for data file. The file name can be
/// passed to `LocationGenerator` to generate the location of the file.
/// The file name format is "{prefix}-{file_count}[-{suffix}].{file_format}".
#[derive(Clone, Debug)]
pub struct DefaultFileNameGenerator {
    prefix: String,
    suffix: String,
    format: String,
    file_count: Arc<AtomicU64>,
}

impl DefaultFileNameGenerator {
    /// Create a new `FileNameGenerator`.
    pub fn new(prefix: String, suffix: Option<String>, format: DataFileFormat) -> Self {
        let suffix = if let Some(suffix) = suffix {
            format!("-{suffix}")
        } else {
            "".to_string()
        };

        Self {
            prefix,
            suffix,
            format: format.to_string(),
            file_count: Arc::new(AtomicU64::new(0)),
        }
    }
}

impl FileNameGenerator for DefaultFileNameGenerator {
    fn generate_file_name(&self) -> String {
        let file_id = self
            .file_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        format!(
            "{}-{:05}{}.{}",
            self.prefix, file_id, self.suffix, self.format
        )
    }
}

#[cfg(test)]
pub(crate) mod test {
    use std::collections::HashMap;
    use std::sync::Arc;

    use uuid::Uuid;

    use super::LocationGenerator;
    use crate::spec::{
        FormatVersion, Literal, NestedField, PartitionKey, PartitionSpec, PrimitiveType, Schema,
        Struct, StructType, TableMetadata, Transform, Type,
    };
    use crate::writer::file_writer::location_generator::{
        DefaultLocationGenerator, FileNameGenerator, ObjectStorageLocationGenerator,
        WRITE_DATA_LOCATION, WRITE_FOLDER_STORAGE_LOCATION, WRITE_OBJECT_STORAGE_PARTITIONED_PATHS,
    };

    /// Build a minimal `TableMetadata` for location generator tests.
    fn table_metadata_with(location: &str, properties: HashMap<String, String>) -> TableMetadata {
        TableMetadata {
            format_version: FormatVersion::V2,
            table_uuid: Uuid::parse_str("fb072c92-a02b-11e9-ae9c-1bb7bc9eca94").unwrap(),
            location: location.to_string(),
            last_updated_ms: 1515100955770,
            last_column_id: 1,
            schemas: HashMap::new(),
            current_schema_id: 1,
            partition_specs: HashMap::new(),
            default_spec: PartitionSpec::unpartition_spec().into(),
            default_partition_type: StructType::new(vec![]),
            last_partition_id: 1000,
            default_sort_order_id: 0,
            sort_orders: HashMap::from_iter(vec![]),
            snapshots: HashMap::default(),
            current_snapshot_id: None,
            last_sequence_number: 1,
            properties,
            snapshot_log: Vec::new(),
            metadata_log: vec![],
            refs: HashMap::new(),
            statistics: HashMap::new(),
            partition_statistics: HashMap::new(),
            encryption_keys: HashMap::new(),
            next_row_id: 0,
        }
    }

    #[test]
    fn test_default_location_generate() {
        let mut table_metadata = TableMetadata {
            format_version: FormatVersion::V2,
            table_uuid: Uuid::parse_str("fb072c92-a02b-11e9-ae9c-1bb7bc9eca94").unwrap(),
            location: "s3://data.db/table".to_string(),
            last_updated_ms: 1515100955770,
            last_column_id: 1,
            schemas: HashMap::new(),
            current_schema_id: 1,
            partition_specs: HashMap::new(),
            default_spec: PartitionSpec::unpartition_spec().into(),
            default_partition_type: StructType::new(vec![]),
            last_partition_id: 1000,
            default_sort_order_id: 0,
            sort_orders: HashMap::from_iter(vec![]),
            snapshots: HashMap::default(),
            current_snapshot_id: None,
            last_sequence_number: 1,
            properties: HashMap::new(),
            snapshot_log: Vec::new(),
            metadata_log: vec![],
            refs: HashMap::new(),
            statistics: HashMap::new(),
            partition_statistics: HashMap::new(),
            encryption_keys: HashMap::new(),
            next_row_id: 0,
        };

        let file_name_generator = super::DefaultFileNameGenerator::new(
            "part".to_string(),
            Some("test".to_string()),
            crate::spec::DataFileFormat::Parquet,
        );

        // test default data location
        let location_generator = DefaultLocationGenerator::new(&table_metadata).unwrap();
        let location =
            location_generator.generate_location(None, &file_name_generator.generate_file_name());
        assert_eq!(location, "s3://data.db/table/data/part-00000-test.parquet");

        // test custom data location
        table_metadata.properties.insert(
            WRITE_FOLDER_STORAGE_LOCATION.to_string(),
            "s3://data.db/table/data_1".to_string(),
        );
        let location_generator = DefaultLocationGenerator::new(&table_metadata).unwrap();
        let location =
            location_generator.generate_location(None, &file_name_generator.generate_file_name());
        assert_eq!(
            location,
            "s3://data.db/table/data_1/part-00001-test.parquet"
        );

        table_metadata.properties.insert(
            WRITE_DATA_LOCATION.to_string(),
            "s3://data.db/table/data_2".to_string(),
        );
        let location_generator = DefaultLocationGenerator::new(&table_metadata).unwrap();
        let location =
            location_generator.generate_location(None, &file_name_generator.generate_file_name());
        assert_eq!(
            location,
            "s3://data.db/table/data_2/part-00002-test.parquet"
        );

        table_metadata.properties.insert(
            WRITE_DATA_LOCATION.to_string(),
            // invalid table location
            "s3://data.db/data_3".to_string(),
        );
        let location_generator = DefaultLocationGenerator::new(&table_metadata).unwrap();
        let location =
            location_generator.generate_location(None, &file_name_generator.generate_file_name());
        assert_eq!(location, "s3://data.db/data_3/part-00003-test.parquet");
    }

    #[test]
    fn test_location_generate_with_partition() {
        // Create a schema with two fields: id (int) and name (string)
        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::required(2, "name", Type::Primitive(PrimitiveType::String)).into(),
                ])
                .build()
                .unwrap(),
        );

        // Create a partition spec with both fields
        let partition_spec = PartitionSpec::builder(schema.clone())
            .add_partition_field("id", "id", Transform::Identity)
            .unwrap()
            .add_partition_field("name", "name", Transform::Identity)
            .unwrap()
            .build()
            .unwrap();

        // Create partition data with values
        let partition_data =
            Struct::from_iter([Some(Literal::int(42)), Some(Literal::string("alice"))]);

        // Create a partition key
        let partition_key = PartitionKey::new(partition_spec, schema, partition_data);

        let location_gen = DefaultLocationGenerator::with_data_location("/base/path".to_string());
        let file_name = "data-00000.parquet";
        let location = location_gen.generate_location(Some(&partition_key), file_name);
        assert_eq!(location, "/base/path/id=42/name=alice/data-00000.parquet");

        // Create a table metadata for DefaultLocationGenerator
        let table_metadata = TableMetadata {
            format_version: FormatVersion::V2,
            table_uuid: Uuid::parse_str("fb072c92-a02b-11e9-ae9c-1bb7bc9eca94").unwrap(),
            location: "s3://data.db/table".to_string(),
            last_updated_ms: 1515100955770,
            last_column_id: 2,
            schemas: HashMap::new(),
            current_schema_id: 1,
            partition_specs: HashMap::new(),
            default_spec: PartitionSpec::unpartition_spec().into(),
            default_partition_type: StructType::new(vec![]),
            last_partition_id: 1000,
            default_sort_order_id: 0,
            sort_orders: HashMap::from_iter(vec![]),
            snapshots: HashMap::default(),
            current_snapshot_id: None,
            last_sequence_number: 1,
            properties: HashMap::new(),
            snapshot_log: Vec::new(),
            metadata_log: vec![],
            refs: HashMap::new(),
            statistics: HashMap::new(),
            partition_statistics: HashMap::new(),
            encryption_keys: HashMap::new(),
            next_row_id: 0,
        };

        // Test with DefaultLocationGenerator
        let default_location_gen = DefaultLocationGenerator::new(&table_metadata).unwrap();
        let location = default_location_gen.generate_location(Some(&partition_key), file_name);
        assert_eq!(
            location,
            "s3://data.db/table/data/id=42/name=alice/data-00000.parquet"
        );
    }

    #[test]
    fn test_object_storage_hash_injection() {
        // Golden vectors ported from Java's TestLocationProvider#testHashInjection, verifying that
        // the murmur3 entropy directories match Java Iceberg exactly.
        let table_metadata = table_metadata_with("s3://data.db/table", HashMap::new());
        let location_gen = ObjectStorageLocationGenerator::new(&table_metadata).unwrap();

        for (file_name, expected) in [
            ("a", "s3://data.db/table/data/0101/0110/1001/10110010/a"),
            ("b", "s3://data.db/table/data/1110/0111/1110/00000011/b"),
            ("c", "s3://data.db/table/data/0010/1101/0110/01011111/c"),
            ("d", "s3://data.db/table/data/1001/0001/0100/01110011/d"),
        ] {
            assert_eq!(location_gen.generate_location(None, file_name), expected);
        }
    }

    #[test]
    fn test_object_storage_include_partition_paths() {
        // With partitioned paths enabled (the default), the partition path is part of the hashed
        // name and appears after the entropy directories.
        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::required(2, "a", Type::Primitive(PrimitiveType::String)).into(),
                ])
                .build()
                .unwrap(),
        );
        let partition_spec = PartitionSpec::builder(schema.clone())
            .add_partition_field("id", "id", Transform::Identity)
            .unwrap()
            .add_partition_field("a", "a_trunc_3", Transform::Truncate(3))
            .unwrap()
            .build()
            .unwrap();
        let partition_data =
            Struct::from_iter([Some(Literal::int(0)), Some(Literal::string("apa"))]);
        let partition_key = PartitionKey::new(partition_spec, schema, partition_data);

        let table_metadata = table_metadata_with("s3://data.db/table", HashMap::new());
        let location_gen = ObjectStorageLocationGenerator::new(&table_metadata).unwrap();
        let location = location_gen.generate_location(Some(&partition_key), "test.parquet");

        assert!(
            location.starts_with("s3://data.db/table/data/"),
            "unexpected location: {location}"
        );
        // The partition path is included
        assert!(
            location.ends_with("/id=0/a_trunc_3=apa/test.parquet"),
            "unexpected location: {location}"
        );
    }

    /// See: https://github.com/apache/iceberg/pull/10329
    #[test]
    fn test_object_storage_include_partition_paths_with_special_characters() {
        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "data#1", Type::Primitive(PrimitiveType::Int)).into(),
                ])
                .build()
                .unwrap(),
        );
        let partition_spec = PartitionSpec::builder(schema.clone())
            .add_partition_field("data#1", "data#1", Transform::Identity)
            .unwrap()
            .build()
            .unwrap();
        let partition_data = Struct::from_iter([Some(Literal::string("val#1"))]);
        let partition_key = PartitionKey::new(partition_spec, schema, partition_data);

        let table_metadata = table_metadata_with(
            "s3://data.db/table",
            HashMap::from([(
                WRITE_OBJECT_STORAGE_PARTITIONED_PATHS.to_string(),
                "true".to_string(),
            )]),
        );
        let location_gen = ObjectStorageLocationGenerator::new(&table_metadata).unwrap();
        let location = location_gen.generate_location(Some(&partition_key), "test.parquet");

        assert_eq!(
            location,
            "s3://data.db/table/data/0000/1011/0110/00001000/data%231=val%231/test.parquet"
        );
    }

    #[test]
    fn test_object_storage_exclude_partition_paths() {
        // Golden vector ported from Java's TestLocationProvider#testExcludePartitionInPath. With
        // partitioned paths disabled, the partition value is dropped and the last entropy dir is
        // joined to the file name with `-`.
        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                ])
                .build()
                .unwrap(),
        );
        let partition_spec = PartitionSpec::builder(schema.clone())
            .add_partition_field("id", "id", Transform::Identity)
            .unwrap()
            .build()
            .unwrap();
        let partition_data = Struct::from_iter([Some(Literal::int(0))]);
        let partition_key = PartitionKey::new(partition_spec, schema, partition_data);

        let table_metadata = table_metadata_with(
            "s3://data.db/table",
            HashMap::from([(
                WRITE_OBJECT_STORAGE_PARTITIONED_PATHS.to_string(),
                "false".to_string(),
            )]),
        );
        let location_gen = ObjectStorageLocationGenerator::new(&table_metadata).unwrap();
        let location = location_gen.generate_location(Some(&partition_key), "test.parquet");

        assert_eq!(
            location,
            "s3://data.db/table/data/0110/1010/0011/11101000-test.parquet"
        );
    }

    #[test]
    fn test_object_storage_context_when_data_location_outside_table() {
        // When the data location is outside the table location, the database/table context is
        // injected after the entropy directories to avoid cross-table collisions.
        let table_metadata = table_metadata_with(
            "s3://data.db/table",
            HashMap::from([(
                WRITE_DATA_LOCATION.to_string(),
                "s3://custom-bucket/objects".to_string(),
            )]),
        );
        let location_gen = ObjectStorageLocationGenerator::new(&table_metadata).unwrap();
        let location = location_gen.generate_location(None, "a");

        // Entropy for "a" is 0101/0110/1001/10110010, then the "data.db/table" context, then file.
        assert_eq!(
            location,
            "s3://custom-bucket/objects/0101/0110/1001/10110010/data.db/table/a"
        );
    }

    #[test]
    fn test_object_storage_data_location_precedence() {
        // write.data.path takes precedence over the deprecated folder-storage fallback.
        let table_metadata = table_metadata_with(
            "s3://data.db/table",
            HashMap::from([
                (
                    WRITE_DATA_LOCATION.to_string(),
                    "s3://data.db/table/data_primary".to_string(),
                ),
                (
                    WRITE_FOLDER_STORAGE_LOCATION.to_string(),
                    "s3://data.db/table/data_legacy".to_string(),
                ),
            ]),
        );
        let location_gen = ObjectStorageLocationGenerator::new(&table_metadata).unwrap();
        let location = location_gen.generate_location(None, "a");

        assert_eq!(
            location,
            "s3://data.db/table/data_primary/0101/0110/1001/10110010/a"
        );
    }
}
