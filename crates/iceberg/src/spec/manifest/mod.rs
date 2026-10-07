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

mod _serde;

mod data_file;
pub use data_file::*;
mod entry;
pub use entry::*;
mod metadata;
pub use metadata::*;
mod reader;
pub use reader::*;
mod writer;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use apache_avro::Reader as AvroReader;
use apache_avro::error::Details;
pub use writer::*;

use super::{
    Datum, FormatVersion, ManifestContentType, PartitionSpec, PrimitiveType, Schema, Struct, Type,
    UNASSIGNED_SEQUENCE_NUMBER,
};
use crate::avro::{Resolved, define_named_types_once};
use crate::error::{Error, Result, invalid_data};

/// Whether a manifest with repeated Avro named type definitions was logged at
/// warn level. A table written by an affected release can have many of them.
static WARNED_REPEATED_DEFINITIONS: AtomicBool = AtomicBool::new(false);

/// A manifest contains metadata and a list of entries.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Manifest {
    metadata: ManifestMetadata,
    entries: Vec<ManifestEntryRef>,
}

impl Manifest {
    /// Parse manifest metadata and entries from bytes of avro file. `location`
    /// names the manifest in warnings.
    pub(crate) fn try_from_avro_bytes(
        bs: &[u8],
        location: Option<&str>,
    ) -> Result<(ManifestMetadata, Vec<ManifestEntry>)> {
        let rewritten;
        let reader = match AvroReader::new(bs) {
            Ok(reader) => reader,
            // iceberg-rust repeated `decimal` definitions before
            // `schema_to_avro_schema` defined each named type once, so this
            // fallback stays while tables can contain manifests it wrote.
            Err(e) if matches!(e.details(), Details::AmbiguousSchemaDefinition(_)) => {
                let Ok(Some((bs, repeated))) = define_named_types_once(bs) else {
                    return Err(e.into());
                };
                let location = location.unwrap_or("<unknown location>");
                if WARNED_REPEATED_DEFINITIONS.swap(true, Ordering::Relaxed) {
                    tracing::debug!(
                        "Manifest {location} defines Avro named types {repeated:?} more than once."
                    );
                } else {
                    tracing::warn!(
                        "Manifest {location} defines Avro named types {repeated:?} more than once, \
                         which the Avro specification doesn't allow. Reading it with each repeated \
                         definition replaced by a reference to the first. Later manifests like \
                         this are logged at debug level."
                    );
                }
                rewritten = bs;
                AvroReader::new(rewritten.as_slice()).map_err(|retry_error| {
                    Error::from(e).with_context(
                        "error after defining each named type once",
                        retry_error.to_string(),
                    )
                })?
            }
            Err(e) => return Err(e.into()),
        };

        // Parse manifest metadata
        let meta = reader.user_metadata();
        let metadata = ManifestMetadata::parse(meta)?;

        // Parse manifest entries
        let partition_type = metadata.partition_spec.partition_type(&metadata.schema)?;
        // Wrap the partition type once and share it across all entries: the
        // per-entry conversion needs a `&Type`, and building it here keeps the
        // lazily-populated field-name lookup from being rebuilt for every entry.
        let partition_struct_type = Type::Struct(partition_type.clone());

        let entries = match metadata.format_version {
            FormatVersion::V1 => reader
                .into_deser_iter::<Resolved<_serde::ManifestEntryV1>>()
                .map(|entry| {
                    entry?.0.try_into(
                        metadata.partition_spec.spec_id(),
                        &partition_struct_type,
                        &metadata.schema,
                    )
                })
                .collect::<Result<Vec<_>>>()?,
            // Manifest Schema & Manifest Entry did not change between V2 and V3
            FormatVersion::V2 | FormatVersion::V3 => reader
                .into_deser_iter::<Resolved<_serde::ManifestEntryV2>>()
                .map(|entry| {
                    entry?.0.try_into(
                        metadata.partition_spec.spec_id(),
                        &partition_struct_type,
                        &metadata.schema,
                    )
                })
                .collect::<Result<Vec<_>>>()?,
        };

        Ok((metadata, entries))
    }

    /// Parse manifest from bytes of avro file.
    pub fn parse_avro(bs: &[u8]) -> Result<Self> {
        let (metadata, entries) = Self::try_from_avro_bytes(bs, None)?;
        Ok(Self::new(metadata, entries))
    }

    /// Entries slice.
    pub fn entries(&self) -> &[ManifestEntryRef] {
        &self.entries
    }

    /// Get metadata.
    pub fn metadata(&self) -> &ManifestMetadata {
        &self.metadata
    }

    /// Consume this Manifest, returning its constituent parts
    pub fn into_parts(self) -> (Vec<ManifestEntryRef>, ManifestMetadata) {
        let Self { entries, metadata } = self;
        (entries, metadata)
    }

    /// Constructor from [`ManifestMetadata`] and [`ManifestEntry`]s.
    pub fn new(metadata: ManifestMetadata, entries: Vec<ManifestEntry>) -> Self {
        Self {
            metadata,
            entries: entries.into_iter().map(Arc::new).collect(),
        }
    }
}

/// Serialize a DataFile to a JSON string.
pub fn serialize_data_file_to_json(
    data_file: DataFile,
    partition_type: &super::StructType,
    format_version: FormatVersion,
) -> Result<String> {
    let partition_struct_type = Type::Struct(partition_type.clone());
    let serde = _serde::DataFileSerde::try_from(data_file, &partition_struct_type, format_version)?;
    serde_json::to_string(&serde)
        .map_err(|e| invalid_data!("Failed to serialize DataFile to JSON!").with_source(e))
}

/// Deserialize a DataFile from a JSON string.
pub fn deserialize_data_file_from_json(
    json: &str,
    partition_spec_id: i32,
    partition_type: &super::StructType,
    schema: &Schema,
) -> Result<DataFile> {
    let serde = serde_json::from_str::<_serde::DataFileSerde>(json)
        .map_err(|e| invalid_data!("Failed to deserialize JSON to DataFile!").with_source(e))?;

    let partition_struct_type = Type::Struct(partition_type.clone());
    serde.try_into(partition_spec_id, &partition_struct_type, schema)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fs;
    use std::sync::Arc;

    use apache_avro::types::Value as AvroValue;
    use apache_avro::{Codec, Writer, to_value};
    use serde_json::{Value, to_vec};
    use tempfile::TempDir;

    use super::*;
    use crate::ErrorKind;
    use crate::io::FileIO;
    use crate::spec::{Literal, NestedField, PrimitiveType, Struct, Transform, Type};

    #[tokio::test]
    async fn test_parse_manifest_v2_unpartition() {
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    // id v_int v_long v_float v_double v_varchar v_bool v_date v_timestamp v_decimal v_ts_ntz
                    Arc::new(NestedField::optional(
                        1,
                        "id",
                        Type::Primitive(PrimitiveType::Long),
                    )),
                    Arc::new(NestedField::optional(
                        2,
                        "v_int",
                        Type::Primitive(PrimitiveType::Int),
                    )),
                    Arc::new(NestedField::optional(
                        3,
                        "v_long",
                        Type::Primitive(PrimitiveType::Long),
                    )),
                    Arc::new(NestedField::optional(
                        4,
                        "v_float",
                        Type::Primitive(PrimitiveType::Float),
                    )),
                    Arc::new(NestedField::optional(
                        5,
                        "v_double",
                        Type::Primitive(PrimitiveType::Double),
                    )),
                    Arc::new(NestedField::optional(
                        6,
                        "v_varchar",
                        Type::Primitive(PrimitiveType::String),
                    )),
                    Arc::new(NestedField::optional(
                        7,
                        "v_bool",
                        Type::Primitive(PrimitiveType::Boolean),
                    )),
                    Arc::new(NestedField::optional(
                        8,
                        "v_date",
                        Type::Primitive(PrimitiveType::Date),
                    )),
                    Arc::new(NestedField::optional(
                        9,
                        "v_timestamp",
                        Type::Primitive(PrimitiveType::Timestamptz),
                    )),
                    Arc::new(NestedField::optional(
                        10,
                        "v_decimal",
                        Type::Primitive(PrimitiveType::Decimal {
                            precision: 36,
                            scale: 10,
                        }),
                    )),
                    Arc::new(NestedField::optional(
                        11,
                        "v_ts_ntz",
                        Type::Primitive(PrimitiveType::Timestamp),
                    )),
                    Arc::new(NestedField::optional(
                        12,
                        "v_ts_ns_ntz",
                        Type::Primitive(PrimitiveType::TimestampNs),
                    )),
                ])
                .build()
                .unwrap(),
        );
        let metadata = ManifestMetadata {
            schema_id: 0,
            schema: schema.clone(),
            partition_spec: PartitionSpec::builder(schema)
                .with_spec_id(0)
                .build()
                .unwrap(),
            content: ManifestContentType::Data,
            format_version: FormatVersion::V2,
        };
        let mut entries = vec![
                ManifestEntry {
                    status: ManifestStatus::Added,
                    snapshot_id: None,
                    sequence_number: None,
                    file_sequence_number: None,
                    data_file: DataFile {content:DataContentType::Data,file_path:"s3a://icebergdata/demo/s1/t1/data/00000-0-ba56fbfa-f2ff-40c9-bb27-565ad6dc2be8-00000.parquet".to_string(),file_format:DataFileFormat::Parquet,partition:Struct::empty(),record_count:1,file_size_in_bytes:5442,column_sizes:HashMap::from([(0,73),(6,34),(2,73),(7,61),(3,61),(5,62),(9,79),(10,73),(1,61),(4,73),(8,73)]),value_counts:HashMap::from([(4,1),(5,1),(2,1),(0,1),(3,1),(6,1),(8,1),(1,1),(10,1),(7,1),(9,1)]),null_value_counts:HashMap::from([(1,0),(6,0),(2,0),(8,0),(0,0),(3,0),(5,0),(9,0),(7,0),(4,0),(10,0)]),nan_value_counts:HashMap::new(),lower_bounds:HashMap::new(),upper_bounds:HashMap::new(),key_metadata:None,split_offsets:Some(vec![4]),equality_ids:Some(Vec::new()),sort_order_id:None, partition_spec_id: 0,first_row_id: None,referenced_data_file: None,content_offset: None,content_size_in_bytes: None }
                }
            ];

        // write manifest to file
        let tmp_dir = TempDir::new().unwrap();
        let path = tmp_dir.path().join("test_manifest.avro");
        let io = FileIO::new_with_fs();
        let output_file = io.new_output(path.to_str().unwrap()).unwrap();
        let mut writer = ManifestWriterBuilder::new(
            output_file,
            Some(1),
            metadata.schema.clone(),
            metadata.partition_spec.clone(),
        )
        .build_v2_data();
        for entry in &entries {
            writer.add_entry(entry.clone()).unwrap();
        }
        writer.write_manifest_file().await.unwrap();

        // read back the manifest file and check the content
        let actual_manifest =
            Manifest::parse_avro(fs::read(path).expect("read_file must succeed").as_slice())
                .unwrap();
        // The snapshot id is assigned when the entry is added to the manifest.
        entries[0].snapshot_id = Some(1);
        assert_eq!(actual_manifest, Manifest::new(metadata, entries));
    }

    #[test]
    fn test_parse_snappy_manifest_v2() {
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![Arc::new(NestedField::optional(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Long),
                ))])
                .build()
                .unwrap(),
        );
        let partition_spec = PartitionSpec::builder(schema.clone())
            .with_spec_id(0)
            .build()
            .unwrap();

        for (manifest_content, file_content, file_path) in [
            (
                ManifestContentType::Data,
                DataContentType::Data,
                "s3://bucket/table/data/data.parquet",
            ),
            (
                ManifestContentType::Deletes,
                DataContentType::PositionDeletes,
                "s3://bucket/table/data/delete.parquet",
            ),
        ] {
            let metadata = ManifestMetadata {
                schema_id: 0,
                schema: schema.clone(),
                partition_spec: partition_spec.clone(),
                content: manifest_content,
                format_version: FormatVersion::V2,
            };
            let entry = ManifestEntry {
                status: ManifestStatus::Added,
                snapshot_id: Some(1),
                sequence_number: None,
                file_sequence_number: None,
                data_file: DataFile {
                    content: file_content,
                    file_path: file_path.to_string(),
                    file_format: DataFileFormat::Parquet,
                    partition: Struct::empty(),
                    record_count: 1,
                    file_size_in_bytes: 1024,
                    column_sizes: HashMap::new(),
                    value_counts: HashMap::new(),
                    null_value_counts: HashMap::new(),
                    nan_value_counts: HashMap::new(),
                    lower_bounds: HashMap::new(),
                    upper_bounds: HashMap::new(),
                    key_metadata: None,
                    split_offsets: None,
                    equality_ids: None,
                    sort_order_id: None,
                    partition_spec_id: 0,
                    first_row_id: None,
                    referenced_data_file: None,
                    content_offset: None,
                    content_size_in_bytes: None,
                },
            };

            let partition_type = metadata
                .partition_spec
                .partition_type(&metadata.schema)
                .unwrap();
            let avro_schema = manifest_schema_v2(&partition_type).unwrap();
            let mut writer = Writer::with_codec(&avro_schema, Vec::new(), Codec::Snappy).unwrap();
            writer
                .add_user_metadata("schema".to_string(), to_vec(&metadata.schema).unwrap())
                .unwrap();
            writer
                .add_user_metadata(
                    "schema-id".to_string(),
                    metadata.schema.schema_id().to_string(),
                )
                .unwrap();
            writer
                .add_user_metadata(
                    "partition-spec".to_string(),
                    to_vec(&metadata.partition_spec.fields()).unwrap(),
                )
                .unwrap();
            writer
                .add_user_metadata(
                    "partition-spec-id".to_string(),
                    metadata.partition_spec.spec_id().to_string(),
                )
                .unwrap();
            writer
                .add_user_metadata(
                    "format-version".to_string(),
                    (metadata.format_version as u8).to_string(),
                )
                .unwrap();
            writer
                .add_user_metadata("content".to_string(), metadata.content.to_string())
                .unwrap();
            let value = to_value(
                _serde::ManifestEntryV2::try_from(
                    entry.clone(),
                    &Type::Struct(partition_type.clone()),
                )
                .unwrap(),
            )
            .unwrap()
            .resolve(&avro_schema)
            .unwrap();
            writer.append_value(value).unwrap();
            let bs = writer.into_inner().unwrap();

            let parsed_manifest = Manifest::parse_avro(&bs).unwrap();

            assert_eq!(parsed_manifest, Manifest::new(metadata, vec![entry]));
        }
    }

    #[tokio::test]
    async fn test_parse_manifest_v2_partition() {
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    Arc::new(NestedField::optional(
                        1,
                        "id",
                        Type::Primitive(PrimitiveType::Long),
                    )),
                    Arc::new(NestedField::optional(
                        2,
                        "v_int",
                        Type::Primitive(PrimitiveType::Int),
                    )),
                    Arc::new(NestedField::optional(
                        3,
                        "v_long",
                        Type::Primitive(PrimitiveType::Long),
                    )),
                    Arc::new(NestedField::optional(
                        4,
                        "v_float",
                        Type::Primitive(PrimitiveType::Float),
                    )),
                    Arc::new(NestedField::optional(
                        5,
                        "v_double",
                        Type::Primitive(PrimitiveType::Double),
                    )),
                    Arc::new(NestedField::optional(
                        6,
                        "v_varchar",
                        Type::Primitive(PrimitiveType::String),
                    )),
                    Arc::new(NestedField::optional(
                        7,
                        "v_bool",
                        Type::Primitive(PrimitiveType::Boolean),
                    )),
                    Arc::new(NestedField::optional(
                        8,
                        "v_date",
                        Type::Primitive(PrimitiveType::Date),
                    )),
                    Arc::new(NestedField::optional(
                        9,
                        "v_timestamp",
                        Type::Primitive(PrimitiveType::Timestamptz),
                    )),
                    Arc::new(NestedField::optional(
                        10,
                        "v_decimal",
                        Type::Primitive(PrimitiveType::Decimal {
                            precision: 36,
                            scale: 10,
                        }),
                    )),
                    Arc::new(NestedField::optional(
                        11,
                        "v_ts_ntz",
                        Type::Primitive(PrimitiveType::Timestamp),
                    )),
                    Arc::new(NestedField::optional(
                        12,
                        "v_ts_ns_ntz",
                        Type::Primitive(PrimitiveType::TimestampNs),
                    )),
                ])
                .build()
                .unwrap(),
        );
        let metadata = ManifestMetadata {
            schema_id: 0,
            schema: schema.clone(),
            partition_spec: PartitionSpec::builder(schema)
                .with_spec_id(0)
                .add_partition_field("v_int", "v_int", Transform::Identity)
                .unwrap()
                .add_partition_field("v_long", "v_long", Transform::Identity)
                .unwrap()
                .build()
                .unwrap(),
            content: ManifestContentType::Data,
            format_version: FormatVersion::V2,
        };
        let mut entries = vec![ManifestEntry {
                status: ManifestStatus::Added,
                snapshot_id: None,
                sequence_number: None,
                file_sequence_number: None,
                data_file: DataFile {
                    content: DataContentType::Data,
                    file_format: DataFileFormat::Parquet,
                    file_path: "s3a://icebergdata/demo/s1/t1/data/00000-0-378b56f5-5c52-4102-a2c2-f05f8a7cbe4a-00000.parquet".to_string(),
                    partition: Struct::from_iter(
                        vec![
                            Some(Literal::int(1)),
                            Some(Literal::long(1000)),
                        ]
                            .into_iter()
                    ),
                    record_count: 1,
                    file_size_in_bytes: 5442,
                    column_sizes: HashMap::from([
                        (0, 73),
                        (6, 34),
                        (2, 73),
                        (7, 61),
                        (3, 61),
                        (5, 62),
                        (9, 79),
                        (10, 73),
                        (1, 61),
                        (4, 73),
                        (8, 73)
                    ]),
                    value_counts: HashMap::from([
                        (4, 1),
                        (5, 1),
                        (2, 1),
                        (0, 1),
                        (3, 1),
                        (6, 1),
                        (8, 1),
                        (1, 1),
                        (10, 1),
                        (7, 1),
                        (9, 1)
                    ]),
                    null_value_counts: HashMap::from([
                        (1, 0),
                        (6, 0),
                        (2, 0),
                        (8, 0),
                        (0, 0),
                        (3, 0),
                        (5, 0),
                        (9, 0),
                        (7, 0),
                        (4, 0),
                        (10, 0)
                    ]),
                    nan_value_counts: HashMap::new(),
                    lower_bounds: HashMap::new(),
                    upper_bounds: HashMap::new(),
                    key_metadata: None,
                    split_offsets: Some(vec![4]),
                    equality_ids: Some(Vec::new()),
                    sort_order_id: None,
                    partition_spec_id: 0,
                    first_row_id: None,
                    referenced_data_file: None,
                    content_offset: None,
                    content_size_in_bytes: None,
                },
            }];

        // write manifest to file and check the return manifest file.
        let tmp_dir = TempDir::new().unwrap();
        let path = tmp_dir.path().join("test_manifest.avro");
        let io = FileIO::new_with_fs();
        let output_file = io.new_output(path.to_str().unwrap()).unwrap();
        let mut writer = ManifestWriterBuilder::new(
            output_file,
            Some(2),
            metadata.schema.clone(),
            metadata.partition_spec.clone(),
        )
        .build_v2_data();
        for entry in &entries {
            writer.add_entry(entry.clone()).unwrap();
        }
        let manifest_file = writer.write_manifest_file().await.unwrap();
        assert_eq!(manifest_file.sequence_number, UNASSIGNED_SEQUENCE_NUMBER);
        assert_eq!(
            manifest_file.min_sequence_number,
            UNASSIGNED_SEQUENCE_NUMBER
        );

        // read back the manifest file and check the content
        let actual_manifest =
            Manifest::parse_avro(fs::read(path).expect("read_file must succeed").as_slice())
                .unwrap();
        // The snapshot id is assigned when the entry is added to the manifest.
        entries[0].snapshot_id = Some(2);
        assert_eq!(actual_manifest, Manifest::new(metadata, entries));
    }

    #[tokio::test]
    async fn test_parse_manifest_v1_unpartition() {
        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    Arc::new(NestedField::optional(
                        1,
                        "id",
                        Type::Primitive(PrimitiveType::Int),
                    )),
                    Arc::new(NestedField::optional(
                        2,
                        "data",
                        Type::Primitive(PrimitiveType::String),
                    )),
                    Arc::new(NestedField::optional(
                        3,
                        "comment",
                        Type::Primitive(PrimitiveType::String),
                    )),
                ])
                .build()
                .unwrap(),
        );
        let metadata = ManifestMetadata {
            schema_id: 1,
            schema: schema.clone(),
            partition_spec: PartitionSpec::builder(schema)
                .with_spec_id(0)
                .build()
                .unwrap(),
            content: ManifestContentType::Data,
            format_version: FormatVersion::V1,
        };
        let mut entries = vec![ManifestEntry {
                status: ManifestStatus::Added,
                snapshot_id: Some(0),
                sequence_number: Some(0),
                file_sequence_number: Some(0),
                data_file: DataFile {
                    content: DataContentType::Data,
                    file_path: "s3://testbucket/iceberg_data/iceberg_ctl/iceberg_db/iceberg_tbl/data/00000-7-45268d71-54eb-476c-b42c-942d880c04a1-00001.parquet".to_string(),
                    file_format: DataFileFormat::Parquet,
                    partition: Struct::empty(),
                    record_count: 1,
                    file_size_in_bytes: 875,
                    column_sizes: HashMap::from([(1,47),(2,48),(3,52)]),
                    value_counts: HashMap::from([(1,1),(2,1),(3,1)]),
                    null_value_counts: HashMap::from([(1,0),(2,0),(3,0)]),
                    nan_value_counts: HashMap::new(),
                    lower_bounds: HashMap::from([(1,Datum::int(1)),(2,Datum::string("a")),(3,Datum::string("AC/DC"))]),
                    upper_bounds: HashMap::from([(1,Datum::int(1)),(2,Datum::string("a")),(3,Datum::string("AC/DC"))]),
                    key_metadata: None,
                    split_offsets: Some(vec![4]),
                    equality_ids: None,
                    sort_order_id: Some(0),
                    partition_spec_id: 0,
                    first_row_id: None,
                    referenced_data_file: None,
                    content_offset: None,
                    content_size_in_bytes: None,
                }
            }];

        // write manifest to file
        let tmp_dir = TempDir::new().unwrap();
        let path = tmp_dir.path().join("test_manifest.avro");
        let io = FileIO::new_with_fs();
        let output_file = io.new_output(path.to_str().unwrap()).unwrap();
        let mut writer = ManifestWriterBuilder::new(
            output_file,
            Some(3),
            metadata.schema.clone(),
            metadata.partition_spec.clone(),
        )
        .build_v1();
        for entry in &entries {
            writer.add_entry(entry.clone()).unwrap();
        }
        writer.write_manifest_file().await.unwrap();

        // read back the manifest file and check the content
        let actual_manifest =
            Manifest::parse_avro(fs::read(path).expect("read_file must succeed").as_slice())
                .unwrap();
        // The snapshot id is assigned when the entry is added to the manifest.
        entries[0].snapshot_id = Some(3);
        assert_eq!(actual_manifest, Manifest::new(metadata, entries));
    }

    #[tokio::test]
    async fn test_parse_manifest_v1_partition() {
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    Arc::new(NestedField::optional(
                        1,
                        "id",
                        Type::Primitive(PrimitiveType::Long),
                    )),
                    Arc::new(NestedField::optional(
                        2,
                        "data",
                        Type::Primitive(PrimitiveType::String),
                    )),
                    Arc::new(NestedField::optional(
                        3,
                        "category",
                        Type::Primitive(PrimitiveType::String),
                    )),
                ])
                .build()
                .unwrap(),
        );
        let metadata = ManifestMetadata {
            schema_id: 0,
            schema: schema.clone(),
            partition_spec: PartitionSpec::builder(schema)
                .add_partition_field("category", "category", Transform::Identity)
                .unwrap()
                .build()
                .unwrap(),
            content: ManifestContentType::Data,
            format_version: FormatVersion::V1,
        };
        let mut entries = vec![
                ManifestEntry {
                    status: ManifestStatus::Added,
                    snapshot_id: Some(0),
                    sequence_number: Some(0),
                    file_sequence_number: Some(0),
                    data_file: DataFile {
                        content: DataContentType::Data,
                        file_path: "s3://testbucket/prod/db/sample/data/category=x/00010-1-d5c93668-1e52-41ac-92a6-bba590cbf249-00001.parquet".to_string(),
                        file_format: DataFileFormat::Parquet,
                        partition: Struct::from_iter(
                            vec![
                                Some(
                                    Literal::string("x"),
                                ),
                            ]
                                .into_iter()
                        ),
                        record_count: 1,
                        file_size_in_bytes: 874,
                        column_sizes: HashMap::from([(1, 46), (2, 48), (3, 48)]),
                        value_counts: HashMap::from([(1, 1), (2, 1), (3, 1)]),
                        null_value_counts: HashMap::from([(1, 0), (2, 0), (3, 0)]),
                        nan_value_counts: HashMap::new(),
                        lower_bounds: HashMap::from([
                        (1, Datum::long(1)),
                        (2, Datum::string("a")),
                        (3, Datum::string("x"))
                        ]),
                        upper_bounds: HashMap::from([
                        (1, Datum::long(1)),
                        (2, Datum::string("a")),
                        (3, Datum::string("x"))
                        ]),
                        key_metadata: None,
                        split_offsets: Some(vec![4]),
                        equality_ids: None,
                        sort_order_id: Some(0),
                        partition_spec_id: 0,
                        first_row_id: None,
                        referenced_data_file: None,
                        content_offset: None,
                        content_size_in_bytes: None,
                    },
                }
            ];

        // write manifest to file
        let tmp_dir = TempDir::new().unwrap();
        let path = tmp_dir.path().join("test_manifest.avro");
        let io = FileIO::new_with_fs();
        let output_file = io.new_output(path.to_str().unwrap()).unwrap();
        let mut writer = ManifestWriterBuilder::new(
            output_file,
            Some(2),
            metadata.schema.clone(),
            metadata.partition_spec.clone(),
        )
        .build_v1();
        for entry in &entries {
            writer.add_entry(entry.clone()).unwrap();
        }
        let manifest_file = writer.write_manifest_file().await.unwrap();
        let partitions = manifest_file.partitions.unwrap();
        assert_eq!(partitions.len(), 1);
        assert_eq!(
            partitions[0].clone().lower_bound.unwrap(),
            Datum::string("x").to_bytes().unwrap()
        );
        assert_eq!(
            partitions[0].clone().upper_bound.unwrap(),
            Datum::string("x").to_bytes().unwrap()
        );

        // read back the manifest file and check the content
        let actual_manifest =
            Manifest::parse_avro(fs::read(path).expect("read_file must succeed").as_slice())
                .unwrap();
        // The snapshot id is assigned when the entry is added to the manifest.
        entries[0].snapshot_id = Some(2);
        assert_eq!(actual_manifest, Manifest::new(metadata, entries));
    }

    #[tokio::test]
    async fn test_parse_manifest_with_schema_evolution() {
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    Arc::new(NestedField::optional(
                        1,
                        "id",
                        Type::Primitive(PrimitiveType::Long),
                    )),
                    Arc::new(NestedField::optional(
                        2,
                        "v_int",
                        Type::Primitive(PrimitiveType::Int),
                    )),
                ])
                .build()
                .unwrap(),
        );
        let metadata = ManifestMetadata {
            schema_id: 0,
            schema: schema.clone(),
            partition_spec: PartitionSpec::builder(schema)
                .with_spec_id(0)
                .build()
                .unwrap(),
            content: ManifestContentType::Data,
            format_version: FormatVersion::V2,
        };
        let entries = vec![ManifestEntry {
                status: ManifestStatus::Added,
                snapshot_id: None,
                sequence_number: None,
                file_sequence_number: None,
                data_file: DataFile {
                    content: DataContentType::Data,
                    file_format: DataFileFormat::Parquet,
                    file_path: "s3a://icebergdata/demo/s1/t1/data/00000-0-378b56f5-5c52-4102-a2c2-f05f8a7cbe4a-00000.parquet".to_string(),
                    partition: Struct::empty(),
                    record_count: 1,
                    file_size_in_bytes: 5442,
                    column_sizes: HashMap::from([
                        (1, 61),
                        (2, 73),
                        (3, 61),
                    ]),
                    value_counts: HashMap::default(),
                    null_value_counts: HashMap::default(),
                    nan_value_counts: HashMap::new(),
                    lower_bounds: HashMap::from([
                        (1, Datum::long(1)),
                        (2, Datum::int(2)),
                        (3, Datum::string("x"))
                    ]),
                    upper_bounds: HashMap::from([
                        (1, Datum::long(1)),
                        (2, Datum::int(2)),
                        (3, Datum::string("x"))
                    ]),
                    key_metadata: None,
                    split_offsets: Some(vec![4]),
                    equality_ids: None,
                    sort_order_id: None,
                    partition_spec_id: 0,
                    first_row_id: None,
                    referenced_data_file: None,
                    content_offset: None,
                    content_size_in_bytes: None,
                },
            }];

        // write manifest to file
        let tmp_dir = TempDir::new().unwrap();
        let path = tmp_dir.path().join("test_manifest.avro");
        let io = FileIO::new_with_fs();
        let output_file = io.new_output(path.to_str().unwrap()).unwrap();
        let mut writer = ManifestWriterBuilder::new(
            output_file,
            Some(2),
            metadata.schema.clone(),
            metadata.partition_spec.clone(),
        )
        .build_v2_data();
        for entry in &entries {
            writer.add_entry(entry.clone()).unwrap();
        }
        writer.write_manifest_file().await.unwrap();

        // read back the manifest file and check the content
        let actual_manifest =
            Manifest::parse_avro(fs::read(path).expect("read_file must succeed").as_slice())
                .unwrap();

        // Compared with original manifest, the lower_bounds and upper_bounds no longer has data for field 3, and
        // other parts should be same.
        // The snapshot id is assigned when the entry is added to the manifest.
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    Arc::new(NestedField::optional(
                        1,
                        "id",
                        Type::Primitive(PrimitiveType::Long),
                    )),
                    Arc::new(NestedField::optional(
                        2,
                        "v_int",
                        Type::Primitive(PrimitiveType::Int),
                    )),
                ])
                .build()
                .unwrap(),
        );
        let expected_manifest = Manifest {
            metadata: ManifestMetadata {
                schema_id: 0,
                schema: schema.clone(),
                partition_spec: PartitionSpec::builder(schema).with_spec_id(0).build().unwrap(),
                content: ManifestContentType::Data,
                format_version: FormatVersion::V2,
            },
            entries: vec![Arc::new(ManifestEntry {
                status: ManifestStatus::Added,
                snapshot_id: Some(2),
                sequence_number: None,
                file_sequence_number: None,
                data_file: DataFile {
                    content: DataContentType::Data,
                    file_format: DataFileFormat::Parquet,
                    file_path: "s3a://icebergdata/demo/s1/t1/data/00000-0-378b56f5-5c52-4102-a2c2-f05f8a7cbe4a-00000.parquet".to_string(),
                    partition: Struct::empty(),
                    record_count: 1,
                    file_size_in_bytes: 5442,
                    column_sizes: HashMap::from([
                        (1, 61),
                        (2, 73),
                        (3, 61),
                    ]),
                    value_counts: HashMap::default(),
                    null_value_counts: HashMap::default(),
                    nan_value_counts: HashMap::new(),
                    lower_bounds: HashMap::from([
                        (1, Datum::long(1)),
                        (2, Datum::int(2)),
                    ]),
                    upper_bounds: HashMap::from([
                        (1, Datum::long(1)),
                        (2, Datum::int(2)),
                    ]),
                    key_metadata: None,
                    split_offsets: Some(vec![4]),
                    equality_ids: None,
                    sort_order_id: None,
                    partition_spec_id: 0,
                    first_row_id: None,
                    referenced_data_file: None,
                    content_offset: None,
                    content_size_in_bytes: None,
                },
            })],
        };

        assert_eq!(actual_manifest, expected_manifest);
    }

    #[tokio::test]
    async fn test_manifest_summary() {
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    Arc::new(NestedField::optional(
                        1,
                        "time",
                        Type::Primitive(PrimitiveType::Date),
                    )),
                    Arc::new(NestedField::optional(
                        2,
                        "v_float",
                        Type::Primitive(PrimitiveType::Float),
                    )),
                    Arc::new(NestedField::optional(
                        3,
                        "v_double",
                        Type::Primitive(PrimitiveType::Double),
                    )),
                ])
                .build()
                .unwrap(),
        );
        let partition_spec = PartitionSpec::builder(schema.clone())
            .with_spec_id(0)
            .add_partition_field("time", "year_of_time", Transform::Year)
            .unwrap()
            .add_partition_field("v_float", "f", Transform::Identity)
            .unwrap()
            .add_partition_field("v_double", "d", Transform::Identity)
            .unwrap()
            .build()
            .unwrap();
        let metadata = ManifestMetadata {
            schema_id: 0,
            schema,
            partition_spec,
            content: ManifestContentType::Data,
            format_version: FormatVersion::V2,
        };
        let entries = vec![
            ManifestEntry {
                status: ManifestStatus::Added,
                snapshot_id: None,
                sequence_number: None,
                file_sequence_number: None,
                data_file: DataFile {
                    content: DataContentType::Data,
                    file_path: "s3a://icebergdata/demo/s1/t1/data/00000-0-ba56fbfa-f2ff-40c9-bb27-565ad6dc2be8-00000.parquet".to_string(),
                    file_format: DataFileFormat::Parquet,
                    partition: Struct::from_iter(
                        vec![
                            Some(Literal::int(2021)),
                            Some(Literal::float(1.0_f32)),
                            Some(Literal::double(2.0)),
                        ]
                    ),
                    record_count: 1,
                    file_size_in_bytes: 5442,
                    column_sizes: HashMap::from([(0,73),(6,34),(2,73),(7,61),(3,61),(5,62),(9,79),(10,73),(1,61),(4,73),(8,73)]),
                    value_counts: HashMap::from([(4,1),(5,1),(2,1),(0,1),(3,1),(6,1),(8,1),(1,1),(10,1),(7,1),(9,1)]),
                    null_value_counts: HashMap::from([(1,0),(6,0),(2,0),(8,0),(0,0),(3,0),(5,0),(9,0),(7,0),(4,0),(10,0)]),
                    nan_value_counts: HashMap::new(),
                    lower_bounds: HashMap::new(),
                    upper_bounds: HashMap::new(),
                    key_metadata: None,
                    split_offsets: Some(vec![4]),
                    equality_ids: None,
                    sort_order_id: None,
                    partition_spec_id: 0,
                    first_row_id: None,
                    referenced_data_file: None,
                    content_offset: None,
                    content_size_in_bytes: None,
                }
            },
                ManifestEntry {
                    status: ManifestStatus::Added,
                    snapshot_id: None,
                    sequence_number: None,
                    file_sequence_number: None,
                    data_file: DataFile {
                        content: DataContentType::Data,
                        file_path: "s3a://icebergdata/demo/s1/t1/data/00000-0-ba56fbfa-f2ff-40c9-bb27-565ad6dc2be8-00000.parquet".to_string(),
                        file_format: DataFileFormat::Parquet,
                        partition: Struct::from_iter(
                            vec![
                                Some(Literal::int(1111)),
                                Some(Literal::float(15.5_f32)),
                                Some(Literal::double(25.5)),
                            ]
                        ),
                        record_count: 1,
                        file_size_in_bytes: 5442,
                        column_sizes: HashMap::from([(0,73),(6,34),(2,73),(7,61),(3,61),(5,62),(9,79),(10,73),(1,61),(4,73),(8,73)]),
                        value_counts: HashMap::from([(4,1),(5,1),(2,1),(0,1),(3,1),(6,1),(8,1),(1,1),(10,1),(7,1),(9,1)]),
                        null_value_counts: HashMap::from([(1,0),(6,0),(2,0),(8,0),(0,0),(3,0),(5,0),(9,0),(7,0),(4,0),(10,0)]),
                        nan_value_counts: HashMap::new(),
                        lower_bounds: HashMap::new(),
                        upper_bounds: HashMap::new(),
                        key_metadata: None,
                        split_offsets: Some(vec![4]),
                        equality_ids: None,
                        sort_order_id: None,
                        partition_spec_id: 0,
                        first_row_id: None,
                        referenced_data_file: None,
                        content_offset: None,
                        content_size_in_bytes: None,
                    }
                },
                ManifestEntry {
                    status: ManifestStatus::Added,
                    snapshot_id: None,
                    sequence_number: None,
                    file_sequence_number: None,
                    data_file: DataFile {
                        content: DataContentType::Data,
                        file_path: "s3a://icebergdata/demo/s1/t1/data/00000-0-ba56fbfa-f2ff-40c9-bb27-565ad6dc2be8-00000.parquet".to_string(),
                        file_format: DataFileFormat::Parquet,
                        partition: Struct::from_iter(
                            vec![
                                Some(Literal::int(1211)),
                                Some(Literal::float(f32::NAN)),
                                Some(Literal::double(1.0)),
                            ]
                        ),
                        record_count: 1,
                        file_size_in_bytes: 5442,
                        column_sizes: HashMap::from([(0,73),(6,34),(2,73),(7,61),(3,61),(5,62),(9,79),(10,73),(1,61),(4,73),(8,73)]),
                        value_counts: HashMap::from([(4,1),(5,1),(2,1),(0,1),(3,1),(6,1),(8,1),(1,1),(10,1),(7,1),(9,1)]),
                        null_value_counts: HashMap::from([(1,0),(6,0),(2,0),(8,0),(0,0),(3,0),(5,0),(9,0),(7,0),(4,0),(10,0)]),
                        nan_value_counts: HashMap::new(),
                        lower_bounds: HashMap::new(),
                        upper_bounds: HashMap::new(),
                        key_metadata: None,
                        split_offsets: Some(vec![4]),
                        equality_ids: None,
                        sort_order_id: None,
                        partition_spec_id: 0,
                        first_row_id: None,
                        referenced_data_file: None,
                        content_offset: None,
                        content_size_in_bytes: None,
                    }
                },
                ManifestEntry {
                    status: ManifestStatus::Added,
                    snapshot_id: None,
                    sequence_number: None,
                    file_sequence_number: None,
                    data_file: DataFile {
                        content: DataContentType::Data,
                        file_path: "s3a://icebergdata/demo/s1/t1/data/00000-0-ba56fbfa-f2ff-40c9-bb27-565ad6dc2be8-00000.parquet".to_string(),
                        file_format: DataFileFormat::Parquet,
                        partition: Struct::from_iter(
                            vec![
                                Some(Literal::int(1111)),
                                None,
                                Some(Literal::double(11.0)),
                            ]
                        ),
                        record_count: 1,
                        file_size_in_bytes: 5442,
                        column_sizes: HashMap::from([(0,73),(6,34),(2,73),(7,61),(3,61),(5,62),(9,79),(10,73),(1,61),(4,73),(8,73)]),
                        value_counts: HashMap::from([(4,1),(5,1),(2,1),(0,1),(3,1),(6,1),(8,1),(1,1),(10,1),(7,1),(9,1)]),
                        null_value_counts: HashMap::from([(1,0),(6,0),(2,0),(8,0),(0,0),(3,0),(5,0),(9,0),(7,0),(4,0),(10,0)]),
                        nan_value_counts: HashMap::new(),
                        lower_bounds: HashMap::new(),
                        upper_bounds: HashMap::new(),
                        key_metadata: None,
                        split_offsets: Some(vec![4]),
                        equality_ids: None,
                        sort_order_id: None,
                        partition_spec_id: 0,
                        first_row_id: None,
                        referenced_data_file: None,
                        content_offset: None,
                        content_size_in_bytes: None,
                    }
                },
        ];

        // write manifest to file
        let tmp_dir = TempDir::new().unwrap();
        let path = tmp_dir.path().join("test_manifest.avro");
        let io = FileIO::new_with_fs();
        let output_file = io.new_output(path.to_str().unwrap()).unwrap();
        let mut writer = ManifestWriterBuilder::new(
            output_file,
            Some(1),
            metadata.schema.clone(),
            metadata.partition_spec.clone(),
        )
        .build_v2_data();
        for entry in &entries {
            writer.add_entry(entry.clone()).unwrap();
        }
        let res = writer.write_manifest_file().await.unwrap();

        let partitions = res.partitions.unwrap();

        assert_eq!(partitions.len(), 3);
        assert_eq!(
            partitions[0].clone().lower_bound.unwrap(),
            Datum::int(1111).to_bytes().unwrap()
        );
        assert_eq!(
            partitions[0].clone().upper_bound.unwrap(),
            Datum::int(2021).to_bytes().unwrap()
        );
        assert!(!partitions[0].clone().contains_null);
        assert_eq!(partitions[0].clone().contains_nan, Some(false));

        assert_eq!(
            partitions[1].clone().lower_bound.unwrap(),
            Datum::float(1.0_f32).to_bytes().unwrap()
        );
        assert_eq!(
            partitions[1].clone().upper_bound.unwrap(),
            Datum::float(15.5_f32).to_bytes().unwrap()
        );
        assert!(partitions[1].clone().contains_null);
        assert_eq!(partitions[1].clone().contains_nan, Some(true));

        assert_eq!(
            partitions[2].clone().lower_bound.unwrap(),
            Datum::double(1.0).to_bytes().unwrap()
        );
        assert_eq!(
            partitions[2].clone().upper_bound.unwrap(),
            Datum::double(25.5).to_bytes().unwrap()
        );
        assert!(!partitions[2].clone().contains_null);
        assert_eq!(partitions[2].clone().contains_nan, Some(false));
    }

    #[test]
    fn test_data_file_serialization() {
        // Create a simple schema
        let schema = Schema::builder()
            .with_schema_id(1)
            .with_identifier_field_ids(vec![1])
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::required(2, "name", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
            .unwrap();

        // Create a partition spec
        let partition_spec = PartitionSpec::builder(schema.clone())
            .with_spec_id(1)
            .add_partition_field("id", "id_partition", Transform::Identity)
            .unwrap()
            .build()
            .unwrap();

        // Get partition type from the partition spec
        let partition_type = partition_spec.partition_type(&schema).unwrap();

        // Create a vector of DataFile objects
        let data_files = vec![
            DataFileBuilder::default()
                .content(DataContentType::Data)
                .file_format(DataFileFormat::Parquet)
                .file_path("path/to/file1.parquet".to_string())
                .file_size_in_bytes(1024)
                .record_count(100)
                .partition_spec_id(1)
                .partition(Struct::empty())
                .column_sizes(HashMap::from([(1, 512), (2, 1024)]))
                .value_counts(HashMap::from([(1, 100), (2, 500)]))
                .null_value_counts(HashMap::from([(1, 0), (2, 1)]))
                .build()
                .unwrap(),
            DataFileBuilder::default()
                .content(DataContentType::Data)
                .file_format(DataFileFormat::Parquet)
                .file_path("path/to/file2.parquet".to_string())
                .file_size_in_bytes(2048)
                .record_count(200)
                .partition_spec_id(1)
                .partition(Struct::empty())
                .column_sizes(HashMap::from([(1, 1024), (2, 2048)]))
                .value_counts(HashMap::from([(1, 200), (2, 600)]))
                .null_value_counts(HashMap::from([(1, 10), (2, 999)]))
                .build()
                .unwrap(),
        ];

        // Serialize the DataFile objects
        let serialized_files = data_files
            .clone()
            .into_iter()
            .map(|f| serialize_data_file_to_json(f, &partition_type, FormatVersion::V2).unwrap())
            .collect::<Vec<String>>();

        // Verify we have the expected serialized files
        assert_eq!(serialized_files.len(), 2);
        let pretty_json1: Value = serde_json::from_str(serialized_files.first().unwrap()).unwrap();
        let pretty_json2: Value = serde_json::from_str(serialized_files.get(1).unwrap()).unwrap();
        let expected_serialized_file1 = serde_json::json!({
            "content": 0,
            "file_path": "path/to/file1.parquet",
            "file_format": "PARQUET",
            "partition": {},
            "record_count": 100,
            "file_size_in_bytes": 1024,
            "column_sizes": [
                { "key": 1, "value": 512 },
                { "key": 2, "value": 1024 }
            ],
            "value_counts": [
                { "key": 1, "value": 100 },
                { "key": 2, "value": 500 }
            ],
            "null_value_counts": [
                { "key": 1, "value": 0 },
                { "key": 2, "value": 1 }
            ],
            "nan_value_counts": [],
            "lower_bounds": [],
            "upper_bounds": [],
            "key_metadata": null,
            "split_offsets": null,
            "equality_ids": null,
            "sort_order_id": null,
            "first_row_id": null,
            "referenced_data_file": null,
            "content_offset": null,
            "content_size_in_bytes": null
        });
        let expected_serialized_file2 = serde_json::json!({
            "content": 0,
            "file_path": "path/to/file2.parquet",
            "file_format": "PARQUET",
            "partition": {},
            "record_count": 200,
            "file_size_in_bytes": 2048,
            "column_sizes": [
                { "key": 1, "value": 1024 },
                { "key": 2, "value": 2048 }
            ],
            "value_counts": [
                { "key": 1, "value": 200 },
                { "key": 2, "value": 600 }
            ],
            "null_value_counts": [
                { "key": 1, "value": 10 },
                { "key": 2, "value": 999 }
            ],
            "nan_value_counts": [],
            "lower_bounds": [],
            "upper_bounds": [],
            "key_metadata": null,
            "split_offsets": null,
            "equality_ids": null,
            "sort_order_id": null,
            "first_row_id": null,
            "referenced_data_file": null,
            "content_offset": null,
            "content_size_in_bytes": null
        });
        assert_eq!(pretty_json1, expected_serialized_file1);
        assert_eq!(pretty_json2, expected_serialized_file2);

        // Now deserialize the JSON strings back into DataFile objects
        let deserialized_files: Vec<DataFile> = serialized_files
            .into_iter()
            .map(|json| {
                deserialize_data_file_from_json(
                    &json,
                    partition_spec.spec_id(),
                    &partition_type,
                    &schema,
                )
                .unwrap()
            })
            .collect();

        // Verify we have the expected number of deserialized files
        assert_eq!(deserialized_files.len(), 2);
        let deserialized_data_file1 = deserialized_files.first().unwrap();
        let deserialized_data_file2 = deserialized_files.get(1).unwrap();
        let original_data_file1 = data_files.first().unwrap();
        let original_data_file2 = data_files.get(1).unwrap();

        assert_eq!(deserialized_data_file1, original_data_file1);
        assert_eq!(deserialized_data_file2, original_data_file2);
    }

    /// Metadata for the writer schema tests: a V2 data manifest whose spec has
    /// identity partitions on a long, a string, and a double column.
    fn writer_schema_test_metadata() -> ManifestMetadata {
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::optional(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                    NestedField::optional(2, "name", Type::Primitive(PrimitiveType::String)).into(),
                    NestedField::optional(3, "score", Type::Primitive(PrimitiveType::Double))
                        .into(),
                ])
                .build()
                .unwrap(),
        );
        let partition_spec = PartitionSpec::builder(schema.clone())
            .with_spec_id(0)
            .add_partition_field("id", "id", Transform::Identity)
            .unwrap()
            .add_partition_field("name", "name", Transform::Identity)
            .unwrap()
            .add_partition_field("score", "score", Transform::Identity)
            .unwrap()
            .build()
            .unwrap();
        ManifestMetadata {
            schema_id: 0,
            schema,
            partition_spec,
            content: ManifestContentType::Data,
            format_version: FormatVersion::V2,
        }
    }

    /// A V2 manifest entry writer schema with Java's record names and only the
    /// required `data_file` fields.
    fn v2_writer_schema(partition_fields: Value) -> Value {
        serde_json::json!({
            "type": "record",
            "name": "manifest_entry",
            "fields": [
                {"name": "status", "type": "int", "field-id": 0},
                {"name": "snapshot_id", "type": ["null", "long"], "default": null, "field-id": 1},
                {"name": "sequence_number", "type": ["null", "long"], "default": null, "field-id": 3},
                {"name": "file_sequence_number", "type": ["null", "long"], "default": null, "field-id": 4},
                {"name": "data_file", "field-id": 2, "type": {
                    "type": "record",
                    "name": "r2",
                    "fields": [
                        {"name": "content", "type": "int", "field-id": 134},
                        {"name": "file_path", "type": "string", "field-id": 100},
                        {"name": "file_format", "type": "string", "field-id": 101},
                        {"name": "partition", "field-id": 102, "type": {
                            "type": "record",
                            "name": "r102",
                            "fields": partition_fields,
                        }},
                        {"name": "record_count", "type": "long", "field-id": 103},
                        {"name": "file_size_in_bytes", "type": "long", "field-id": 104},
                    ],
                }},
            ],
        })
    }

    /// The field named by `path` in a record schema, descending through nested
    /// record types.
    fn writer_schema_field<'a>(record: &'a mut Value, path: &[&str]) -> &'a mut Value {
        let (name, rest) = path.split_first().unwrap();
        let field = record["fields"]
            .as_array_mut()
            .unwrap()
            .iter_mut()
            .find(|field| field["name"] == *name)
            .unwrap();
        if rest.is_empty() {
            field
        } else {
            writer_schema_field(&mut field["type"], rest)
        }
    }

    fn v2_entry(partition: Value) -> Value {
        serde_json::json!({
            "status": 1,
            "snapshot_id": 7,
            "sequence_number": null,
            "file_sequence_number": null,
            "data_file": {
                "content": 0,
                "file_path": "s3://bucket/table/data/a.parquet",
                "file_format": "PARQUET",
                "partition": partition,
                "record_count": 10,
                "file_size_in_bytes": 100,
            },
        })
    }

    fn expected_entry(partition: Struct) -> ManifestEntry {
        ManifestEntry {
            status: ManifestStatus::Added,
            snapshot_id: Some(7),
            sequence_number: None,
            file_sequence_number: None,
            data_file: DataFile {
                content: DataContentType::Data,
                file_path: "s3://bucket/table/data/a.parquet".to_string(),
                file_format: DataFileFormat::Parquet,
                partition,
                record_count: 10,
                file_size_in_bytes: 100,
                column_sizes: HashMap::new(),
                value_counts: HashMap::new(),
                null_value_counts: HashMap::new(),
                nan_value_counts: HashMap::new(),
                lower_bounds: HashMap::new(),
                upper_bounds: HashMap::new(),
                key_metadata: None,
                split_offsets: None,
                equality_ids: None,
                sort_order_id: None,
                partition_spec_id: 0,
                first_row_id: None,
                referenced_data_file: None,
                content_offset: None,
                content_size_in_bytes: None,
            },
        }
    }

    /// Writes `entries` as a manifest with the given writer schema, encoding each
    /// JSON value with the writer schema's types.
    fn write_with_writer_schema(
        metadata: &ManifestMetadata,
        writer_schema: &Value,
        entries: Vec<Value>,
    ) -> Vec<u8> {
        write_avro_values_with_writer_schema(
            metadata,
            writer_schema,
            entries
                .into_iter()
                .map(|entry| AvroValue::try_from(entry).unwrap())
                .collect(),
        )
    }

    /// Like [`write_with_writer_schema`], for entries that JSON can't express.
    fn write_avro_values_with_writer_schema(
        metadata: &ManifestMetadata,
        writer_schema: &Value,
        entries: Vec<AvroValue>,
    ) -> Vec<u8> {
        let avro_schema = apache_avro::Schema::parse(writer_schema).unwrap();
        let mut writer = Writer::new(&avro_schema, Vec::new()).unwrap();
        for (key, value) in [
            ("schema", to_vec(&metadata.schema).unwrap()),
            ("schema-id", metadata.schema_id.to_string().into_bytes()),
            (
                "partition-spec",
                to_vec(&metadata.partition_spec.fields()).unwrap(),
            ),
            (
                "partition-spec-id",
                metadata.partition_spec.spec_id().to_string().into_bytes(),
            ),
            (
                "format-version",
                (metadata.format_version as u8).to_string().into_bytes(),
            ),
            ("content", metadata.content.to_string().into_bytes()),
        ] {
            writer.add_user_metadata(key.to_string(), value).unwrap();
        }
        for entry in entries {
            writer
                .append_value(entry.resolve(&avro_schema).unwrap())
                .unwrap();
        }
        writer.into_inner().unwrap()
    }

    /// Removes the field named by `path` from a record schema and from a record
    /// value of that schema.
    fn remove_writer_field(schema: &mut Value, entry: &mut AvroValue, path: &[&str]) {
        let (name, rest) = path.split_first().unwrap();
        let AvroValue::Record(values) = entry else {
            unreachable!("the entry is a record");
        };
        let fields = schema["fields"].as_array_mut().unwrap();
        if rest.is_empty() {
            fields.retain(|field| field["name"] != *name);
            values.retain(|(value_name, _)| value_name != name);
        } else {
            let field = fields.iter_mut().find(|field| field["name"] == *name);
            let value = values.iter_mut().find(|(value_name, _)| value_name == name);
            remove_writer_field(&mut field.unwrap()["type"], &mut value.unwrap().1, rest);
        }
    }

    type ResetField = fn(&mut ManifestEntry);

    /// Removes each field in `optional` and `required` in turn from a manifest
    /// that `full_schema` and `full_value` write as `full_entry`. Checks that the
    /// manifest reads with an optional field reset and fails naming a required one.
    fn assert_reads_without_each_field(
        metadata: &ManifestMetadata,
        full_schema: &Value,
        full_value: &AvroValue,
        full_entry: &ManifestEntry,
        optional: &[(&[&str], ResetField)],
        required: &[&[&str]],
    ) {
        for (path, reset) in optional {
            let (mut schema, mut value) = (full_schema.clone(), full_value.clone());
            remove_writer_field(&mut schema, &mut value, path);
            let bs = write_avro_values_with_writer_schema(metadata, &schema, vec![value]);

            let manifest = Manifest::parse_avro(&bs).unwrap();

            let mut expected = full_entry.clone();
            reset(&mut expected);
            assert_eq!(
                manifest,
                Manifest::new(metadata.clone(), vec![expected]),
                "{path:?}"
            );
        }

        for path in required {
            let (mut schema, mut value) = (full_schema.clone(), full_value.clone());
            remove_writer_field(&mut schema, &mut value, path);
            let bs = write_avro_values_with_writer_schema(metadata, &schema, vec![value]);

            let err = Manifest::parse_avro(&bs).unwrap_err();

            assert!(
                err.to_string().contains(path.last().unwrap()),
                "{path:?}: {err}"
            );
        }
    }

    #[test]
    fn test_parse_manifest_without_each_field() {
        // Fields that the spec doesn't require in every version must read as
        // their default when the writer omits them.
        let metadata = writer_schema_test_metadata();
        let partition_type = metadata
            .partition_spec
            .partition_type(&metadata.schema)
            .unwrap();
        let full_schema =
            serde_json::to_value(manifest_schema_v2(&partition_type).unwrap()).unwrap();
        let mut full_entry = expected_entry(Struct::from_iter([
            Some(Literal::long(5)),
            Some(Literal::string("a")),
            Some(Literal::double(2.5)),
        ]));
        full_entry.sequence_number = Some(3);
        full_entry.file_sequence_number = Some(4);
        let data_file = &mut full_entry.data_file;
        data_file.content = DataContentType::PositionDeletes;
        data_file.column_sizes = HashMap::from([(1, 40)]);
        data_file.value_counts = HashMap::from([(1, 10)]);
        data_file.null_value_counts = HashMap::from([(1, 1)]);
        data_file.nan_value_counts = HashMap::from([(3, 2)]);
        data_file.lower_bounds = HashMap::from([(1, Datum::long(1))]);
        data_file.upper_bounds = HashMap::from([(1, Datum::long(9))]);
        data_file.key_metadata = Some(vec![1, 2]);
        data_file.split_offsets = Some(vec![4]);
        data_file.equality_ids = Some(vec![1]);
        data_file.sort_order_id = Some(0);
        data_file.first_row_id = Some(100);
        data_file.referenced_data_file = Some("s3://bucket/table/data/b.parquet".to_string());
        data_file.content_offset = Some(4);
        data_file.content_size_in_bytes = Some(8);
        let full_value = to_value(
            _serde::ManifestEntryV2::try_from(
                full_entry.clone(),
                &Type::Struct(partition_type.clone()),
            )
            .unwrap(),
        )
        .unwrap();

        let optional: [(&[&str], ResetField); 18] = [
            (&["snapshot_id"], |e| e.snapshot_id = None),
            (&["sequence_number"], |e| e.sequence_number = None),
            (&["file_sequence_number"], |e| e.file_sequence_number = None),
            (&["data_file", "content"], |e| {
                e.data_file.content = DataContentType::Data
            }),
            (&["data_file", "column_sizes"], |e| {
                e.data_file.column_sizes.clear()
            }),
            (&["data_file", "value_counts"], |e| {
                e.data_file.value_counts.clear()
            }),
            (&["data_file", "null_value_counts"], |e| {
                e.data_file.null_value_counts.clear()
            }),
            (&["data_file", "nan_value_counts"], |e| {
                e.data_file.nan_value_counts.clear()
            }),
            (&["data_file", "lower_bounds"], |e| {
                e.data_file.lower_bounds.clear()
            }),
            (&["data_file", "upper_bounds"], |e| {
                e.data_file.upper_bounds.clear()
            }),
            (&["data_file", "key_metadata"], |e| {
                e.data_file.key_metadata = None
            }),
            (&["data_file", "split_offsets"], |e| {
                e.data_file.split_offsets = None
            }),
            (&["data_file", "equality_ids"], |e| {
                e.data_file.equality_ids = None
            }),
            (&["data_file", "sort_order_id"], |e| {
                e.data_file.sort_order_id = None
            }),
            (&["data_file", "first_row_id"], |e| {
                e.data_file.first_row_id = None
            }),
            (&["data_file", "referenced_data_file"], |e| {
                e.data_file.referenced_data_file = None
            }),
            (&["data_file", "content_offset"], |e| {
                e.data_file.content_offset = None
            }),
            (&["data_file", "content_size_in_bytes"], |e| {
                e.data_file.content_size_in_bytes = None
            }),
        ];
        let required: [&[&str]; 7] = [
            &["status"],
            &["data_file"],
            &["data_file", "file_path"],
            &["data_file", "file_format"],
            &["data_file", "partition"],
            &["data_file", "record_count"],
            &["data_file", "file_size_in_bytes"],
        ];
        assert_reads_without_each_field(
            &metadata,
            &full_schema,
            &full_value,
            &full_entry,
            &optional,
            &required,
        );
    }

    #[test]
    fn test_parse_v1_manifest_without_each_field() {
        let mut metadata = writer_schema_test_metadata();
        metadata.format_version = FormatVersion::V1;
        let partition_type = metadata
            .partition_spec
            .partition_type(&metadata.schema)
            .unwrap();
        let full_schema =
            serde_json::to_value(manifest_schema_v1(&partition_type).unwrap()).unwrap();
        let mut full_entry = expected_entry(Struct::from_iter([
            Some(Literal::long(5)),
            Some(Literal::string("a")),
            Some(Literal::double(2.5)),
        ]));
        // V1 has no sequence numbers, and every V1 entry reads with 0.
        full_entry.sequence_number = Some(0);
        full_entry.file_sequence_number = Some(0);
        let data_file = &mut full_entry.data_file;
        data_file.column_sizes = HashMap::from([(1, 40)]);
        data_file.value_counts = HashMap::from([(1, 10)]);
        data_file.null_value_counts = HashMap::from([(1, 1)]);
        data_file.nan_value_counts = HashMap::from([(3, 2)]);
        data_file.lower_bounds = HashMap::from([(1, Datum::long(1))]);
        data_file.upper_bounds = HashMap::from([(1, Datum::long(9))]);
        data_file.key_metadata = Some(vec![1, 2]);
        data_file.split_offsets = Some(vec![4]);
        data_file.sort_order_id = Some(0);
        let full_value = to_value(
            _serde::ManifestEntryV1::try_from(
                full_entry.clone(),
                &Type::Struct(partition_type.clone()),
            )
            .unwrap(),
        )
        .unwrap();

        let optional: [(&[&str], ResetField); 10] = [
            // Required in V1, but deprecated and not read.
            (&["data_file", "block_size_in_bytes"], |_| {}),
            (&["data_file", "column_sizes"], |e| {
                e.data_file.column_sizes.clear()
            }),
            (&["data_file", "value_counts"], |e| {
                e.data_file.value_counts.clear()
            }),
            (&["data_file", "null_value_counts"], |e| {
                e.data_file.null_value_counts.clear()
            }),
            (&["data_file", "nan_value_counts"], |e| {
                e.data_file.nan_value_counts.clear()
            }),
            (&["data_file", "lower_bounds"], |e| {
                e.data_file.lower_bounds.clear()
            }),
            (&["data_file", "upper_bounds"], |e| {
                e.data_file.upper_bounds.clear()
            }),
            (&["data_file", "key_metadata"], |e| {
                e.data_file.key_metadata = None
            }),
            (&["data_file", "split_offsets"], |e| {
                e.data_file.split_offsets = None
            }),
            (&["data_file", "sort_order_id"], |e| {
                e.data_file.sort_order_id = None
            }),
        ];
        let required: [&[&str]; 8] = [
            &["status"],
            &["snapshot_id"],
            &["data_file"],
            &["data_file", "file_path"],
            &["data_file", "file_format"],
            &["data_file", "partition"],
            &["data_file", "record_count"],
            &["data_file", "file_size_in_bytes"],
        ];
        assert_reads_without_each_field(
            &metadata,
            &full_schema,
            &full_value,
            &full_entry,
            &optional,
            &required,
        );
    }

    #[test]
    fn test_parse_manifest_matches_partition_fields_by_name() {
        // The writer orders the partition fields differently from the spec, omits
        // `name`, and adds a field the spec doesn't have.
        let metadata = writer_schema_test_metadata();
        let writer_schema = v2_writer_schema(serde_json::json!([
            {"name": "score", "type": ["null", "double"], "default": null, "field-id": 1002},
            {"name": "extra", "type": ["null", "int"], "default": null, "field-id": 1003},
            {"name": "id", "type": ["null", "long"], "default": null, "field-id": 1000},
        ]));
        let bs = write_with_writer_schema(&metadata, &writer_schema, vec![v2_entry(
            serde_json::json!({"score": 2.5, "extra": 9, "id": 5}),
        )]);

        let manifest = Manifest::parse_avro(&bs).unwrap();

        let partition =
            Struct::from_iter([Some(Literal::long(5)), None, Some(Literal::double(2.5))]);
        assert_eq!(
            manifest,
            Manifest::new(metadata, vec![expected_entry(partition)])
        );
    }

    #[test]
    fn test_parse_manifest_promotes_writer_types() {
        // Avro promotes int to long and float to double.
        let metadata = writer_schema_test_metadata();
        let mut writer_schema = v2_writer_schema(serde_json::json!([
            {"name": "id", "type": ["null", "int"], "default": null, "field-id": 1000},
            {"name": "name", "type": ["null", "string"], "default": null, "field-id": 1001},
            {"name": "score", "type": ["null", "float"], "default": null, "field-id": 1002},
        ]));
        for field in ["record_count", "file_size_in_bytes"] {
            writer_schema_field(&mut writer_schema, &["data_file", field])["type"] =
                serde_json::json!("int");
        }
        let bs = write_with_writer_schema(&metadata, &writer_schema, vec![v2_entry(
            serde_json::json!({"id": 5, "name": "a", "score": 2.5}),
        )]);

        let manifest = Manifest::parse_avro(&bs).unwrap();

        let partition = Struct::from_iter([
            Some(Literal::long(5)),
            Some(Literal::string("a")),
            Some(Literal::double(2.5)),
        ]);
        assert_eq!(
            manifest,
            Manifest::new(metadata, vec![expected_entry(partition)])
        );
    }

    #[test]
    fn test_parse_manifest_reads_required_writer_fields_as_optional() {
        // Fields that are optional in the reader schema are written without a
        // union.
        let metadata = writer_schema_test_metadata();
        let mut writer_schema = v2_writer_schema(serde_json::json!([
            {"name": "id", "type": "long", "field-id": 1000},
            {"name": "name", "type": "string", "field-id": 1001},
            {"name": "score", "type": "double", "field-id": 1002},
        ]));
        *writer_schema_field(&mut writer_schema, &["snapshot_id"]) =
            serde_json::json!({"name": "snapshot_id", "type": "long", "field-id": 1});
        writer_schema_field(&mut writer_schema, &["data_file"])["type"]["fields"]
            .as_array_mut()
            .unwrap()
            .push(serde_json::json!({"name": "sort_order_id", "type": "int", "field-id": 140}));
        let mut entry = v2_entry(serde_json::json!({"id": 5, "name": "a", "score": 2.5}));
        entry["data_file"]["sort_order_id"] = serde_json::json!(3);
        let bs = write_with_writer_schema(&metadata, &writer_schema, vec![entry]);

        let manifest = Manifest::parse_avro(&bs).unwrap();

        let mut expected = expected_entry(Struct::from_iter([
            Some(Literal::long(5)),
            Some(Literal::string("a")),
            Some(Literal::double(2.5)),
        ]));
        expected.data_file.sort_order_id = Some(3);
        assert_eq!(manifest, Manifest::new(metadata, vec![expected]));
    }

    #[test]
    fn test_parse_manifest_reads_union_writer_fields_as_required() {
        // A field that is required in the reader schema is written as a union with
        // null.
        let metadata = writer_schema_test_metadata();
        let mut writer_schema = v2_writer_schema(serde_json::json!([
            {"name": "id", "type": ["null", "long"], "default": null, "field-id": 1000},
            {"name": "name", "type": ["null", "string"], "default": null, "field-id": 1001},
            {"name": "score", "type": ["null", "double"], "default": null, "field-id": 1002},
        ]));
        writer_schema_field(&mut writer_schema, &["data_file", "record_count"])["type"] =
            serde_json::json!(["null", "long"]);
        let partition = serde_json::json!({"id": 5, "name": "a", "score": 2.5});
        let bs =
            write_with_writer_schema(&metadata, &writer_schema, vec![v2_entry(partition.clone())]);

        let manifest = Manifest::parse_avro(&bs).unwrap();

        let expected = expected_entry(Struct::from_iter([
            Some(Literal::long(5)),
            Some(Literal::string("a")),
            Some(Literal::double(2.5)),
        ]));
        assert_eq!(manifest, Manifest::new(metadata.clone(), vec![expected]));

        let mut entry = v2_entry(partition);
        entry["data_file"]["record_count"] = Value::Null;
        let bs = write_with_writer_schema(&metadata, &writer_schema, vec![entry]);

        let err = Manifest::parse_avro(&bs).unwrap_err();

        assert_eq!(err.kind(), ErrorKind::DataInvalid);
    }

    #[test]
    fn test_parse_manifest_with_fixed_uuid_partition() {
        // The spec stores a uuid in Avro as a 16-byte fixed with logical type uuid.
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::optional(1, "u", Type::Primitive(PrimitiveType::Uuid)).into(),
                ])
                .build()
                .unwrap(),
        );
        let partition_spec = PartitionSpec::builder(schema.clone())
            .with_spec_id(0)
            .add_partition_field("u", "u", Transform::Identity)
            .unwrap()
            .build()
            .unwrap();
        let metadata = ManifestMetadata {
            schema_id: 0,
            schema,
            partition_spec,
            content: ManifestContentType::Data,
            format_version: FormatVersion::V2,
        };
        let writer_schema = v2_writer_schema(serde_json::json!([
            {"name": "u", "default": null, "field-id": 1000, "type": ["null", {
                "type": "fixed", "name": "uuid_fixed", "size": 16, "logicalType": "uuid",
            }]},
        ]));
        let uuid = uuid::Uuid::from_u128(0xf79c3e09_677c_4bbd_a479_3f349cb785e7);
        // `Value::resolve` doesn't convert a JSON string to a fixed uuid, so the
        // partition value is set as an Avro value.
        let mut entry = AvroValue::try_from(v2_entry(serde_json::json!({}))).unwrap();
        let AvroValue::Map(fields) = &mut entry else {
            unreachable!("a JSON object converts to an Avro map");
        };
        let Some(AvroValue::Map(data_file)) = fields.get_mut("data_file") else {
            unreachable!("a JSON object converts to an Avro map");
        };
        data_file.insert(
            "partition".to_string(),
            AvroValue::Map(HashMap::from([("u".to_string(), AvroValue::Uuid(uuid))])),
        );
        let bs = write_avro_values_with_writer_schema(&metadata, &writer_schema, vec![entry]);

        let manifest = Manifest::parse_avro(&bs).unwrap();

        let expected = expected_entry(Struct::from_iter([Some(Literal::uuid(uuid))]));
        assert_eq!(manifest, Manifest::new(metadata, vec![expected]));
    }

    #[test]
    fn test_parse_manifest_ignores_record_names_and_unknown_fields() {
        // Record names differ from the ones Java writes, and the writer has fields
        // the reader schema doesn't.
        let metadata = writer_schema_test_metadata();
        let mut writer_schema = v2_writer_schema(serde_json::json!([
            {"name": "id", "type": ["null", "long"], "default": null, "field-id": 1000},
            {"name": "name", "type": ["null", "string"], "default": null, "field-id": 1001},
            {"name": "score", "type": ["null", "double"], "default": null, "field-id": 1002},
        ]));
        writer_schema["name"] = serde_json::json!("entry");
        writer_schema["fields"]
            .as_array_mut()
            .unwrap()
            .push(serde_json::json!({"name": "unknown", "type": "string"}));
        let data_file = &mut writer_schema_field(&mut writer_schema, &["data_file"])["type"];
        data_file["name"] = serde_json::json!("file");
        data_file["fields"][3]["type"]["name"] = serde_json::json!("part");
        let data_file_fields = data_file["fields"].as_array_mut().unwrap();
        data_file_fields.push(serde_json::json!({
            "name": "column_sizes",
            "field-id": 108,
            "default": null,
            "type": ["null", {
                "type": "array",
                "logicalType": "map",
                "items": {
                    "type": "record",
                    "name": "sizes",
                    "fields": [
                        {"name": "key", "type": "int", "field-id": 117},
                        {"name": "value", "type": "long", "field-id": 118},
                    ],
                },
            }],
        }));
        data_file_fields.push(
            serde_json::json!({"name": "block_size_in_bytes", "type": "long", "field-id": 105}),
        );
        let mut entry = v2_entry(serde_json::json!({"id": 5, "name": "a", "score": 2.5}));
        entry["unknown"] = serde_json::json!("ignored");
        entry["data_file"]["column_sizes"] = serde_json::json!([{"key": 1, "value": 40}]);
        entry["data_file"]["block_size_in_bytes"] = serde_json::json!(64);
        let bs = write_with_writer_schema(&metadata, &writer_schema, vec![entry]);

        let manifest = Manifest::parse_avro(&bs).unwrap();

        let mut expected = expected_entry(Struct::from_iter([
            Some(Literal::long(5)),
            Some(Literal::string("a")),
            Some(Literal::double(2.5)),
        ]));
        expected.data_file.column_sizes = HashMap::from([(1, 40)]);
        assert_eq!(manifest, Manifest::new(metadata, vec![expected]));
    }

    #[test]
    fn test_parse_manifest_equality_ids_written_as_long() {
        // PyIceberg wrote `equality_ids` as `array<long>` before
        // apache/iceberg-python#3842.
        let metadata = writer_schema_test_metadata();
        let mut writer_schema = v2_writer_schema(serde_json::json!([
            {"name": "id", "type": ["null", "long"], "default": null, "field-id": 1000},
            {"name": "name", "type": ["null", "string"], "default": null, "field-id": 1001},
            {"name": "score", "type": ["null", "double"], "default": null, "field-id": 1002},
        ]));
        writer_schema_field(&mut writer_schema, &["data_file"])["type"]["fields"]
            .as_array_mut()
            .unwrap()
            .push(serde_json::json!({
                "name": "equality_ids",
                "field-id": 135,
                "default": null,
                "type": ["null", {"type": "array", "items": "long", "element-id": 136}],
            }));
        let mut entry = v2_entry(serde_json::json!({"id": 5, "name": "a", "score": 2.5}));
        entry["data_file"]["equality_ids"] = serde_json::json!([1, 2]);
        let bs = write_with_writer_schema(&metadata, &writer_schema, vec![entry]);

        let manifest = Manifest::parse_avro(&bs).unwrap();

        let mut expected = expected_entry(Struct::from_iter([
            Some(Literal::long(5)),
            Some(Literal::string("a")),
            Some(Literal::double(2.5)),
        ]));
        expected.data_file.equality_ids = Some(vec![1, 2]);
        assert_eq!(manifest, Manifest::new(metadata, vec![expected]));
    }

    #[test]
    fn test_parse_manifest_rejects_equality_id_above_int_range() {
        let metadata = writer_schema_test_metadata();
        let mut writer_schema = v2_writer_schema(serde_json::json!([
            {"name": "id", "type": ["null", "long"], "default": null, "field-id": 1000},
            {"name": "name", "type": ["null", "string"], "default": null, "field-id": 1001},
            {"name": "score", "type": ["null", "double"], "default": null, "field-id": 1002},
        ]));
        writer_schema_field(&mut writer_schema, &["data_file"])["type"]["fields"]
            .as_array_mut()
            .unwrap()
            .push(serde_json::json!({
                "name": "equality_ids",
                "field-id": 135,
                "default": null,
                "type": ["null", {"type": "array", "items": "long", "element-id": 136}],
            }));
        let mut entry = v2_entry(serde_json::json!({"id": 5, "name": "a", "score": 2.5}));
        entry["data_file"]["equality_ids"] = serde_json::json!([i64::from(i32::MAX) + 1]);
        let bs = write_with_writer_schema(&metadata, &writer_schema, vec![entry]);

        let err = Manifest::parse_avro(&bs).unwrap_err();

        assert_eq!(err.kind(), ErrorKind::DataInvalid);
    }

    #[test]
    fn test_parse_manifest_written_by_pyiceberg() {
        let bs = fs::read(format!(
            "{}/testdata/manifests/pyiceberg-v2-data.avro",
            env!("CARGO_MANIFEST_DIR")
        ))
        .unwrap();

        let manifest = Manifest::parse_avro(&bs).unwrap();

        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::optional(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                    NestedField::optional(2, "category", Type::Primitive(PrimitiveType::String))
                        .into(),
                    NestedField::optional(3, "score", Type::Primitive(PrimitiveType::Double))
                        .into(),
                ])
                .build()
                .unwrap(),
        );
        let partition_spec = PartitionSpec::builder(schema.clone())
            .with_spec_id(0)
            .add_partition_field("category", "category", Transform::Identity)
            .unwrap()
            .build()
            .unwrap();
        let metadata = ManifestMetadata {
            schema_id: 0,
            schema,
            partition_spec,
            content: ManifestContentType::Data,
            format_version: FormatVersion::V2,
        };
        let entries = (0..2)
            .map(|i: i64| {
                let category = format!("c{i}");
                ManifestEntry {
                    status: ManifestStatus::Added,
                    snapshot_id: Some(7),
                    sequence_number: Some(1),
                    file_sequence_number: Some(1),
                    data_file: DataFile {
                        content: DataContentType::Data,
                        file_path: format!(
                            "s3://bucket/table/data/category={category}/0000{i}.parquet"
                        ),
                        file_format: DataFileFormat::Parquet,
                        partition: Struct::from_iter([Some(Literal::string(&category))]),
                        record_count: 100 + i as u64,
                        file_size_in_bytes: 1000 + i as u64,
                        column_sizes: HashMap::from([
                            (1, 10 + i as u64),
                            (2, 20 + i as u64),
                            (3, 30 + i as u64),
                        ]),
                        value_counts: HashMap::from([
                            (1, 100 + i as u64),
                            (2, 100 + i as u64),
                            (3, 100 + i as u64),
                        ]),
                        null_value_counts: HashMap::from([(1, 0), (2, i as u64), (3, 1)]),
                        nan_value_counts: HashMap::from([(3, i as u64)]),
                        lower_bounds: HashMap::from([
                            (1, Datum::long(i)),
                            (2, Datum::string(&category)),
                        ]),
                        upper_bounds: HashMap::from([
                            (1, Datum::long(i + 50)),
                            (2, Datum::string(&category)),
                        ]),
                        key_metadata: None,
                        split_offsets: Some(vec![4]),
                        equality_ids: None,
                        sort_order_id: Some(0),
                        partition_spec_id: 0,
                        first_row_id: None,
                        referenced_data_file: None,
                        content_offset: None,
                        content_size_in_bytes: None,
                    },
                }
            })
            .collect();
        assert_eq!(manifest, Manifest::new(metadata, entries));
    }

    #[test]
    fn test_parse_manifest_with_repeated_named_type_definitions() {
        let bs = fs::read(format!(
            "{}/testdata/manifests/repeated-decimal-type-definitions.avro",
            env!("CARGO_MANIFEST_DIR")
        ))
        .unwrap();

        let manifest = Manifest::parse_avro(&bs).unwrap();

        assert_eq!(
            *manifest.entries()[0].data_file().partition(),
            Struct::from_iter([Some(Literal::decimal(12345)), Some(Literal::decimal(-678))])
        );
    }

    #[test]
    fn test_parse_manifest_with_repeated_named_type_definitions_reports_original_error() {
        let bs = fs::read(format!(
            "{}/testdata/manifests/repeated-decimal-type-definitions.avro",
            env!("CARGO_MANIFEST_DIR")
        ))
        .unwrap();
        // Cut the header's sync marker in half, so the rewritten header parses
        // its schema but fails to read the marker. The file ends with the marker.
        let marker = &bs[bs.len() - 16..];
        let header_marker = bs.windows(16).position(|w| w == marker).unwrap();
        let truncated = &bs[..header_marker + 8];

        let err = Manifest::parse_avro(truncated).unwrap_err();

        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        let message = err.to_string();
        assert!(
            message.contains("Two named schema defined for same fullname"),
            "{message}"
        );
        assert!(message.contains("Failed to read marker bytes"), "{message}");
    }
}
