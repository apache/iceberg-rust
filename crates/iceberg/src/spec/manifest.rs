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

//! Manifest for Iceberg.
use std::cmp::min;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use apache_avro::{from_value, to_value, Reader as AvroReader, Writer as AvroWriter};
use bytes::Bytes;
use serde_json::to_vec;
use typed_builder::TypedBuilder;

use self::_const_schema::{manifest_schema_v1, manifest_schema_v2};
use super::{
    Datum, FieldSummary, FormatVersion, ManifestContentType, ManifestFile, PartitionSpec, Schema,
    SchemaId, Struct, INITIAL_SEQUENCE_NUMBER, UNASSIGNED_SEQUENCE_NUMBER,
};
use crate::error::Result;
use crate::io::OutputFile;
use crate::spec::PartitionField;
use crate::{Error, ErrorKind};

/// A manifest contains metadata and a list of entries.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Manifest {
    metadata: ManifestMetadata,
    entries: Vec<ManifestEntryRef>,
}

impl Manifest {
    /// Parse manifest metadata and entries from bytes of avro file.
    pub(crate) fn try_from_avro_bytes(bs: &[u8]) -> Result<(ManifestMetadata, Vec<ManifestEntry>)> {
        let reader = AvroReader::new(bs)?;

        // Parse manifest metadata
        let meta = reader.user_metadata();
        let metadata = ManifestMetadata::parse(meta)?;

        // Parse manifest entries
        let partition_type = metadata.partition_spec.partition_type(&metadata.schema)?;

        let entries = match metadata.format_version {
            FormatVersion::V1 => {
                let schema = manifest_schema_v1(partition_type.clone())?;
                let reader = AvroReader::with_schema(&schema, bs)?;
                reader
                    .into_iter()
                    .map(|value| {
                        from_value::<_serde::ManifestEntryV1>(&value?)?
                            .try_into(&partition_type, &metadata.schema)
                    })
                    .collect::<Result<Vec<_>>>()?
            }
            FormatVersion::V2 => {
                let schema = manifest_schema_v2(partition_type.clone())?;
                let reader = AvroReader::with_schema(&schema, bs)?;
                reader
                    .into_iter()
                    .map(|value| {
                        from_value::<_serde::ManifestEntryV2>(&value?)?
                            .try_into(&partition_type, &metadata.schema)
                    })
                    .collect::<Result<Vec<_>>>()?
            }
        };

        Ok((metadata, entries))
    }

    /// Parse manifest from bytes of avro file.
    pub fn parse_avro(bs: &[u8]) -> Result<Self> {
        let (metadata, entries) = Self::try_from_avro_bytes(bs)?;
        Ok(Self::new(metadata, entries))
    }

    /// Entries slice.
    pub fn entries(&self) -> &[ManifestEntryRef] {
        &self.entries
    }

    /// Consume this Manifest, returning its constituent parts
    pub fn consume(self) -> (Vec<ManifestEntryRef>, ManifestMetadata) {
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

/// A manifest writer.
pub struct ManifestWriter {
    output: OutputFile,

    snapshot_id: i64,

    added_files: u32,
    added_rows: u64,
    existing_files: u32,
    existing_rows: u64,
    deleted_files: u32,
    deleted_rows: u64,

    min_seq_num: Option<i64>,

    key_metadata: Vec<u8>,

    field_summary: HashMap<i32, FieldSummary>,
}

impl ManifestWriter {
    /// Create a new manifest writer.
    pub fn new(output: OutputFile, snapshot_id: i64, key_metadata: Vec<u8>) -> Self {
        Self {
            output,
            snapshot_id,
            added_files: 0,
            added_rows: 0,
            existing_files: 0,
            existing_rows: 0,
            deleted_files: 0,
            deleted_rows: 0,
            min_seq_num: None,
            key_metadata,
            field_summary: HashMap::new(),
        }
    }

    fn update_field_summary(&mut self, entry: &ManifestEntry) {
        // Update field summary
        for (&k, &v) in &entry.data_file.null_value_counts {
            let field_summary = self.field_summary.entry(k).or_default();
            if v > 0 {
                field_summary.contains_null = true;
            }
        }

        for (&k, &v) in &entry.data_file.nan_value_counts {
            let field_summary = self.field_summary.entry(k).or_default();
            if v > 0 {
                field_summary.contains_nan = Some(true);
            }
            if v == 0 {
                field_summary.contains_nan = Some(false);
            }
        }

        for (&k, v) in &entry.data_file.lower_bounds {
            let field_summary = self.field_summary.entry(k).or_default();
            if let Some(cur) = &field_summary.lower_bound {
                if v < cur {
                    field_summary.lower_bound = Some(v.clone());
                }
            } else {
                field_summary.lower_bound = Some(v.clone());
            }
        }

        for (&k, v) in &entry.data_file.upper_bounds {
            let field_summary = self.field_summary.entry(k).or_default();
            if let Some(cur) = &field_summary.upper_bound {
                if v > cur {
                    field_summary.upper_bound = Some(v.clone());
                }
            } else {
                field_summary.upper_bound = Some(v.clone());
            }
        }
    }

    fn get_field_summary_vec(&mut self, spec_fields: &[PartitionField]) -> Vec<FieldSummary> {
        let mut partition_summary = Vec::with_capacity(self.field_summary.len());
        for field in spec_fields {
            let entry = self
                .field_summary
                .remove(&field.source_id)
                .unwrap_or_default();
            partition_summary.push(entry);
        }
        partition_summary
    }

    /// Write a manifest.
    pub async fn write(mut self, manifest: Manifest) -> Result<ManifestFile> {
        // Create the avro writer
        let partition_type = manifest
            .metadata
            .partition_spec
            .partition_type(&manifest.metadata.schema)?;
        let table_schema = &manifest.metadata.schema;
        let avro_schema = match manifest.metadata.format_version {
            FormatVersion::V1 => manifest_schema_v1(partition_type.clone())?,
            FormatVersion::V2 => manifest_schema_v2(partition_type.clone())?,
        };
        let mut avro_writer = AvroWriter::new(&avro_schema, Vec::new());
        avro_writer.add_user_metadata(
            "schema".to_string(),
            to_vec(table_schema).map_err(|err| {
                Error::new(ErrorKind::DataInvalid, "Fail to serialize table schema")
                    .with_source(err)
            })?,
        )?;
        avro_writer.add_user_metadata(
            "schema-id".to_string(),
            table_schema.schema_id().to_string(),
        )?;
        avro_writer.add_user_metadata(
            "partition-spec".to_string(),
            to_vec(&manifest.metadata.partition_spec.fields).map_err(|err| {
                Error::new(ErrorKind::DataInvalid, "Fail to serialize partition spec")
                    .with_source(err)
            })?,
        )?;
        avro_writer.add_user_metadata(
            "partition-spec-id".to_string(),
            manifest.metadata.partition_spec.spec_id.to_string(),
        )?;
        avro_writer.add_user_metadata(
            "format-version".to_string(),
            (manifest.metadata.format_version as u8).to_string(),
        )?;
        if manifest.metadata.format_version == FormatVersion::V2 {
            avro_writer
                .add_user_metadata("content".to_string(), manifest.metadata.content.to_string())?;
        }

        // Write manifest entries
        for entry in manifest.entries {
            if (entry.status == ManifestStatus::Deleted || entry.status == ManifestStatus::Existing)
                && (entry.sequence_number.is_none() || entry.file_sequence_number.is_none())
            {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    "Manifest entry with status Existing or Deleted should have sequence number",
                ));
            }

            match entry.status {
                ManifestStatus::Added => {
                    self.added_files += 1;
                    self.added_rows += entry.data_file.record_count;
                }
                ManifestStatus::Deleted => {
                    self.deleted_files += 1;
                    self.deleted_rows += entry.data_file.record_count;
                }
                ManifestStatus::Existing => {
                    self.existing_files += 1;
                    self.existing_rows += entry.data_file.record_count;
                }
            }

            if entry.is_alive() {
                if let Some(seq_num) = entry.sequence_number {
                    self.min_seq_num = Some(self.min_seq_num.map_or(seq_num, |v| min(v, seq_num)));
                }
            }

            self.update_field_summary(&entry);

            let value = match manifest.metadata.format_version {
                FormatVersion::V1 => to_value(_serde::ManifestEntryV1::try_from(
                    (*entry).clone(),
                    &partition_type,
                )?)?
                .resolve(&avro_schema)?,
                FormatVersion::V2 => to_value(_serde::ManifestEntryV2::try_from(
                    (*entry).clone(),
                    &partition_type,
                )?)?
                .resolve(&avro_schema)?,
            };

            avro_writer.append(value)?;
        }

        let content = avro_writer.into_inner()?;
        let length = content.len();
        self.output.write(Bytes::from(content)).await?;

        let partition_summary =
            self.get_field_summary_vec(&manifest.metadata.partition_spec.fields);

        Ok(ManifestFile {
            manifest_path: self.output.location().to_string(),
            manifest_length: length as i64,
            partition_spec_id: manifest.metadata.partition_spec.spec_id,
            content: manifest.metadata.content,
            // sequence_number and min_sequence_number with UNASSIGNED_SEQUENCE_NUMBER will be replace with
            // real sequence number in `ManifestListWriter`.
            sequence_number: UNASSIGNED_SEQUENCE_NUMBER,
            min_sequence_number: self.min_seq_num.unwrap_or(UNASSIGNED_SEQUENCE_NUMBER),
            added_snapshot_id: self.snapshot_id,
            added_files_count: Some(self.added_files),
            existing_files_count: Some(self.existing_files),
            deleted_files_count: Some(self.deleted_files),
            added_rows_count: Some(self.added_rows),
            existing_rows_count: Some(self.existing_rows),
            deleted_rows_count: Some(self.deleted_rows),
            partitions: partition_summary,
            key_metadata: self.key_metadata,
        })
    }
}

/// This is a helper module that defines the schema field of the manifest list entry.
mod _const_schema {
    use std::sync::Arc;

    use apache_avro::Schema as AvroSchema;
    use once_cell::sync::Lazy;

    use crate::avro::schema_to_avro_schema;
    use crate::spec::{
        ListType, MapType, NestedField, NestedFieldRef, PrimitiveType, Schema, StructType, Type,
    };
    use crate::Error;

    static STATUS: Lazy<NestedFieldRef> = {
        Lazy::new(|| {
            Arc::new(NestedField::required(
                0,
                "status",
                Type::Primitive(PrimitiveType::Int),
            ))
        })
    };

    static SNAPSHOT_ID_V1: Lazy<NestedFieldRef> = {
        Lazy::new(|| {
            Arc::new(NestedField::required(
                1,
                "snapshot_id",
                Type::Primitive(PrimitiveType::Long),
            ))
        })
    };

    static SNAPSHOT_ID_V2: Lazy<NestedFieldRef> = {
        Lazy::new(|| {
            Arc::new(NestedField::optional(
                1,
                "snapshot_id",
                Type::Primitive(PrimitiveType::Long),
            ))
        })
    };

    static SEQUENCE_NUMBER: Lazy<NestedFieldRef> = {
        Lazy::new(|| {
            Arc::new(NestedField::optional(
                3,
                "sequence_number",
                Type::Primitive(PrimitiveType::Long),
            ))
        })
    };

    static FILE_SEQUENCE_NUMBER: Lazy<NestedFieldRef> = {
        Lazy::new(|| {
            Arc::new(NestedField::optional(
                4,
                "file_sequence_number",
                Type::Primitive(PrimitiveType::Long),
            ))
        })
    };

    static CONTENT: Lazy<NestedFieldRef> = {
        Lazy::new(|| {
            Arc::new(NestedField::required(
                134,
                "content",
                Type::Primitive(PrimitiveType::Int),
            ))
        })
    };

    static FILE_PATH: Lazy<NestedFieldRef> = {
        Lazy::new(|| {
            Arc::new(NestedField::required(
                100,
                "file_path",
                Type::Primitive(PrimitiveType::String),
            ))
        })
    };

    static FILE_FORMAT: Lazy<NestedFieldRef> = {
        Lazy::new(|| {
            Arc::new(NestedField::required(
                101,
                "file_format",
                Type::Primitive(PrimitiveType::String),
            ))
        })
    };

    static RECORD_COUNT: Lazy<NestedFieldRef> = {
        Lazy::new(|| {
            Arc::new(NestedField::required(
                103,
                "record_count",
                Type::Primitive(PrimitiveType::Long),
            ))
        })
    };

    static FILE_SIZE_IN_BYTES: Lazy<NestedFieldRef> = {
        Lazy::new(|| {
            Arc::new(NestedField::required(
                104,
                "file_size_in_bytes",
                Type::Primitive(PrimitiveType::Long),
            ))
        })
    };

    // Deprecated. Always write a default in v1. Do not write in v2.
    static BLOCK_SIZE_IN_BYTES: Lazy<NestedFieldRef> = {
        Lazy::new(|| {
            Arc::new(NestedField::required(
                105,
                "block_size_in_bytes",
                Type::Primitive(PrimitiveType::Long),
            ))
        })
    };

    static COLUMN_SIZES: Lazy<NestedFieldRef> = {
        Lazy::new(|| {
            Arc::new(NestedField::optional(
                108,
                "column_sizes",
                Type::Map(MapType {
                    key_field: Arc::new(NestedField::required(
                        117,
                        "key",
                        Type::Primitive(PrimitiveType::Int),
                    )),
                    value_field: Arc::new(NestedField::required(
                        118,
                        "value",
                        Type::Primitive(PrimitiveType::Long),
                    )),
                }),
            ))
        })
    };

    static VALUE_COUNTS: Lazy<NestedFieldRef> = {
        Lazy::new(|| {
            Arc::new(NestedField::optional(
                109,
                "value_counts",
                Type::Map(MapType {
                    key_field: Arc::new(NestedField::required(
                        119,
                        "key",
                        Type::Primitive(PrimitiveType::Int),
                    )),
                    value_field: Arc::new(NestedField::required(
                        120,
                        "value",
                        Type::Primitive(PrimitiveType::Long),
                    )),
                }),
            ))
        })
    };

    static NULL_VALUE_COUNTS: Lazy<NestedFieldRef> = {
        Lazy::new(|| {
            Arc::new(NestedField::optional(
                110,
                "null_value_counts",
                Type::Map(MapType {
                    key_field: Arc::new(NestedField::required(
                        121,
                        "key",
                        Type::Primitive(PrimitiveType::Int),
                    )),
                    value_field: Arc::new(NestedField::required(
                        122,
                        "value",
                        Type::Primitive(PrimitiveType::Long),
                    )),
                }),
            ))
        })
    };

    static NAN_VALUE_COUNTS: Lazy<NestedFieldRef> = {
        Lazy::new(|| {
            Arc::new(NestedField::optional(
                137,
                "nan_value_counts",
                Type::Map(MapType {
                    key_field: Arc::new(NestedField::required(
                        138,
                        "key",
                        Type::Primitive(PrimitiveType::Int),
                    )),
                    value_field: Arc::new(NestedField::required(
                        139,
                        "value",
                        Type::Primitive(PrimitiveType::Long),
                    )),
                }),
            ))
        })
    };

    static LOWER_BOUNDS: Lazy<NestedFieldRef> = {
        Lazy::new(|| {
            Arc::new(NestedField::optional(
                125,
                "lower_bounds",
                Type::Map(MapType {
                    key_field: Arc::new(NestedField::required(
                        126,
                        "key",
                        Type::Primitive(PrimitiveType::Int),
                    )),
                    value_field: Arc::new(NestedField::required(
                        127,
                        "value",
                        Type::Primitive(PrimitiveType::Binary),
                    )),
                }),
            ))
        })
    };

    static UPPER_BOUNDS: Lazy<NestedFieldRef> = {
        Lazy::new(|| {
            Arc::new(NestedField::optional(
                128,
                "upper_bounds",
                Type::Map(MapType {
                    key_field: Arc::new(NestedField::required(
                        129,
                        "key",
                        Type::Primitive(PrimitiveType::Int),
                    )),
                    value_field: Arc::new(NestedField::required(
                        130,
                        "value",
                        Type::Primitive(PrimitiveType::Binary),
                    )),
                }),
            ))
        })
    };

    static KEY_METADATA: Lazy<NestedFieldRef> = {
        Lazy::new(|| {
            Arc::new(NestedField::optional(
                131,
                "key_metadata",
                Type::Primitive(PrimitiveType::Binary),
            ))
        })
    };

    static SPLIT_OFFSETS: Lazy<NestedFieldRef> = {
        Lazy::new(|| {
            Arc::new(NestedField::optional(
                132,
                "split_offsets",
                Type::List(ListType {
                    element_field: Arc::new(NestedField::required(
                        133,
                        "element",
                        Type::Primitive(PrimitiveType::Long),
                    )),
                }),
            ))
        })
    };

    static EQUALITY_IDS: Lazy<NestedFieldRef> = {
        Lazy::new(|| {
            Arc::new(NestedField::optional(
                135,
                "equality_ids",
                Type::List(ListType {
                    element_field: Arc::new(NestedField::required(
                        136,
                        "element",
                        Type::Primitive(PrimitiveType::Int),
                    )),
                }),
            ))
        })
    };

    static SORT_ORDER_ID: Lazy<NestedFieldRef> = {
        Lazy::new(|| {
            Arc::new(NestedField::optional(
                140,
                "sort_order_id",
                Type::Primitive(PrimitiveType::Int),
            ))
        })
    };

    pub(super) fn manifest_schema_v2(partition_type: StructType) -> Result<AvroSchema, Error> {
        let fields = vec![
            STATUS.clone(),
            SNAPSHOT_ID_V2.clone(),
            SEQUENCE_NUMBER.clone(),
            FILE_SEQUENCE_NUMBER.clone(),
            Arc::new(NestedField::required(
                2,
                "data_file",
                Type::Struct(StructType::new(vec![
                    CONTENT.clone(),
                    FILE_PATH.clone(),
                    FILE_FORMAT.clone(),
                    Arc::new(NestedField::required(
                        102,
                        "partition",
                        Type::Struct(partition_type),
                    )),
                    RECORD_COUNT.clone(),
                    FILE_SIZE_IN_BYTES.clone(),
                    COLUMN_SIZES.clone(),
                    VALUE_COUNTS.clone(),
                    NULL_VALUE_COUNTS.clone(),
                    NAN_VALUE_COUNTS.clone(),
                    LOWER_BOUNDS.clone(),
                    UPPER_BOUNDS.clone(),
                    KEY_METADATA.clone(),
                    SPLIT_OFFSETS.clone(),
                    EQUALITY_IDS.clone(),
                    SORT_ORDER_ID.clone(),
                ])),
            )),
        ];
        let schema = Schema::builder().with_fields(fields).build()?;
        schema_to_avro_schema("manifest_entry", &schema)
    }

    pub(super) fn manifest_schema_v1(partition_type: StructType) -> Result<AvroSchema, Error> {
        let fields = vec![
            STATUS.clone(),
            SNAPSHOT_ID_V1.clone(),
            Arc::new(NestedField::required(
                2,
                "data_file",
                Type::Struct(StructType::new(vec![
                    FILE_PATH.clone(),
                    FILE_FORMAT.clone(),
                    Arc::new(NestedField::required(
                        102,
                        "partition",
                        Type::Struct(partition_type),
                    )),
                    RECORD_COUNT.clone(),
                    FILE_SIZE_IN_BYTES.clone(),
                    BLOCK_SIZE_IN_BYTES.clone(),
                    COLUMN_SIZES.clone(),
                    VALUE_COUNTS.clone(),
                    NULL_VALUE_COUNTS.clone(),
                    NAN_VALUE_COUNTS.clone(),
                    LOWER_BOUNDS.clone(),
                    UPPER_BOUNDS.clone(),
                    KEY_METADATA.clone(),
                    SPLIT_OFFSETS.clone(),
                    SORT_ORDER_ID.clone(),
                ])),
            )),
        ];
        let schema = Schema::builder().with_fields(fields).build()?;
        schema_to_avro_schema("manifest_entry", &schema)
    }
}

/// Meta data of a manifest that is stored in the key-value metadata of the Avro file
#[derive(Debug, PartialEq, Clone, Eq, TypedBuilder)]
pub struct ManifestMetadata {
    /// The table schema at the time the manifest
    /// was written
    schema: Schema,
    /// ID of the schema used to write the manifest as a string
    schema_id: SchemaId,
    /// The partition spec used  to write the manifest
    partition_spec: PartitionSpec,
    /// Table format version number of the manifest as a string
    format_version: FormatVersion,
    /// Type of content files tracked by the manifest: “data” or “deletes”
    content: ManifestContentType,
}

impl ManifestMetadata {
    /// Parse from metadata in avro file.
    pub fn parse(meta: &HashMap<String, Vec<u8>>) -> Result<Self> {
        let schema = {
            let bs = meta.get("schema").ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    "schema is required in manifest metadata but not found",
                )
            })?;
            serde_json::from_slice::<Schema>(bs).map_err(|err| {
                Error::new(
                    ErrorKind::DataInvalid,
                    "Fail to parse schema in manifest metadata",
                )
                .with_source(err)
            })?
        };
        let schema_id: i32 = meta
            .get("schema-id")
            .map(|bs| {
                String::from_utf8_lossy(bs).parse().map_err(|err| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        "Fail to parse schema id in manifest metadata",
                    )
                    .with_source(err)
                })
            })
            .transpose()?
            .unwrap_or(0);
        let partition_spec = {
            let fields = {
                let bs = meta.get("partition-spec").ok_or_else(|| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        "partition-spec is required in manifest metadata but not found",
                    )
                })?;
                serde_json::from_slice::<Vec<PartitionField>>(bs).map_err(|err| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        "Fail to parse partition spec in manifest metadata",
                    )
                    .with_source(err)
                })?
            };
            let spec_id = meta
                .get("partition-spec-id")
                .map(|bs| {
                    String::from_utf8_lossy(bs).parse().map_err(|err| {
                        Error::new(
                            ErrorKind::DataInvalid,
                            "Fail to parse partition spec id in manifest metadata",
                        )
                        .with_source(err)
                    })
                })
                .transpose()?
                .unwrap_or(0);
            PartitionSpec { spec_id, fields }
        };
        let format_version = if let Some(bs) = meta.get("format-version") {
            serde_json::from_slice::<FormatVersion>(bs).map_err(|err| {
                Error::new(
                    ErrorKind::DataInvalid,
                    "Fail to parse format version in manifest metadata",
                )
                .with_source(err)
            })?
        } else {
            FormatVersion::V1
        };
        let content = if let Some(v) = meta.get("content") {
            let v = String::from_utf8_lossy(v);
            v.parse()?
        } else {
            ManifestContentType::Data
        };
        Ok(ManifestMetadata {
            schema,
            schema_id,
            partition_spec,
            format_version,
            content,
        })
    }
}

/// Reference to [`ManifestEntry`].
pub type ManifestEntryRef = Arc<ManifestEntry>;

/// A manifest is an immutable Avro file that lists data files or delete
/// files, along with each file’s partition data tuple, metrics, and tracking
/// information.
#[derive(Debug, PartialEq, Eq, Clone, TypedBuilder)]
pub struct ManifestEntry {
    /// field: 0
    ///
    /// Used to track additions and deletions.
    status: ManifestStatus,
    /// field id: 1
    ///
    /// Snapshot id where the file was added, or deleted if status is 2.
    /// Inherited when null.
    #[builder(default, setter(strip_option))]
    snapshot_id: Option<i64>,
    /// field id: 3
    ///
    /// Data sequence number of the file.
    /// Inherited when null and status is 1 (added).
    #[builder(default, setter(strip_option))]
    sequence_number: Option<i64>,
    /// field id: 4
    ///
    /// File sequence number indicating when the file was added.
    /// Inherited when null and status is 1 (added).
    #[builder(default, setter(strip_option))]
    file_sequence_number: Option<i64>,
    /// field id: 2
    ///
    /// File path, partition tuple, metrics, …
    data_file: DataFile,
}

impl ManifestEntry {
    /// Check if this manifest entry is deleted.
    pub fn is_alive(&self) -> bool {
        matches!(
            self.status,
            ManifestStatus::Added | ManifestStatus::Existing
        )
    }

    /// Content type of this manifest entry.
    #[inline]
    pub fn content_type(&self) -> DataContentType {
        self.data_file.content
    }

    /// File format of this manifest entry.
    #[inline]
    pub fn file_format(&self) -> DataFileFormat {
        self.data_file.file_format
    }

    /// Data file path of this manifest entry.
    #[inline]
    pub fn file_path(&self) -> &str {
        &self.data_file.file_path
    }

    /// Inherit data from manifest list, such as snapshot id, sequence number.
    pub(crate) fn inherit_data(&mut self, snapshot_entry: &ManifestFile) {
        if self.snapshot_id.is_none() {
            self.snapshot_id = Some(snapshot_entry.added_snapshot_id);
        }

        if self.sequence_number.is_none()
            && (self.status == ManifestStatus::Added
                || snapshot_entry.sequence_number == INITIAL_SEQUENCE_NUMBER)
        {
            self.sequence_number = Some(snapshot_entry.sequence_number);
        }

        if self.file_sequence_number.is_none()
            && (self.status == ManifestStatus::Added
                || snapshot_entry.sequence_number == INITIAL_SEQUENCE_NUMBER)
        {
            self.file_sequence_number = Some(snapshot_entry.sequence_number);
        }
    }

    /// Data sequence number.
    #[inline]
    pub fn sequence_number(&self) -> Option<i64> {
        self.sequence_number
    }

    /// File size in bytes.
    #[inline]
    pub fn file_size_in_bytes(&self) -> u64 {
        self.data_file.file_size_in_bytes
    }

    /// get a reference to the actual data file
    #[inline]
    pub fn data_file(&self) -> &DataFile {
        &self.data_file
    }
}

/// Used to track additions and deletions in ManifestEntry.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum ManifestStatus {
    /// Value: 0
    Existing = 0,
    /// Value: 1
    Added = 1,
    /// Value: 2
    ///
    /// Deletes are informational only and not used in scans.
    Deleted = 2,
}

impl TryFrom<i32> for ManifestStatus {
    type Error = Error;

    fn try_from(v: i32) -> Result<ManifestStatus> {
        match v {
            0 => Ok(ManifestStatus::Existing),
            1 => Ok(ManifestStatus::Added),
            2 => Ok(ManifestStatus::Deleted),
            _ => Err(Error::new(
                ErrorKind::DataInvalid,
                format!("manifest status {} is invalid", v),
            )),
        }
    }
}

/// Data file carries data file path, partition tuple, metrics, …
#[derive(Debug, PartialEq, Clone, Eq, Builder)]
pub struct DataFile {
    /// field id: 134
    ///
    /// Type of content stored by the data file: data, equality deletes,
    /// or position deletes (all v1 files are data files)
    pub(crate) content: DataContentType,
    /// field id: 100
    ///
    /// Full URI for the file with FS scheme
    pub(crate) file_path: String,
    /// field id: 101
    ///
    /// String file format name, avro, orc or parquet
    pub(crate) file_format: DataFileFormat,
    /// field id: 102
    ///
    /// Partition data tuple, schema based on the partition spec output using
    /// partition field ids for the struct field ids
    pub(crate) partition: Struct,
    /// field id: 103
    ///
    /// Number of records in this file
    pub(crate) record_count: u64,
    /// field id: 104
    ///
    /// Total file size in bytes
    pub(crate) file_size_in_bytes: u64,
    /// field id: 108
    /// key field id: 117
    /// value field id: 118
    ///
    /// Map from column id to the total size on disk of all regions that
    /// store the column. Does not include bytes necessary to read other
    /// columns, like footers. Leave null for row-oriented formats (Avro)
    #[builder(default)]
    pub(crate) column_sizes: HashMap<i32, u64>,
    /// field id: 109
    /// key field id: 119
    /// value field id: 120
    ///
    /// Map from column id to number of values in the column (including null
    /// and NaN values)
    #[builder(default)]
    pub(crate) value_counts: HashMap<i32, u64>,
    /// field id: 110
    /// key field id: 121
    /// value field id: 122
    ///
    /// Map from column id to number of null values in the column
    #[builder(default)]
    pub(crate) null_value_counts: HashMap<i32, u64>,
    /// field id: 137
    /// key field id: 138
    /// value field id: 139
    ///
    /// Map from column id to number of NaN values in the column
    #[builder(default)]
    pub(crate) nan_value_counts: HashMap<i32, u64>,
    /// field id: 125
    /// key field id: 126
    /// value field id: 127
    ///
    /// Map from column id to lower bound in the column serialized as binary.
    /// Each value must be less than or equal to all non-null, non-NaN values
    /// in the column for the file.
    ///
    /// Reference:
    ///
    /// - [Binary single-value serialization](https://iceberg.apache.org/spec/#binary-single-value-serialization)
    #[builder(default)]
    pub(crate) lower_bounds: HashMap<i32, Datum>,
    /// field id: 128
    /// key field id: 129
    /// value field id: 130
    ///
    /// Map from column id to upper bound in the column serialized as binary.
    /// Each value must be greater than or equal to all non-null, non-Nan
    /// values in the column for the file.
    ///
    /// Reference:
    ///
    /// - [Binary single-value serialization](https://iceberg.apache.org/spec/#binary-single-value-serialization)
    #[builder(default)]
    pub(crate) upper_bounds: HashMap<i32, Datum>,
    /// field id: 131
    ///
    /// Implementation-specific key metadata for encryption
    #[builder(default)]
    pub(crate) key_metadata: Vec<u8>,
    /// field id: 132
    /// element field id: 133
    ///
    /// Split offsets for the data file. For example, all row group offsets
    /// in a Parquet file. Must be sorted ascending
    #[builder(default)]
    pub(crate) split_offsets: Vec<i64>,
    /// field id: 135
    /// element field id: 136
    ///
    /// Field ids used to determine row equality in equality delete files.
    /// Required when content is EqualityDeletes and should be null
    /// otherwise. Fields with ids listed in this column must be present
    /// in the delete file
    #[builder(default)]
    pub(crate) equality_ids: Vec<i32>,
    /// field id: 140
    ///
    /// ID representing sort order for this file.
    ///
    /// If sort order ID is missing or unknown, then the order is assumed to
    /// be unsorted. Only data files and equality delete files should be
    /// written with a non-null order id. Position deletes are required to be
    /// sorted by file and position, not a table order, and should set sort
    /// order id to null. Readers must ignore sort order id for position
    /// delete files.
    #[builder(default, setter(strip_option))]
    pub(crate) sort_order_id: Option<i32>,
}

impl DataFile {
    /// Get the content type of the data file (data, equality deletes, or position deletes)
    pub fn content_type(&self) -> DataContentType {
        self.content
    }
    /// Get the file path as full URI with FS scheme
    pub fn file_path(&self) -> &str {
        &self.file_path
    }
    /// Get the file format of the file (avro, orc or parquet).
    pub fn file_format(&self) -> DataFileFormat {
        self.file_format
    }
    /// Get the partition values of the file.
    pub fn partition(&self) -> &Struct {
        &self.partition
    }
    /// Get the record count in the data file.
    pub fn record_count(&self) -> u64 {
        self.record_count
    }
    /// Get the file size in bytes.
    pub fn file_size_in_bytes(&self) -> u64 {
        self.file_size_in_bytes
    }
    /// Get the column sizes.
    /// Map from column id to the total size on disk of all regions that
    /// store the column. Does not include bytes necessary to read other
    /// columns, like footers. Null for row-oriented formats (Avro)
    pub fn column_sizes(&self) -> &HashMap<i32, u64> {
        &self.column_sizes
    }
    /// Get the columns value counts for the data file.
    /// Map from column id to number of values in the column (including null
    /// and NaN values)
    pub fn value_counts(&self) -> &HashMap<i32, u64> {
        &self.value_counts
    }
    /// Get the null value counts of the data file.
    /// Map from column id to number of null values in the column
    pub fn null_value_counts(&self) -> &HashMap<i32, u64> {
        &self.null_value_counts
    }
    /// Get the nan value counts of the data file.
    /// Map from column id to number of NaN values in the column
    pub fn nan_value_counts(&self) -> &HashMap<i32, u64> {
        &self.nan_value_counts
    }
    /// Get the lower bounds of the data file values per column.
    /// Map from column id to lower bound in the column serialized as binary.
    pub fn lower_bounds(&self) -> &HashMap<i32, Datum> {
        &self.lower_bounds
    }
    /// Get the upper bounds of the data file values per column.
    /// Map from column id to upper bound in the column serialized as binary.
    pub fn upper_bounds(&self) -> &HashMap<i32, Datum> {
        &self.upper_bounds
    }
    /// Get the Implementation-specific key metadata for the data file.
    pub fn key_metadata(&self) -> &[u8] {
        &self.key_metadata
    }
    /// Get the split offsets of the data file.
    /// For example, all row group offsets in a Parquet file.
    pub fn split_offsets(&self) -> &[i64] {
        &self.split_offsets
    }
    /// Get the equality ids of the data file.
    /// Field ids used to determine row equality in equality delete files.
    /// null when content is not EqualityDeletes.
    pub fn equality_ids(&self) -> &[i32] {
        &self.equality_ids
    }
    /// Get the sort order id of the data file.
    /// Only data files and equality delete files should be
    /// written with a non-null order id. Position deletes are required to be
    /// sorted by file and position, not a table order, and should set sort
    /// order id to null. Readers must ignore sort order id for position
    /// delete files.
    pub fn sort_order_id(&self) -> Option<i32> {
        self.sort_order_id
    }
}
/// Type of content stored by the data file: data, equality deletes, or
/// position deletes (all v1 files are data files)
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum DataContentType {
    /// value: 0
    Data = 0,
    /// value: 1
    PositionDeletes = 1,
    /// value: 2
    EqualityDeletes = 2,
}

impl TryFrom<i32> for DataContentType {
    type Error = Error;

    fn try_from(v: i32) -> Result<DataContentType> {
        match v {
            0 => Ok(DataContentType::Data),
            1 => Ok(DataContentType::PositionDeletes),
            2 => Ok(DataContentType::EqualityDeletes),
            _ => Err(Error::new(
                ErrorKind::DataInvalid,
                format!("data content type {} is invalid", v),
            )),
        }
    }
}

/// Format of this data.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum DataFileFormat {
    /// Avro file format: <https://avro.apache.org/>
    Avro,
    /// Orc file format: <https://orc.apache.org/>
    Orc,
    /// Parquet file format: <https://parquet.apache.org/>
    Parquet,
}

impl FromStr for DataFileFormat {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "avro" => Ok(Self::Avro),
            "orc" => Ok(Self::Orc),
            "parquet" => Ok(Self::Parquet),
            _ => Err(Error::new(
                ErrorKind::DataInvalid,
                format!("Unsupported data file format: {}", s),
            )),
        }
    }
}

impl std::fmt::Display for DataFileFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DataFileFormat::Avro => write!(f, "avro"),
            DataFileFormat::Orc => write!(f, "orc"),
            DataFileFormat::Parquet => write!(f, "parquet"),
        }
    }
}

mod _serde {
    use std::collections::HashMap;

    use serde_derive::{Deserialize, Serialize};
    use serde_with::serde_as;

    use super::ManifestEntry;
    use crate::spec::{Datum, Literal, RawLiteral, Schema, Struct, StructType, Type};
    use crate::{Error, ErrorKind};

    #[derive(Serialize, Deserialize)]
    pub(super) struct ManifestEntryV2 {
        status: i32,
        snapshot_id: Option<i64>,
        sequence_number: Option<i64>,
        file_sequence_number: Option<i64>,
        data_file: DataFile,
    }

    impl ManifestEntryV2 {
        pub fn try_from(value: ManifestEntry, partition_type: &StructType) -> Result<Self, Error> {
            Ok(Self {
                status: value.status as i32,
                snapshot_id: value.snapshot_id,
                sequence_number: value.sequence_number,
                file_sequence_number: value.file_sequence_number,
                data_file: DataFile::try_from(value.data_file, partition_type, false)?,
            })
        }

        pub fn try_into(
            self,
            partition_type: &StructType,
            schema: &Schema,
        ) -> Result<ManifestEntry, Error> {
            Ok(ManifestEntry {
                status: self.status.try_into()?,
                snapshot_id: self.snapshot_id,
                sequence_number: self.sequence_number,
                file_sequence_number: self.file_sequence_number,
                data_file: self.data_file.try_into(partition_type, schema)?,
            })
        }
    }

    #[derive(Serialize, Deserialize)]
    pub(super) struct ManifestEntryV1 {
        status: i32,
        pub snapshot_id: i64,
        data_file: DataFile,
    }

    impl ManifestEntryV1 {
        pub fn try_from(value: ManifestEntry, partition_type: &StructType) -> Result<Self, Error> {
            Ok(Self {
                status: value.status as i32,
                snapshot_id: value.snapshot_id.unwrap_or_default(),
                data_file: DataFile::try_from(value.data_file, partition_type, true)?,
            })
        }

        pub fn try_into(
            self,
            partition_type: &StructType,
            schema: &Schema,
        ) -> Result<ManifestEntry, Error> {
            Ok(ManifestEntry {
                status: self.status.try_into()?,
                snapshot_id: Some(self.snapshot_id),
                sequence_number: Some(0),
                file_sequence_number: Some(0),
                data_file: self.data_file.try_into(partition_type, schema)?,
            })
        }
    }

    #[serde_as]
    #[derive(Serialize, Deserialize)]
    pub(super) struct DataFile {
        #[serde(default)]
        content: i32,
        file_path: String,
        file_format: String,
        partition: RawLiteral,
        record_count: i64,
        file_size_in_bytes: i64,
        #[serde(skip_deserializing, skip_serializing_if = "Option::is_none")]
        block_size_in_bytes: Option<i64>,
        column_sizes: Option<Vec<I64Entry>>,
        value_counts: Option<Vec<I64Entry>>,
        null_value_counts: Option<Vec<I64Entry>>,
        nan_value_counts: Option<Vec<I64Entry>>,
        lower_bounds: Option<Vec<BytesEntry>>,
        upper_bounds: Option<Vec<BytesEntry>>,
        key_metadata: Option<serde_bytes::ByteBuf>,
        split_offsets: Option<Vec<i64>>,
        #[serde(default)]
        equality_ids: Option<Vec<i32>>,
        sort_order_id: Option<i32>,
    }

    impl DataFile {
        pub fn try_from(
            value: super::DataFile,
            partition_type: &StructType,
            is_version_1: bool,
        ) -> Result<Self, Error> {
            let block_size_in_bytes = if is_version_1 { Some(0) } else { None };
            Ok(Self {
                content: value.content as i32,
                file_path: value.file_path,
                file_format: value.file_format.to_string(),
                partition: RawLiteral::try_from(
                    Literal::Struct(value.partition),
                    &Type::Struct(partition_type.clone()),
                )?,
                record_count: value.record_count.try_into()?,
                file_size_in_bytes: value.file_size_in_bytes.try_into()?,
                block_size_in_bytes,
                column_sizes: Some(to_i64_entry(value.column_sizes)?),
                value_counts: Some(to_i64_entry(value.value_counts)?),
                null_value_counts: Some(to_i64_entry(value.null_value_counts)?),
                nan_value_counts: Some(to_i64_entry(value.nan_value_counts)?),
                lower_bounds: Some(to_bytes_entry(value.lower_bounds)),
                upper_bounds: Some(to_bytes_entry(value.upper_bounds)),
                key_metadata: Some(serde_bytes::ByteBuf::from(value.key_metadata)),
                split_offsets: Some(value.split_offsets),
                equality_ids: Some(value.equality_ids),
                sort_order_id: value.sort_order_id,
            })
        }
        pub fn try_into(
            self,
            partition_type: &StructType,
            schema: &Schema,
        ) -> Result<super::DataFile, Error> {
            let partition = self
                .partition
                .try_into(&Type::Struct(partition_type.clone()))?
                .map(|v| {
                    if let Literal::Struct(v) = v {
                        Ok(v)
                    } else {
                        Err(Error::new(
                            ErrorKind::DataInvalid,
                            "partition value is not a struct",
                        ))
                    }
                })
                .transpose()?
                .unwrap_or(Struct::empty());
            Ok(super::DataFile {
                content: self.content.try_into()?,
                file_path: self.file_path,
                file_format: self.file_format.parse()?,
                partition,
                record_count: self.record_count.try_into()?,
                file_size_in_bytes: self.file_size_in_bytes.try_into()?,
                column_sizes: self
                    .column_sizes
                    .map(parse_i64_entry)
                    .transpose()?
                    .unwrap_or_default(),
                value_counts: self
                    .value_counts
                    .map(parse_i64_entry)
                    .transpose()?
                    .unwrap_or_default(),
                null_value_counts: self
                    .null_value_counts
                    .map(parse_i64_entry)
                    .transpose()?
                    .unwrap_or_default(),
                nan_value_counts: self
                    .nan_value_counts
                    .map(parse_i64_entry)
                    .transpose()?
                    .unwrap_or_default(),
                lower_bounds: self
                    .lower_bounds
                    .map(|v| parse_bytes_entry(v, schema))
                    .transpose()?
                    .unwrap_or_default(),
                upper_bounds: self
                    .upper_bounds
                    .map(|v| parse_bytes_entry(v, schema))
                    .transpose()?
                    .unwrap_or_default(),
                key_metadata: self.key_metadata.map(|v| v.to_vec()).unwrap_or_default(),
                split_offsets: self.split_offsets.unwrap_or_default(),
                equality_ids: self.equality_ids.unwrap_or_default(),
                sort_order_id: self.sort_order_id,
            })
        }
    }

    #[serde_as]
    #[derive(Serialize, Deserialize)]
    #[cfg_attr(test, derive(Debug, PartialEq, Eq))]
    struct BytesEntry {
        key: i32,
        value: serde_bytes::ByteBuf,
    }

    fn parse_bytes_entry(
        v: Vec<BytesEntry>,
        schema: &Schema,
    ) -> Result<HashMap<i32, Datum>, Error> {
        let mut m = HashMap::with_capacity(v.len());
        for entry in v {
            // We ignore the entry if the field is not found in the schema, due to schema evolution.
            if let Some(field) = schema.field_by_id(entry.key) {
                let data_type = field
                    .field_type
                    .as_primitive_type()
                    .ok_or_else(|| {
                        Error::new(
                            ErrorKind::DataInvalid,
                            format!("field {} is not a primitive type", field.name),
                        )
                    })?
                    .clone();
                m.insert(entry.key, Datum::try_from_bytes(&entry.value, data_type)?);
            }
        }
        Ok(m)
    }

    fn to_bytes_entry(v: impl IntoIterator<Item = (i32, Datum)>) -> Vec<BytesEntry> {
        v.into_iter()
            .map(|e| BytesEntry {
                key: e.0,
                value: e.1.to_bytes(),
            })
            .collect()
    }

    #[derive(Serialize, Deserialize)]
    #[cfg_attr(test, derive(Debug, PartialEq, Eq))]
    struct I64Entry {
        key: i32,
        value: i64,
    }

    fn parse_i64_entry(v: Vec<I64Entry>) -> Result<HashMap<i32, u64>, Error> {
        let mut m = HashMap::with_capacity(v.len());
        for entry in v {
            // We ignore the entry if it's value is negative since these entries are supposed to be used for
            // counting, which should never be negative.
            if let Ok(v) = entry.value.try_into() {
                m.insert(entry.key, v);
            }
        }
        Ok(m)
    }

    fn to_i64_entry(entries: HashMap<i32, u64>) -> Result<Vec<I64Entry>, Error> {
        entries
            .iter()
            .map(|e| {
                Ok(I64Entry {
                    key: *e.0,
                    value: (*e.1).try_into()?,
                })
            })
            .collect()
    }

    #[cfg(test)]
    mod tests {
        use std::collections::HashMap;

        use crate::spec::manifest::_serde::{parse_i64_entry, I64Entry};

        #[test]
        fn test_parse_negative_manifest_entry() {
            let entries = vec![I64Entry { key: 1, value: -1 }, I64Entry {
                key: 2,
                value: 3,
            }];

            let ret = parse_i64_entry(entries).unwrap();

            let expected_ret = HashMap::from([(2, 3)]);
            assert_eq!(ret, expected_ret, "Negative i64 entry should be ignored!");
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::sync::Arc;

    use tempfile::TempDir;

    use super::*;
    use crate::io::FileIOBuilder;
    use crate::spec::{Literal, NestedField, PrimitiveType, Struct, Transform, Type};

    #[tokio::test]
    async fn test_parse_manifest_v2_unpartition() {
        let manifest = Manifest {
            metadata: ManifestMetadata {
                schema_id: 0,
                schema: Schema::builder()
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
                    ])
                    .build()
                    .unwrap(),
                partition_spec: PartitionSpec {
                    spec_id: 0,
                    fields: vec![],
                },
                content: ManifestContentType::Data,
                format_version: FormatVersion::V2,
            },
            entries: vec![
                Arc::new(ManifestEntry {
                    status: ManifestStatus::Added,
                    snapshot_id: None,
                    sequence_number: None,
                    file_sequence_number: None,
                    data_file: DataFile {
                        content: DataContentType::Data,
                        file_path: "s3a://icebergdata/demo/s1/t1/data/00000-0-ba56fbfa-f2ff-40c9-bb27-565ad6dc2be8-00000.parquet".to_string(),
                        file_format: DataFileFormat::Parquet,
                        partition: Struct::empty(),
                        record_count: 1,
                        file_size_in_bytes: 5442,
                        column_sizes: HashMap::from([(0,73),(6,34),(2,73),(7,61),(3,61),(5,62),(9,79),(10,73),(1,61),(4,73),(8,73)]),
                        value_counts: HashMap::from([(4,1),(5,1),(2,1),(0,1),(3,1),(6,1),(8,1),(1,1),(10,1),(7,1),(9,1)]),
                        null_value_counts: HashMap::from([(1,0),(6,0),(2,0),(8,0),(0,0),(3,0),(5,0),(9,0),(7,0),(4,0),(10,0)]),
                        nan_value_counts: HashMap::new(),
                        lower_bounds: HashMap::new(),
                        upper_bounds: HashMap::new(),
                        key_metadata: Vec::new(),
                        split_offsets: vec![4],
                        equality_ids: Vec::new(),
                        sort_order_id: None,
                    }
                })
            ]
        };

        let writer = |output_file: OutputFile| ManifestWriter::new(output_file, 1, vec![]);

        test_manifest_read_write(manifest, writer).await;
    }

    #[tokio::test]
    async fn test_parse_manifest_v2_partition() {
        let manifest = Manifest {
            metadata: ManifestMetadata {
                schema_id: 0,
                schema: Schema::builder()
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
                    ])
                    .build()
                    .unwrap(),
                partition_spec: PartitionSpec {
                    spec_id: 0,
                    fields: vec![
                        PartitionField {
                            name: "v_int".to_string(),
                            transform: Transform::Identity,
                            source_id: 2,
                            field_id: 1000,
                        },
                        PartitionField {
                            name: "v_long".to_string(),
                            transform: Transform::Identity,
                            source_id: 3,
                            field_id: 1001,
                        },
                    ],
                },
                content: ManifestContentType::Data,
                format_version: FormatVersion::V2,
            },
            entries: vec![Arc::new(ManifestEntry {
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
                    key_metadata: vec![],
                    split_offsets: vec![4],
                    equality_ids: vec![],
                    sort_order_id: None,
                },
            })],
        };

        let writer = |output_file: OutputFile| ManifestWriter::new(output_file, 1, vec![]);

        let res = test_manifest_read_write(manifest, writer).await;

        assert_eq!(res.sequence_number, UNASSIGNED_SEQUENCE_NUMBER);
        assert_eq!(res.min_sequence_number, UNASSIGNED_SEQUENCE_NUMBER);
    }

    #[tokio::test]
    async fn test_parse_manifest_v1_unpartition() {
        let manifest = Manifest {
            metadata: ManifestMetadata {
                schema_id: 1,
                schema: Schema::builder()
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
                partition_spec: PartitionSpec {
                    spec_id: 0,
                    fields: vec![],
                },
                content: ManifestContentType::Data,
                format_version: FormatVersion::V1,
            },
            entries: vec![Arc::new(ManifestEntry {
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
                    key_metadata: vec![],
                    split_offsets: vec![4],
                    equality_ids: vec![],
                    sort_order_id: Some(0),
                }
            })],
        };

        let writer =
            |output_file: OutputFile| ManifestWriter::new(output_file, 2966623707104393227, vec![]);

        test_manifest_read_write(manifest, writer).await;
    }

    #[tokio::test]
    async fn test_parse_manifest_v1_partition() {
        let manifest = Manifest {
            metadata: ManifestMetadata {
                schema_id: 0,
                schema: Schema::builder()
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
                partition_spec: PartitionSpec {
                    spec_id: 0,
                    fields: vec![PartitionField {
                        name: "category".to_string(),
                        transform: Transform::Identity,
                        source_id: 3,
                        field_id: 1000,
                    }],
                },
                content: ManifestContentType::Data,
                format_version: FormatVersion::V1,
            },
            entries: vec![
                Arc::new(ManifestEntry {
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
                        key_metadata: vec![],
                        split_offsets: vec![4],
                        equality_ids: vec![],
                        sort_order_id: Some(0),
                    },
                })
            ]
        };

        let writer = |output_file: OutputFile| ManifestWriter::new(output_file, 1, vec![]);

        let entry = test_manifest_read_write(manifest, writer).await;

        assert_eq!(entry.partitions.len(), 1);
        assert_eq!(entry.partitions[0].lower_bound, Some(Datum::string("x")));
        assert_eq!(entry.partitions[0].upper_bound, Some(Datum::string("x")));
    }

    #[tokio::test]
    async fn test_parse_manifest_with_schema_evolution() {
        let manifest = Manifest {
            metadata: ManifestMetadata {
                schema_id: 0,
                schema: Schema::builder()
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
                partition_spec: PartitionSpec {
                    spec_id: 0,
                    fields: vec![],
                },
                content: ManifestContentType::Data,
                format_version: FormatVersion::V2,
            },
            entries: vec![Arc::new(ManifestEntry {
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
                    key_metadata: vec![],
                    split_offsets: vec![4],
                    equality_ids: vec![],
                    sort_order_id: None,
                },
            })],
        };

        let writer = |output_file: OutputFile| ManifestWriter::new(output_file, 1, vec![]);

        let (avro_bytes, _) = write_manifest(&manifest, writer).await;

        // The parse should succeed.
        let actual_manifest = Manifest::parse_avro(avro_bytes.as_slice()).unwrap();

        // Compared with original manifest, the lower_bounds and upper_bounds no longer has data for field 3, and
        // other parts should be same.
        let expected_manifest = Manifest {
            metadata: ManifestMetadata {
                schema_id: 0,
                schema: Schema::builder()
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
                partition_spec: PartitionSpec {
                    spec_id: 0,
                    fields: vec![],
                },
                content: ManifestContentType::Data,
                format_version: FormatVersion::V2,
            },
            entries: vec![Arc::new(ManifestEntry {
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
                    ]),
                    upper_bounds: HashMap::from([
                        (1, Datum::long(1)),
                        (2, Datum::int(2)),
                    ]),
                    key_metadata: vec![],
                    split_offsets: vec![4],
                    equality_ids: vec![],
                    sort_order_id: None,
                },
            })],
        };

        assert_eq!(actual_manifest, expected_manifest);
    }

    async fn test_manifest_read_write(
        manifest: Manifest,
        writer_builder: impl FnOnce(OutputFile) -> ManifestWriter,
    ) -> ManifestFile {
        let (bs, res) = write_manifest(&manifest, writer_builder).await;
        let actual_manifest = Manifest::parse_avro(bs.as_slice()).unwrap();

        assert_eq!(actual_manifest, manifest);
        res
    }

    /// Utility method which writes out a manifest and returns the bytes.
    async fn write_manifest(
        manifest: &Manifest,
        writer_builder: impl FnOnce(OutputFile) -> ManifestWriter,
    ) -> (Vec<u8>, ManifestFile) {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test_manifest.avro");
        let io = FileIOBuilder::new_fs_io().build().unwrap();
        let output_file = io.new_output(path.to_str().unwrap()).unwrap();
        let writer = writer_builder(output_file);
        let res = writer.write(manifest.clone()).await.unwrap();

        // Verify manifest
        (fs::read(path).expect("read_file must succeed"), res)
    }
}
