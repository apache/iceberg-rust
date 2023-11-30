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
use self::_const_schema::{manifest_schema_v1, manifest_schema_v2};

use super::Literal;
use super::{
    FieldSummary, FormatVersion, ManifestContentType, ManifestListEntry, PartitionSpec, Schema,
    Struct, UNASSIGNED_SEQ_NUMBER,
};
use crate::io::OutputFile;
use crate::spec::PartitionField;
use crate::{Error, ErrorKind};
use apache_avro::{from_value, to_value, Reader as AvroReader, Writer as AvroWriter};
use futures::AsyncWriteExt;
use serde_json::to_vec;
use std::cmp::min;
use std::collections::HashMap;
use std::str::FromStr;

/// A manifest contains metadata and a list of entries.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Manifest {
    metadata: ManifestMetadata,
    entries: Vec<ManifestEntry>,
}

impl Manifest {
    /// Parse manifest from bytes of avro file.
    pub fn parse_avro(bs: &[u8]) -> Result<Self, Error> {
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
                    .collect::<Result<Vec<_>, Error>>()?
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
                    .collect::<Result<Vec<_>, Error>>()?
            }
        };

        Ok(Manifest { metadata, entries })
    }
}

/// A manifest writer.
pub struct ManifestWriter {
    output: OutputFile,

    snapshot_id: i64,

    added_files: i32,
    added_rows: i64,
    existing_files: i32,
    existing_rows: i64,
    deleted_files: i32,
    deleted_rows: i64,

    seq_num: i64,
    min_seq_num: Option<i64>,

    key_metadata: Option<Vec<u8>>,

    field_summary: HashMap<i32, FieldSummary>,
}

impl ManifestWriter {
    /// Create a new manifest writer.
    pub fn new(
        output: OutputFile,
        snapshot_id: i64,
        seq_num: i64,
        key_metadata: Option<Vec<u8>>,
    ) -> Self {
        Self {
            output,
            snapshot_id,
            added_files: 0,
            added_rows: 0,
            existing_files: 0,
            existing_rows: 0,
            deleted_files: 0,
            deleted_rows: 0,
            seq_num,
            min_seq_num: None,
            key_metadata,
            field_summary: HashMap::new(),
        }
    }

    fn update_field_summary(&mut self, entry: &ManifestEntry) {
        // Update field summary
        if let Some(null) = &entry.data_file.null_value_counts {
            for (&k, &v) in null {
                let field_summary = self.field_summary.entry(k).or_default();
                if v > 0 {
                    field_summary.contains_null = true;
                }
            }
        }
        if let Some(nan) = &entry.data_file.nan_value_counts {
            for (&k, &v) in nan {
                let field_summary = self.field_summary.entry(k).or_default();
                assert!(v >= 0);
                if v > 0 {
                    field_summary.contains_nan = Some(true);
                }
                if v == 0 {
                    field_summary.contains_nan = Some(false);
                }
            }
        }
        if let Some(lower_bound) = &entry.data_file.lower_bounds {
            for (&k, v) in lower_bound {
                let field_summary = self.field_summary.entry(k).or_default();
                if let Some(cur) = &field_summary.lower_bound {
                    if v < cur {
                        field_summary.lower_bound = Some(v.clone());
                    }
                } else {
                    field_summary.lower_bound = Some(v.clone());
                }
            }
        }
        if let Some(upper_bound) = &entry.data_file.upper_bounds {
            for (&k, v) in upper_bound {
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
    }

    fn get_field_summary_vec(&mut self, spec_fields: &[PartitionField]) -> Vec<FieldSummary> {
        let mut partition_summary = Vec::with_capacity(self.field_summary.len());
        for field in spec_fields {
            let entry = self
                .field_summary
                .remove(&field.source_id)
                .unwrap_or(FieldSummary::default());
            partition_summary.push(entry);
        }
        partition_summary
    }

    /// Write a manifest entry.
    pub async fn write(mut self, manifest: Manifest) -> Result<ManifestListEntry, Error> {
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
            manifest.metadata.format_version.to_string(),
        )?;
        if manifest.metadata.format_version == FormatVersion::V2 {
            avro_writer
                .add_user_metadata("content".to_string(), manifest.metadata.content.to_string())?;
        }

        // Write manifest entries
        for entry in manifest.entries {
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
                if let Some(cur_min_seq_num) = self.min_seq_num {
                    self.min_seq_num = Some(
                        entry
                            .sequence_number
                            .map(|v| min(v, cur_min_seq_num))
                            .unwrap_or(cur_min_seq_num),
                    );
                } else {
                    self.min_seq_num = entry.sequence_number;
                }
            }

            self.update_field_summary(&entry);

            let value = match manifest.metadata.format_version {
                FormatVersion::V1 => {
                    to_value(_serde::ManifestEntryV1::try_from(entry, &partition_type)?)?
                        .resolve(&avro_schema)?
                }
                FormatVersion::V2 => {
                    to_value(_serde::ManifestEntryV2::try_from(entry, &partition_type)?)?
                        .resolve(&avro_schema)?
                }
            };

            avro_writer.append(value)?;
        }

        let length = avro_writer.flush()?;
        let content = avro_writer.into_inner()?;
        let mut writer = self.output.writer().await?;
        writer.write_all(&content).await.map_err(|err| {
            Error::new(ErrorKind::Unexpected, "Fail to write Manifest Entry").with_source(err)
        })?;
        writer.close().await.map_err(|err| {
            Error::new(ErrorKind::Unexpected, "Fail to write Manifest Entry").with_source(err)
        })?;

        let partition_summary =
            self.get_field_summary_vec(&manifest.metadata.partition_spec.fields);

        Ok(ManifestListEntry {
            manifest_path: self.output.location().to_string(),
            manifest_length: length as i64,
            partition_spec_id: manifest.metadata.partition_spec.spec_id,
            content: manifest.metadata.content,
            sequence_number: self.seq_num,
            min_sequence_number: self.min_seq_num.unwrap_or(UNASSIGNED_SEQ_NUMBER),
            added_snapshot_id: self.snapshot_id,
            added_data_files_count: Some(self.added_files),
            existing_data_files_count: Some(self.existing_files),
            deleted_data_files_count: Some(self.deleted_files),
            added_rows_count: Some(self.added_rows),
            existing_rows_count: Some(self.existing_rows),
            deleted_rows_count: Some(self.deleted_rows),
            partitions: partition_summary,
            key_metadata: self.key_metadata.unwrap_or_default(),
        })
    }
}

/// This is a helper module that defines the schema field of the manifest list entry.
mod _const_schema {
    use std::sync::Arc;

    use apache_avro::Schema as AvroSchema;
    use once_cell::sync::Lazy;

    use crate::{
        avro::schema_to_avro_schema,
        spec::{
            ListType, MapType, NestedField, NestedFieldRef, PrimitiveType, Schema, StructType, Type,
        },
        Error,
    };

    pub static STATUS: Lazy<NestedFieldRef> = {
        Lazy::new(|| {
            Arc::new(NestedField::required(
                0,
                "status",
                Type::Primitive(PrimitiveType::Int),
            ))
        })
    };

    pub static SNAPSHOT_ID_V1: Lazy<NestedFieldRef> = {
        Lazy::new(|| {
            Arc::new(NestedField::required(
                1,
                "snapshot_id",
                Type::Primitive(PrimitiveType::Long),
            ))
        })
    };

    pub static SNAPSHOT_ID_V2: Lazy<NestedFieldRef> = {
        Lazy::new(|| {
            Arc::new(NestedField::optional(
                1,
                "snapshot_id",
                Type::Primitive(PrimitiveType::Long),
            ))
        })
    };

    pub static SEQUENCE_NUMBER: Lazy<NestedFieldRef> = {
        Lazy::new(|| {
            Arc::new(NestedField::optional(
                3,
                "sequence_number",
                Type::Primitive(PrimitiveType::Long),
            ))
        })
    };

    pub static FILE_SEQUENCE_NUMBER: Lazy<NestedFieldRef> = {
        Lazy::new(|| {
            Arc::new(NestedField::optional(
                4,
                "file_sequence_number",
                Type::Primitive(PrimitiveType::Long),
            ))
        })
    };

    pub static CONTENT: Lazy<NestedFieldRef> = {
        Lazy::new(|| {
            Arc::new(NestedField::required(
                134,
                "content",
                Type::Primitive(PrimitiveType::Int),
            ))
        })
    };

    pub static FILE_PATH: Lazy<NestedFieldRef> = {
        Lazy::new(|| {
            Arc::new(NestedField::required(
                100,
                "file_path",
                Type::Primitive(PrimitiveType::String),
            ))
        })
    };

    pub static FILE_FORMAT: Lazy<NestedFieldRef> = {
        Lazy::new(|| {
            Arc::new(NestedField::required(
                101,
                "file_format",
                Type::Primitive(PrimitiveType::String),
            ))
        })
    };

    pub static RECORD_COUNT: Lazy<NestedFieldRef> = {
        Lazy::new(|| {
            Arc::new(NestedField::required(
                103,
                "record_count",
                Type::Primitive(PrimitiveType::Long),
            ))
        })
    };

    pub static FILE_SIZE_IN_BYTES: Lazy<NestedFieldRef> = {
        Lazy::new(|| {
            Arc::new(NestedField::required(
                104,
                "file_size_in_bytes",
                Type::Primitive(PrimitiveType::Long),
            ))
        })
    };

    // Deprecated. Always write a default in v1. Do not write in v2.
    pub static BLOCK_SIZE_IN_BYTES: Lazy<NestedFieldRef> = {
        Lazy::new(|| {
            Arc::new(NestedField::required(
                105,
                "block_size_in_bytes",
                Type::Primitive(PrimitiveType::Long),
            ))
        })
    };

    pub static COLUMN_SIZES: Lazy<NestedFieldRef> = {
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

    pub static VALUE_COUNTS: Lazy<NestedFieldRef> = {
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

    pub static NULL_VALUE_COUNTS: Lazy<NestedFieldRef> = {
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

    pub static NAN_VALUE_COUNTS: Lazy<NestedFieldRef> = {
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

    pub static DISTINCT_COUNTS: Lazy<NestedFieldRef> = {
        Lazy::new(|| {
            Arc::new(NestedField::optional(
                111,
                "distinct_counts",
                Type::Map(MapType {
                    key_field: Arc::new(NestedField::required(
                        123,
                        "key",
                        Type::Primitive(PrimitiveType::Int),
                    )),
                    value_field: Arc::new(NestedField::required(
                        124,
                        "value",
                        Type::Primitive(PrimitiveType::Long),
                    )),
                }),
            ))
        })
    };

    pub static LOWER_BOUNDS: Lazy<NestedFieldRef> = {
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

    pub static UPPER_BOUNDS: Lazy<NestedFieldRef> = {
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

    pub static KEY_METADATA: Lazy<NestedFieldRef> = {
        Lazy::new(|| {
            Arc::new(NestedField::optional(
                131,
                "key_metadata",
                Type::Primitive(PrimitiveType::Binary),
            ))
        })
    };

    pub static SPLIT_OFFSETS: Lazy<NestedFieldRef> = {
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

    pub static EQUALITY_IDS: Lazy<NestedFieldRef> = {
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

    pub static SORT_ORDER_ID: Lazy<NestedFieldRef> = {
        Lazy::new(|| {
            Arc::new(NestedField::optional(
                140,
                "sort_order_id",
                Type::Primitive(PrimitiveType::Int),
            ))
        })
    };

    pub fn manifest_schema_v2(partition_type: StructType) -> Result<AvroSchema, Error> {
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
                    DISTINCT_COUNTS.clone(),
                    LOWER_BOUNDS.clone(),
                    UPPER_BOUNDS.clone(),
                    KEY_METADATA.clone(),
                    SPLIT_OFFSETS.clone(),
                    EQUALITY_IDS.clone(),
                    SORT_ORDER_ID.clone(),
                ])),
            )),
        ];
        let schema = Schema::builder().with_fields(fields).build().unwrap();
        schema_to_avro_schema("manifest", &schema)
    }

    pub fn manifest_schema_v1(partition_type: StructType) -> Result<AvroSchema, Error> {
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
                    DISTINCT_COUNTS.clone(),
                    LOWER_BOUNDS.clone(),
                    UPPER_BOUNDS.clone(),
                    KEY_METADATA.clone(),
                    SPLIT_OFFSETS.clone(),
                    SORT_ORDER_ID.clone(),
                ])),
            )),
        ];
        let schema = Schema::builder().with_fields(fields).build().unwrap();
        schema_to_avro_schema("manifest", &schema)
    }
}

/// Meta data of a manifest that is stored in the key-value metadata of the Avro file
#[derive(Debug, PartialEq, Clone, Eq)]
pub struct ManifestMetadata {
    /// The table schema at the time the manifest
    /// was written
    schema: Schema,
    /// ID of the schema used to write the manifest as a string
    schema_id: i32,
    /// The partition spec used  to write the manifest
    partition_spec: PartitionSpec,
    /// Table format version number of the manifest as a string
    format_version: FormatVersion,
    /// Type of content files tracked by the manifest: “data” or “deletes”
    content: ManifestContentType,
}

impl ManifestMetadata {
    /// Parse from metadata in avro file.
    pub fn parse(meta: &HashMap<String, Vec<u8>>) -> Result<Self, Error> {
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

/// A manifest is an immutable Avro file that lists data files or delete
/// files, along with each file’s partition data tuple, metrics, and tracking
/// information.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct ManifestEntry {
    /// field: 0
    ///
    /// Used to track additions and deletions.
    status: ManifestStatus,
    /// field id: 1
    ///
    /// Snapshot id where the file was added, or deleted if status is 2.
    /// Inherited when null.
    snapshot_id: Option<i64>,
    /// field id: 3
    ///
    /// Data sequence number of the file.
    /// Inherited when null and status is 1 (added).
    sequence_number: Option<i64>,
    /// field id: 4
    ///
    /// File sequence number indicating when the file was added.
    /// Inherited when null and status is 1 (added).
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

    fn try_from(v: i32) -> Result<ManifestStatus, Error> {
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
#[derive(Debug, PartialEq, Clone, Eq)]
pub struct DataFile {
    /// field id: 134
    ///
    /// Type of content stored by the data file: data, equality deletes,
    /// or position deletes (all v1 files are data files)
    content: DataContentType,
    /// field id: 100
    ///
    /// Full URI for the file with FS scheme
    file_path: String,
    /// field id: 101
    ///
    /// String file format name, avro, orc or parquet
    file_format: DataFileFormat,
    /// field id: 102
    ///
    /// Partition data tuple, schema based on the partition spec output using
    /// partition field ids for the struct field ids
    partition: Struct,
    /// field id: 103
    ///
    /// Number of records in this file
    record_count: i64,
    /// field id: 104
    ///
    /// Total file size in bytes
    file_size_in_bytes: i64,
    /// field id: 108
    /// key field id: 117
    /// value field id: 118
    ///
    /// Map from column id to the total size on disk of all regions that
    /// store the column. Does not include bytes necessary to read other
    /// columns, like footers. Leave null for row-oriented formats (Avro)
    column_sizes: Option<HashMap<i32, i64>>,
    /// field id: 109
    /// key field id: 119
    /// value field id: 120
    ///
    /// Map from column id to number of values in the column (including null
    /// and NaN values)
    value_counts: Option<HashMap<i32, i64>>,
    /// field id: 110
    /// key field id: 121
    /// value field id: 122
    ///
    /// Map from column id to number of null values in the column
    null_value_counts: Option<HashMap<i32, i64>>,
    /// field id: 137
    /// key field id: 138
    /// value field id: 139
    ///
    /// Map from column id to number of NaN values in the column
    nan_value_counts: Option<HashMap<i32, i64>>,
    /// field id: 111
    /// key field id: 123
    /// value field id: 124
    ///
    /// Map from column id to number of distinct values in the column;
    /// distinct counts must be derived using values in the file by counting
    /// or using sketches, but not using methods like merging existing
    /// distinct counts
    distinct_counts: Option<HashMap<i32, i64>>,
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
    lower_bounds: Option<HashMap<i32, Literal>>,
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
    upper_bounds: Option<HashMap<i32, Literal>>,
    /// field id: 131
    ///
    /// Implementation-specific key metadata for encryption
    key_metadata: Option<Vec<u8>>,
    /// field id: 132
    /// element field id: 133
    ///
    /// Split offsets for the data file. For example, all row group offsets
    /// in a Parquet file. Must be sorted ascending
    split_offsets: Option<Vec<i64>>,
    /// field id: 135
    /// element field id: 136
    ///
    /// Field ids used to determine row equality in equality delete files.
    /// Required when content is EqualityDeletes and should be null
    /// otherwise. Fields with ids listed in this column must be present
    /// in the delete file
    equality_ids: Option<Vec<i32>>,
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
    sort_order_id: Option<i32>,
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

    fn try_from(v: i32) -> Result<DataContentType, Error> {
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

    fn from_str(s: &str) -> Result<Self, Error> {
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

impl ToString for DataFileFormat {
    fn to_string(&self) -> String {
        match self {
            DataFileFormat::Avro => "avro",
            DataFileFormat::Orc => "orc",
            DataFileFormat::Parquet => "parquet",
        }
        .to_string()
    }
}

mod _serde {
    use std::collections::HashMap;

    use serde_bytes::ByteBuf;
    use serde_derive::{Deserialize, Serialize};
    use serde_with::serde_as;

    use crate::spec::Literal;
    use crate::spec::RawLiteral;
    use crate::spec::Schema;
    use crate::spec::Struct;
    use crate::spec::StructType;
    use crate::spec::Type;
    use crate::Error;
    use crate::ErrorKind;

    use super::ManifestEntry;

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
                sequence_number: None,
                file_sequence_number: None,
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
        distinct_counts: Option<Vec<I64Entry>>,
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
                record_count: value.record_count,
                file_size_in_bytes: value.file_size_in_bytes,
                block_size_in_bytes,
                column_sizes: value.column_sizes.map(to_i64_entry),
                value_counts: value.value_counts.map(to_i64_entry),
                null_value_counts: value.null_value_counts.map(to_i64_entry),
                nan_value_counts: value.nan_value_counts.map(to_i64_entry),
                distinct_counts: value.distinct_counts.map(to_i64_entry),
                lower_bounds: value.lower_bounds.map(to_bytes_entry),
                upper_bounds: value.upper_bounds.map(to_bytes_entry),
                key_metadata: value.key_metadata.map(serde_bytes::ByteBuf::from),
                split_offsets: value.split_offsets,
                equality_ids: value.equality_ids,
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
                record_count: self.record_count,
                file_size_in_bytes: self.file_size_in_bytes,
                column_sizes: self.column_sizes.map(parse_i64_entry),
                value_counts: self.value_counts.map(parse_i64_entry),
                null_value_counts: self.null_value_counts.map(parse_i64_entry),
                nan_value_counts: self.nan_value_counts.map(parse_i64_entry),
                distinct_counts: self.distinct_counts.map(parse_i64_entry),
                lower_bounds: self
                    .lower_bounds
                    .map(|v| parse_bytes_entry(v, schema))
                    .transpose()?,
                upper_bounds: self
                    .upper_bounds
                    .map(|v| parse_bytes_entry(v, schema))
                    .transpose()?,
                key_metadata: self.key_metadata.map(|v| v.to_vec()),
                split_offsets: self.split_offsets,
                equality_ids: self.equality_ids,
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
    ) -> Result<HashMap<i32, Literal>, Error> {
        let mut m = HashMap::with_capacity(v.len());
        for entry in v {
            let data_type = &schema
                .field_by_id(entry.key)
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        format!("Can't find field id {} for upper/lower_bounds", entry.key),
                    )
                })?
                .field_type;
            m.insert(entry.key, Literal::try_from_bytes(&entry.value, data_type)?);
        }
        Ok(m)
    }

    fn to_bytes_entry(v: HashMap<i32, Literal>) -> Vec<BytesEntry> {
        v.into_iter()
            .map(|e| BytesEntry {
                key: e.0,
                value: Into::<ByteBuf>::into(e.1),
            })
            .collect()
    }

    #[derive(Serialize, Deserialize)]
    #[cfg_attr(test, derive(Debug, PartialEq, Eq))]
    struct I64Entry {
        key: i32,
        value: i64,
    }

    fn parse_i64_entry(v: Vec<I64Entry>) -> HashMap<i32, i64> {
        let mut m = HashMap::with_capacity(v.len());
        for entry in v {
            m.insert(entry.key, entry.value);
        }
        m
    }

    fn to_i64_entry(entries: HashMap<i32, i64>) -> Vec<I64Entry> {
        entries
            .iter()
            .map(|e| I64Entry {
                key: *e.0,
                value: *e.1,
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::TempDir;

    use super::*;
    use crate::io::FileIOBuilder;
    use crate::spec::NestedField;
    use crate::spec::PrimitiveType;
    use crate::spec::Struct;
    use crate::spec::Transform;
    use crate::spec::Type;
    use std::sync::Arc;

    #[test]
    fn test_parse_manifest_v2_unpartition() {
        let path = format!(
            "{}/testdata/unpartition_manifest_v2.avro",
            env!("CARGO_MANIFEST_DIR")
        );
        let bs = fs::read(path).expect("read_file must succeed");
        let manifest = Manifest::parse_avro(bs.as_slice()).unwrap();
        // test metadata
        assert!(manifest.metadata.schema_id == 0);
        assert_eq!(manifest.metadata.schema, {
            let fields = vec![
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
            ];
            Schema::builder().with_fields(fields).build().unwrap()
        });
        assert!(manifest.metadata.partition_spec.fields.is_empty());
        assert!(manifest.metadata.content == ManifestContentType::Data);
        assert!(manifest.metadata.format_version == FormatVersion::V2);
        // test entries
        assert!(manifest.entries.len() == 1);
        let entry = &manifest.entries[0];
        assert!(entry.status == ManifestStatus::Added);
        assert!(entry.snapshot_id == Some(0));
        assert!(entry.sequence_number == Some(1));
        assert!(entry.file_sequence_number == Some(1));
        assert_eq!(
            entry.data_file,
            DataFile {
                content: DataContentType::Data,
                file_path: "s3a://icebergdata/demo/s1/t1/data/00000-0-ba56fbfa-f2ff-40c9-bb27-565ad6dc2be8-00000.parquet".to_string(),
                file_format: DataFileFormat::Parquet,
                partition: Struct::empty(),
                record_count: 1,
                file_size_in_bytes: 5442,
                column_sizes: Some(HashMap::from([(0,73),(6,34),(2,73),(7,61),(3,61),(5,62),(9,79),(10,73),(1,61),(4,73),(8,73)])),
                value_counts: Some(HashMap::from([(4,1),(5,1),(2,1),(0,1),(3,1),(6,1),(8,1),(1,1),(10,1),(7,1),(9,1)])),
                null_value_counts: Some(HashMap::from([(1,0),(6,0),(2,0),(8,0),(0,0),(3,0),(5,0),(9,0),(7,0),(4,0),(10,0)])),
                nan_value_counts: None,
                distinct_counts: None,
                lower_bounds: None,
                upper_bounds: None,
                key_metadata: None,
                split_offsets: Some(vec![4]),
                equality_ids: None,
                sort_order_id: None,
            }
        );
    }

    #[test]
    fn test_parse_manifest_v2_partition() {
        let path = format!(
            "{}/testdata/partition_manifest_v2.avro",
            env!("CARGO_MANIFEST_DIR")
        );
        let bs = fs::read(path).expect("read_file must succeed");
        let manifest = Manifest::parse_avro(bs.as_slice()).unwrap();
        assert_eq!(manifest.metadata.schema_id, 0);
        assert_eq!(manifest.metadata.schema, {
            let fields = vec![
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
            ];
            Schema::builder().with_fields(fields).build().unwrap()
        });
        assert_eq!(manifest.metadata.partition_spec, {
            let fields = vec![
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
            ];
            PartitionSpec { spec_id: 0, fields }
        });
        assert!(manifest.metadata.content == ManifestContentType::Data);
        assert!(manifest.metadata.format_version == FormatVersion::V2);
        assert_eq!(manifest.entries.len(), 1);
        let entry = &manifest.entries[0];
        assert_eq!(entry.status, ManifestStatus::Added);
        assert_eq!(entry.snapshot_id, Some(0));
        assert_eq!(entry.sequence_number, Some(1));
        assert_eq!(entry.file_sequence_number, Some(1));
        assert_eq!(entry.data_file.content, DataContentType::Data);
        assert_eq!(
            entry.data_file.file_path,
            "s3a://icebergdata/demo/s1/t1/data/00000-0-378b56f5-5c52-4102-a2c2-f05f8a7cbe4a-00000.parquet"
        );
        assert_eq!(entry.data_file.file_format, DataFileFormat::Parquet);
        assert_eq!(
            entry.data_file.partition,
            Struct::from_iter(
                vec![
                    (1000, Some(Literal::int(1)), "v_int".to_string()),
                    (1001, Some(Literal::long(1000)), "v_long".to_string())
                ]
                .into_iter()
            )
        );
        assert_eq!(entry.data_file.record_count, 1);
        assert_eq!(entry.data_file.file_size_in_bytes, 5442);
        assert_eq!(
            entry.data_file.column_sizes,
            Some(HashMap::from([
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
            ]))
        );
        assert_eq!(
            entry.data_file.value_counts,
            Some(HashMap::from([
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
            ]))
        );
        assert_eq!(
            entry.data_file.null_value_counts,
            Some(HashMap::from([
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
            ]))
        );
        assert_eq!(entry.data_file.nan_value_counts, None);
        assert_eq!(entry.data_file.distinct_counts, None);
        assert_eq!(entry.data_file.lower_bounds, None);
        assert_eq!(entry.data_file.upper_bounds, None);
        assert_eq!(entry.data_file.key_metadata, None);
        assert_eq!(entry.data_file.split_offsets, Some(vec![4]));
        assert_eq!(entry.data_file.equality_ids, None);
        assert_eq!(entry.data_file.sort_order_id, None);
    }

    #[test]
    fn test_parse_manifest_v1_unpartition() {
        let path = format!(
            "{}/testdata/unpartition_manifest_v1.avro",
            env!("CARGO_MANIFEST_DIR")
        );
        let bs = fs::read(path).expect("read_file must succeed");
        let manifest = Manifest::parse_avro(bs.as_slice()).unwrap();
        // test metadata
        assert!(manifest.metadata.schema_id == 0);
        assert_eq!(manifest.metadata.schema, {
            let fields = vec![
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
            ];
            Schema::builder()
                .with_schema_id(1)
                .with_fields(fields)
                .build()
                .unwrap()
        });
        assert!(manifest.metadata.partition_spec.fields.is_empty());
        assert!(manifest.metadata.content == ManifestContentType::Data);
        assert!(manifest.metadata.format_version == FormatVersion::V1);
        assert_eq!(manifest.entries.len(), 4);
        let entry = &manifest.entries[0];
        assert!(entry.status == ManifestStatus::Added);
        assert!(entry.snapshot_id == Some(2966623707104393227));
        assert!(entry.sequence_number.is_none());
        assert!(entry.file_sequence_number.is_none());
        assert_eq!(
            entry.data_file,
            DataFile {
                content: DataContentType::Data,
                file_path: "s3://testbucket/iceberg_data/iceberg_ctl/iceberg_db/iceberg_tbl/data/00000-7-45268d71-54eb-476c-b42c-942d880c04a1-00001.parquet".to_string(),
                file_format: DataFileFormat::Parquet,
                partition: Struct::empty(),
                record_count: 1,
                file_size_in_bytes: 875,
                column_sizes: Some(HashMap::from([(1,47),(2,48),(3,52)])),
                value_counts: Some(HashMap::from([(1,1),(2,1),(3,1)])),
                null_value_counts: Some(HashMap::from([(1,0),(2,0),(3,0)])),
                nan_value_counts: Some(HashMap::new()),
                distinct_counts: None,
                lower_bounds: Some(HashMap::from([(1,Literal::int(1)),(2,Literal::string("a")),(3,Literal::string("AC/DC"))])),
                upper_bounds: Some(HashMap::from([(1,Literal::int(1)),(2,Literal::string("a")),(3,Literal::string("AC/DC"))])),
                key_metadata: None,
                split_offsets: Some(vec![4]),
                equality_ids: None,
                sort_order_id: Some(0),
            }
        );
    }

    #[test]
    fn test_parse_manifest_v1_partition() {
        let path = format!(
            "{}/testdata/partition_manifest_v1.avro",
            env!("CARGO_MANIFEST_DIR")
        );
        let bs = fs::read(path).expect("read_file must succeed");
        let manifest = Manifest::parse_avro(bs.as_slice()).unwrap();
        // test metadata
        assert!(manifest.metadata.schema_id == 0);
        assert_eq!(manifest.metadata.schema, {
            let fields = vec![
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
            ];
            Schema::builder().with_fields(fields).build().unwrap()
        });
        assert_eq!(manifest.metadata.partition_spec, {
            let fields = vec![PartitionField {
                name: "category".to_string(),
                transform: Transform::Identity,
                source_id: 3,
                field_id: 1000,
            }];
            PartitionSpec { spec_id: 0, fields }
        });
        assert!(manifest.metadata.content == ManifestContentType::Data);
        assert!(manifest.metadata.format_version == FormatVersion::V1);

        // test entries
        assert!(manifest.entries.len() == 1);
        let entry = &manifest.entries[0];
        assert!(entry.status == ManifestStatus::Added);
        assert!(entry.snapshot_id == Some(8205833995881562618));
        assert!(entry.sequence_number.is_none());
        assert!(entry.file_sequence_number.is_none());
        assert_eq!(entry.data_file.content, DataContentType::Data);
        assert_eq!(
            entry.data_file.file_path,
            "s3://testbucket/prod/db/sample/data/category=x/00010-1-d5c93668-1e52-41ac-92a6-bba590cbf249-00001.parquet"
        );
        assert_eq!(entry.data_file.file_format, DataFileFormat::Parquet);
        assert_eq!(
            entry.data_file.partition,
            Struct::from_iter(
                vec![(
                    1000,
                    Some(
                        Literal::try_from_bytes(&[120], &Type::Primitive(PrimitiveType::String))
                            .unwrap()
                    ),
                    "category".to_string()
                )]
                .into_iter()
            )
        );
        assert_eq!(entry.data_file.record_count, 1);
        assert_eq!(entry.data_file.file_size_in_bytes, 874);
        assert_eq!(
            entry.data_file.column_sizes,
            Some(HashMap::from([(1, 46), (2, 48), (3, 48)]))
        );
        assert_eq!(
            entry.data_file.value_counts,
            Some(HashMap::from([(1, 1), (2, 1), (3, 1)]))
        );
        assert_eq!(
            entry.data_file.null_value_counts,
            Some(HashMap::from([(1, 0), (2, 0), (3, 0)]))
        );
        assert_eq!(entry.data_file.nan_value_counts, Some(HashMap::new()));
        assert_eq!(entry.data_file.distinct_counts, None);
        assert_eq!(
            entry.data_file.lower_bounds,
            Some(HashMap::from([
                (1, Literal::long(1)),
                (2, Literal::string("a")),
                (3, Literal::string("x"))
            ]))
        );
        assert_eq!(
            entry.data_file.upper_bounds,
            Some(HashMap::from([
                (1, Literal::long(1)),
                (2, Literal::string("a")),
                (3, Literal::string("x"))
            ]))
        );
        assert_eq!(entry.data_file.key_metadata, None);
        assert_eq!(entry.data_file.split_offsets, Some(vec![4]));
        assert_eq!(entry.data_file.equality_ids, None);
        assert_eq!(entry.data_file.sort_order_id, Some(0));
    }

    #[tokio::test]
    async fn test_writer_manifest_v1_partition() {
        // Read manifest
        let path = format!(
            "{}/testdata/partition_manifest_v1.avro",
            env!("CARGO_MANIFEST_DIR")
        );
        let bs = fs::read(path).expect("read_file must succeed");
        let manifest = Manifest::parse_avro(bs.as_slice()).unwrap();

        // Write manifest
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("manifest_list_v1.avro");
        let io = FileIOBuilder::new_fs_io().build().unwrap();
        let output_file = io.new_output(path.to_str().unwrap()).unwrap();
        let writer = ManifestWriter::new(output_file, 1, 1, None);
        let entry = writer.write(manifest.clone()).await.unwrap();

        // Check partition summary
        assert_eq!(entry.partitions.len(), 1);
        assert_eq!(entry.partitions[0].lower_bound, Some(Literal::string("x")));
        assert_eq!(entry.partitions[0].upper_bound, Some(Literal::string("x")));

        // Verify manifest
        let bs = fs::read(path).expect("read_file must succeed");
        let actual_manifest = Manifest::parse_avro(bs.as_slice()).unwrap();

        assert_eq!(actual_manifest, manifest);
    }

    #[tokio::test]
    async fn test_writer_manifest_v2_partition() {
        // Read manifest
        let path = format!(
            "{}/testdata/partition_manifest_v2.avro",
            env!("CARGO_MANIFEST_DIR")
        );
        let bs = fs::read(path).expect("read_file must succeed");
        let manifest = Manifest::parse_avro(bs.as_slice()).unwrap();

        // Write manifest
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("manifest_list_v2.avro");
        let io = FileIOBuilder::new_fs_io().build().unwrap();
        let output_file = io.new_output(path.to_str().unwrap()).unwrap();
        let writer = ManifestWriter::new(output_file, 1, 1, None);
        let _ = writer.write(manifest.clone()).await.unwrap();

        // Verify manifest
        let bs = fs::read(path).expect("read_file must succeed");
        let actual_manifest = Manifest::parse_avro(bs.as_slice()).unwrap();

        assert_eq!(actual_manifest, manifest);
    }
}
