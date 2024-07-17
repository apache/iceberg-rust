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

//! ManifestList for Iceberg.

use std::{collections::HashMap, str::FromStr};

use crate::io::FileIO;
use crate::{io::OutputFile, Error, ErrorKind};
use apache_avro::{from_value, types::Value, Reader, Writer};
use bytes::Bytes;

use self::{
    _const_schema::{MANIFEST_LIST_AVRO_SCHEMA_V1, MANIFEST_LIST_AVRO_SCHEMA_V2},
    _serde::{ManifestFileV1, ManifestFileV2},
};

use super::{Datum, FormatVersion, Manifest, StructType};
use crate::error::Result;

/// Placeholder for sequence number. The field with this value must be replaced with the actual sequence number before it write.
pub const UNASSIGNED_SEQUENCE_NUMBER: i64 = -1;

/// Snapshots are embedded in table metadata, but the list of manifests for a
/// snapshot are stored in a separate manifest list file.
///
/// A new manifest list is written for each attempt to commit a snapshot
/// because the list of manifests always changes to produce a new snapshot.
/// When a manifest list is written, the (optimistic) sequence number of the
/// snapshot is written for all new manifest files tracked by the list.
///
/// A manifest list includes summary metadata that can be used to avoid
/// scanning all of the manifests in a snapshot when planning a table scan.
/// This includes the number of added, existing, and deleted files, and a
/// summary of values for each field of the partition spec used to write the
/// manifest.
#[derive(Debug, Clone, PartialEq)]
pub struct ManifestList {
    /// Entries in a manifest list.
    entries: Vec<ManifestFile>,
}

impl ManifestList {
    /// Parse manifest list from bytes.
    pub fn parse_with_version(
        bs: &[u8],
        version: FormatVersion,
        partition_type_provider: impl Fn(i32) -> Result<Option<StructType>>,
    ) -> Result<ManifestList> {
        match version {
            FormatVersion::V1 => {
                let reader = Reader::with_schema(&MANIFEST_LIST_AVRO_SCHEMA_V1, bs)?;
                let values = Value::Array(reader.collect::<std::result::Result<Vec<Value>, _>>()?);
                from_value::<_serde::ManifestListV1>(&values)?.try_into(partition_type_provider)
            }
            FormatVersion::V2 => {
                let reader = Reader::new(bs)?;
                let values = Value::Array(reader.collect::<std::result::Result<Vec<Value>, _>>()?);
                from_value::<_serde::ManifestListV2>(&values)?.try_into(partition_type_provider)
            }
        }
    }

    /// Get the entries in the manifest list.
    pub fn entries(&self) -> &[ManifestFile] {
        &self.entries
    }

    /// Take ownership of the entries in the manifest list, consuming it
    pub fn consume_entries(self) -> Box<dyn Iterator<Item = ManifestFile>> {
        Box::new(self.entries.into_iter())
    }
}

/// A manifest list writer.
pub struct ManifestListWriter {
    format_version: FormatVersion,
    output_file: OutputFile,
    avro_writer: Writer<'static, Vec<u8>>,
    sequence_number: i64,
    snapshot_id: i64,
}

impl std::fmt::Debug for ManifestListWriter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ManifestListWriter")
            .field("format_version", &self.format_version)
            .field("output_file", &self.output_file)
            .field("avro_writer", &self.avro_writer.schema())
            .finish_non_exhaustive()
    }
}

impl ManifestListWriter {
    /// Construct a v1 [`ManifestListWriter`] that writes to a provided [`OutputFile`].
    pub fn v1(output_file: OutputFile, snapshot_id: i64, parent_snapshot_id: i64) -> Self {
        let metadata = HashMap::from_iter([
            ("snapshot-id".to_string(), snapshot_id.to_string()),
            (
                "parent-snapshot-id".to_string(),
                parent_snapshot_id.to_string(),
            ),
            ("format-version".to_string(), "1".to_string()),
        ]);
        Self::new(FormatVersion::V1, output_file, metadata, 0, snapshot_id)
    }

    /// Construct a v2 [`ManifestListWriter`] that writes to a provided [`OutputFile`].
    pub fn v2(
        output_file: OutputFile,
        snapshot_id: i64,
        parent_snapshot_id: i64,
        sequence_number: i64,
    ) -> Self {
        let metadata = HashMap::from_iter([
            ("snapshot-id".to_string(), snapshot_id.to_string()),
            (
                "parent-snapshot-id".to_string(),
                parent_snapshot_id.to_string(),
            ),
            ("sequence-number".to_string(), sequence_number.to_string()),
            ("format-version".to_string(), "2".to_string()),
        ]);
        Self::new(
            FormatVersion::V2,
            output_file,
            metadata,
            sequence_number,
            snapshot_id,
        )
    }

    fn new(
        format_version: FormatVersion,
        output_file: OutputFile,
        metadata: HashMap<String, String>,
        sequence_number: i64,
        snapshot_id: i64,
    ) -> Self {
        let avro_schema = match format_version {
            FormatVersion::V1 => &MANIFEST_LIST_AVRO_SCHEMA_V1,
            FormatVersion::V2 => &MANIFEST_LIST_AVRO_SCHEMA_V2,
        };
        let mut avro_writer = Writer::new(avro_schema, Vec::new());
        for (key, value) in metadata {
            avro_writer
                .add_user_metadata(key, value)
                .expect("Avro metadata should be added to the writer before the first record.");
        }
        Self {
            format_version,
            output_file,
            avro_writer,
            sequence_number,
            snapshot_id,
        }
    }

    /// Append manifests to be written.
    pub fn add_manifests(&mut self, manifests: impl Iterator<Item = ManifestFile>) -> Result<()> {
        match self.format_version {
            FormatVersion::V1 => {
                for manifest in manifests {
                    let manifes: ManifestFileV1 = manifest.try_into()?;
                    self.avro_writer.append_ser(manifes)?;
                }
            }
            FormatVersion::V2 => {
                for mut manifest in manifests {
                    if manifest.sequence_number == UNASSIGNED_SEQUENCE_NUMBER {
                        if manifest.added_snapshot_id != self.snapshot_id {
                            return Err(Error::new(
                                ErrorKind::DataInvalid,
                                format!(
                                    "Found unassigned sequence number for a manifest from snapshot {}.",
                                    manifest.added_snapshot_id
                                ),
                            ));
                        }
                        manifest.sequence_number = self.sequence_number;
                    }
                    if manifest.min_sequence_number == UNASSIGNED_SEQUENCE_NUMBER {
                        if manifest.added_snapshot_id != self.snapshot_id {
                            return Err(Error::new(
                                ErrorKind::DataInvalid,
                                format!(
                                    "Found unassigned sequence number for a manifest from snapshot {}.",
                                    manifest.added_snapshot_id
                                ),
                            ));
                        }
                        manifest.min_sequence_number = self.sequence_number;
                    }
                    let manifest_entry: ManifestFileV2 = manifest.try_into()?;
                    self.avro_writer.append_ser(manifest_entry)?;
                }
            }
        }
        Ok(())
    }

    /// Write the manifest list to the output file.
    pub async fn close(self) -> Result<()> {
        let data = self.avro_writer.into_inner()?;
        let mut writer = self.output_file.writer().await?;
        writer.write(Bytes::from(data)).await?;
        writer.close().await?;
        Ok(())
    }
}

/// This is a helper module that defines the schema field of the manifest list entry.
mod _const_schema {
    use std::sync::Arc;

    use apache_avro::Schema as AvroSchema;
    use once_cell::sync::Lazy;

    use crate::{
        avro::schema_to_avro_schema,
        spec::{ListType, NestedField, NestedFieldRef, PrimitiveType, Schema, StructType, Type},
    };

    static MANIFEST_PATH: Lazy<NestedFieldRef> = {
        Lazy::new(|| {
            Arc::new(NestedField::required(
                500,
                "manifest_path",
                Type::Primitive(PrimitiveType::String),
            ))
        })
    };
    static MANIFEST_LENGTH: Lazy<NestedFieldRef> = {
        Lazy::new(|| {
            Arc::new(NestedField::required(
                501,
                "manifest_length",
                Type::Primitive(PrimitiveType::Long),
            ))
        })
    };
    static PARTITION_SPEC_ID: Lazy<NestedFieldRef> = {
        Lazy::new(|| {
            Arc::new(NestedField::required(
                502,
                "partition_spec_id",
                Type::Primitive(PrimitiveType::Int),
            ))
        })
    };
    static CONTENT: Lazy<NestedFieldRef> = {
        Lazy::new(|| {
            Arc::new(NestedField::required(
                517,
                "content",
                Type::Primitive(PrimitiveType::Int),
            ))
        })
    };
    static SEQUENCE_NUMBER: Lazy<NestedFieldRef> = {
        Lazy::new(|| {
            Arc::new(NestedField::required(
                515,
                "sequence_number",
                Type::Primitive(PrimitiveType::Long),
            ))
        })
    };
    static MIN_SEQUENCE_NUMBER: Lazy<NestedFieldRef> = {
        Lazy::new(|| {
            Arc::new(NestedField::required(
                516,
                "min_sequence_number",
                Type::Primitive(PrimitiveType::Long),
            ))
        })
    };
    static ADDED_SNAPSHOT_ID: Lazy<NestedFieldRef> = {
        Lazy::new(|| {
            Arc::new(NestedField::required(
                503,
                "added_snapshot_id",
                Type::Primitive(PrimitiveType::Long),
            ))
        })
    };
    static ADDED_FILES_COUNT_V2: Lazy<NestedFieldRef> = {
        Lazy::new(|| {
            Arc::new(NestedField::required(
                504,
                "added_files_count",
                Type::Primitive(PrimitiveType::Int),
            ))
        })
    };
    static ADDED_FILES_COUNT_V1: Lazy<NestedFieldRef> = {
        Lazy::new(|| {
            Arc::new(NestedField::optional(
                504,
                "added_data_files_count",
                Type::Primitive(PrimitiveType::Int),
            ))
        })
    };
    static EXISTING_FILES_COUNT_V2: Lazy<NestedFieldRef> = {
        Lazy::new(|| {
            Arc::new(NestedField::required(
                505,
                "existing_files_count",
                Type::Primitive(PrimitiveType::Int),
            ))
        })
    };
    static EXISTING_FILES_COUNT_V1: Lazy<NestedFieldRef> = {
        Lazy::new(|| {
            Arc::new(NestedField::optional(
                505,
                "existing_data_files_count",
                Type::Primitive(PrimitiveType::Int),
            ))
        })
    };
    static DELETED_FILES_COUNT_V2: Lazy<NestedFieldRef> = {
        Lazy::new(|| {
            Arc::new(NestedField::required(
                506,
                "deleted_files_count",
                Type::Primitive(PrimitiveType::Int),
            ))
        })
    };
    static DELETED_FILES_COUNT_V1: Lazy<NestedFieldRef> = {
        Lazy::new(|| {
            Arc::new(NestedField::optional(
                506,
                "deleted_data_files_count",
                Type::Primitive(PrimitiveType::Int),
            ))
        })
    };
    static ADDED_ROWS_COUNT_V2: Lazy<NestedFieldRef> = {
        Lazy::new(|| {
            Arc::new(NestedField::required(
                512,
                "added_rows_count",
                Type::Primitive(PrimitiveType::Long),
            ))
        })
    };
    static ADDED_ROWS_COUNT_V1: Lazy<NestedFieldRef> = {
        Lazy::new(|| {
            Arc::new(NestedField::optional(
                512,
                "added_rows_count",
                Type::Primitive(PrimitiveType::Long),
            ))
        })
    };
    static EXISTING_ROWS_COUNT_V2: Lazy<NestedFieldRef> = {
        Lazy::new(|| {
            Arc::new(NestedField::required(
                513,
                "existing_rows_count",
                Type::Primitive(PrimitiveType::Long),
            ))
        })
    };
    static EXISTING_ROWS_COUNT_V1: Lazy<NestedFieldRef> = {
        Lazy::new(|| {
            Arc::new(NestedField::optional(
                513,
                "existing_rows_count",
                Type::Primitive(PrimitiveType::Long),
            ))
        })
    };
    static DELETED_ROWS_COUNT_V2: Lazy<NestedFieldRef> = {
        Lazy::new(|| {
            Arc::new(NestedField::required(
                514,
                "deleted_rows_count",
                Type::Primitive(PrimitiveType::Long),
            ))
        })
    };
    static DELETED_ROWS_COUNT_V1: Lazy<NestedFieldRef> = {
        Lazy::new(|| {
            Arc::new(NestedField::optional(
                514,
                "deleted_rows_count",
                Type::Primitive(PrimitiveType::Long),
            ))
        })
    };
    static PARTITIONS: Lazy<NestedFieldRef> = {
        Lazy::new(|| {
            // element type
            let fields = vec![
                Arc::new(NestedField::required(
                    509,
                    "contains_null",
                    Type::Primitive(PrimitiveType::Boolean),
                )),
                Arc::new(NestedField::optional(
                    518,
                    "contains_nan",
                    Type::Primitive(PrimitiveType::Boolean),
                )),
                Arc::new(NestedField::optional(
                    510,
                    "lower_bound",
                    Type::Primitive(PrimitiveType::Binary),
                )),
                Arc::new(NestedField::optional(
                    511,
                    "upper_bound",
                    Type::Primitive(PrimitiveType::Binary),
                )),
            ];
            let element_field = Arc::new(NestedField::required(
                508,
                "r_508",
                Type::Struct(StructType::new(fields)),
            ));
            Arc::new(NestedField::optional(
                507,
                "partitions",
                Type::List(ListType { element_field }),
            ))
        })
    };
    static KEY_METADATA: Lazy<NestedFieldRef> = {
        Lazy::new(|| {
            Arc::new(NestedField::optional(
                519,
                "key_metadata",
                Type::Primitive(PrimitiveType::Binary),
            ))
        })
    };

    static V1_SCHEMA: Lazy<Schema> = {
        Lazy::new(|| {
            let fields = vec![
                MANIFEST_PATH.clone(),
                MANIFEST_LENGTH.clone(),
                PARTITION_SPEC_ID.clone(),
                ADDED_SNAPSHOT_ID.clone(),
                ADDED_FILES_COUNT_V1.clone().to_owned(),
                EXISTING_FILES_COUNT_V1.clone(),
                DELETED_FILES_COUNT_V1.clone(),
                ADDED_ROWS_COUNT_V1.clone(),
                EXISTING_ROWS_COUNT_V1.clone(),
                DELETED_ROWS_COUNT_V1.clone(),
                PARTITIONS.clone(),
                KEY_METADATA.clone(),
            ];
            Schema::builder().with_fields(fields).build().unwrap()
        })
    };

    static V2_SCHEMA: Lazy<Schema> = {
        Lazy::new(|| {
            let fields = vec![
                MANIFEST_PATH.clone(),
                MANIFEST_LENGTH.clone(),
                PARTITION_SPEC_ID.clone(),
                CONTENT.clone(),
                SEQUENCE_NUMBER.clone(),
                MIN_SEQUENCE_NUMBER.clone(),
                ADDED_SNAPSHOT_ID.clone(),
                ADDED_FILES_COUNT_V2.clone(),
                EXISTING_FILES_COUNT_V2.clone(),
                DELETED_FILES_COUNT_V2.clone(),
                ADDED_ROWS_COUNT_V2.clone(),
                EXISTING_ROWS_COUNT_V2.clone(),
                DELETED_ROWS_COUNT_V2.clone(),
                PARTITIONS.clone(),
                KEY_METADATA.clone(),
            ];
            Schema::builder().with_fields(fields).build().unwrap()
        })
    };

    pub(super) static MANIFEST_LIST_AVRO_SCHEMA_V1: Lazy<AvroSchema> =
        Lazy::new(|| schema_to_avro_schema("manifest_file", &V1_SCHEMA).unwrap());

    pub(super) static MANIFEST_LIST_AVRO_SCHEMA_V2: Lazy<AvroSchema> =
        Lazy::new(|| schema_to_avro_schema("manifest_file", &V2_SCHEMA).unwrap());
}

/// Entry in a manifest list.
#[derive(Debug, PartialEq, Clone)]
pub struct ManifestFile {
    /// field: 500
    ///
    /// Location of the manifest file
    pub manifest_path: String,
    /// field: 501
    ///
    /// Length of the manifest file in bytes
    pub manifest_length: i64,
    /// field: 502
    ///
    /// ID of a partition spec used to write the manifest; must be listed
    /// in table metadata partition-specs
    pub partition_spec_id: i32,
    /// field: 517
    ///
    /// The type of files tracked by the manifest, either data or delete
    /// files; 0 for all v1 manifests
    pub content: ManifestContentType,
    /// field: 515
    ///
    /// The sequence number when the manifest was added to the table; use 0
    /// when reading v1 manifest lists
    pub sequence_number: i64,
    /// field: 516
    ///
    /// The minimum data sequence number of all live data or delete files in
    /// the manifest; use 0 when reading v1 manifest lists
    pub min_sequence_number: i64,
    /// field: 503
    ///
    /// ID of the snapshot where the manifest file was added
    pub added_snapshot_id: i64,
    /// field: 504
    ///
    /// Number of entries in the manifest that have status ADDED, when null
    /// this is assumed to be non-zero
    pub added_files_count: Option<u32>,
    /// field: 505
    ///
    /// Number of entries in the manifest that have status EXISTING (0),
    /// when null this is assumed to be non-zero
    pub existing_files_count: Option<u32>,
    /// field: 506
    ///
    /// Number of entries in the manifest that have status DELETED (2),
    /// when null this is assumed to be non-zero
    pub deleted_files_count: Option<u32>,
    /// field: 512
    ///
    /// Number of rows in all of files in the manifest that have status
    /// ADDED, when null this is assumed to be non-zero
    pub added_rows_count: Option<u64>,
    /// field: 513
    ///
    /// Number of rows in all of files in the manifest that have status
    /// EXISTING, when null this is assumed to be non-zero
    pub existing_rows_count: Option<u64>,
    /// field: 514
    ///
    /// Number of rows in all of files in the manifest that have status
    /// DELETED, when null this is assumed to be non-zero
    pub deleted_rows_count: Option<u64>,
    /// field: 507
    /// element_field: 508
    ///
    /// A list of field summaries for each partition field in the spec. Each
    /// field in the list corresponds to a field in the manifest file’s
    /// partition spec.
    pub partitions: Vec<FieldSummary>,
    /// field: 519
    ///
    /// Implementation-specific key metadata for encryption
    pub key_metadata: Vec<u8>,
}

/// The type of files tracked by the manifest, either data or delete files; Data(0) for all v1 manifests
#[derive(Debug, PartialEq, Clone, Eq)]
pub enum ManifestContentType {
    /// The manifest content is data.
    Data = 0,
    /// The manifest content is deletes.
    Deletes = 1,
}

impl FromStr for ManifestContentType {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "data" => Ok(ManifestContentType::Data),
            "deletes" => Ok(ManifestContentType::Deletes),
            _ => Err(Error::new(
                ErrorKind::DataInvalid,
                format!("Invalid manifest content type: {s}"),
            )),
        }
    }
}

impl std::fmt::Display for ManifestContentType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ManifestContentType::Data => write!(f, "data"),
            ManifestContentType::Deletes => write!(f, "deletes"),
        }
    }
}

impl TryFrom<i32> for ManifestContentType {
    type Error = Error;

    fn try_from(value: i32) -> std::result::Result<Self, Self::Error> {
        match value {
            0 => Ok(ManifestContentType::Data),
            1 => Ok(ManifestContentType::Deletes),
            _ => Err(Error::new(
                crate::ErrorKind::DataInvalid,
                format!(
                    "Invalid manifest content type. Expected 0 or 1, got {}",
                    value
                ),
            )),
        }
    }
}

impl ManifestFile {
    /// Load [`Manifest`].
    ///
    /// This method will also initialize inherited values of [`ManifestEntry`], such as `sequence_number`.
    pub async fn load_manifest(&self, file_io: &FileIO) -> Result<Manifest> {
        let avro = file_io.new_input(&self.manifest_path)?.read().await?;

        let (metadata, mut entries) = Manifest::try_from_avro_bytes(&avro)?;

        // Let entries inherit values from the manifest list entry.
        for entry in &mut entries {
            entry.inherit_data(self);
        }

        Ok(Manifest::new(metadata, entries))
    }
}

/// Field summary for partition field in the spec.
///
/// Each field in the list corresponds to a field in the manifest file’s partition spec.
#[derive(Debug, PartialEq, Eq, Clone, Default)]
pub struct FieldSummary {
    /// field: 509
    ///
    /// Whether the manifest contains at least one partition with a null
    /// value for the field
    pub contains_null: bool,
    /// field: 518
    /// Whether the manifest contains at least one partition with a NaN
    /// value for the field
    pub contains_nan: Option<bool>,
    /// field: 510
    /// The minimum value for the field in the manifests
    /// partitions.
    pub lower_bound: Option<Datum>,
    /// field: 511
    /// The maximum value for the field in the manifests
    /// partitions.
    pub upper_bound: Option<Datum>,
}

/// This is a helper module that defines types to help with serialization/deserialization.
/// For deserialization the input first gets read into either the [ManifestFileV1] or [ManifestFileV2] struct
/// and then converted into the [ManifestFile] struct. Serialization works the other way around.
/// [ManifestFileV1] and [ManifestFileV2] are internal struct that are only used for serialization and deserialization.
pub(super) mod _serde {
    use crate::{
        spec::{Datum, PrimitiveType, StructType},
        Error,
    };
    pub use serde_bytes::ByteBuf;
    use serde_derive::{Deserialize, Serialize};

    use super::ManifestFile;
    use crate::error::Result;

    #[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
    #[serde(transparent)]
    pub(crate) struct ManifestListV2 {
        entries: Vec<ManifestFileV2>,
    }

    #[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
    #[serde(transparent)]
    pub(crate) struct ManifestListV1 {
        entries: Vec<ManifestFileV1>,
    }

    impl ManifestListV2 {
        /// Converts the [ManifestListV2] into a [ManifestList].
        /// The convert of [entries] need the partition_type info so use this function instead of [std::TryFrom] trait.
        pub fn try_into(
            self,
            partition_type_provider: impl Fn(i32) -> Result<Option<StructType>>,
        ) -> Result<super::ManifestList> {
            Ok(super::ManifestList {
                entries: self
                    .entries
                    .into_iter()
                    .map(|v| {
                        let partition_spec_id = v.partition_spec_id;
                        let manifest_path = v.manifest_path.clone();
                        v.try_into(partition_type_provider(partition_spec_id)?.as_ref())
                            .map_err(|err| {
                                err.with_context("manifest file path", manifest_path)
                                    .with_context(
                                        "partition spec id",
                                        partition_spec_id.to_string(),
                                    )
                            })
                    })
                    .collect::<Result<Vec<_>>>()?,
            })
        }
    }

    impl TryFrom<super::ManifestList> for ManifestListV2 {
        type Error = Error;

        fn try_from(value: super::ManifestList) -> std::result::Result<Self, Self::Error> {
            Ok(Self {
                entries: value
                    .entries
                    .into_iter()
                    .map(TryInto::try_into)
                    .collect::<std::result::Result<Vec<_>, _>>()?,
            })
        }
    }

    impl ManifestListV1 {
        /// Converts the [ManifestListV1] into a [ManifestList].
        /// The convert of [entries] need the partition_type info so use this function instead of [std::TryFrom] trait.
        pub fn try_into(
            self,
            partition_type_provider: impl Fn(i32) -> Result<Option<StructType>>,
        ) -> Result<super::ManifestList> {
            Ok(super::ManifestList {
                entries: self
                    .entries
                    .into_iter()
                    .map(|v| {
                        let partition_spec_id = v.partition_spec_id;
                        let manifest_path = v.manifest_path.clone();
                        v.try_into(partition_type_provider(partition_spec_id)?.as_ref())
                            .map_err(|err| {
                                err.with_context("manifest file path", manifest_path)
                                    .with_context(
                                        "partition spec id",
                                        partition_spec_id.to_string(),
                                    )
                            })
                    })
                    .collect::<Result<Vec<_>>>()?,
            })
        }
    }

    impl TryFrom<super::ManifestList> for ManifestListV1 {
        type Error = Error;

        fn try_from(value: super::ManifestList) -> std::result::Result<Self, Self::Error> {
            Ok(Self {
                entries: value
                    .entries
                    .into_iter()
                    .map(TryInto::try_into)
                    .collect::<std::result::Result<Vec<_>, _>>()?,
            })
        }
    }

    #[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
    pub(super) struct ManifestFileV1 {
        pub manifest_path: String,
        pub manifest_length: i64,
        pub partition_spec_id: i32,
        pub added_snapshot_id: i64,
        pub added_data_files_count: Option<i32>,
        pub existing_data_files_count: Option<i32>,
        pub deleted_data_files_count: Option<i32>,
        pub added_rows_count: Option<i64>,
        pub existing_rows_count: Option<i64>,
        pub deleted_rows_count: Option<i64>,
        pub partitions: Option<Vec<FieldSummary>>,
        pub key_metadata: Option<ByteBuf>,
    }

    // Aliases were added to fields that were renamed in Iceberg  1.5.0 (https://github.com/apache/iceberg/pull/5338), in order to support both conventions/versions.
    // In the current implementation deserialization is done using field names, and therefore these fields may appear as either.
    // see issue that raised this here: https://github.com/apache/iceberg-rust/issues/338
    #[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
    pub(super) struct ManifestFileV2 {
        pub manifest_path: String,
        pub manifest_length: i64,
        pub partition_spec_id: i32,
        pub content: i32,
        pub sequence_number: i64,
        pub min_sequence_number: i64,
        pub added_snapshot_id: i64,
        #[serde(alias = "added_data_files_count", alias = "added_files_count")]
        pub added_files_count: i32,
        #[serde(alias = "existing_data_files_count", alias = "existing_files_count")]
        pub existing_files_count: i32,
        #[serde(alias = "deleted_data_files_count", alias = "deleted_files_count")]
        pub deleted_files_count: i32,
        pub added_rows_count: i64,
        pub existing_rows_count: i64,
        pub deleted_rows_count: i64,
        pub partitions: Option<Vec<FieldSummary>>,
        pub key_metadata: Option<ByteBuf>,
    }

    #[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
    pub(super) struct FieldSummary {
        pub contains_null: bool,
        pub contains_nan: Option<bool>,
        pub lower_bound: Option<ByteBuf>,
        pub upper_bound: Option<ByteBuf>,
    }

    impl FieldSummary {
        /// Converts the [FieldSummary] into a [super::FieldSummary].
        /// [lower_bound] and [upper_bound] are converted into [Literal]s need the type info so use
        /// this function instead of [std::TryFrom] trait.
        pub(crate) fn try_into(self, r#type: &PrimitiveType) -> Result<super::FieldSummary> {
            Ok(super::FieldSummary {
                contains_null: self.contains_null,
                contains_nan: self.contains_nan,
                lower_bound: self
                    .lower_bound
                    .map(|v| Datum::try_from_bytes(&v, r#type.clone()))
                    .transpose()?,
                upper_bound: self
                    .upper_bound
                    .map(|v| Datum::try_from_bytes(&v, r#type.clone()))
                    .transpose()?,
            })
        }
    }

    fn try_convert_to_field_summary(
        partitions: Option<Vec<FieldSummary>>,
        partition_type: Option<&StructType>,
    ) -> Result<Vec<super::FieldSummary>> {
        if let Some(partitions) = partitions {
            if let Some(partition_type) = partition_type {
                let partition_types = partition_type.fields();
                if partitions.len() != partition_types.len() {
                    return Err(Error::new(
                        crate::ErrorKind::DataInvalid,
                        format!(
                            "Invalid partition spec. Expected {} fields, got {}",
                            partition_types.len(),
                            partitions.len()
                        ),
                    ));
                }
                partitions
                    .into_iter()
                    .zip(partition_types)
                    .map(|(v, field)| {
                        v.try_into(field.field_type.as_primitive_type().ok_or_else(|| {
                            Error::new(
                                crate::ErrorKind::DataInvalid,
                                "Invalid partition spec. Field type is not primitive",
                            )
                        })?)
                    })
                    .collect::<Result<Vec<_>>>()
            } else {
                Err(Error::new(
                    crate::ErrorKind::DataInvalid,
                    "Invalid partition spec. Partition type is required",
                ))
            }
        } else {
            Ok(Vec::new())
        }
    }

    impl ManifestFileV2 {
        /// Converts the [ManifestFileV2] into a [ManifestFile].
        /// The convert of [partitions] need the partition_type info so use this function instead of [std::TryFrom] trait.
        pub fn try_into(self, partition_type: Option<&StructType>) -> Result<ManifestFile> {
            let partitions = try_convert_to_field_summary(self.partitions, partition_type)?;
            Ok(ManifestFile {
                manifest_path: self.manifest_path,
                manifest_length: self.manifest_length,
                partition_spec_id: self.partition_spec_id,
                content: self.content.try_into()?,
                sequence_number: self.sequence_number,
                min_sequence_number: self.min_sequence_number,
                added_snapshot_id: self.added_snapshot_id,
                added_files_count: Some(self.added_files_count.try_into()?),
                existing_files_count: Some(self.existing_files_count.try_into()?),
                deleted_files_count: Some(self.deleted_files_count.try_into()?),
                added_rows_count: Some(self.added_rows_count.try_into()?),
                existing_rows_count: Some(self.existing_rows_count.try_into()?),
                deleted_rows_count: Some(self.deleted_rows_count.try_into()?),
                partitions,
                key_metadata: self.key_metadata.map(|b| b.into_vec()).unwrap_or_default(),
            })
        }
    }

    impl ManifestFileV1 {
        /// Converts the [MManifestFileV1] into a [ManifestFile].
        /// The convert of [partitions] need the partition_type info so use this function instead of [std::TryFrom] trait.
        pub fn try_into(self, partition_type: Option<&StructType>) -> Result<ManifestFile> {
            let partitions = try_convert_to_field_summary(self.partitions, partition_type)?;
            Ok(ManifestFile {
                manifest_path: self.manifest_path,
                manifest_length: self.manifest_length,
                partition_spec_id: self.partition_spec_id,
                added_snapshot_id: self.added_snapshot_id,
                added_files_count: self
                    .added_data_files_count
                    .map(TryInto::try_into)
                    .transpose()?,
                existing_files_count: self
                    .existing_data_files_count
                    .map(TryInto::try_into)
                    .transpose()?,
                deleted_files_count: self
                    .deleted_data_files_count
                    .map(TryInto::try_into)
                    .transpose()?,
                added_rows_count: self.added_rows_count.map(TryInto::try_into).transpose()?,
                existing_rows_count: self
                    .existing_rows_count
                    .map(TryInto::try_into)
                    .transpose()?,
                deleted_rows_count: self.deleted_rows_count.map(TryInto::try_into).transpose()?,
                partitions,
                key_metadata: self.key_metadata.map(|b| b.into_vec()).unwrap_or_default(),
                // as ref: https://iceberg.apache.org/spec/#partitioning
                // use 0 when reading v1 manifest lists
                content: super::ManifestContentType::Data,
                sequence_number: 0,
                min_sequence_number: 0,
            })
        }
    }

    fn convert_to_serde_field_summary(
        partitions: Vec<super::FieldSummary>,
    ) -> Option<Vec<FieldSummary>> {
        if partitions.is_empty() {
            None
        } else {
            Some(
                partitions
                    .into_iter()
                    .map(|v| FieldSummary {
                        contains_null: v.contains_null,
                        contains_nan: v.contains_nan,
                        lower_bound: v.lower_bound.map(|v| v.to_bytes()),
                        upper_bound: v.upper_bound.map(|v| v.to_bytes()),
                    })
                    .collect(),
            )
        }
    }

    fn convert_to_serde_key_metadata(key_metadata: Vec<u8>) -> Option<ByteBuf> {
        if key_metadata.is_empty() {
            None
        } else {
            Some(ByteBuf::from(key_metadata))
        }
    }

    impl TryFrom<ManifestFile> for ManifestFileV2 {
        type Error = Error;

        fn try_from(value: ManifestFile) -> std::result::Result<Self, Self::Error> {
            let partitions = convert_to_serde_field_summary(value.partitions);
            let key_metadata = convert_to_serde_key_metadata(value.key_metadata);
            Ok(Self {
                manifest_path: value.manifest_path,
                manifest_length: value.manifest_length,
                partition_spec_id: value.partition_spec_id,
                content: value.content as i32,
                sequence_number: value.sequence_number,
                min_sequence_number: value.min_sequence_number,
                added_snapshot_id: value.added_snapshot_id,
                added_files_count: value
                    .added_files_count
                    .ok_or_else(|| {
                        Error::new(
                            crate::ErrorKind::DataInvalid,
                            "added_data_files_count in ManifestFileV2 should be require",
                        )
                    })?
                    .try_into()?,
                existing_files_count: value
                    .existing_files_count
                    .ok_or_else(|| {
                        Error::new(
                            crate::ErrorKind::DataInvalid,
                            "existing_data_files_count in ManifestFileV2 should be require",
                        )
                    })?
                    .try_into()?,
                deleted_files_count: value
                    .deleted_files_count
                    .ok_or_else(|| {
                        Error::new(
                            crate::ErrorKind::DataInvalid,
                            "deleted_data_files_count in ManifestFileV2 should be require",
                        )
                    })?
                    .try_into()?,
                added_rows_count: value
                    .added_rows_count
                    .ok_or_else(|| {
                        Error::new(
                            crate::ErrorKind::DataInvalid,
                            "added_rows_count in ManifestFileV2 should be require",
                        )
                    })?
                    .try_into()?,
                existing_rows_count: value
                    .existing_rows_count
                    .ok_or_else(|| {
                        Error::new(
                            crate::ErrorKind::DataInvalid,
                            "existing_rows_count in ManifestFileV2 should be require",
                        )
                    })?
                    .try_into()?,
                deleted_rows_count: value
                    .deleted_rows_count
                    .ok_or_else(|| {
                        Error::new(
                            crate::ErrorKind::DataInvalid,
                            "deleted_rows_count in ManifestFileV2 should be require",
                        )
                    })?
                    .try_into()?,
                partitions,
                key_metadata,
            })
        }
    }

    impl TryFrom<ManifestFile> for ManifestFileV1 {
        type Error = Error;

        fn try_from(value: ManifestFile) -> std::result::Result<Self, Self::Error> {
            let partitions = convert_to_serde_field_summary(value.partitions);
            let key_metadata = convert_to_serde_key_metadata(value.key_metadata);
            Ok(Self {
                manifest_path: value.manifest_path,
                manifest_length: value.manifest_length,
                partition_spec_id: value.partition_spec_id,
                added_snapshot_id: value.added_snapshot_id,
                added_data_files_count: value
                    .added_files_count
                    .map(TryInto::try_into)
                    .transpose()?,
                existing_data_files_count: value
                    .existing_files_count
                    .map(TryInto::try_into)
                    .transpose()?,
                deleted_data_files_count: value
                    .deleted_files_count
                    .map(TryInto::try_into)
                    .transpose()?,
                added_rows_count: value.added_rows_count.map(TryInto::try_into).transpose()?,
                existing_rows_count: value
                    .existing_rows_count
                    .map(TryInto::try_into)
                    .transpose()?,
                deleted_rows_count: value
                    .deleted_rows_count
                    .map(TryInto::try_into)
                    .transpose()?,
                partitions,
                key_metadata,
            })
        }
    }
}

#[cfg(test)]
mod test {
    use apache_avro::{Reader, Schema};
    use std::{collections::HashMap, fs, sync::Arc};
    use tempfile::TempDir;

    use crate::{
        io::FileIOBuilder,
        spec::{
            manifest_list::_serde::ManifestListV1, Datum, FieldSummary, ManifestContentType,
            ManifestFile, ManifestList, ManifestListWriter, NestedField, PrimitiveType, StructType,
            Type, UNASSIGNED_SEQUENCE_NUMBER,
        },
    };

    use super::_serde::ManifestListV2;

    #[tokio::test]
    async fn test_parse_manifest_list_v1() {
        let manifest_list = ManifestList {
            entries: vec![
                ManifestFile {
                    manifest_path: "/opt/bitnami/spark/warehouse/db/table/metadata/10d28031-9739-484c-92db-cdf2975cead4-m0.avro".to_string(),
                    manifest_length: 5806,
                    partition_spec_id: 0,
                    content: ManifestContentType::Data,
                    sequence_number: 0,
                    min_sequence_number: 0,
                    added_snapshot_id: 1646658105718557341,
                    added_files_count: Some(3),
                    existing_files_count: Some(0),
                    deleted_files_count: Some(0),
                    added_rows_count: Some(3),
                    existing_rows_count: Some(0),
                    deleted_rows_count: Some(0),
                    partitions: vec![],
                    key_metadata: vec![],
                }
            ]
        };

        let file_io = FileIOBuilder::new_fs_io().build().unwrap();

        let tmp_dir = TempDir::new().unwrap();
        let file_name = "simple_manifest_list_v1.avro";
        let full_path = format!("{}/{}", tmp_dir.path().to_str().unwrap(), file_name);

        let mut writer = ManifestListWriter::v1(
            file_io.new_output(full_path.clone()).unwrap(),
            1646658105718557341,
            1646658105718557341,
        );

        writer
            .add_manifests(manifest_list.entries.clone().into_iter())
            .unwrap();
        writer.close().await.unwrap();

        let bs = fs::read(full_path).expect("read_file must succeed");

        let parsed_manifest_list =
            ManifestList::parse_with_version(&bs, crate::spec::FormatVersion::V1, |_id| Ok(None))
                .unwrap();

        assert_eq!(manifest_list, parsed_manifest_list);
    }

    #[tokio::test]
    async fn test_parse_manifest_list_v2() {
        let manifest_list = ManifestList {
            entries: vec![
                ManifestFile {
                    manifest_path: "s3a://icebergdata/demo/s1/t1/metadata/05ffe08b-810f-49b3-a8f4-e88fc99b254a-m0.avro".to_string(),
                    manifest_length: 6926,
                    partition_spec_id: 1,
                    content: ManifestContentType::Data,
                    sequence_number: 1,
                    min_sequence_number: 1,
                    added_snapshot_id: 377075049360453639,
                    added_files_count: Some(1),
                    existing_files_count: Some(0),
                    deleted_files_count: Some(0),
                    added_rows_count: Some(3),
                    existing_rows_count: Some(0),
                    deleted_rows_count: Some(0),
                    partitions: vec![FieldSummary { contains_null: false, contains_nan: Some(false), lower_bound: Some(Datum::long(1)), upper_bound: Some(Datum::long(1))}],
                    key_metadata: vec![],
                },
                ManifestFile {
                    manifest_path: "s3a://icebergdata/demo/s1/t1/metadata/05ffe08b-810f-49b3-a8f4-e88fc99b254a-m1.avro".to_string(),
                    manifest_length: 6926,
                    partition_spec_id: 2,
                    content: ManifestContentType::Data,
                    sequence_number: 1,
                    min_sequence_number: 1,
                    added_snapshot_id: 377075049360453639,
                    added_files_count: Some(1),
                    existing_files_count: Some(0),
                    deleted_files_count: Some(0),
                    added_rows_count: Some(3),
                    existing_rows_count: Some(0),
                    deleted_rows_count: Some(0),
                    partitions: vec![FieldSummary { contains_null: false, contains_nan: Some(false), lower_bound: Some(Datum::float(1.1)), upper_bound: Some(Datum::float(2.1))}],
                    key_metadata: vec![],
                }
            ]
        };

        let file_io = FileIOBuilder::new_fs_io().build().unwrap();

        let tmp_dir = TempDir::new().unwrap();
        let file_name = "simple_manifest_list_v1.avro";
        let full_path = format!("{}/{}", tmp_dir.path().to_str().unwrap(), file_name);

        let mut writer = ManifestListWriter::v2(
            file_io.new_output(full_path.clone()).unwrap(),
            1646658105718557341,
            1646658105718557341,
            1,
        );

        writer
            .add_manifests(manifest_list.entries.clone().into_iter())
            .unwrap();
        writer.close().await.unwrap();

        let bs = fs::read(full_path).expect("read_file must succeed");

        let parsed_manifest_list =
            ManifestList::parse_with_version(&bs, crate::spec::FormatVersion::V2, |id| {
                Ok(HashMap::from([
                    (
                        1,
                        StructType::new(vec![Arc::new(NestedField::required(
                            1,
                            "test",
                            Type::Primitive(PrimitiveType::Long),
                        ))]),
                    ),
                    (
                        2,
                        StructType::new(vec![Arc::new(NestedField::required(
                            1,
                            "test",
                            Type::Primitive(PrimitiveType::Float),
                        ))]),
                    ),
                ])
                .get(&id)
                .cloned())
            })
            .unwrap();

        assert_eq!(manifest_list, parsed_manifest_list);
    }

    #[test]
    fn test_serialize_manifest_list_v1() {
        let manifest_list:ManifestListV1 = ManifestList {
            entries: vec![ManifestFile {
                manifest_path: "/opt/bitnami/spark/warehouse/db/table/metadata/10d28031-9739-484c-92db-cdf2975cead4-m0.avro".to_string(),
                manifest_length: 5806,
                partition_spec_id: 0,
                content: ManifestContentType::Data,
                sequence_number: 0,
                min_sequence_number: 0,
                added_snapshot_id: 1646658105718557341,
                added_files_count: Some(3),
                existing_files_count: Some(0),
                deleted_files_count: Some(0),
                added_rows_count: Some(3),
                existing_rows_count: Some(0),
                deleted_rows_count: Some(0),
                partitions: vec![],
                key_metadata: vec![],
            }]
        }.try_into().unwrap();
        let result = serde_json::to_string(&manifest_list).unwrap();
        assert_eq!(
            result,
            r#"[{"manifest_path":"/opt/bitnami/spark/warehouse/db/table/metadata/10d28031-9739-484c-92db-cdf2975cead4-m0.avro","manifest_length":5806,"partition_spec_id":0,"added_snapshot_id":1646658105718557341,"added_data_files_count":3,"existing_data_files_count":0,"deleted_data_files_count":0,"added_rows_count":3,"existing_rows_count":0,"deleted_rows_count":0,"partitions":null,"key_metadata":null}]"#
        );
    }

    #[test]
    fn test_serialize_manifest_list_v2() {
        let manifest_list:ManifestListV2 = ManifestList {
            entries: vec![ManifestFile {
                manifest_path: "s3a://icebergdata/demo/s1/t1/metadata/05ffe08b-810f-49b3-a8f4-e88fc99b254a-m0.avro".to_string(),
                manifest_length: 6926,
                partition_spec_id: 1,
                content: ManifestContentType::Data,
                sequence_number: 1,
                min_sequence_number: 1,
                added_snapshot_id: 377075049360453639,
                added_files_count: Some(1),
                existing_files_count: Some(0),
                deleted_files_count: Some(0),
                added_rows_count: Some(3),
                existing_rows_count: Some(0),
                deleted_rows_count: Some(0),
                partitions: vec![FieldSummary { contains_null: false, contains_nan: Some(false), lower_bound: Some(Datum::long(1)), upper_bound: Some(Datum::long(1))}],
                key_metadata: vec![],
            }]
        }.try_into().unwrap();
        let result = serde_json::to_string(&manifest_list).unwrap();
        assert_eq!(
            result,
            r#"[{"manifest_path":"s3a://icebergdata/demo/s1/t1/metadata/05ffe08b-810f-49b3-a8f4-e88fc99b254a-m0.avro","manifest_length":6926,"partition_spec_id":1,"content":0,"sequence_number":1,"min_sequence_number":1,"added_snapshot_id":377075049360453639,"added_files_count":1,"existing_files_count":0,"deleted_files_count":0,"added_rows_count":3,"existing_rows_count":0,"deleted_rows_count":0,"partitions":[{"contains_null":false,"contains_nan":false,"lower_bound":[1,0,0,0,0,0,0,0],"upper_bound":[1,0,0,0,0,0,0,0]}],"key_metadata":null}]"#
        );
    }

    #[tokio::test]
    async fn test_manifest_list_writer_v1() {
        let expected_manifest_list = ManifestList {
            entries: vec![ManifestFile {
                manifest_path: "/opt/bitnami/spark/warehouse/db/table/metadata/10d28031-9739-484c-92db-cdf2975cead4-m0.avro".to_string(),
                manifest_length: 5806,
                partition_spec_id: 1,
                content: ManifestContentType::Data,
                sequence_number: 0,
                min_sequence_number: 0,
                added_snapshot_id: 1646658105718557341,
                added_files_count: Some(3),
                existing_files_count: Some(0),
                deleted_files_count: Some(0),
                added_rows_count: Some(3),
                existing_rows_count: Some(0),
                deleted_rows_count: Some(0),
                partitions: vec![FieldSummary { contains_null: false, contains_nan: Some(false), lower_bound: Some(Datum::long(1)), upper_bound: Some(Datum::long(1))}],
                key_metadata: vec![],
            }]
        };

        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("manifest_list_v1.avro");
        let io = FileIOBuilder::new_fs_io().build().unwrap();
        let output_file = io.new_output(path.to_str().unwrap()).unwrap();

        let mut writer = ManifestListWriter::v1(output_file, 1646658105718557341, 0);
        writer
            .add_manifests(expected_manifest_list.entries.clone().into_iter())
            .unwrap();
        writer.close().await.unwrap();

        let bs = fs::read(path).unwrap();

        let partition_types = HashMap::from([(
            1,
            StructType::new(vec![Arc::new(NestedField::required(
                1,
                "test",
                Type::Primitive(PrimitiveType::Long),
            ))]),
        )]);

        let manifest_list =
            ManifestList::parse_with_version(&bs, crate::spec::FormatVersion::V1, move |id| {
                Ok(partition_types.get(&id).cloned())
            })
            .unwrap();
        assert_eq!(manifest_list, expected_manifest_list);

        temp_dir.close().unwrap();
    }

    #[tokio::test]
    async fn test_manifest_list_writer_v2() {
        let snapshot_id = 377075049360453639;
        let seq_num = 1;
        let mut expected_manifest_list = ManifestList {
            entries: vec![ManifestFile {
                manifest_path: "s3a://icebergdata/demo/s1/t1/metadata/05ffe08b-810f-49b3-a8f4-e88fc99b254a-m0.avro".to_string(),
                manifest_length: 6926,
                partition_spec_id: 1,
                content: ManifestContentType::Data,
                sequence_number: UNASSIGNED_SEQUENCE_NUMBER,
                min_sequence_number: UNASSIGNED_SEQUENCE_NUMBER,
                added_snapshot_id: snapshot_id,
                added_files_count: Some(1),
                existing_files_count: Some(0),
                deleted_files_count: Some(0),
                added_rows_count: Some(3),
                existing_rows_count: Some(0),
                deleted_rows_count: Some(0),
                partitions: vec![FieldSummary { contains_null: false, contains_nan: Some(false), lower_bound: Some(Datum::long(1)), upper_bound: Some(Datum::long(1))}],
                key_metadata: vec![],
            }]
        };

        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("manifest_list_v2.avro");
        let io = FileIOBuilder::new_fs_io().build().unwrap();
        let output_file = io.new_output(path.to_str().unwrap()).unwrap();

        let mut writer = ManifestListWriter::v2(output_file, snapshot_id, 0, seq_num);
        writer
            .add_manifests(expected_manifest_list.entries.clone().into_iter())
            .unwrap();
        writer.close().await.unwrap();

        let bs = fs::read(path).unwrap();
        let partition_types = HashMap::from([(
            1,
            StructType::new(vec![Arc::new(NestedField::required(
                1,
                "test",
                Type::Primitive(PrimitiveType::Long),
            ))]),
        )]);
        let manifest_list =
            ManifestList::parse_with_version(&bs, crate::spec::FormatVersion::V2, move |id| {
                Ok(partition_types.get(&id).cloned())
            })
            .unwrap();
        expected_manifest_list.entries[0].sequence_number = seq_num;
        expected_manifest_list.entries[0].min_sequence_number = seq_num;
        assert_eq!(manifest_list, expected_manifest_list);

        temp_dir.close().unwrap();
    }

    #[tokio::test]
    async fn test_manifest_list_writer_v1_as_v2() {
        let expected_manifest_list = ManifestList {
            entries: vec![ManifestFile {
                manifest_path: "/opt/bitnami/spark/warehouse/db/table/metadata/10d28031-9739-484c-92db-cdf2975cead4-m0.avro".to_string(),
                manifest_length: 5806,
                partition_spec_id: 1,
                content: ManifestContentType::Data,
                sequence_number: 0,
                min_sequence_number: 0,
                added_snapshot_id: 1646658105718557341,
                added_files_count: Some(3),
                existing_files_count: Some(0),
                deleted_files_count: Some(0),
                added_rows_count: Some(3),
                existing_rows_count: Some(0),
                deleted_rows_count: Some(0),
                partitions: vec![FieldSummary { contains_null: false, contains_nan: Some(false), lower_bound: Some(Datum::long(1)), upper_bound: Some(Datum::long(1))}],
                key_metadata: vec![],
            }]
        };

        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("manifest_list_v1.avro");
        let io = FileIOBuilder::new_fs_io().build().unwrap();
        let output_file = io.new_output(path.to_str().unwrap()).unwrap();

        let mut writer = ManifestListWriter::v2(output_file, 1646658105718557341, 0, 1);
        writer
            .add_manifests(expected_manifest_list.entries.clone().into_iter())
            .unwrap();
        writer.close().await.unwrap();

        let bs = fs::read(path).unwrap();

        let partition_types = HashMap::from([(
            1,
            StructType::new(vec![Arc::new(NestedField::required(
                1,
                "test",
                Type::Primitive(PrimitiveType::Long),
            ))]),
        )]);

        let manifest_list =
            ManifestList::parse_with_version(&bs, crate::spec::FormatVersion::V2, move |id| {
                Ok(partition_types.get(&id).cloned())
            })
            .unwrap();
        assert_eq!(manifest_list, expected_manifest_list);

        temp_dir.close().unwrap();
    }

    #[tokio::test]
    async fn test_manifest_list_v2_deserializer_aliases() {
        // reading avro manifest file generated by iceberg 1.4.0
        let avro_1_path = "testdata/manifests_lists/manifest-list-v2-1.avro";
        let bs_1 = fs::read(avro_1_path).unwrap();
        let avro_1_fields = read_avro_schema_fields_as_str(bs_1.clone()).await;
        assert_eq!(
            avro_1_fields,
            "manifest_path, manifest_length, partition_spec_id, content, sequence_number, min_sequence_number, added_snapshot_id, added_data_files_count, existing_data_files_count, deleted_data_files_count, added_rows_count, existing_rows_count, deleted_rows_count, partitions"
        );
        // reading avro manifest file generated by iceberg 1.5.0
        let avro_2_path = "testdata/manifests_lists/manifest-list-v2-2.avro";
        let bs_2 = fs::read(avro_2_path).unwrap();
        let avro_2_fields = read_avro_schema_fields_as_str(bs_2.clone()).await;
        assert_eq!(
            avro_2_fields,
            "manifest_path, manifest_length, partition_spec_id, content, sequence_number, min_sequence_number, added_snapshot_id, added_files_count, existing_files_count, deleted_files_count, added_rows_count, existing_rows_count, deleted_rows_count, partitions"
        );
        // deserializing both files to ManifestList struct
        let _manifest_list_1 =
            ManifestList::parse_with_version(&bs_1, crate::spec::FormatVersion::V2, move |_id| {
                Ok(Some(StructType::new(vec![])))
            })
            .unwrap();
        let _manifest_list_2 =
            ManifestList::parse_with_version(&bs_2, crate::spec::FormatVersion::V2, move |_id| {
                Ok(Some(StructType::new(vec![])))
            })
            .unwrap();
    }

    async fn read_avro_schema_fields_as_str(bs: Vec<u8>) -> String {
        let reader = Reader::new(&bs[..]).unwrap();
        let schema = reader.writer_schema();
        let fields: String = match schema {
            Schema::Record(record) => record
                .fields
                .iter()
                .map(|field| field.name.clone())
                .collect::<Vec<String>>()
                .join(", "),
            _ => "".to_string(),
        };
        fields
    }
}
