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

use apache_avro::{from_value, Reader};

use crate::{spec::Literal, Error};

use super::{FormatVersion, StructType};

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
#[derive(Debug, Clone)]
pub struct ManifestList {
    version: FormatVersion,
    /// Entries in a manifest list.
    entries: Vec<ManifestListEntry>,
}

impl ManifestList {
    /// Parse manifest list from bytes.
    ///
    /// QUESTION: Will we have more than one manifest list in a single file?
    pub fn parse(
        bs: &[u8],
        version: FormatVersion,
        partition_type: &StructType,
    ) -> Result<ManifestList, Error> {
        let reader = Reader::new(bs)?;
        let entries = match version {
            FormatVersion::V2 => reader
                .map(|v| {
                    v.and_then(|value| from_value::<_serde::ManifestListEntryV2>(&value))
                        .map_err(|e| {
                            Error::new(
                                crate::ErrorKind::DataInvalid,
                                "Failed to parse manifest list entry",
                            )
                            .with_source(e)
                        })
                })
                .map(|v| v.and_then(|v| v.try_into(partition_type)))
                .collect::<Result<Vec<_>, Error>>()?,
            FormatVersion::V1 => reader
                .map(|v| {
                    v.and_then(|value| from_value::<_serde::ManifestListEntryV1>(&value))
                        .map_err(|e| {
                            Error::new(
                                crate::ErrorKind::DataInvalid,
                                "Failed to parse manifest list entry",
                            )
                            .with_source(e)
                        })
                })
                .map(|v| v.and_then(|v| v.try_into(partition_type)))
                .collect::<Result<Vec<_>, Error>>()?,
        };
        Ok(ManifestList { version, entries })
    }

    /// Get the entries in the manifest list.
    pub fn entries(&self) -> &[ManifestListEntry] {
        &self.entries
    }

    /// Get the version of the manifest list.
    pub fn version(&self) -> &FormatVersion {
        &self.version
    }
}

/// Entry in a manifest list.
#[derive(Debug, PartialEq, Clone)]
pub struct ManifestListEntry {
    /// field: 500
    ///
    /// Location of the manifest file
    manifest_path: String,
    /// field: 501
    ///
    /// Length of the manifest file in bytes
    manifest_length: i64,
    /// field: 502
    ///
    /// ID of a partition spec used to write the manifest; must be listed
    /// in table metadata partition-specs
    partition_spec_id: i32,
    /// field: 517
    ///
    /// The type of files tracked by the manifest, either data or delete
    /// files; 0 for all v1 manifests
    content: ManifestContentType,
    /// field: 515
    ///
    /// The sequence number when the manifest was added to the table; use 0
    /// when reading v1 manifest lists
    sequence_number: i64,
    /// field: 516
    ///
    /// The minimum data sequence number of all live data or delete files in
    /// the manifest; use 0 when reading v1 manifest lists
    min_sequence_number: i64,
    /// field: 503
    ///
    /// ID of the snapshot where the manifest file was added
    added_snapshot_id: i64,
    /// field: 504
    ///
    /// Number of entries in the manifest that have status ADDED, when null
    /// this is assumed to be non-zero
    added_data_files_count: Option<i32>,
    /// field: 505
    ///
    /// Number of entries in the manifest that have status EXISTING (0),
    /// when null this is assumed to be non-zero
    existing_data_files_count: Option<i32>,
    /// field: 506
    ///
    /// Number of entries in the manifest that have status DELETED (2),
    /// when null this is assumed to be non-zero
    deleted_data_files_count: Option<i32>,
    /// field: 512
    ///
    /// Number of rows in all of files in the manifest that have status
    /// ADDED, when null this is assumed to be non-zero
    added_rows_count: Option<i64>,
    /// field: 513
    ///
    /// Number of rows in all of files in the manifest that have status
    /// EXISTING, when null this is assumed to be non-zero
    existing_rows_count: Option<i64>,
    /// field: 514
    ///
    /// Number of rows in all of files in the manifest that have status
    /// DELETED, when null this is assumed to be non-zero
    deleted_rows_count: Option<i64>,
    /// field: 507
    /// element_field: 508
    ///
    /// A list of field summaries for each partition field in the spec. Each
    /// field in the list corresponds to a field in the manifest file’s
    /// partition spec.
    partitions: Option<Vec<FieldSummary>>,
    /// field: 519
    ///
    /// Implementation-specific key metadata for encryption
    key_metadata: Option<Vec<u8>>,
}

/// The type of files tracked by the manifest, either data or delete files; Data(0) for all v1 manifests
#[derive(Debug, PartialEq, Clone)]
pub enum ManifestContentType {
    /// The manifest content is data.
    Data = 0,
    /// The manifest content is deletes.
    Deletes = 1,
}

impl TryFrom<i32> for ManifestContentType {
    type Error = Error;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
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

/// Field summary for partition field in the spec.
///
/// Each field in the list corresponds to a field in the manifest file’s partition spec.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct FieldSummary {
    /// field: 509
    ///
    /// Whether the manifest contains at least one partition with a null
    /// value for the field
    contains_null: bool,
    /// field: 518
    /// Whether the manifest contains at least one partition with a NaN
    /// value for the field
    contains_nan: Option<bool>,
    /// field: 510
    /// The minimum value for the field in the manifests
    /// partitions.
    lower_bound: Option<Literal>,
    /// field: 511
    /// The maximum value for the field in the manifests
    /// partitions.
    upper_bound: Option<Literal>,
}

pub(super) mod _serde {
    /// This is a helper module that defines types to help with serialization/deserialization.
    /// For deserialization the input first gets read into either the [ManifestListEntryV1] or [ManifestListEntryV2] struct
    /// and then converted into the [ManifestListEntry] struct. Serialization works the other way around.
    /// [ManifestListEntryV1] and [ManifestListEntryV2] are internal struct that are only used for serialization and deserialization.
    pub use serde_bytes::ByteBuf;
    use serde_derive::{Deserialize, Serialize};

    use crate::{
        spec::{Literal, StructType, Type},
        Error,
    };

    use super::ManifestListEntry;

    #[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
    pub(super) struct ManifestListEntryV1 {
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

    #[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
    pub(super) struct ManifestListEntryV2 {
        pub manifest_path: String,
        pub manifest_length: i64,
        pub partition_spec_id: i32,
        pub content: i32,
        pub sequence_number: i64,
        pub min_sequence_number: i64,
        pub added_snapshot_id: i64,
        pub added_data_files_count: i32,
        pub existing_data_files_count: i32,
        pub deleted_data_files_count: i32,
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
        pub fn try_into(self, r#type: &Type) -> Result<super::FieldSummary, Error> {
            Ok(super::FieldSummary {
                contains_null: self.contains_null,
                contains_nan: self.contains_nan,
                lower_bound: self
                    .lower_bound
                    .map(|v| Literal::try_from_bytes(&v, r#type))
                    .transpose()?,
                upper_bound: self
                    .upper_bound
                    .map(|v| Literal::try_from_bytes(&v, r#type))
                    .transpose()?,
            })
        }
    }

    impl ManifestListEntryV2 {
        /// Converts the [ManifestListEntryV2] into a [ManifestListEntry].
        /// The convert of [partitions] need the partition_type info so use this function instead of [std::TryFrom] trait.
        pub fn try_into(self, partition_type: &StructType) -> Result<ManifestListEntry, Error> {
            let partitions = if let Some(partitions) = self.partitions {
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
                Some(
                    partitions
                        .into_iter()
                        .zip(partition_types)
                        .map(|(v, field)| v.try_into(&field.field_type))
                        .collect::<Result<Vec<_>, _>>()?,
                )
            } else {
                None
            };
            Ok(ManifestListEntry {
                manifest_path: self.manifest_path,
                manifest_length: self.manifest_length,
                partition_spec_id: self.partition_spec_id,
                content: self.content.try_into()?,
                sequence_number: self.sequence_number,
                min_sequence_number: self.min_sequence_number,
                added_snapshot_id: self.added_snapshot_id,
                added_data_files_count: Some(self.added_data_files_count),
                existing_data_files_count: Some(self.existing_data_files_count),
                deleted_data_files_count: Some(self.deleted_data_files_count),
                added_rows_count: Some(self.added_rows_count),
                existing_rows_count: Some(self.existing_rows_count),
                deleted_rows_count: Some(self.deleted_rows_count),
                partitions,
                key_metadata: self.key_metadata.map(|b| b.into_vec()),
            })
        }
    }

    impl ManifestListEntryV1 {
        /// Converts the [ManifestListEntryV1] into a [ManifestListEntry].
        /// The convert of [partitions] need the partition_type info so use this function instead of [std::TryFrom] trait.
        pub fn try_into(self, partition_type: &StructType) -> Result<ManifestListEntry, Error> {
            let partitions = if let Some(partitions) = self.partitions {
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
                Some(
                    partitions
                        .into_iter()
                        .zip(partition_types)
                        .map(|(v, field)| v.try_into(&field.field_type))
                        .collect::<Result<Vec<_>, _>>()?,
                )
            } else {
                None
            };
            Ok(ManifestListEntry {
                manifest_path: self.manifest_path,
                manifest_length: self.manifest_length,
                partition_spec_id: self.partition_spec_id,
                added_snapshot_id: self.added_snapshot_id,
                added_data_files_count: self.added_data_files_count,
                existing_data_files_count: self.existing_data_files_count,
                deleted_data_files_count: self.deleted_data_files_count,
                added_rows_count: self.added_rows_count,
                existing_rows_count: self.existing_rows_count,
                deleted_rows_count: self.deleted_rows_count,
                partitions,
                key_metadata: self.key_metadata.map(|b| b.into_vec()),
                // as ref: https://iceberg.apache.org/spec/#partitioning
                // use 0 when reading v1 manifest lists
                content: super::ManifestContentType::Data,
                sequence_number: 0,
                min_sequence_number: 0,
            })
        }
    }

    impl TryFrom<ManifestListEntry> for ManifestListEntryV2 {
        type Error = Error;

        fn try_from(value: ManifestListEntry) -> Result<Self, Self::Error> {
            Ok(Self {
                manifest_path: value.manifest_path,
                manifest_length: value.manifest_length,
                partition_spec_id: value.partition_spec_id,
                content: value.content as i32,
                sequence_number: value.sequence_number,
                min_sequence_number: value.min_sequence_number,
                added_snapshot_id: value.added_snapshot_id,
                added_data_files_count: value.added_data_files_count.ok_or_else(|| {
                    Error::new(
                        crate::ErrorKind::DataInvalid,
                        "added_data_files_count in ManifestListEntryV2 shold be require",
                    )
                })?,
                existing_data_files_count: value.existing_data_files_count.ok_or_else(|| {
                    Error::new(
                        crate::ErrorKind::DataInvalid,
                        "existing_data_files_count in ManifestListEntryV2 shold be require",
                    )
                })?,
                deleted_data_files_count: value.deleted_data_files_count.ok_or_else(|| {
                    Error::new(
                        crate::ErrorKind::DataInvalid,
                        "deleted_data_files_count in ManifestListEntryV2 shold be require",
                    )
                })?,
                added_rows_count: value.added_rows_count.ok_or_else(|| {
                    Error::new(
                        crate::ErrorKind::DataInvalid,
                        "added_rows_count in ManifestListEntryV2 shold be require",
                    )
                })?,
                existing_rows_count: value.existing_rows_count.ok_or_else(|| {
                    Error::new(
                        crate::ErrorKind::DataInvalid,
                        "existing_rows_count in ManifestListEntryV2 shold be require",
                    )
                })?,
                deleted_rows_count: value.deleted_rows_count.ok_or_else(|| {
                    Error::new(
                        crate::ErrorKind::DataInvalid,
                        "deleted_rows_count in ManifestListEntryV2 shold be require",
                    )
                })?,
                partitions: value.partitions.map(|v| {
                    v.into_iter()
                        .map(|v| FieldSummary {
                            contains_null: v.contains_null,
                            contains_nan: v.contains_nan,
                            lower_bound: v.lower_bound.map(|v| v.into()),
                            upper_bound: v.upper_bound.map(|v| v.into()),
                        })
                        .collect()
                }),
                key_metadata: value.key_metadata.map(ByteBuf::from),
            })
        }
    }

    impl From<ManifestListEntry> for ManifestListEntryV1 {
        fn from(value: ManifestListEntry) -> Self {
            Self {
                manifest_path: value.manifest_path,
                manifest_length: value.manifest_length,
                partition_spec_id: value.partition_spec_id,
                added_snapshot_id: value.added_snapshot_id,
                added_data_files_count: value.added_data_files_count,
                existing_data_files_count: value.existing_data_files_count,
                deleted_data_files_count: value.deleted_data_files_count,
                added_rows_count: value.added_rows_count,
                existing_rows_count: value.existing_rows_count,
                deleted_rows_count: value.deleted_rows_count,
                partitions: value.partitions.map(|v| {
                    v.into_iter()
                        .map(|v| FieldSummary {
                            contains_null: v.contains_null,
                            contains_nan: v.contains_nan,
                            lower_bound: v.lower_bound.map(|v| v.into()),
                            upper_bound: v.upper_bound.map(|v| v.into()),
                        })
                        .collect()
                }),
                key_metadata: value.key_metadata.map(ByteBuf::from),
            }
        }
    }
}

#[cfg(test)]
mod test {
    use std::{fs, sync::Arc};

    use crate::spec::{
        FieldSummary, Literal, ManifestContentType, ManifestList, ManifestListEntry, NestedField,
        PrimitiveType, StructType, Type,
    };

    #[test]
    fn test_parse_manifest_list_v1() {
        let path = format!(
            "{}/testdata/simple_manifest_list_v1.avro",
            env!("CARGO_MANIFEST_DIR")
        );

        let bs = fs::read(path).expect("read_file must succeed");

        let manifest_list = ManifestList::parse(
            &bs,
            crate::spec::FormatVersion::V1,
            &StructType::new(vec![]),
        )
        .unwrap();

        assert_eq!(1, manifest_list.entries.len());
        assert_eq!(
            manifest_list.entries[0],
            ManifestListEntry {
                manifest_path: "/opt/bitnami/spark/warehouse/db/table/metadata/10d28031-9739-484c-92db-cdf2975cead4-m0.avro".to_string(),
                manifest_length: 5806,
                partition_spec_id: 0,
                content: ManifestContentType::Data,
                sequence_number: 0,
                min_sequence_number: 0,
                added_snapshot_id: 1646658105718557341,
                added_data_files_count: Some(3),
                existing_data_files_count: Some(0),
                deleted_data_files_count: Some(0),
                added_rows_count: Some(3),
                existing_rows_count: Some(0),
                deleted_rows_count: Some(0),
                partitions: Some(vec![]),
                key_metadata: None,
            }
        );
    }

    #[test]
    fn test_parse_manifest_list_v2() {
        let path = format!(
            "{}/testdata/simple_manifest_list_v2.avro",
            env!("CARGO_MANIFEST_DIR")
        );

        let bs = fs::read(path).expect("read_file must succeed");

        let manifest_list = ManifestList::parse(
            &bs,
            crate::spec::FormatVersion::V2,
            &StructType::new(vec![Arc::new(NestedField::required(
                1,
                "test",
                Type::Primitive(PrimitiveType::Long),
            ))]),
        )
        .unwrap();

        assert_eq!(1, manifest_list.entries.len());
        assert_eq!(
            manifest_list.entries[0],
            ManifestListEntry {
                manifest_path: "s3a://icebergdata/demo/s1/t1/metadata/05ffe08b-810f-49b3-a8f4-e88fc99b254a-m0.avro".to_string(),
                manifest_length: 6926,
                partition_spec_id: 0,
                content: ManifestContentType::Data,
                sequence_number: 1,
                min_sequence_number: 1,
                added_snapshot_id: 377075049360453639,
                added_data_files_count: Some(1),
                existing_data_files_count: Some(0),
                deleted_data_files_count: Some(0),
                added_rows_count: Some(3),
                existing_rows_count: Some(0),
                deleted_rows_count: Some(0),
                partitions: Some(vec![FieldSummary { contains_null: false, contains_nan: Some(false), lower_bound: Some(Literal::long(1)), upper_bound: Some(Literal::long(1))}]),
                key_metadata: None,
            }
        );
    }
}
