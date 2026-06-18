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

/// This is a helper module that defines types to help with serialization/deserialization.
/// For deserialization the input first gets read into either the [ManifestFileV1] or [ManifestFileV2] struct
/// and then converted into the [ManifestFile] struct. Serialization works the other way around.
/// [ManifestFileV1] and [ManifestFileV2] are internal struct that are only used for serialization and deserialization.
pub use serde_bytes::ByteBuf;
use serde_derive::{Deserialize, Serialize};

use super::ManifestFile;
use crate::Error;
use crate::error::Result;
use crate::spec::FieldSummary;

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(transparent)]
pub(crate) struct ManifestListV3 {
    entries: Vec<ManifestFileV3>,
}

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

impl ManifestListV3 {
    /// Converts the [ManifestListV3] into a [ManifestList].
    pub fn try_into(self) -> Result<super::ManifestList> {
        Ok(super::ManifestList {
            entries: self
                .entries
                .into_iter()
                .map(|v| v.try_into())
                .collect::<Result<Vec<_>>>()?,
        })
    }
}

impl TryFrom<super::ManifestList> for ManifestListV3 {
    type Error = Error;

    fn try_from(value: super::ManifestList) -> std::result::Result<Self, Self::Error> {
        Ok(Self {
            entries: value
                .entries
                .into_iter()
                .map(|v| v.try_into())
                .collect::<std::result::Result<Vec<_>, _>>()?,
        })
    }
}

impl ManifestListV2 {
    /// Converts the [ManifestListV2] into a [ManifestList].
    pub fn try_into(self) -> Result<super::ManifestList> {
        Ok(super::ManifestList {
            entries: self
                .entries
                .into_iter()
                .map(|v| v.try_into())
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
                .map(|v| v.try_into())
                .collect::<std::result::Result<Vec<_>, _>>()?,
        })
    }
}

impl ManifestListV1 {
    /// Converts the [ManifestListV1] into a [ManifestList].
    pub fn try_into(self) -> Result<super::ManifestList> {
        Ok(super::ManifestList {
            entries: self
                .entries
                .into_iter()
                .map(|v| v.try_into())
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
                .map(|v| v.try_into())
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
    #[serde(default = "v2_default_content_for_v1")]
    pub content: i32,
    #[serde(default = "v2_default_sequence_number_for_v1")]
    pub sequence_number: i64,
    #[serde(default = "v2_default_min_sequence_number_for_v1")]
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
pub(super) struct ManifestFileV3 {
    pub manifest_path: String,
    pub manifest_length: i64,
    pub partition_spec_id: i32,
    #[serde(default = "v2_default_content_for_v1")]
    pub content: i32,
    #[serde(default = "v2_default_sequence_number_for_v1")]
    pub sequence_number: i64,
    #[serde(default = "v2_default_min_sequence_number_for_v1")]
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
    pub first_row_id: Option<u64>,
}

impl ManifestFileV3 {
    /// Converts the [ManifestFileV3] into a [ManifestFile].
    pub fn try_into(self) -> Result<ManifestFile> {
        let manifest_file = ManifestFile {
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
            partitions: self.partitions,
            key_metadata: self.key_metadata.map(|b| b.into_vec()),
            first_row_id: self.first_row_id,
        };

        Ok(manifest_file)
    }
}

impl ManifestFileV2 {
    /// Converts the [ManifestFileV2] into a [ManifestFile].
    pub fn try_into(self) -> Result<ManifestFile> {
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
            partitions: self.partitions,
            key_metadata: self.key_metadata.map(|b| b.into_vec()),
            first_row_id: None,
        })
    }
}

fn v2_default_content_for_v1() -> i32 {
    super::ManifestContentType::Data as i32
}

fn v2_default_sequence_number_for_v1() -> i64 {
    0
}

fn v2_default_min_sequence_number_for_v1() -> i64 {
    0
}

impl ManifestFileV1 {
    /// Converts the [ManifestFileV1] into a [ManifestFile].
    pub fn try_into(self) -> Result<ManifestFile> {
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
            partitions: self.partitions,
            key_metadata: self.key_metadata.map(|b| b.into_vec()),
            // as ref: https://iceberg.apache.org/spec/#partitioning
            // use 0 when reading v1 manifest lists
            content: super::ManifestContentType::Data,
            sequence_number: 0,
            min_sequence_number: 0,
            first_row_id: None,
        })
    }
}

fn convert_to_serde_key_metadata(key_metadata: Option<Vec<u8>>) -> Option<ByteBuf> {
    match key_metadata {
        Some(metadata) if !metadata.is_empty() => Some(ByteBuf::from(metadata)),
        _ => None,
    }
}

impl TryFrom<ManifestFile> for ManifestFileV3 {
    type Error = Error;

    fn try_from(value: ManifestFile) -> std::result::Result<Self, Self::Error> {
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
                        "added_data_files_count in ManifestFileV3 is required",
                    )
                })?
                .try_into()?,
            existing_files_count: value
                .existing_files_count
                .ok_or_else(|| {
                    Error::new(
                        crate::ErrorKind::DataInvalid,
                        "existing_data_files_count in ManifestFileV3 is required",
                    )
                })?
                .try_into()?,
            deleted_files_count: value
                .deleted_files_count
                .ok_or_else(|| {
                    Error::new(
                        crate::ErrorKind::DataInvalid,
                        "deleted_data_files_count in ManifestFileV3 is required",
                    )
                })?
                .try_into()?,
            added_rows_count: value
                .added_rows_count
                .ok_or_else(|| {
                    Error::new(
                        crate::ErrorKind::DataInvalid,
                        "added_rows_count in ManifestFileV3 is required",
                    )
                })?
                .try_into()?,
            existing_rows_count: value
                .existing_rows_count
                .ok_or_else(|| {
                    Error::new(
                        crate::ErrorKind::DataInvalid,
                        "existing_rows_count in ManifestFileV3 is required",
                    )
                })?
                .try_into()?,
            deleted_rows_count: value
                .deleted_rows_count
                .ok_or_else(|| {
                    Error::new(
                        crate::ErrorKind::DataInvalid,
                        "deleted_rows_count in ManifestFileV3 is required",
                    )
                })?
                .try_into()?,
            partitions: value.partitions,
            key_metadata,
            first_row_id: value.first_row_id,
        })
    }
}

impl TryFrom<ManifestFile> for ManifestFileV2 {
    type Error = Error;

    fn try_from(value: ManifestFile) -> std::result::Result<Self, Self::Error> {
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
            partitions: value.partitions,
            key_metadata,
        })
    }
}

impl TryFrom<ManifestFile> for ManifestFileV1 {
    type Error = Error;

    fn try_from(value: ManifestFile) -> std::result::Result<Self, Self::Error> {
        let key_metadata = convert_to_serde_key_metadata(value.key_metadata);
        Ok(Self {
            manifest_path: value.manifest_path,
            manifest_length: value.manifest_length,
            partition_spec_id: value.partition_spec_id,
            added_snapshot_id: value.added_snapshot_id,
            added_data_files_count: value.added_files_count.map(TryInto::try_into).transpose()?,
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
            partitions: value.partitions,
            key_metadata,
        })
    }
}

#[cfg(test)]
mod test {
    use std::fs;

    use apache_avro::{Reader, Schema};

    use super::{ManifestListV1, ManifestListV2, ManifestListV3};
    use crate::spec::{Datum, FieldSummary, ManifestContentType, ManifestFile, ManifestList};

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
                partitions: None,
                key_metadata: None,
                first_row_id: None,
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
                partitions: Some(
                    vec![FieldSummary { contains_null: false, contains_nan: Some(false), lower_bound: Some(Datum::long(1).to_bytes().unwrap()), upper_bound: Some(Datum::long(1).to_bytes().unwrap())}]
                ),
                key_metadata: None,
                first_row_id: None,
            }]
        }.try_into().unwrap();
        let result = serde_json::to_string(&manifest_list).unwrap();
        assert_eq!(
            result,
            r#"[{"manifest_path":"s3a://icebergdata/demo/s1/t1/metadata/05ffe08b-810f-49b3-a8f4-e88fc99b254a-m0.avro","manifest_length":6926,"partition_spec_id":1,"content":0,"sequence_number":1,"min_sequence_number":1,"added_snapshot_id":377075049360453639,"added_files_count":1,"existing_files_count":0,"deleted_files_count":0,"added_rows_count":3,"existing_rows_count":0,"deleted_rows_count":0,"partitions":[{"contains_null":false,"contains_nan":false,"lower_bound":[1,0,0,0,0,0,0,0],"upper_bound":[1,0,0,0,0,0,0,0]}],"key_metadata":null}]"#
        );
    }

    #[test]
    fn test_serialize_manifest_list_v3() {
        let manifest_list: ManifestListV3 = ManifestList {
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
                partitions: Some(
                    vec![FieldSummary { contains_null: false, contains_nan: Some(false), lower_bound: Some(Datum::long(1).to_bytes().unwrap()), upper_bound: Some(Datum::long(1).to_bytes().unwrap())}]
                ),
                key_metadata: None,
                first_row_id: Some(10),
            }]
        }.try_into().unwrap();
        let result = serde_json::to_string(&manifest_list).unwrap();
        assert_eq!(
            result,
            r#"[{"manifest_path":"s3a://icebergdata/demo/s1/t1/metadata/05ffe08b-810f-49b3-a8f4-e88fc99b254a-m0.avro","manifest_length":6926,"partition_spec_id":1,"content":0,"sequence_number":1,"min_sequence_number":1,"added_snapshot_id":377075049360453639,"added_files_count":1,"existing_files_count":0,"deleted_files_count":0,"added_rows_count":3,"existing_rows_count":0,"deleted_rows_count":0,"partitions":[{"contains_null":false,"contains_nan":false,"lower_bound":[1,0,0,0,0,0,0,0],"upper_bound":[1,0,0,0,0,0,0,0]}],"key_metadata":null,"first_row_id":10}]"#
        );
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
            ManifestList::parse_with_version(&bs_1, crate::spec::FormatVersion::V2).unwrap();
        let _manifest_list_2 =
            ManifestList::parse_with_version(&bs_2, crate::spec::FormatVersion::V2).unwrap();
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

    #[test]
    fn test_manifest_file_v1_to_v2_projection() {
        use super::ManifestFileV1;

        // Create a V1 manifest file object (without V2 fields)
        let v1_manifest = ManifestFileV1 {
            manifest_path: "/test/manifest.avro".to_string(),
            manifest_length: 5806,
            partition_spec_id: 0,
            added_snapshot_id: 1646658105718557341,
            added_data_files_count: Some(3),
            existing_data_files_count: Some(0),
            deleted_data_files_count: Some(0),
            added_rows_count: Some(3),
            existing_rows_count: Some(0),
            deleted_rows_count: Some(0),
            partitions: None,
            key_metadata: None,
        };

        // Convert V1 to V2 - this should apply defaults for missing V2 fields
        let v2_manifest: ManifestFile = v1_manifest.try_into().unwrap();

        // Verify V1→V2 projection defaults are applied correctly
        assert_eq!(
            v2_manifest.content,
            ManifestContentType::Data,
            "V1 manifest content should default to Data (0)"
        );
        assert_eq!(
            v2_manifest.sequence_number, 0,
            "V1 manifest sequence_number should default to 0"
        );
        assert_eq!(
            v2_manifest.min_sequence_number, 0,
            "V1 manifest min_sequence_number should default to 0"
        );

        // Verify other fields are preserved correctly
        assert_eq!(v2_manifest.manifest_path, "/test/manifest.avro");
        assert_eq!(v2_manifest.manifest_length, 5806);
        assert_eq!(v2_manifest.partition_spec_id, 0);
        assert_eq!(v2_manifest.added_snapshot_id, 1646658105718557341);
        assert_eq!(v2_manifest.added_files_count, Some(3));
        assert_eq!(v2_manifest.existing_files_count, Some(0));
        assert_eq!(v2_manifest.deleted_files_count, Some(0));
        assert_eq!(v2_manifest.added_rows_count, Some(3));
        assert_eq!(v2_manifest.existing_rows_count, Some(0));
        assert_eq!(v2_manifest.deleted_rows_count, Some(0));
        assert_eq!(v2_manifest.partitions, None);
        assert_eq!(v2_manifest.key_metadata, None);
    }
}
