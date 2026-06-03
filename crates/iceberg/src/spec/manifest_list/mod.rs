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

mod _const_schema;
pub(super) mod _serde;
mod manifest_file;
mod reader;
mod writer;

use apache_avro::types::Value;
use apache_avro::{Reader, from_value};
pub use manifest_file::*;
pub use reader::*;
pub use serde_bytes::ByteBuf;
pub use writer::*;

use self::_const_schema::MANIFEST_LIST_AVRO_SCHEMA_V1;
use super::FormatVersion;
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
    pub fn parse_with_version(bs: &[u8], version: FormatVersion) -> Result<ManifestList> {
        match version {
            FormatVersion::V1 => {
                let reader = Reader::with_schema(&MANIFEST_LIST_AVRO_SCHEMA_V1, bs)?;
                let values = Value::Array(reader.collect::<std::result::Result<Vec<Value>, _>>()?);
                from_value::<_serde::ManifestListV1>(&values)?.try_into()
            }
            FormatVersion::V2 => {
                let reader = Reader::new(bs)?;
                let values = Value::Array(reader.collect::<std::result::Result<Vec<Value>, _>>()?);
                from_value::<_serde::ManifestListV2>(&values)?.try_into()
            }
            FormatVersion::V3 => {
                let reader = Reader::new(bs)?;
                let values = Value::Array(reader.collect::<std::result::Result<Vec<Value>, _>>()?);
                from_value::<_serde::ManifestListV3>(&values)?.try_into()
            }
        }
    }

    /// Get the entries in the manifest list.
    pub fn entries(&self) -> &[ManifestFile] {
        &self.entries
    }

    /// Take ownership of the entries in the manifest list, consuming it
    pub fn consume_entries(self) -> impl IntoIterator<Item = ManifestFile> {
        Box::new(self.entries.into_iter())
    }
}

#[cfg(test)]
mod test {
    use std::fs;

    use apache_avro::{Codec, Writer};
    use tempfile::TempDir;

    use super::_const_schema::MANIFEST_LIST_AVRO_SCHEMA_V2;
    use super::_serde::ManifestFileV2;
    use super::*;
    use crate::io::FileIO;
    use crate::spec::{Datum, FieldSummary, ManifestContentType, ManifestFile};

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
                    partitions: Some(vec![]),
                    key_metadata: None,
                    first_row_id: None,
                }
            ]
        };

        let file_io = FileIO::new_with_fs();

        let tmp_dir = TempDir::new().unwrap();
        let file_name = "simple_manifest_list_v1.avro";
        let full_path = format!("{}/{}", tmp_dir.path().to_str().unwrap(), file_name);

        let mut writer = ManifestListWriter::v1(
            file_io.new_output(full_path.clone()).unwrap(),
            1646658105718557341,
            Some(1646658105718557341),
        );

        writer
            .add_manifests(manifest_list.entries.clone().into_iter())
            .unwrap();
        writer.close().await.unwrap();

        let bs = fs::read(full_path).expect("read_file must succeed");

        let parsed_manifest_list =
            ManifestList::parse_with_version(&bs, crate::spec::FormatVersion::V1).unwrap();

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
                    partitions: Some(
                        vec![FieldSummary { contains_null: false, contains_nan: Some(false), lower_bound: Some(Datum::long(1).to_bytes().unwrap()), upper_bound: Some(Datum::long(1).to_bytes().unwrap())}]
                    ),
                    key_metadata: None,
                    first_row_id: None,
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
                    partitions: Some(
                        vec![FieldSummary { contains_null: false, contains_nan: Some(false), lower_bound: Some(Datum::float(1.1).to_bytes().unwrap()), upper_bound: Some(Datum::float(2.1).to_bytes().unwrap())}]
                    ),
                    key_metadata: None,
                    first_row_id: None,
                }
            ]
        };

        let file_io = FileIO::new_with_fs();

        let tmp_dir = TempDir::new().unwrap();
        let file_name = "simple_manifest_list_v1.avro";
        let full_path = format!("{}/{}", tmp_dir.path().to_str().unwrap(), file_name);

        let mut writer = ManifestListWriter::v2(
            file_io.new_output(full_path.clone()).unwrap(),
            1646658105718557341,
            Some(1646658105718557341),
            1,
        );

        writer
            .add_manifests(manifest_list.entries.clone().into_iter())
            .unwrap();
        writer.close().await.unwrap();

        let bs = fs::read(full_path).expect("read_file must succeed");

        let parsed_manifest_list =
            ManifestList::parse_with_version(&bs, crate::spec::FormatVersion::V2).unwrap();

        assert_eq!(manifest_list, parsed_manifest_list);
    }

    #[test]
    fn test_parse_snappy_manifest_list_v2() {
        let manifest_list = ManifestList {
            entries: vec![ManifestFile {
                manifest_path: "s3a://icebergdata/demo/s1/t1/metadata/snappy-m0.avro".to_string(),
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
                partitions: Some(vec![FieldSummary {
                    contains_null: false,
                    contains_nan: Some(false),
                    lower_bound: Some(Datum::long(1).to_bytes().unwrap()),
                    upper_bound: Some(Datum::long(1).to_bytes().unwrap()),
                }]),
                key_metadata: None,
                first_row_id: None,
            }],
        };

        let manifest_entry: ManifestFileV2 = manifest_list.entries[0].clone().try_into().unwrap();
        let mut writer =
            Writer::with_codec(&MANIFEST_LIST_AVRO_SCHEMA_V2, Vec::new(), Codec::Snappy);
        writer.append_ser(manifest_entry).unwrap();
        let bs = writer.into_inner().unwrap();

        let parsed_manifest_list =
            ManifestList::parse_with_version(&bs, crate::spec::FormatVersion::V2).unwrap();

        assert_eq!(manifest_list, parsed_manifest_list);
    }

    #[tokio::test]
    async fn test_parse_manifest_list_v3() {
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
                    partitions: Some(
                        vec![FieldSummary { contains_null: false, contains_nan: Some(false), lower_bound: Some(Datum::long(1).to_bytes().unwrap()), upper_bound: Some(Datum::long(1).to_bytes().unwrap())}]
                    ),
                    key_metadata: None,
                    first_row_id: Some(10),
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
                    partitions: Some(
                        vec![FieldSummary { contains_null: false, contains_nan: Some(false), lower_bound: Some(Datum::float(1.1).to_bytes().unwrap()), upper_bound: Some(Datum::float(2.1).to_bytes().unwrap())}]
                    ),
                    key_metadata: None,
                    first_row_id: Some(13),
                }
            ]
        };

        let file_io = FileIO::new_with_fs();

        let tmp_dir = TempDir::new().unwrap();
        let file_name = "simple_manifest_list_v3.avro";
        let full_path = format!("{}/{}", tmp_dir.path().to_str().unwrap(), file_name);

        let mut writer = ManifestListWriter::v3(
            file_io.new_output(full_path.clone()).unwrap(),
            377075049360453639,
            Some(377075049360453639),
            1,
            Some(10),
        );

        writer
            .add_manifests(manifest_list.entries.clone().into_iter())
            .unwrap();
        writer.close().await.unwrap();

        let bs = fs::read(full_path).expect("read_file must succeed");

        let parsed_manifest_list =
            ManifestList::parse_with_version(&bs, crate::spec::FormatVersion::V3).unwrap();

        assert_eq!(manifest_list, parsed_manifest_list);
    }
}
