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

use apache_avro::Reader;
pub use manifest_file::*;
pub use reader::*;
pub use serde_bytes::ByteBuf;
pub use writer::*;

use super::FormatVersion;
use crate::avro::Resolved;
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
        let reader = Reader::new(bs)?;
        let entries = match version {
            FormatVersion::V1 => reader
                .into_deser_iter::<Resolved<_serde::ManifestFileV1>>()
                .map(|entry| entry?.0.try_into())
                .collect::<Result<Vec<_>>>()?,
            FormatVersion::V2 => reader
                .into_deser_iter::<Resolved<_serde::ManifestFileV2>>()
                .map(|entry| entry?.0.try_into())
                .collect::<Result<Vec<_>>>()?,
            FormatVersion::V3 => reader
                .into_deser_iter::<Resolved<_serde::ManifestFileV3>>()
                .map(|entry| entry?.0.try_into())
                .collect::<Result<Vec<_>>>()?,
        };
        Ok(ManifestList { entries })
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
                    key_metadata: Some(vec![1, 2, 3]),
                    first_row_id: None,
                }
            ]
        };

        let file_io = FileIO::new_with_fs();

        let tmp_dir = TempDir::new().unwrap();
        let file_name = "simple_manifest_list_v1.avro";
        let full_path = format!("{}/{}", tmp_dir.path().to_str().unwrap(), file_name);

        let mut writer = ManifestListWriter::v1(
            file_io
                .new_output(full_path.clone())
                .unwrap()
                .writer()
                .await
                .unwrap(),
            1646658105718557341,
            Some(1646658105718557341),
        );

        writer
            .add_manifests(manifest_list.entries.clone().into_iter())
            .unwrap();
        writer.close().await.unwrap();

        let bs = fs::read(full_path).expect("read_file must succeed");

        let parsed_manifest_list =
            ManifestList::parse_with_version(&bs, FormatVersion::V1).unwrap();

        assert_eq!(manifest_list, parsed_manifest_list);
    }

    const VERSIONS: [FormatVersion; 3] = [FormatVersion::V1, FormatVersion::V2, FormatVersion::V3];

    fn entry(version: FormatVersion) -> ManifestFile {
        ManifestFile {
            manifest_path: "s3://test/metadata/manifest.avro".to_string(),
            manifest_length: 1234,
            partition_spec_id: 0,
            content: ManifestContentType::Data,
            sequence_number: if version == FormatVersion::V1 { 0 } else { 7 },
            min_sequence_number: if version == FormatVersion::V1 { 0 } else { 3 },
            added_snapshot_id: 42,
            added_files_count: Some(1),
            existing_files_count: Some(0),
            deleted_files_count: Some(0),
            added_rows_count: Some(9),
            existing_rows_count: Some(0),
            deleted_rows_count: Some(0),
            partitions: Some(vec![FieldSummary {
                contains_null: true,
                contains_nan: None,
                lower_bound: Some(vec![1, 0, 0, 0].into()),
                upper_bound: None,
            }]),
            key_metadata: Some(vec![1, 2, 3]),
            first_row_id: (version == FormatVersion::V3).then_some(100),
        }
    }

    async fn encode(version: FormatVersion, entries: Vec<ManifestFile>) -> Vec<u8> {
        let io = FileIO::new_with_memory();
        let output = io.new_output("memory:///manifest-list.avro").unwrap();
        let writer = output.writer().await.unwrap();
        let mut writer = match version {
            FormatVersion::V1 => ManifestListWriter::v1(writer, 42, None),
            FormatVersion::V2 => ManifestListWriter::v2(writer, 42, None, 7),
            FormatVersion::V3 => ManifestListWriter::v3(writer, 42, None, 7, Some(100)),
        };
        writer.add_manifests(entries.into_iter()).unwrap();
        writer.close().await.unwrap();
        io.new_input("memory:///manifest-list.avro")
            .unwrap()
            .read()
            .await
            .unwrap()
            .to_vec()
    }

    #[tokio::test]
    async fn test_parse_empty_manifest_list() {
        for version in VERSIONS {
            let bs = encode(version, vec![]).await;
            let parsed = ManifestList::parse_with_version(&bs, version).unwrap();
            assert!(parsed.entries().is_empty(), "{version:?}");
        }
    }

    #[tokio::test]
    async fn test_parse_truncated_manifest_list() {
        for version in VERSIONS {
            let bs = encode(version, vec![entry(version)]).await;
            let result = ManifestList::parse_with_version(&bs[..bs.len() - 1], version);
            assert!(result.is_err(), "{version:?}");
        }
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
                        vec![FieldSummary { contains_null: false, contains_nan: None, lower_bound: Some(Datum::float(1.1_f32).to_bytes().unwrap()), upper_bound: None}]
                    ),
                    key_metadata: Some(vec![1, 2, 3]),
                    first_row_id: None,
                }
            ]
        };

        let file_io = FileIO::new_with_fs();

        let tmp_dir = TempDir::new().unwrap();
        let file_name = "simple_manifest_list_v1.avro";
        let full_path = format!("{}/{}", tmp_dir.path().to_str().unwrap(), file_name);

        let mut writer = ManifestListWriter::v2(
            file_io
                .new_output(full_path.clone())
                .unwrap()
                .writer()
                .await
                .unwrap(),
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
            ManifestList::parse_with_version(&bs, FormatVersion::V2).unwrap();

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
            Writer::with_codec(&MANIFEST_LIST_AVRO_SCHEMA_V2, Vec::new(), Codec::Snappy).unwrap();
        writer.append_ser(manifest_entry).unwrap();
        let bs = writer.into_inner().unwrap();

        let parsed_manifest_list =
            ManifestList::parse_with_version(&bs, FormatVersion::V2).unwrap();

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
                        vec![FieldSummary { contains_null: false, contains_nan: None, lower_bound: Some(Datum::float(1.1_f32).to_bytes().unwrap()), upper_bound: None}]
                    ),
                    key_metadata: Some(vec![1, 2, 3]),
                    first_row_id: Some(13),
                }
            ]
        };

        let file_io = FileIO::new_with_fs();

        let tmp_dir = TempDir::new().unwrap();
        let file_name = "simple_manifest_list_v3.avro";
        let full_path = format!("{}/{}", tmp_dir.path().to_str().unwrap(), file_name);

        let mut writer = ManifestListWriter::v3(
            file_io
                .new_output(full_path.clone())
                .unwrap()
                .writer()
                .await
                .unwrap(),
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
            ManifestList::parse_with_version(&bs, FormatVersion::V3).unwrap();

        assert_eq!(manifest_list, parsed_manifest_list);
    }
}
