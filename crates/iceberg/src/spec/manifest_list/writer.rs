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

use apache_avro::Writer;
use bytes::Bytes;

use super::_const_schema::{
    MANIFEST_LIST_AVRO_SCHEMA_V1, MANIFEST_LIST_AVRO_SCHEMA_V2, MANIFEST_LIST_AVRO_SCHEMA_V3,
};
use super::_serde::{ManifestFileV1, ManifestFileV2, ManifestFileV3};
use super::{FormatVersion, ManifestContentType, ManifestFile, UNASSIGNED_SEQUENCE_NUMBER};
use crate::compression::CompressionCodec;
use crate::error::{Result, invalid_data};
use crate::io::{FileMetadata, FileWrite};
use crate::spec::avro_util::to_avro_codec;
use crate::{Error, ErrorKind};

/// A manifest list writer.
pub struct ManifestListWriter {
    format_version: FormatVersion,
    writer: Box<dyn FileWrite>,
    avro_writer: Writer<'static, Vec<u8>>,
    sequence_number: i64,
    snapshot_id: i64,
    next_row_id: Option<u64>,
}

impl std::fmt::Debug for ManifestListWriter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ManifestListWriter")
            .field("format_version", &self.format_version)
            .field("avro_writer", &self.avro_writer.schema())
            .finish_non_exhaustive()
    }
}

impl ManifestListWriter {
    /// Get the next row ID that will be assigned to the next data manifest added.
    pub fn next_row_id(&self) -> Option<u64> {
        self.next_row_id
    }

    /// Construct a v1 [`ManifestListWriter`] that writes to a provided [`FileWrite`].
    pub fn v1(
        writer: Box<dyn FileWrite>,
        snapshot_id: i64,
        parent_snapshot_id: Option<i64>,
        compression: CompressionCodec,
    ) -> Result<Self> {
        let mut metadata = HashMap::from_iter([
            ("snapshot-id".to_string(), snapshot_id.to_string()),
            ("format-version".to_string(), "1".to_string()),
        ]);
        if let Some(parent_snapshot_id) = parent_snapshot_id {
            metadata.insert(
                "parent-snapshot-id".to_string(),
                parent_snapshot_id.to_string(),
            );
        }
        Self::new(
            FormatVersion::V1,
            writer,
            metadata,
            0,
            snapshot_id,
            None,
            compression,
        )
    }

    /// Construct a v2 [`ManifestListWriter`] that writes to a provided [`FileWrite`].
    pub fn v2(
        writer: Box<dyn FileWrite>,
        snapshot_id: i64,
        parent_snapshot_id: Option<i64>,
        sequence_number: i64,
        compression: CompressionCodec,
    ) -> Result<Self> {
        let mut metadata = HashMap::from_iter([
            ("snapshot-id".to_string(), snapshot_id.to_string()),
            ("sequence-number".to_string(), sequence_number.to_string()),
            ("format-version".to_string(), "2".to_string()),
        ]);
        metadata.insert(
            "parent-snapshot-id".to_string(),
            parent_snapshot_id
                .map(|v| v.to_string())
                .unwrap_or("null".to_string()),
        );
        Self::new(
            FormatVersion::V2,
            writer,
            metadata,
            sequence_number,
            snapshot_id,
            None,
            compression,
        )
    }

    /// Construct a v3 [`ManifestListWriter`] that writes to a provided [`FileWrite`].
    pub fn v3(
        writer: Box<dyn FileWrite>,
        snapshot_id: i64,
        parent_snapshot_id: Option<i64>,
        sequence_number: i64,
        first_row_id: Option<u64>, // Always None for delete manifests
        compression: CompressionCodec,
    ) -> Result<Self> {
        let mut metadata = HashMap::from_iter([
            ("snapshot-id".to_string(), snapshot_id.to_string()),
            ("sequence-number".to_string(), sequence_number.to_string()),
            ("format-version".to_string(), "3".to_string()),
        ]);
        metadata.insert(
            "parent-snapshot-id".to_string(),
            parent_snapshot_id
                .map(|v| v.to_string())
                .unwrap_or("null".to_string()),
        );
        metadata.insert(
            "first-row-id".to_string(),
            first_row_id
                .map(|v| v.to_string())
                .unwrap_or("null".to_string()),
        );
        Self::new(
            FormatVersion::V3,
            writer,
            metadata,
            sequence_number,
            snapshot_id,
            first_row_id,
            compression,
        )
    }

    fn new(
        format_version: FormatVersion,
        writer: Box<dyn FileWrite>,
        metadata: HashMap<String, String>,
        sequence_number: i64,
        snapshot_id: i64,
        first_row_id: Option<u64>,
        compression: CompressionCodec,
    ) -> Result<Self> {
        let codec = to_avro_codec(compression)?;
        let avro_schema = match format_version {
            FormatVersion::V1 => &MANIFEST_LIST_AVRO_SCHEMA_V1,
            FormatVersion::V2 => &MANIFEST_LIST_AVRO_SCHEMA_V2,
            FormatVersion::V3 => &MANIFEST_LIST_AVRO_SCHEMA_V3,
        };
        let mut avro_writer = Writer::with_codec(avro_schema, Vec::new(), codec);
        for (key, value) in metadata {
            avro_writer
                .add_user_metadata(key, value)
                .expect("Avro metadata should be added to the writer before the first record.");
        }
        Ok(Self {
            format_version,
            writer,
            avro_writer,
            sequence_number,
            snapshot_id,
            next_row_id: first_row_id,
        })
    }

    /// Append manifests to be written.
    ///
    /// If V3 Manifests are added and the `first_row_id` of any data manifest is unassigned,
    /// it will be assigned based on the `next_row_id` of the writer, and the `next_row_id` of the writer will be updated accordingly.
    /// If `first_row_id` is already assigned, it will be validated against the `next_row_id` of the writer.
    pub fn add_manifests(&mut self, manifests: impl Iterator<Item = ManifestFile>) -> Result<()> {
        match self.format_version {
            FormatVersion::V1 => {
                for manifest in manifests {
                    let manifests: ManifestFileV1 = manifest.try_into()?;
                    self.avro_writer.append_ser(manifests)?;
                }
            }
            FormatVersion::V2 | FormatVersion::V3 => {
                for mut manifest in manifests {
                    self.assign_sequence_numbers(&mut manifest)?;

                    if self.format_version == FormatVersion::V2 {
                        let manifest_entry: ManifestFileV2 = manifest.try_into()?;
                        self.avro_writer.append_ser(manifest_entry)?;
                    } else if self.format_version == FormatVersion::V3 {
                        self.assign_first_row_id(&mut manifest)?;
                        let manifest_entry: ManifestFileV3 = manifest.try_into()?;
                        self.avro_writer.append_ser(manifest_entry)?;
                    }
                }
            }
        }
        Ok(())
    }

    /// Write the manifest list and return its stored size.
    pub async fn close(mut self) -> Result<FileMetadata> {
        let data = self.avro_writer.into_inner()?;
        self.writer.write(Bytes::from(data)).await?;
        self.writer.close().await
    }

    /// Assign sequence numbers to manifest if they are unassigned
    fn assign_sequence_numbers(&self, manifest: &mut ManifestFile) -> Result<()> {
        if manifest.sequence_number == UNASSIGNED_SEQUENCE_NUMBER {
            if manifest.added_snapshot_id != self.snapshot_id {
                return Err(invalid_data!(
                    "Found unassigned sequence number for a manifest from snapshot {}.",
                    manifest.added_snapshot_id
                ));
            }
            manifest.sequence_number = self.sequence_number;
        }

        if manifest.min_sequence_number == UNASSIGNED_SEQUENCE_NUMBER {
            if manifest.added_snapshot_id != self.snapshot_id {
                return Err(invalid_data!(
                    "Found unassigned sequence number for a manifest from snapshot {}.",
                    manifest.added_snapshot_id
                ));
            }
            manifest.min_sequence_number = self.sequence_number;
        }

        Ok(())
    }

    /// Assigns `manifest.first_row_id` if not already set, advancing `self.next_row_id`
    /// by the manifest's record count.
    fn assign_first_row_id(&mut self, manifest: &mut ManifestFile) -> Result<()> {
        match manifest.content {
            ManifestContentType::Data => {
                match (self.next_row_id, manifest.first_row_id) {
                    (Some(_), Some(_)) => {
                        // Case: Manifest with already assigned first row ID.
                        // No need to increase next_row_id, as this manifest is already assigned.
                    }
                    (None, Some(manifest_first_row_id)) => {
                        // Case: Assigned first row ID for data manifest, but the writer does not have a next-row-id assigned.
                        return Err(Error::new(
                            ErrorKind::Unexpected,
                            format!(
                                "Found invalid first-row-id assignment for Manifest {}. Writer does not have a next-row-id assigned, but the manifest has first-row-id assigned to {}.",
                                manifest.manifest_path, manifest_first_row_id,
                            ),
                        ));
                    }
                    (Some(writer_next_row_id), None) => {
                        // Case: Unassigned first row ID for data manifest. This is either a new
                        // manifest, or a manifest from a pre-v3 snapshot. We need to assign one.
                        let (existing_rows_count, added_rows_count) =
                            require_row_counts_in_manifest(manifest)?;
                        manifest.first_row_id = Some(writer_next_row_id);

                        self.next_row_id = writer_next_row_id
                        .checked_add(existing_rows_count)
                        .and_then(|sum| sum.checked_add(added_rows_count))
                        .ok_or_else(|| {
                            invalid_data!(
                                    "Row ID overflow when computing next row ID for Manifest {}. Next Row ID: {writer_next_row_id}, Existing Rows Count: {existing_rows_count}, Added Rows Count: {added_rows_count}",
                                    manifest.manifest_path
                                )
                        }).map(Some)?;
                    }
                    (None, None) => {
                        // Case: Table without row lineage. No action needed.
                    }
                }
            }
            ManifestContentType::Deletes => {
                // Deletes never have a first-row-id assigned.
                manifest.first_row_id = None;
            }
        };

        Ok(())
    }
}

fn require_row_counts_in_manifest(manifest: &ManifestFile) -> Result<(u64, u64)> {
    let existing_rows_count = manifest.existing_rows_count.ok_or_else(|| {
        invalid_data!(
                "Cannot include a Manifest without existing-rows-count to a table with row lineage enabled. Manifest path: {}",
                manifest.manifest_path,
            )
    })?;
    let added_rows_count = manifest.added_rows_count.ok_or_else(|| {
        invalid_data!(
                "Cannot include a Manifest without added-rows-count to a table with row lineage enabled. Manifest path: {}",
                manifest.manifest_path,
            )
    })?;
    Ok((existing_rows_count, added_rows_count))
}

#[cfg(test)]
mod test {
    use std::fs;
    use std::path::Path;
    use std::sync::Arc;

    use tempfile::TempDir;

    use super::ManifestListWriter;
    use crate::compression::CompressionCodec;
    use crate::encryption::kms::{KeyManagementClient, MemoryKeyManagementClient};
    use crate::encryption::{EncryptedInputFile, EncryptionManager};
    use crate::io::{FileIO, FileWrite};
    use crate::spec::{
        Datum, FieldSummary, FormatVersion, ManifestContentType, ManifestFile, ManifestList,
        UNASSIGNED_SEQUENCE_NUMBER,
    };

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
                partitions: Some(
                    vec![FieldSummary { contains_null: false, contains_nan: Some(false), lower_bound: Some(Datum::long(1).to_bytes().unwrap()), upper_bound: Some(Datum::long(1).to_bytes().unwrap())}],
                ),
                key_metadata: None,
                first_row_id: None,
            }]
        };

        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("manifest_list_v1.avro");
        let io = FileIO::new_with_fs();
        let file_writer = file_writer(&path, io).await;

        let mut writer = ManifestListWriter::v1(
            file_writer,
            1646658105718557341,
            Some(0),
            CompressionCodec::None,
        )
        .unwrap();
        writer
            .add_manifests(expected_manifest_list.entries.clone().into_iter())
            .unwrap();
        writer.close().await.unwrap();

        let bs = fs::read(path).unwrap();

        let manifest_list = ManifestList::parse_with_version(&bs, FormatVersion::V1).unwrap();
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
                partitions: Some(
                    vec![FieldSummary { contains_null: false, contains_nan: Some(false), lower_bound: Some(Datum::long(1).to_bytes().unwrap()), upper_bound: Some(Datum::long(1).to_bytes().unwrap())}]
                ),
                key_metadata: None,
                first_row_id: None,
            }]
        };

        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("manifest_list_v2.avro");
        let io = FileIO::new_with_fs();
        let file_writer = file_writer(&path, io).await;

        let mut writer = ManifestListWriter::v2(
            file_writer,
            snapshot_id,
            Some(0),
            seq_num,
            CompressionCodec::None,
        )
        .unwrap();
        writer
            .add_manifests(expected_manifest_list.entries.clone().into_iter())
            .unwrap();
        writer.close().await.unwrap();

        let bs = fs::read(path).unwrap();
        let manifest_list = ManifestList::parse_with_version(&bs, FormatVersion::V2).unwrap();
        expected_manifest_list.entries[0].sequence_number = seq_num;
        expected_manifest_list.entries[0].min_sequence_number = seq_num;
        assert_eq!(manifest_list, expected_manifest_list);

        temp_dir.close().unwrap();
    }

    #[tokio::test]
    async fn test_manifest_list_writer_v3() {
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
                partitions: Some(
                    vec![FieldSummary { contains_null: false, contains_nan: Some(false), lower_bound: Some(Datum::long(1).to_bytes().unwrap()), upper_bound: Some(Datum::long(1).to_bytes().unwrap())}]
                ),
                key_metadata: None,
                first_row_id: Some(10),
            }]
        };

        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("manifest_list_v2.avro");
        let io = FileIO::new_with_fs();
        let file_writer = file_writer(&path, io).await;

        let mut writer = ManifestListWriter::v3(
            file_writer,
            snapshot_id,
            Some(0),
            seq_num,
            Some(10),
            CompressionCodec::None,
        )
        .unwrap();
        writer
            .add_manifests(expected_manifest_list.entries.clone().into_iter())
            .unwrap();
        writer.close().await.unwrap();

        let bs = fs::read(path).unwrap();
        let manifest_list = ManifestList::parse_with_version(&bs, FormatVersion::V3).unwrap();
        expected_manifest_list.entries[0].sequence_number = seq_num;
        expected_manifest_list.entries[0].min_sequence_number = seq_num;
        expected_manifest_list.entries[0].first_row_id = Some(10);
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
                partitions: Some(
                    vec![FieldSummary { contains_null: false, contains_nan: Some(false), lower_bound: Some(Datum::long(1).to_bytes().unwrap()), upper_bound: Some(Datum::long(1).to_bytes().unwrap())}]
                ),
                key_metadata: None,
                first_row_id: None,
            }]
        };

        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("manifest_list_v1.avro");
        let io = FileIO::new_with_fs();
        let file_writer = file_writer(&path, io).await;

        let mut writer = ManifestListWriter::v1(
            file_writer,
            1646658105718557341,
            Some(0),
            CompressionCodec::None,
        )
        .unwrap();
        writer
            .add_manifests(expected_manifest_list.entries.clone().into_iter())
            .unwrap();
        writer.close().await.unwrap();

        let bs = fs::read(path).unwrap();

        let manifest_list = ManifestList::parse_with_version(&bs, FormatVersion::V2).unwrap();
        assert_eq!(manifest_list, expected_manifest_list);

        temp_dir.close().unwrap();
    }

    #[tokio::test]
    async fn test_manifest_list_writer_v1_as_v3() {
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
                partitions: Some(
                    vec![FieldSummary { contains_null: false, contains_nan: Some(false), lower_bound: Some(Datum::long(1).to_bytes().unwrap()), upper_bound: Some(Datum::long(1).to_bytes().unwrap())}]
                ),
                key_metadata: None,
                first_row_id: None,
            }]
        };

        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("manifest_list_v1.avro");
        let io = FileIO::new_with_fs();
        let file_writer = file_writer(&path, io).await;

        let mut writer = ManifestListWriter::v1(
            file_writer,
            1646658105718557341,
            Some(0),
            CompressionCodec::None,
        )
        .unwrap();
        writer
            .add_manifests(expected_manifest_list.entries.clone().into_iter())
            .unwrap();
        writer.close().await.unwrap();

        let bs = fs::read(path).unwrap();

        let manifest_list = ManifestList::parse_with_version(&bs, FormatVersion::V3).unwrap();
        assert_eq!(manifest_list, expected_manifest_list);

        temp_dir.close().unwrap();
    }

    #[tokio::test]
    async fn test_manifest_list_writer_v2_as_v3() {
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
                partitions: Some(
                    vec![FieldSummary { contains_null: false, contains_nan: Some(false), lower_bound: Some(Datum::long(1).to_bytes().unwrap()), upper_bound: Some(Datum::long(1).to_bytes().unwrap())}]
                ),
                key_metadata: None,
                first_row_id: None,
            }]
        };

        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("manifest_list_v2.avro");
        let io = FileIO::new_with_fs();
        let file_writer = file_writer(&path, io).await;

        let mut writer = ManifestListWriter::v2(
            file_writer,
            snapshot_id,
            Some(0),
            seq_num,
            CompressionCodec::None,
        )
        .unwrap();
        writer
            .add_manifests(expected_manifest_list.entries.clone().into_iter())
            .unwrap();
        writer.close().await.unwrap();

        let bs = fs::read(path).unwrap();

        let manifest_list = ManifestList::parse_with_version(&bs, FormatVersion::V3).unwrap();
        expected_manifest_list.entries[0].sequence_number = seq_num;
        expected_manifest_list.entries[0].min_sequence_number = seq_num;
        assert_eq!(manifest_list, expected_manifest_list);

        temp_dir.close().unwrap();
    }

    #[tokio::test]
    async fn test_manifest_list_writer_v3_encrypted_round_trip() {
        let (mgr, file_io) = fresh_encryption_manager_and_io();
        let path = "memory:///manifest_list_v3_encrypted.avro";

        let encrypted_output = mgr.encrypt(file_io.new_output(path).unwrap());

        let snapshot_id = 9_000_000_000_000_001i64;
        let seq_num = 7i64;
        let mut expected = ManifestList {
            entries: vec![ManifestFile {
                manifest_path: "memory:///encrypted/v3_m0.avro".to_string(),
                manifest_length: 1234,
                partition_spec_id: 0,
                content: ManifestContentType::Data,
                sequence_number: UNASSIGNED_SEQUENCE_NUMBER,
                min_sequence_number: UNASSIGNED_SEQUENCE_NUMBER,
                added_snapshot_id: snapshot_id,
                added_files_count: Some(2),
                existing_files_count: Some(0),
                deleted_files_count: Some(0),
                added_rows_count: Some(10),
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

        let file_writer = encrypted_output.writer().await.unwrap();
        let mut writer = ManifestListWriter::v3(
            file_writer,
            snapshot_id,
            Some(0),
            seq_num,
            None,
            CompressionCodec::None,
        )
        .unwrap();
        writer
            .add_manifests(expected.entries.clone().into_iter())
            .unwrap();
        let file_metadata = writer.close().await.unwrap();

        let raw_bytes = file_io.new_input(path).unwrap().read().await.unwrap();
        assert!(
            ManifestList::parse_with_version(&raw_bytes, FormatVersion::V3).is_err(),
            "raw bytes should be ciphertext, not parseable as Avro"
        );

        let key_metadata = encrypted_output.key_metadata_with_saved_file_metadata(&file_metadata);
        assert_eq!(key_metadata.file_length(), Some(raw_bytes.len() as u64));
        let plaintext = EncryptedInputFile::new(file_io.new_input(path).unwrap(), key_metadata)
            .read()
            .await
            .unwrap();
        let manifest_list =
            ManifestList::parse_with_version(&plaintext, FormatVersion::V3).unwrap();

        expected.entries[0].sequence_number = seq_num;
        expected.entries[0].min_sequence_number = seq_num;
        assert_eq!(manifest_list, expected);
    }

    fn fresh_encryption_manager_and_io() -> (EncryptionManager, FileIO) {
        let kms = MemoryKeyManagementClient::new();
        kms.add_master_key("master-1").unwrap();
        let mgr = EncryptionManager::builder()
            .kms_client(Arc::new(kms) as Arc<dyn KeyManagementClient>)
            .table_key_id("master-1")
            .build();
        (mgr, FileIO::new_with_memory())
    }

    #[tokio::test]
    async fn test_manifest_list_writer_with_compression() {
        let snapshot_id = 377075049360453639;
        let seq_num = 1;

        let entries: Vec<ManifestFile> = (0..1000)
            .map(|i| ManifestFile {
                manifest_path: format!(
                    "s3a://icebergdata/demo/s1/t1/metadata/very-long-path-for-compression-test/manifest-file-number-{i}.avro"
                ),
                manifest_length: 6926 + i,
                partition_spec_id: 1,
                content: ManifestContentType::Data,
                sequence_number: seq_num,
                min_sequence_number: seq_num,
                added_snapshot_id: snapshot_id,
                added_files_count: Some(1),
                existing_files_count: Some(0),
                deleted_files_count: Some(0),
                added_rows_count: Some(3),
                existing_rows_count: Some(0),
                deleted_rows_count: Some(0),
                partitions: None,
                key_metadata: None,
                first_row_id: None,
            })
            .collect();

        let tmp_dir = TempDir::new().unwrap();
        let io = FileIO::new_with_fs();

        // Write the same entries with a given codec and return (file size, round-tripped entry count).
        async fn write_and_read(
            io: &FileIO,
            path: &Path,
            snapshot_id: i64,
            seq_num: i64,
            entries: &[ManifestFile],
            compression: CompressionCodec,
        ) -> (u64, usize) {
            let mut writer = ManifestListWriter::v2(
                file_writer(path, io.clone()).await,
                snapshot_id,
                Some(0),
                seq_num,
                compression,
            )
            .unwrap();
            writer.add_manifests(entries.iter().cloned()).unwrap();
            writer.close().await.unwrap();

            let size = fs::metadata(path).unwrap().len();
            let bytes = fs::read(path).unwrap();
            let manifest_list =
                ManifestList::parse_with_version(&bytes, FormatVersion::V2).unwrap();
            (size, manifest_list.entries().len())
        }

        let (uncompressed_size, uncompressed_count) = write_and_read(
            &io,
            &tmp_dir.path().join("manifest_list_uncompressed.avro"),
            snapshot_id,
            seq_num,
            &entries,
            CompressionCodec::None,
        )
        .await;
        assert_eq!(uncompressed_count, 1000);

        // Every compressed codec must shrink the file and still round-trip all entries.
        for codec in [CompressionCodec::Gzip(9), CompressionCodec::Zstd(3)] {
            let path = tmp_dir
                .path()
                .join(format!("manifest_list_{}.avro", codec.name()));
            let (compressed_size, compressed_count) =
                write_and_read(&io, &path, snapshot_id, seq_num, &entries, codec).await;
            assert_eq!(
                compressed_count, 1000,
                "{codec} should round-trip all entries"
            );
            assert!(
                compressed_size < uncompressed_size,
                "{codec} size ({compressed_size}) should be less than uncompressed size ({uncompressed_size})"
            );
        }
    }

    async fn file_writer(path: &Path, io: FileIO) -> Box<dyn FileWrite> {
        io.new_output(path.to_str().unwrap())
            .unwrap()
            .writer()
            .await
            .unwrap()
    }
}
