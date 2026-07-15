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

use std::str::FromStr;

use serde_derive::{Deserialize, Serialize};

use super::ByteBuf;
use crate::encryption::{EncryptedInputFile, StandardKeyMetadata};
use crate::error::Result;
use crate::io::FileIO;
use crate::spec::Manifest;
use crate::{Error, ErrorKind};

/// Entry in a manifest list.
#[derive(Debug, PartialEq, Clone, Eq, Hash)]
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
    pub partitions: Option<Vec<FieldSummary>>,
    /// field: 519
    ///
    /// Implementation-specific key metadata for encryption
    pub key_metadata: Option<Vec<u8>>,
    /// field 520
    ///
    /// The starting _row_id to assign to rows added by ADDED data files
    pub first_row_id: Option<u64>,
}

impl ManifestFile {
    /// Checks if the manifest file has any added files.
    pub fn has_added_files(&self) -> bool {
        self.added_files_count.map(|c| c > 0).unwrap_or(true)
    }

    /// Checks whether this manifest contains entries with DELETED status.
    pub fn has_deleted_files(&self) -> bool {
        self.deleted_files_count.map(|c| c > 0).unwrap_or(true)
    }

    /// Checks if the manifest file has any existed files.
    pub fn has_existing_files(&self) -> bool {
        self.existing_files_count.map(|c| c > 0).unwrap_or(true)
    }
}

/// The type of files tracked by the manifest, either data or delete files; Data(0) for all v1 manifests
#[derive(Debug, PartialEq, Clone, Copy, Eq, Hash, Default)]
pub enum ManifestContentType {
    /// The manifest content is data.
    #[default]
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
                format!("Invalid manifest content type. Expected 0 or 1, got {value}"),
            )),
        }
    }
}

impl ManifestFile {
    /// Load [`Manifest`].
    ///
    /// This method will also initialize inherited values of [`ManifestEntry`](crate::spec::ManifestEntry), such as `sequence_number`.
    pub async fn load_manifest(&self, file_io: &FileIO) -> Result<Manifest> {
        let input = file_io.new_input(&self.manifest_path)?;
        let avro = match &self.key_metadata {
            Some(key_metadata_bytes) => {
                let key_metadata = StandardKeyMetadata::decode(key_metadata_bytes)?;
                EncryptedInputFile::new(input, key_metadata).read().await?
            }
            None => input.read().await?,
        };

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
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone, Default, Hash)]
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
    pub lower_bound: Option<ByteBuf>,
    /// field: 511
    /// The maximum value for the field in the manifests
    /// partitions.
    pub upper_bound: Option<ByteBuf>,
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;
    use std::sync::Arc;

    use super::{ManifestContentType, ManifestFile};
    use crate::ErrorKind;
    use crate::encryption::{EncryptedOutputFile, StandardKeyMetadata};
    use crate::io::FileIO;
    use crate::spec::{
        DataContentType, DataFile, DataFileFormat, ManifestEntry, ManifestStatus,
        ManifestWriterBuilder, NestedField, PartitionSpec, PrimitiveType, Schema, Struct, Type,
    };

    #[test]
    fn test_manifest_content_type_default() {
        assert_eq!(ManifestContentType::default(), ManifestContentType::Data);
    }

    #[test]
    fn test_manifest_content_type_default_value() {
        assert_eq!(ManifestContentType::default() as i32, 0);
    }

    /// Writes a single-entry manifest, encrypting it with `key_metadata`, and
    /// returns the resulting [`ManifestFile`]. The manifest is stored in `io`
    /// at `path`.
    async fn write_encrypted_manifest(
        io: &FileIO,
        path: &str,
        key_metadata: StandardKeyMetadata,
    ) -> ManifestFile {
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

        let output_file = io.new_output(path).unwrap();
        let encrypted_output = EncryptedOutputFile::new(output_file, key_metadata);

        let mut writer = ManifestWriterBuilder::new_from_encrypted(
            encrypted_output,
            Some(1),
            schema.clone(),
            partition_spec.clone(),
        )
        .expect("Expected a valid writer")
        .build_v3_data();

        writer
            .add_entry(ManifestEntry {
                status: ManifestStatus::Added,
                snapshot_id: None,
                sequence_number: None,
                file_sequence_number: None,
                data_file: DataFile {
                    content: DataContentType::Data,
                    file_path: "s3://bucket/table/data/00000.parquet".to_string(),
                    file_format: DataFileFormat::Parquet,
                    partition: Struct::empty(),
                    record_count: 100,
                    file_size_in_bytes: 4096,
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
            })
            .unwrap();

        writer.write_manifest_file().await.unwrap()
    }

    #[tokio::test]
    async fn test_load_manifest_decrypts_when_key_metadata_present() {
        let key_metadata = StandardKeyMetadata::new(b"0123456789abcdef")
            .unwrap()
            .with_aad_prefix(b"test-aad-prefix!");
        let encoded_key_metadata = key_metadata.encode().unwrap().to_vec();

        let io = FileIO::new_with_memory();
        let path = "memory:///test/encrypted_manifest.avro";
        let manifest_file = write_encrypted_manifest(&io, path, key_metadata).await;
        assert_eq!(manifest_file.key_metadata, Some(encoded_key_metadata));

        let manifest = manifest_file.load_manifest(&io).await.unwrap();
        assert_eq!(manifest.entries().len(), 1);
        assert_eq!(
            manifest.entries()[0].file_path(),
            "s3://bucket/table/data/00000.parquet"
        );
        assert_eq!(manifest.entries()[0].data_file.record_count, 100);
    }

    #[tokio::test]
    async fn test_load_manifest_fails_with_wrong_key() {
        let key_metadata = StandardKeyMetadata::new(b"0123456789abcdef")
            .unwrap()
            .with_aad_prefix(b"test-aad-prefix!");

        let io = FileIO::new_with_memory();
        let path = "memory:///test/wrong_key_manifest.avro";
        let mut manifest_file = write_encrypted_manifest(&io, path, key_metadata).await;

        // Point the manifest file at key metadata carrying a different DEK (but
        // the same AAD prefix). The bytes on disk were encrypted with the
        // original key, so GCM authentication must fail rather than silently
        // returning garbage.
        let wrong_key_metadata = StandardKeyMetadata::new(b"fedcba9876543210")
            .unwrap()
            .with_aad_prefix(b"test-aad-prefix!");
        manifest_file.key_metadata = Some(wrong_key_metadata.encode().unwrap().to_vec());

        let err = manifest_file
            .load_manifest(&io)
            .await
            .expect_err("load_manifest must fail when decrypting with the wrong key");
        assert_eq!(err.kind(), ErrorKind::Unexpected);
    }

    #[tokio::test]
    async fn test_load_manifest_fails_with_wrong_aad() {
        let key_metadata = StandardKeyMetadata::new(b"0123456789abcdef")
            .unwrap()
            .with_aad_prefix(b"test-aad-prefix!");

        let io = FileIO::new_with_memory();
        let path = "memory:///test/wrong_aad_manifest.avro";
        let mut manifest_file = write_encrypted_manifest(&io, path, key_metadata).await;

        // Point the manifest file at key metadata carrying the correct DEK but a
        // different AAD prefix. The per-block AAD is `aad_prefix || block_index`,
        // so GCM authentication must fail even though the key is right.
        let wrong_aad_metadata = StandardKeyMetadata::new(b"0123456789abcdef")
            .unwrap()
            .with_aad_prefix(b"wrong-aad-prefix");
        manifest_file.key_metadata = Some(wrong_aad_metadata.encode().unwrap().to_vec());

        let err = manifest_file
            .load_manifest(&io)
            .await
            .expect_err("load_manifest must fail when decrypting with the wrong AAD prefix");
        assert_eq!(err.kind(), ErrorKind::Unexpected);
    }
}
