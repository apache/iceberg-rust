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

use super::Manifest;
use crate::encryption::{EncryptedInputFile, StandardKeyMetadata};
use crate::error::Result;
use crate::io::FileIO;
use crate::spec::ManifestFile;

/// Reads a manifest file referenced by a manifest list entry, transparently
/// decrypting it when the entry records key metadata.
///
/// This is the read-side counterpart to [`ManifestWriter`], encapsulating the
/// read -> (decrypt) -> parse -> inherit sequence in one place so callers don't
/// have to repeat it.
///
/// [`ManifestWriter`]: super::ManifestWriter
pub(crate) struct ManifestReader {
    file_io: FileIO,
}

impl ManifestReader {
    /// Create a manifest reader.
    pub(crate) fn new(file_io: FileIO) -> Self {
        Self { file_io }
    }

    /// Read, decrypt, parse and return the manifest described by
    /// `manifest_file`.
    pub(crate) async fn read(self, manifest_file: &ManifestFile) -> Result<Manifest> {
        let input_file = self.file_io.new_input(&manifest_file.manifest_path)?;
        let key_metadata = manifest_file
            .key_metadata
            .as_deref()
            .map(StandardKeyMetadata::decode)
            .transpose()?;
        let bytes = match key_metadata {
            Some(key_metadata) => {
                EncryptedInputFile::new(input_file, key_metadata)
                    .read()
                    .await?
            }
            None => input_file.read().await?,
        };

        let (metadata, mut entries) = Manifest::try_from_avro_bytes(&bytes)?;

        for entry in &mut entries {
            entry.inherit_data(manifest_file);
        }

        Ok(Manifest::new(metadata, entries))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use super::*;
    use crate::encryption::{EncryptedOutputFile, StandardKeyMetadata};
    use crate::io::FileIO;
    use crate::spec::{
        DataContentType, DataFile, DataFileFormat, ManifestEntry, ManifestStatus,
        ManifestWriterBuilder, NestedField, PartitionSpec, PrimitiveType, Schema, Struct, Type,
    };

    #[tokio::test]
    async fn test_read_plaintext_manifest_inherits_entries() {
        let schema = test_schema();
        let partition_spec = PartitionSpec::builder(schema.clone())
            .with_spec_id(0)
            .build()
            .unwrap();

        let io = FileIO::new_with_memory();
        let path = "memory:///table/metadata/plain.avro";
        let mut writer = ManifestWriterBuilder::new(
            io.new_output(path).unwrap(),
            Some(1),
            schema.clone(),
            partition_spec,
        )
        .build_v2_data();
        writer.add_entry(test_entry()).unwrap();
        // Writing the manifest yields the manifest list entry describing it.
        let manifest_file = writer.write_manifest_file().await.unwrap();

        let manifest = ManifestReader::new(io).read(&manifest_file).await.unwrap();
        assert_eq!(manifest.entries().len(), 1);
        assert_eq!(
            manifest.entries()[0].data_file().file_path(),
            "memory:///table/data/00000.parquet"
        );
        // Entries must inherit values from the manifest list entry.
        assert_eq!(
            manifest.entries()[0].sequence_number(),
            Some(manifest_file.sequence_number)
        );
        assert_eq!(
            manifest.entries()[0].snapshot_id(),
            Some(manifest_file.added_snapshot_id)
        );
    }

    #[tokio::test]
    async fn test_read_encrypted_manifest_roundtrip() {
        let schema = test_schema();
        let partition_spec = PartitionSpec::builder(schema.clone())
            .with_spec_id(0)
            .build()
            .unwrap();

        let io = FileIO::new_with_memory();
        let path = "memory:///table/metadata/encrypted.avro";
        let encrypted_output =
            EncryptedOutputFile::new(io.new_output(path).unwrap(), key_metadata());

        let mut writer = ManifestWriterBuilder::new_from_encrypted(
            encrypted_output,
            Some(1),
            schema.clone(),
            partition_spec,
        )
        .unwrap()
        .build_v3_data();
        writer.add_entry(test_entry()).unwrap();
        // The returned manifest list entry records the key metadata.
        let manifest_file = writer.write_manifest_file().await.unwrap();
        assert!(manifest_file.key_metadata.is_some());

        // Reading with the recorded key metadata must recover the entry.
        let manifest = ManifestReader::new(io.clone())
            .read(&manifest_file)
            .await
            .unwrap();
        assert_eq!(manifest.entries().len(), 1);
        assert_eq!(
            manifest.entries()[0].data_file().file_path(),
            "memory:///table/data/00000.parquet"
        );

        // Without the key metadata the encrypted bytes must not read as plaintext.
        let mut plaintext_entry = manifest_file.clone();
        plaintext_entry.key_metadata = None;
        assert!(
            ManifestReader::new(io)
                .read(&plaintext_entry)
                .await
                .is_err(),
            "encrypted manifest must not parse as plaintext"
        );
    }

    fn test_schema() -> Arc<Schema> {
        Arc::new(
            Schema::builder()
                .with_fields(vec![Arc::new(NestedField::optional(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Long),
                ))])
                .build()
                .unwrap(),
        )
    }

    fn test_entry() -> ManifestEntry {
        ManifestEntry {
            status: ManifestStatus::Added,
            snapshot_id: None,
            sequence_number: None,
            file_sequence_number: None,
            data_file: DataFile {
                content: DataContentType::Data,
                file_path: "memory:///table/data/00000.parquet".to_string(),
                file_format: DataFileFormat::Parquet,
                partition: Struct::empty(),
                record_count: 1,
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
        }
    }

    fn key_metadata() -> StandardKeyMetadata {
        StandardKeyMetadata::try_new(b"0123456789abcdef").unwrap()
    }
}
