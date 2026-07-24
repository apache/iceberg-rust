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
use crate::spec::{ManifestContentType, ManifestEntry, ManifestFile};
use crate::{Error, ErrorKind};

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

        self.assign_first_row_ids(manifest_file, &mut entries)?;

        Ok(Manifest::new(metadata, entries))
    }

    /// Assigns `first_row_id` to the live data-file entries, following the
    /// row-lineage inheritance rules in
    /// <https://github.com/apache/iceberg/blob/main/format/spec.md#first-row-id-inheritance>.
    ///
    /// With a manifest-level `first_row_id`, each live entry lacking one is
    /// assigned the running id, which then advances by that entry's record
    /// count; entries that already carry a `first_row_id` keep it and do not
    /// advance the counter. Without a manifest-level `first_row_id`, any
    /// inherited per-entry value is cleared so callers never observe a stale id.
    fn assign_first_row_ids(
        &self,
        manifest_file: &ManifestFile,
        entries: &mut [ManifestEntry],
    ) -> Result<()> {
        // A `first_row_id` is only valid on data manifests. Delete files always
        // have a null `first_row_id`, so there is nothing to assign or clear; a
        // stray value on a delete manifest is a spec violation by the writer,
        // which we surface without failing the read.
        if manifest_file.content != ManifestContentType::Data {
            if let Some(manifest_first_row_id) = manifest_file.first_row_id {
                tracing::warn!(
                    "Ignoring first_row_id {manifest_first_row_id} on delete manifest {}",
                    manifest_file.manifest_path
                );
            }

            return Ok(());
        }

        let Some(manifest_first_row_id) = manifest_file.first_row_id else {
            // A data manifest with no manifest-level `first_row_id` predates row
            // lineage; clear any per-entry value inherited from an earlier read.
            for entry in entries {
                entry.data_file.first_row_id = None;
            }

            return Ok(());
        };

        let mut next_row_id = i64::try_from(manifest_first_row_id).map_err(|_| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("Invalid first_row_id: {manifest_first_row_id} (exceeds i64::MAX)"),
            )
        })?;

        for entry in entries {
            if !entry.is_alive() {
                continue;
            }

            if entry.data_file.first_row_id.is_none() {
                let file_first_row_id = next_row_id;
                entry.data_file.first_row_id = Some(file_first_row_id);
                let record_count = entry.data_file.record_count;
                next_row_id = file_first_row_id.checked_add_unsigned(record_count).ok_or_else(|| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        format!(
                            "Row ID overflow assigning first_row_id in {}. File first_row_id: {file_first_row_id}, record count: {record_count}",
                            manifest_file.manifest_path
                        ),
                    )
                })?;
            }
        }

        Ok(())
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
