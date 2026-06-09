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

use std::sync::Arc;

use super::ManifestList;
use crate::encryption::{EncryptedInputFile, EncryptionManager};
use crate::error::Result;
use crate::io::FileIO;
use crate::spec::{SnapshotRef, TableMetadataRef};
use crate::{Error, ErrorKind};

/// A manifest list reader that encapsulates the logic for loading and parsing a [`ManifestList`]
/// from a snapshot.
pub struct ManifestListReader {
    snapshot: SnapshotRef,
    file_io: FileIO,
    table_metadata: TableMetadataRef,
    encryption_manager: Option<Arc<EncryptionManager>>,
}

impl ManifestListReader {
    pub(crate) fn new(
        snapshot: SnapshotRef,
        file_io: FileIO,
        table_metadata: TableMetadataRef,
        encryption_manager: Option<Arc<EncryptionManager>>,
    ) -> Self {
        Self {
            snapshot,
            file_io,
            table_metadata,
            encryption_manager,
        }
    }

    /// Loads and returns the [`ManifestList`] for this snapshot.
    pub async fn load(&self) -> Result<ManifestList> {
        let manifest_list_content = match self.snapshot.encryption_key_id() {
            Some(key_id) => {
                let em = self.encryption_manager.as_ref().ok_or_else(|| {
                    Error::new(
                        ErrorKind::PreconditionFailed,
                        "Snapshot has encryption_key_id but no EncryptionManager configured on Table",
                    )
                })?;
                let key_metadata = em.decrypt_manifest_list_key_metadata(key_id).await?;
                let input = self.file_io.new_input(self.snapshot.manifest_list())?;
                EncryptedInputFile::new(input, key_metadata).read().await?
            }
            None => {
                self.file_io
                    .new_input(self.snapshot.manifest_list())?
                    .read()
                    .await?
            }
        };
        ManifestList::parse_with_version(
            &manifest_list_content,
            self.table_metadata.format_version(),
        )
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use bytes::Bytes;

    use super::ManifestListReader;
    use crate::encryption::kms::{KeyManagementClient, MemoryKeyManagementClient};
    use crate::encryption::{EncryptionManager, StandardKeyMetadata};
    use crate::io::FileIO;
    use crate::spec::manifest_list::ManifestListWriter;
    use crate::spec::snapshot::{Operation, Snapshot, Summary};
    use crate::spec::{SnapshotRef, TableMetadataRef};

    fn encryption_test_metadata() -> TableMetadataRef {
        let path = format!(
            "{}/testdata/table_metadata/TableMetadataV3ValidEncryption.json",
            env!("CARGO_MANIFEST_DIR"),
        );
        let json = std::fs::read_to_string(path).unwrap();
        TableMetadataRef::new(serde_json::from_str(&json).unwrap())
    }

    fn encryption_test_kms() -> Arc<dyn KeyManagementClient> {
        let kms = MemoryKeyManagementClient::new();
        kms.add_master_key("master-1").unwrap();
        Arc::new(kms)
    }

    fn encryption_test_manager() -> EncryptionManager {
        EncryptionManager::builder()
            .kms_client(encryption_test_kms())
            .table_key_id("master-1")
            .build()
    }

    async fn write_v3_manifest_list_bytes(io: &FileIO, path: &str) -> Bytes {
        let output = io.new_output(path).unwrap().writer().await.unwrap();
        let mut writer = ManifestListWriter::v3(output, 1, None, 0, Some(0));
        writer.add_manifests(std::iter::empty()).unwrap();
        writer.close().await.unwrap();
        io.new_input(path).unwrap().read().await.unwrap()
    }

    fn snapshot_pointing_at(manifest_list_path: &str, key_id: Option<String>) -> Snapshot {
        Snapshot::builder()
            .with_snapshot_id(1)
            .with_sequence_number(0)
            .with_timestamp_ms(0)
            .with_manifest_list(manifest_list_path.to_string())
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: HashMap::new(),
            })
            .with_encryption_key_id(key_id)
            .build()
    }

    #[tokio::test]
    async fn load_manifest_list_errors_when_encrypted_but_no_manager_configured() {
        let io = FileIO::new_with_memory();
        let snapshot = snapshot_pointing_at(
            "memory:///table/metadata/manifest-list-enc.avro",
            Some("k1".to_string()),
        );
        let metadata = encryption_test_metadata();

        let err = ManifestListReader::new(SnapshotRef::new(snapshot), io, metadata, None)
            .load()
            .await
            .unwrap_err();
        assert_eq!(err.kind(), crate::ErrorKind::PreconditionFailed);
    }

    #[tokio::test]
    async fn load_manifest_list_decrypts_roundtrip() {
        let io = FileIO::new_with_memory();
        let plain_path = "memory:///table/metadata/manifest-list-plain.avro";
        let encrypted_path = "memory:///table/metadata/manifest-list-enc.avro";

        // Build raw manifest list bytes via the standard writer.
        let raw_bytes = write_v3_manifest_list_bytes(&io, plain_path).await;

        // Encrypt those bytes to a second path and capture the file's key metadata.
        let mgr = encryption_test_manager();
        let encrypted_output = mgr.encrypt(io.new_output(encrypted_path).unwrap());
        let std_key_metadata: StandardKeyMetadata = encrypted_output.key_metadata().clone();
        encrypted_output.write(raw_bytes).await.unwrap();

        // Wrap the file's key metadata with a KEK and record the resulting wrapped
        // entry's id on the snapshot.
        let key_id = mgr
            .encrypt_manifest_list_key_metadata(&std_key_metadata)
            .await
            .unwrap();

        let snapshot = snapshot_pointing_at(encrypted_path, Some(key_id));
        let metadata = encryption_test_metadata();

        let manifest_list = ManifestListReader::new(
            SnapshotRef::new(snapshot),
            io,
            metadata,
            Some(Arc::new(mgr)),
        )
        .load()
        .await
        .unwrap();
        assert_eq!(manifest_list.entries().len(), 0);
    }
}
