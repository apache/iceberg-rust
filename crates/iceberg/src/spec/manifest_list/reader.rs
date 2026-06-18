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
    use std::sync::Arc;

    use super::ManifestListReader;
    use crate::encryption::kms::MemoryKeyManagementClient;
    use crate::encryption::{EncryptionManager, SensitiveBytes};
    use crate::io::FileIO;
    use crate::spec::{TableMetadata, TableMetadataRef};

    fn encryption_test_metadata() -> TableMetadata {
        let path = format!(
            "{}/testdata/table_metadata/TableMetadataV3ValidEncryption.json",
            env!("CARGO_MANIFEST_DIR"),
        );
        let json = std::fs::read_to_string(path).unwrap();
        serde_json::from_str(&json).unwrap()
    }

    #[tokio::test]
    async fn load_manifest_list_errors_when_encrypted_but_no_manager_configured() {
        let mut metadata = encryption_test_metadata();

        let manifest_list_path = format!(
            "{}/testdata/manifests_lists/manifest-list-v3-encrypted.avro",
            env!("CARGO_MANIFEST_DIR"),
        );
        let snapshot = metadata.snapshots.get_mut(&1).unwrap();
        let mut patched = snapshot.as_ref().clone();
        patched.manifest_list = manifest_list_path;
        *snapshot = Arc::new(patched);

        let snapshot_ref = metadata.current_snapshot().unwrap().clone();
        let metadata_ref = TableMetadataRef::new(metadata);

        let err = ManifestListReader::new(snapshot_ref, FileIO::new_with_fs(), metadata_ref, None)
            .load()
            .await
            .unwrap_err();
        assert_eq!(err.kind(), crate::ErrorKind::PreconditionFailed);
    }

    #[tokio::test]
    async fn load_manifest_list_decrypts_roundtrip() {
        let mut metadata = encryption_test_metadata();

        let manifest_list_path = format!(
            "{}/testdata/manifests_lists/manifest-list-v3-encrypted.avro",
            env!("CARGO_MANIFEST_DIR"),
        );
        let snapshot = metadata.snapshots.get_mut(&1).unwrap();
        let mut patched = snapshot.as_ref().clone();
        patched.manifest_list = manifest_list_path;
        *snapshot = Arc::new(patched);

        let kms = MemoryKeyManagementClient::new();
        kms.add_master_key_bytes(
            "master-1",
            SensitiveBytes::new([
                0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d,
                0x0e, 0x0f,
            ]),
        )
        .unwrap();

        let mgr = EncryptionManager::builder()
            .kms_client(Arc::new(kms) as Arc<dyn crate::encryption::kms::KeyManagementClient>)
            .table_key_id("master-1")
            .encryption_keys(metadata.encryption_keys.clone())
            .build();

        let snapshot_ref = metadata.current_snapshot().unwrap().clone();
        let metadata_ref = TableMetadataRef::new(metadata);

        let manifest_list = ManifestListReader::new(
            snapshot_ref,
            FileIO::new_with_fs(),
            metadata_ref,
            Some(Arc::new(mgr)),
        )
        .load()
        .await
        .unwrap();
        assert_eq!(manifest_list.entries().len(), 0);
    }
}
