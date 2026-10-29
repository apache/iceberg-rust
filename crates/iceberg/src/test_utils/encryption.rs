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

//! Fixtures for tests that need an [`EncryptionManager`] or an encrypted table.

use std::sync::Arc;

use super::test_runtime;
use crate::TableIdent;
use crate::encryption::kms::{KeyManagementClient, MemoryKeyManagementClient};
use crate::encryption::{EncryptionManager, SensitiveBytes};
use crate::io::FileIO;
use crate::spec::TableMetadata;
use crate::table::Table;

/// An [`EncryptionManager`] backed by an in-memory KMS holding `table_key_id`.
pub(crate) fn make_encryption_manager(table_key_id: &str) -> Arc<EncryptionManager> {
    let kms = MemoryKeyManagementClient::new();
    kms.add_master_key(table_key_id).unwrap();
    Arc::new(
        EncryptionManager::builder()
            .kms_client(Arc::new(kms) as Arc<dyn KeyManagementClient>)
            .table_key_id(table_key_id)
            .build(),
    )
}

/// Build a table backed by the V3 encryption fixture and an in-memory KMS,
/// so it has an [`EncryptionManager`](crate::encryption::EncryptionManager).
///
/// The fixture's snapshot references an encrypted manifest list; its bytes
/// (the `manifest-list-v3-encrypted.avro` testdata, an encrypted empty list)
/// are seeded into the in-memory `FileIO` at that path so callers can read
/// the current snapshot's manifest list.
pub(crate) async fn make_encrypted_table() -> Table {
    let metadata_json = std::fs::read_to_string(format!(
        "{}/testdata/table_metadata/TableMetadataV3ValidEncryption.json",
        env!("CARGO_MANIFEST_DIR"),
    ))
    .unwrap();
    let metadata: TableMetadata = serde_json::from_str(&metadata_json).unwrap();

    let kms: Arc<dyn KeyManagementClient> = {
        let k = MemoryKeyManagementClient::new();
        k.add_master_key_bytes(
            "master-1",
            SensitiveBytes::new([
                0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d,
                0x0e, 0x0f,
            ]),
        )
        .unwrap();
        Arc::new(k)
    };

    let file_io = FileIO::new_with_memory();

    // Seed the encrypted (empty) manifest list at the path the snapshot references.
    let manifest_list_bytes = std::fs::read(format!(
        "{}/testdata/manifests_lists/manifest-list-v3-encrypted.avro",
        env!("CARGO_MANIFEST_DIR"),
    ))
    .unwrap();
    file_io
        .new_output(metadata.current_snapshot().unwrap().manifest_list())
        .unwrap()
        .write(manifest_list_bytes.into())
        .await
        .unwrap();

    Table::builder()
        .metadata(metadata)
        .metadata_location("memory:///table/metadata/v1.json")
        .identifier(TableIdent::from_strs(["ns1", "test1"]).unwrap())
        .file_io(file_io)
        .kms_client(kms)
        .runtime(test_runtime())
        .build()
        .unwrap()
}
