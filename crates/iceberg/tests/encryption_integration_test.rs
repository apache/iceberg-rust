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

//! Integration tests for encryption functionality

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use tempfile::TempDir;

use iceberg::encryption::{
    AesGcmEncryptor, EncryptionAlgorithm, EncryptionManager, InMemoryKms,
    KeyManagementClient, SecureKey, StandardKeyMetadata,
};
use iceberg::io::FileIOBuilder;

#[tokio::test]
async fn test_encryption_manager_lifecycle() {
    // Create a master key for KMS
    let master_key = vec![0u8; 16]; // 128-bit key
    let kms = Arc::new(InMemoryKms::new_with_master_key(
        "test-master-key".to_string(),
        master_key,
    ));

    // Create encryption manager
    let encryption_manager = EncryptionManager::new(
        kms.clone(),
        EncryptionAlgorithm::Aes128Gcm,
        Duration::from_secs(3600),
    );

    // Generate a DEK and wrap it
    let dek = SecureKey::generate(EncryptionAlgorithm::Aes128Gcm);
    let aad_prefix = b"test_aad_prefix";

    let wrapped_key = kms
        .wrap_key(dek.as_bytes(), "test-master-key")
        .await
        .unwrap();

    // Create key metadata
    let metadata =
        StandardKeyMetadata::new(wrapped_key, aad_prefix.to_vec(), Some(1024));
    let metadata_bytes = metadata.serialize().unwrap();

    // Prepare decryption (should unwrap key and cache it)
    let encryptor = encryption_manager
        .prepare_decryption(&metadata_bytes)
        .await
        .unwrap();

    // Test encryption round-trip
    let plaintext = b"Hello, encrypted Iceberg!";
    let ciphertext = encryptor
        .encrypt(plaintext, Some(aad_prefix))
        .unwrap();

    let decrypted = encryptor
        .decrypt(&ciphertext, Some(aad_prefix))
        .unwrap();

    assert_eq!(decrypted, plaintext);

    // Test caching - second call should return same encryptor
    let encryptor2 = encryption_manager
        .prepare_decryption(&metadata_bytes)
        .await
        .unwrap();

    assert!(Arc::ptr_eq(&encryptor, &encryptor2));
}

#[tokio::test]
async fn test_file_io_encryption_integration() {
    let tmp_dir = TempDir::new().unwrap();
    let test_file_path = format!(
        "file://{}/test_encrypted.bin",
        tmp_dir.path().to_str().unwrap()
    );

    // Setup encryption
    let master_key = vec![1u8; 16];
    let kms = Arc::new(InMemoryKms::new_with_master_key(
        "test-key".to_string(),
        master_key,
    ));
    let encryption_manager =
        Arc::new(EncryptionManager::with_defaults(kms.clone()));

    // Create FileIO with encryption manager
    let file_io = FileIOBuilder::new_fs_io()
        .with_extension((*encryption_manager).clone())
        .build()
        .unwrap();

    // Write encrypted data (manual encryption for testing)
    let dek = SecureKey::generate(EncryptionAlgorithm::Aes128Gcm);
    let aad_prefix = b"file_aad";

    // Wrap the DEK (save bytes before moving dek into encryptor)
    let dek_bytes = dek.as_bytes().to_vec();
    let wrapped_key = kms.wrap_key(&dek_bytes, "test-key").await.unwrap();
    let encryptor = AesGcmEncryptor::new(dek);
    let metadata = StandardKeyMetadata::new(wrapped_key, aad_prefix.to_vec(), None);
    let key_metadata = metadata.serialize().unwrap();

    // Create AGS1 encrypted file
    let plaintext = b"This is test data for encryption integration";
    let encrypted_file_data =
        create_ags1_file(plaintext, &encryptor, aad_prefix);

    // Write encrypted file
    let output = file_io.new_output(&test_file_path).unwrap();
    output.write(encrypted_file_data).await.unwrap();

    // Read back using encrypted input
    let encrypted_input = file_io
        .new_encrypted_input(&test_file_path, &key_metadata)
        .await
        .unwrap();

    let decrypted_data = encrypted_input.read().await.unwrap();

    assert_eq!(&decrypted_data[..], plaintext);
}

#[tokio::test]
async fn test_bulk_decryption_preparation() {
    let master_key = vec![2u8; 16];
    let kms = Arc::new(InMemoryKms::new_with_master_key(
        "bulk-test-key".to_string(),
        master_key,
    ));
    let encryption_manager = EncryptionManager::with_defaults(kms.clone());

    // Create multiple key metadata entries
    let mut metadata_list = Vec::new();
    for i in 0..10 {
        let dek = SecureKey::generate(EncryptionAlgorithm::Aes128Gcm);
        let aad_prefix = format!("aad_{}", i);
        let wrapped_key = kms
            .wrap_key(dek.as_bytes(), "bulk-test-key")
            .await
            .unwrap();

        let metadata = StandardKeyMetadata::new(
            wrapped_key,
            aad_prefix.as_bytes().to_vec(),
            Some(1024 * i),
        );
        metadata_list.push(metadata.serialize().unwrap());
    }

    // Bulk prepare decryption
    let encryptors = encryption_manager
        .bulk_prepare_decryption(metadata_list)
        .await
        .unwrap();

    assert_eq!(encryptors.len(), 10);

    // Verify each encryptor works
    for (i, encryptor) in encryptors.iter().enumerate() {
        let plaintext = format!("Test data {}", i);
        let aad = format!("aad_{}", i);

        let ciphertext = encryptor
            .encrypt(plaintext.as_bytes(), Some(aad.as_bytes()))
            .unwrap();

        let decrypted = encryptor
            .decrypt(&ciphertext, Some(aad.as_bytes()))
            .unwrap();

        assert_eq!(decrypted, plaintext.as_bytes());
    }
}

#[tokio::test]
async fn test_encryption_manager_extract_aad_prefix() {
    let master_key = vec![3u8; 16];
    let kms = Arc::new(InMemoryKms::new_with_master_key(
        "test-key".to_string(),
        master_key,
    ));
    let encryption_manager = EncryptionManager::with_defaults(kms.clone());

    let dek = SecureKey::generate(EncryptionAlgorithm::Aes128Gcm);
    let expected_aad = b"my_aad_prefix";

    let wrapped_key = kms.wrap_key(dek.as_bytes(), "test-key").await.unwrap();
    let metadata =
        StandardKeyMetadata::new(wrapped_key, expected_aad.to_vec(), Some(2048));
    let metadata_bytes = metadata.serialize().unwrap();

    let extracted_aad = encryption_manager
        .extract_aad_prefix(&metadata_bytes)
        .unwrap();

    assert_eq!(extracted_aad, expected_aad);
}

#[tokio::test]
async fn test_key_cache_expiration() {
    use tokio::time::sleep;

    let master_key = vec![4u8; 16];
    let kms = Arc::new(InMemoryKms::new_with_master_key(
        "cache-test-key".to_string(),
        master_key,
    ));

    // Create manager with short TTL for testing
    let encryption_manager = EncryptionManager::new(
        kms.clone(),
        EncryptionAlgorithm::Aes128Gcm,
        Duration::from_millis(200), // 200ms TTL
    );

    let dek = SecureKey::generate(EncryptionAlgorithm::Aes128Gcm);
    let wrapped_key = kms
        .wrap_key(dek.as_bytes(), "cache-test-key")
        .await
        .unwrap();

    let metadata = StandardKeyMetadata::new(wrapped_key, b"aad".to_vec(), None);
    let metadata_bytes = metadata.serialize().unwrap();

    // First call - should create and cache
    let encryptor1 = encryption_manager
        .prepare_decryption(&metadata_bytes)
        .await
        .unwrap();

    // Immediate second call - should hit cache
    let encryptor2 = encryption_manager
        .prepare_decryption(&metadata_bytes)
        .await
        .unwrap();

    assert!(Arc::ptr_eq(&encryptor1, &encryptor2));

    // Wait for cache to expire
    sleep(Duration::from_millis(250)).await;

    // This call should create a new encryptor (cache expired)
    let encryptor3 = encryption_manager
        .prepare_decryption(&metadata_bytes)
        .await
        .unwrap();

    // Should not be the same Arc pointer
    assert!(!Arc::ptr_eq(&encryptor1, &encryptor3));
}

#[tokio::test]
async fn test_invalid_key_metadata() {
    let master_key = vec![5u8; 16];
    let kms = Arc::new(InMemoryKms::new_with_master_key(
        "test-key".to_string(),
        master_key,
    ));
    let encryption_manager = EncryptionManager::with_defaults(kms);

    let invalid_metadata = b"not valid avro data";

    let result = encryption_manager
        .prepare_decryption(invalid_metadata)
        .await;

    assert!(result.is_err());
}

/// Helper function to create an AGS1-format encrypted file
fn create_ags1_file(
    plaintext: &[u8],
    encryptor: &AesGcmEncryptor,
    aad_prefix: &[u8],
) -> Bytes {
    use bytes::BytesMut;

    const PLAIN_BLOCK_SIZE: usize = 256; // Small for testing
    let mut result = BytesMut::new();

    // Write header
    result.extend_from_slice(b"AGS1");
    result.extend_from_slice(&(PLAIN_BLOCK_SIZE as u32).to_le_bytes());

    // Write blocks
    let mut offset = 0;
    let mut block_index = 0u32;

    while offset < plaintext.len() {
        let block_end = (offset + PLAIN_BLOCK_SIZE).min(plaintext.len());
        let block_data = &plaintext[offset..block_end];

        // Construct AAD: aad_prefix || block_index
        let mut aad = BytesMut::with_capacity(aad_prefix.len() + 4);
        aad.extend_from_slice(aad_prefix);
        aad.extend_from_slice(&block_index.to_le_bytes());

        // Encrypt block
        let ciphertext = encryptor.encrypt(block_data, Some(&aad)).unwrap();
        result.extend_from_slice(&ciphertext);

        offset = block_end;
        block_index += 1;
    }

    result.freeze()
}

#[tokio::test]
async fn test_encrypted_input_file_exists() {
    let tmp_dir = TempDir::new().unwrap();
    let test_file_path = format!(
        "file://{}/test_exists.bin",
        tmp_dir.path().to_str().unwrap()
    );

    let master_key = vec![6u8; 16];
    let kms = Arc::new(InMemoryKms::new_with_master_key(
        "test-key".to_string(),
        master_key,
    ));
    let encryption_manager = Arc::new(EncryptionManager::with_defaults(kms.clone()));

    let file_io = FileIOBuilder::new_fs_io()
        .with_extension((*encryption_manager).clone())
        .build()
        .unwrap();

    // File doesn't exist yet
    let dek = SecureKey::generate(EncryptionAlgorithm::Aes128Gcm);
    let dek_bytes = dek.as_bytes().to_vec();
    let wrapped_key = kms.wrap_key(&dek_bytes, "test-key").await.unwrap();
    let metadata = StandardKeyMetadata::new(wrapped_key, b"aad".to_vec(), None);
    let key_metadata = metadata.serialize().unwrap();

    let encrypted_input = file_io
        .new_encrypted_input(&test_file_path, &key_metadata)
        .await
        .unwrap();

    assert!(!encrypted_input.exists().await.unwrap());

    // Create the file
    let output = file_io.new_output(&test_file_path).unwrap();
    output.write(Bytes::from("test")).await.unwrap();

    // Now it should exist
    assert!(encrypted_input.exists().await.unwrap());
}

#[tokio::test]
async fn test_encrypted_manifest_file_new_encrypted_input() {
    use iceberg::spec::{ManifestContentType, ManifestFile};

    let tmp_dir = TempDir::new().unwrap();
    let manifest_path = format!(
        "file://{}/encrypted_manifest.avro",
        tmp_dir.path().to_str().unwrap()
    );

    // Setup encryption
    let master_key = vec![7u8; 16];
    let kms = Arc::new(InMemoryKms::new_with_master_key(
        "manifest-key".to_string(),
        master_key,
    ));
    let encryption_manager = Arc::new(EncryptionManager::with_defaults(kms.clone()));

    // Create FileIO with encryption manager
    let file_io = FileIOBuilder::new_fs_io()
        .with_extension((*encryption_manager).clone())
        .build()
        .unwrap();

    // Generate encryption key and metadata
    let dek = SecureKey::generate(EncryptionAlgorithm::Aes128Gcm);
    let aad_prefix = b"manifest_aad";
    let dek_bytes = dek.as_bytes().to_vec();
    let wrapped_key = kms.wrap_key(&dek_bytes, "manifest-key").await.unwrap();
    let encryptor = AesGcmEncryptor::new(dek);
    let key_metadata = StandardKeyMetadata::new(wrapped_key.clone(), aad_prefix.to_vec(), None);
    let key_metadata_bytes = key_metadata.serialize().unwrap();

    // Create test data and encrypt it
    let plaintext = b"This is test manifest data";
    let encrypted_data = create_ags1_file(plaintext, &encryptor, aad_prefix);

    // Write encrypted file to storage
    let output = file_io.new_output(&manifest_path).unwrap();
    output.write(encrypted_data).await.unwrap();

    // Test that new_encrypted_input works correctly
    let encrypted_input = file_io
        .new_encrypted_input(&manifest_path, &key_metadata_bytes)
        .await
        .unwrap();

    let decrypted_data = encrypted_input.read().await.unwrap();
    assert_eq!(&decrypted_data[..], plaintext);

    // Create ManifestFile entry with encryption metadata
    let manifest_file = ManifestFile {
        manifest_path: manifest_path.clone(),
        manifest_length: 0,
        partition_spec_id: 0,
        content: ManifestContentType::Data,
        sequence_number: 1,
        min_sequence_number: 1,
        added_snapshot_id: 1,
        added_files_count: Some(0),
        existing_files_count: Some(0),
        deleted_files_count: Some(0),
        added_rows_count: Some(0),
        existing_rows_count: Some(0),
        deleted_rows_count: Some(0),
        partitions: None,
        key_metadata: Some(key_metadata_bytes.clone()),
        first_row_id: None,
    };

    // Verify that the ManifestFile has key_metadata set (which will trigger encryption path)
    assert!(manifest_file.key_metadata.is_some());

    // Note: We can't fully test load_manifest() here without creating a valid Avro manifest,
    // but we've verified the encryption/decryption path works for FileIO.
    // The load_manifest() method will use new_encrypted_input when key_metadata is present.
}

#[tokio::test]
async fn test_end_to_end_encrypted_parquet_read() {
    use std::collections::HashMap;
    use std::fs::File;

    use arrow_array::{Int32Array, RecordBatch, StringArray};
    use arrow_array::cast::AsArray;
    use arrow_schema::{DataType, Field, Schema as ArrowSchema};
    use futures::TryStreamExt;
    use parquet::arrow::{ArrowWriter, PARQUET_FIELD_ID_META_KEY};
    use parquet::basic::Compression;
    use parquet::encryption::encrypt::FileEncryptionProperties;
    use parquet::file::properties::WriterProperties;

    use iceberg::arrow::ArrowReaderBuilder;
    use iceberg::scan::{FileScanTask, FileScanTaskStream};
    use iceberg::spec::{DataFileFormat, NestedField, PrimitiveType, Schema, Type};

    let tmp_dir = TempDir::new().unwrap();
    let table_location = tmp_dir.path().to_str().unwrap().to_string();

    // ===== Setup Encryption =====
    let master_key = vec![8u8; 16];
    let kms = Arc::new(InMemoryKms::new_with_master_key(
        "e2e-test-key".to_string(),
        master_key.clone(),
    ));
    let encryption_manager = Arc::new(EncryptionManager::with_defaults(kms.clone()));

    let file_io = FileIOBuilder::new_fs_io()
        .with_extension((*encryption_manager).clone())
        .build()
        .unwrap();

    // ===== Create Encrypted Parquet File =====

    // 1. Generate DEK and create key metadata for Iceberg
    let dek = SecureKey::generate(EncryptionAlgorithm::Aes128Gcm);
    let dek_bytes = dek.as_bytes().to_vec();
    let wrapped_key = kms.wrap_key(&dek_bytes, "e2e-test-key").await.unwrap();
    let key_metadata = StandardKeyMetadata::new(wrapped_key, b"parquet_aad".to_vec(), None);
    let key_metadata_bytes = key_metadata.serialize().unwrap();

    // 2. Setup Parquet encryption properties
    // Parquet expects a 256-bit footer key (32 bytes) or 128-bit (16 bytes)
    // Pass Iceberg's key metadata so it gets passed back to our KeyRetriever
    let footer_key = dek_bytes.clone();
    let encryption_properties = FileEncryptionProperties::builder(footer_key)
        .with_footer_key_metadata(key_metadata_bytes.clone())
        .build()
        .unwrap();

    // 3. Create Arrow schema with field IDs
    let arrow_schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int32, false).with_metadata(HashMap::from([
            (PARQUET_FIELD_ID_META_KEY.to_string(), "1".to_string()),
        ])),
        Field::new("name", DataType::Utf8, false).with_metadata(HashMap::from([
            (PARQUET_FIELD_ID_META_KEY.to_string(), "2".to_string()),
        ])),
    ]));

    // 4. Create test data
    let test_data = RecordBatch::try_new(
        arrow_schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie", "Dave", "Eve"])),
        ],
    )
    .unwrap();

    // 5. Write encrypted Parquet file
    let parquet_path = format!("{}/encrypted_data.parquet", table_location);
    let file = File::create(&parquet_path).unwrap();

    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .with_file_encryption_properties(encryption_properties)
        .build();

    let mut writer = ArrowWriter::try_new(file, arrow_schema.clone(), Some(props)).unwrap();
    writer.write(&test_data).unwrap();
    writer.close().unwrap();

    // ===== Setup Iceberg Schema and Read =====

    // 6. Create Iceberg schema
    let iceberg_schema = Arc::new(
        Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::required(2, "name", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
            .unwrap(),
    );

    // 7. Create FileScanTask with encryption metadata
    let task = FileScanTask {
        start: 0,
        length: 0,
        record_count: Some(5),
        data_file_path: format!("file://{}", parquet_path),
        data_file_format: DataFileFormat::Parquet,
        schema: iceberg_schema.clone(),
        project_field_ids: vec![1, 2],
        predicate: None,
        deletes: vec![],
        partition: None,
        partition_spec: None,
        name_mapping: None,
        case_sensitive: false,
        key_metadata: Some(key_metadata_bytes),
    };

    // 8. Read through ArrowReader
    let reader = ArrowReaderBuilder::new(file_io).build();
    let tasks = Box::pin(futures::stream::iter(vec![Ok(task)])) as FileScanTaskStream;

    let result = reader
        .read(tasks)
        .unwrap()
        .try_collect::<Vec<RecordBatch>>()
        .await
        .unwrap();

    // ===== Verify Results =====
    assert_eq!(result.len(), 1);
    let batch = &result[0];

    assert_eq!(batch.num_rows(), 5);
    assert_eq!(batch.num_columns(), 2);

    // Verify ID column
    let id_col = batch
        .column(0)
        .as_primitive::<arrow_array::types::Int32Type>();
    assert_eq!(id_col.values(), &[1, 2, 3, 4, 5]);

    // Verify name column
    let name_col = batch.column(1).as_string::<i32>();
    assert_eq!(name_col.value(0), "Alice");
    assert_eq!(name_col.value(1), "Bob");
    assert_eq!(name_col.value(2), "Charlie");
    assert_eq!(name_col.value(3), "Dave");
    assert_eq!(name_col.value(4), "Eve");
}
