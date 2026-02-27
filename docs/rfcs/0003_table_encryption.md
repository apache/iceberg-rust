<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
-->

# Table Encryption

## Background

### Iceberg Spec: Encryption

The [Iceberg table spec](https://iceberg.apache.org/spec/#table-metadata) defines encryption
as a first-class concept. Tables may store an `encryption-keys` map in their metadata,
snapshots may reference an `encryption-key-id`, and manifest files carry optional
`key_metadata` bytes. Data files themselves can be encrypted either at the stream level
(AES-GCM envelope encryption, the "AGS1" format) or natively by the file format (e.g.
Parquet Modular Encryption).

The Java implementation (`org.apache.iceberg.encryption`) is the reference and has been
production-tested. It defines:

- **`EncryptionManager`** -- orchestrates encrypt/decrypt of `InputFile`/`OutputFile`
- **`KeyManagementClient`** -- pluggable KMS integration (wrap/unwrap keys)
- **`EncryptedInputFile` / `EncryptedOutputFile`** -- thin wrappers pairing a raw file handle
  with its `EncryptionKeyMetadata`
- **`StandardEncryptionManager`** -- envelope encryption with key caching, AGS1 streams,
  and Parquet native encryption support
- **`StandardKeyMetadata`** -- Avro-serialized key metadata (wrapped DEK, AAD prefix, file length)
- **`AesGcmInputStream` / `AesGcmOutputStream`** -- block-based stream encryption (AGS1 format)

### Relationship to Storage Trait RFC

[RFC 0002 (Making Storage a Trait)](https://github.com/apache/iceberg-rust/pull/2116) proposes
converting `Storage` from an enum to a trait and removing the `Extensions` mechanism from
`FileIOBuilder`. This encryption RFC is designed to work both with the current `Extensions`-based
`FileIO` and with the future trait-based storage. Specific adaptation points are called out below.

---

## High-Level Architecture

The encryption system uses two-layer envelope encryption, adapted from the Java implementation
to Rust's ownership and async model.

### Key Hierarchy

```
Master Key (in KMS)
  └── wraps → KEK (Key Encryption Key) — stored in table metadata as EncryptedKey
        └── wraps → DEK (Data Encryption Key) — stored in StandardKeyMetadata per file
              └── encrypts → file content (AGS1 stream or Parquet native)
```

- **Master keys** live in the KMS and never leave it
- **KEKs** are wrapped by the master key and stored in `TableMetadata.encryption_keys`
- **DEKs** are wrapped by a KEK and stored per-file in `StandardKeyMetadata`
- KEKs are cached in memory (moka async cache with configurable TTL) to avoid redundant KMS calls
- KEK rotation occurs automatically when a KEK exceeds its configurable lifespan (default 730 days per NIST SP 800-57)

### Component Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              User / Table Scan                              │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                     EncryptionManager (trait)                                │
│                                                                             │
│  StandardEncryptionManager:                                                 │
│  - Two-layer envelope: Master → KEK → DEK                                  │
│  - KEK cache (moka async, configurable TTL)                                │
│  - Automatic KEK rotation                                                  │
│  - encrypt() / decrypt() for AGS1 stream files                            │
│  - encrypt_native() for Parquet Modular Encryption                         │
│  - wrap/unwrap_key_metadata() for manifest list keys                       │
│  - generate_dek() with KEK management                                      │
└─────────────────────────────────────────────────────────────────────────────┘
              │                              │
              ▼                              ▼
┌──────────────────────────┐   ┌──────────────────────────────────────────────┐
│   KeyManagementClient    │   │              KEK Cache                       │
│       (trait)            │   │                                              │
│                          │   │  - moka::future::Cache with configurable TTL │
│  wrap_key(key, key_id)   │   │  - Thread-safe async                        │
│  unwrap_key(wrapped, id) │   │  - Caches plaintext KEK bytes per key ID    │
│  initialize(props)       │   │                                              │
└──────────────────────────┘   └──────────────────────────────────────────────┘
              │
              ▼
┌──────────────────────────┐
│    KMS Implementations   │
│                          │
│  - InMemoryKms (testing) │
│  - AWS KMS (future)      │
│  - Azure KV (future)     │
│  - GCP KMS (future)      │
└──────────────────────────┘
```

### Data Flow

#### Read Path (Decryption)

```
TableMetadata
  └── encryption_keys: {key_id → EncryptedKey}
          │
Snapshot  │
  └── encryption_key_id ──────┘   (V3 format only)
          │
          ▼
  load_manifest_list(file_io, table_metadata)
    1. Look up encryption_key_id in table_metadata.encryption_keys
    2. em.unwrap_key_metadata() → plaintext key metadata
    3. file_io.new_encrypted_input(path, key_metadata) → AGS1-decrypting InputFile
          │
          ▼
ManifestFile
  └── key_metadata: Option<Vec<u8>>
          │
  load_manifest(file_io)
    1. If key_metadata present: file_io.new_encrypted_input() → AGS1-decrypting InputFile
    2. If not: file_io.new_input()
          │
          ▼
FileScanTask
  └── key_metadata: Option<Vec<u8>>
          │
  ArrowReader::create_parquet_record_batch_stream_builder_with_key_metadata()
    1. If key_metadata present:
       a. file_io.new_native_encrypted_input(path, key_metadata) → NativeEncrypted InputFile
       b. Build FileDecryptionProperties from NativeKeyMaterial (DEK + AAD prefix)
       c. Pass to ParquetRecordBatchStreamBuilder
    2. If not: standard Parquet read
```

#### Write Path (Encryption)

```
RollingFileWriter::new_output_file()
    1. If file_io.encryption_manager() is Some:
       a. file_io.new_native_encrypted_output(path) → EncryptedOutputFile
       b. EncryptionManager generates DEK, wraps with KEK
       c. OutputFile::NativeEncrypted carries NativeKeyMaterial for Parquet writer
       d. Store key_metadata bytes on DataFile
    2. ParquetWriter detects NativeEncrypted, configures FileEncryptionProperties

SnapshotProducer::commit()
    1. Manifest writing:
       a. file_io.new_encrypted_output(path) → AGS1-encrypting OutputFile
       b. Store key_metadata on ManifestFile entry
    2. Manifest list writing:
       a. file_io.new_encrypted_output(path) → AGS1-encrypting OutputFile
       b. em.wrap_key_metadata() → EncryptedKey for table metadata
       c. Store key_id on Snapshot.encryption_key_id
    3. Table updates include AddEncryptionKey for new KEKs
```

### Module Structure

```
crates/iceberg/src/
├── encryption/
│   ├── mod.rs                       # Module re-exports
│   ├── crypto.rs                    # AES-GCM primitives (SecureKey, AesGcmEncryptor)
│   ├── key_management.rs            # KeyManagementClient trait
│   ├── key_metadata.rs              # StandardKeyMetadata (Avro V1, Java-compatible)
│   ├── encryption_manager.rs        # EncryptionManager trait + StandardEncryptionManager
│   ├── plaintext_encryption_manager.rs  # No-op pass-through for unencrypted tables
│   ├── file_encryptor.rs            # FileEncryptor (write-side AGS1 wrapper)
│   ├── file_decryptor.rs            # FileDecryptor (read-side AGS1 wrapper)
│   ├── encrypted_io.rs              # EncryptedInputFile / EncryptedOutputFile wrappers
│   ├── stream.rs                    # AesGcmFileRead / AesGcmFileWrite (AGS1 format)
│   ├── kms/
│   │   ├── mod.rs
│   │   └── in_memory.rs             # InMemoryKms (testing only)
│   └── integration_tests.rs         # End-to-end encryption round-trip tests
├── io/
│   └── file_io.rs                   # InputFile/OutputFile enums, FileIO encryption methods
├── arrow/
│   └── reader.rs                    # Parquet decryption via FileDecryptionProperties
├── writer/file_writer/
│   ├── parquet_writer.rs            # Parquet FileEncryptionProperties integration
│   └── rolling_writer.rs            # Encrypted output file creation + key_metadata propagation
├── transaction/
│   └── snapshot.rs                  # Encrypted manifest/manifest list writing, KEK management
├── scan/
│   ├── context.rs                   # key_metadata propagation from DataFile → FileScanTask
│   └── task.rs                      # FileScanTask.key_metadata field
└── spec/
    ├── snapshot.rs                  # Snapshot.encryption_key_id, load_manifest_list decryption
    └── manifest_list.rs             # ManifestFile.key_metadata, load_manifest decryption
```

---

## Design

### Core Cryptographic Primitives

#### EncryptionAlgorithm

```rust
pub enum EncryptionAlgorithm {
    Aes128Gcm,
    // Future: Aes256Gcm (depends on parquet-rs support)
}
```

#### SecureKey

Wraps key material with automatic zeroization on drop via `zeroize::Zeroizing<Vec<u8>>`:

```rust
pub struct SecureKey {
    data: Zeroizing<Vec<u8>>,
    algorithm: EncryptionAlgorithm,
}

impl SecureKey {
    pub fn new(data: Vec<u8>, algorithm: EncryptionAlgorithm) -> Result<Self>;
    pub fn generate(algorithm: EncryptionAlgorithm) -> Self;
}
```

#### AesGcmEncryptor

AES-GCM encrypt/decrypt. Ciphertext format matches the Java implementation:
`[12-byte nonce][ciphertext][16-byte GCM tag]`.

```rust
pub struct AesGcmEncryptor { /* ... */ }

impl AesGcmEncryptor {
    pub fn new(key: SecureKey) -> Self;
    pub fn encrypt(&self, plaintext: &[u8], aad: Option<&[u8]>) -> Result<Vec<u8>>;
    pub fn decrypt(&self, ciphertext: &[u8], aad: Option<&[u8]>) -> Result<Vec<u8>>;
}
```

### Key Management

#### KeyManagementClient Trait

Pluggable interface for KMS integration. Mirrors the Java `KeyManagementClient`:

```rust
#[async_trait]
pub trait KeyManagementClient: Debug + Send + Sync {
    async fn initialize(&mut self, properties: HashMap<String, String>) -> Result<()>;
    async fn wrap_key(&self, key: &[u8], wrapping_key_id: &str) -> Result<Vec<u8>>;
    async fn unwrap_key(&self, wrapped_key: &[u8], wrapping_key_id: &str) -> Result<Vec<u8>>;
}
```

Users implement this trait to integrate with their KMS of choice (AWS KMS, Azure Key Vault,
GCP KMS, HashiCorp Vault, etc.). An `InMemoryKms` is provided for testing.

#### StandardKeyMetadata

Avro-serialized metadata stored alongside encrypted files. Compatible with the Java
`StandardKeyMetadata` format for cross-language interoperability:

```rust
pub struct StandardKeyMetadata {
    encryption_key: Vec<u8>,  // Plaintext DEK (for PME) or wrapped DEK (for AGS1)
    aad_prefix: Vec<u8>,      // Additional authenticated data prefix
    file_length: Option<i64>, // Optional encrypted file length
}
```

Wire format: `[version byte 0x01][Avro binary datum]` — byte-compatible with Java.

### EncryptionManager

The `EncryptionManager` trait abstracts encryption orchestration. `StandardEncryptionManager`
implements two-layer envelope encryption with KEK caching and rotation:

```rust
#[async_trait]
pub trait EncryptionManager: Debug + Send + Sync {
    /// Decrypt an AGS1 stream-encrypted file.
    async fn decrypt(&self, encrypted: EncryptedInputFile) -> Result<InputFile>;

    /// Encrypt a file with AGS1 stream encryption.
    async fn encrypt(&self, raw_output: OutputFile) -> Result<EncryptedOutputFile>;

    /// Encrypt for Parquet Modular Encryption (generates NativeKeyMaterial).
    async fn encrypt_native(&self, raw_output: OutputFile) -> Result<EncryptedOutputFile>;

    /// Unwrap key metadata using the table's KEK hierarchy.
    async fn unwrap_key_metadata(
        &self, encrypted_key: &EncryptedKey,
        encryption_keys: &HashMap<String, EncryptedKey>,
    ) -> Result<Vec<u8>>;

    /// Wrap key metadata with a KEK for storage in table metadata.
    async fn wrap_key_metadata(
        &self, key_metadata: &[u8],
    ) -> Result<(EncryptedKey, Option<EncryptedKey>)>;
}
```

`StandardEncryptionManager` configuration:

```rust
let em = StandardEncryptionManager::new(Arc::new(kms_client))
    .with_table_key_id("master-key-1")   // Master key ID in KMS
    .with_aad_prefix(b"my-table".to_vec()); // AAD prefix for GCM blocks
```

A `PlaintextEncryptionManager` is also provided as a no-op pass-through for unencrypted tables.

### AGS1 Stream Encryption

Block-based stream encryption format compatible with Java's `AesGcmInputStream`/`AesGcmOutputStream`.

#### Format

```
┌──────────────────────────────────────────┐
│ Header (8 bytes)                         │
│   Magic: "AGS1" (4 bytes)               │
│   Plain block size: u32 LE (4 bytes)     │
│     Default: 1,048,576 (1 MiB)           │
├──────────────────────────────────────────┤
│ Block 0                                  │
│   Nonce (12 bytes)                       │
│   Ciphertext (up to plain_block_size)    │
│   GCM Tag (16 bytes)                     │
├──────────────────────────────────────────┤
│ Block 1..N (same structure)              │
├──────────────────────────────────────────┤
│ Final block (may be shorter)             │
└──────────────────────────────────────────┘
```

Each block's AAD is constructed as `aad_prefix || block_index (4 bytes LE)`, binding each
block to its position in the stream to prevent reordering attacks.

**`AesGcmFileRead`** implements the `FileRead` trait for transparent AGS1 decryption with
random-access reads. **`AesGcmFileWrite`** implements `FileWrite` for transparent AGS1
encryption with block buffering.

### InputFile and OutputFile Enums

`InputFile` and `OutputFile` are enums with three variants each:

```rust
pub enum InputFile {
    /// Standard unencrypted file.
    Plain { storage: Arc<dyn Storage>, path: String },

    /// AGS1 stream-encrypted file. Transparent decryption on read.
    Encrypted { storage: Arc<dyn Storage>, path: String, decryptor: Arc<FileDecryptor> },

    /// Parquet Modular Encryption. Raw reads; Parquet reader handles decryption.
    NativeEncrypted { storage: Arc<dyn Storage>, path: String, key_material: NativeKeyMaterial },
}

pub enum OutputFile {
    Plain { storage: Arc<dyn Storage>, path: String },
    Encrypted { storage: Arc<dyn Storage>, path: String, encryptor: Arc<FileEncryptor> },
    NativeEncrypted { storage: Arc<dyn Storage>, path: String, key_material: NativeKeyMaterial },
}
```

`NativeKeyMaterial` carries the plaintext DEK and AAD prefix for Parquet's
`FileEncryptionProperties` / `FileDecryptionProperties`.

Common operations (`location()`, `exists()`, `read()`, `reader()`, `write()`, `writer()`)
delegate to the appropriate variant, with `Encrypted` variants transparently encrypting/decrypting
via `AesGcmFileRead`/`AesGcmFileWrite`.

#### Adaptation for Storage Trait RFC

Once RFC 0002 merges, `InputFile` will hold `Arc<dyn Storage>` instead of `Operator`. This is
already the case in this implementation — the enum structure is stable. Only the underlying
`Storage` trait implementation may change.

### FileIO Integration

The `EncryptionManager` is stored as a type-safe `FileIOBuilder` extension. This integrates
naturally with catalogs that support extensions (e.g. `RestCatalog.with_file_io_extension()`):

```rust
// Via FileIOBuilder extension (works with RestCatalog and any extension-aware catalog)
let file_io = FileIOBuilder::new("s3")
    .with_prop("s3.region", "us-east-1")
    .with_extension(encryption_manager)
    .build()?;

// Or via convenience method on FileIO
let file_io = file_io.with_encryption_manager(encryption_manager);
```

FileIO provides encryption-aware factory methods:

| Method | Purpose |
|--------|---------|
| `new_encrypted_input(path, key_metadata)` | AGS1 stream decryption (manifests, manifest lists) |
| `new_encrypted_output(path)` | AGS1 stream encryption |
| `new_native_encrypted_input(path, key_metadata)` | PME input (Parquet handles decryption) |
| `new_native_encrypted_output(path)` | PME output (Parquet handles encryption) |
| `encryption_manager()` | Returns the configured EncryptionManager, if any |

#### After Storage Trait RFC

RFC 0002 removes `Extensions` from `FileIOBuilder`. The `EncryptionManager` will instead be
provided through the `StorageFactory` or configured at the catalog level:

```rust
// Option A: EncryptionManager on the catalog
let catalog = GlueCatalogBuilder::default()
    .with_storage_factory(Arc::new(OpenDalStorageFactory::S3))
    .with_encryption_manager(encryption_manager)
    .load("my_catalog", props)
    .await?;

// Option B: Wrapping StorageFactory
pub struct EncryptingStorageFactory {
    inner: Arc<dyn StorageFactory>,
    encryption_manager: Arc<EncryptionManager>,
}

impl StorageFactory for EncryptingStorageFactory {
    fn build(&self, config: &StorageConfig) -> Result<Arc<dyn Storage>> {
        let storage = self.inner.build(config)?;
        Ok(Arc::new(EncryptingStorage::new(storage, self.encryption_manager.clone())))
    }
}
```

The exact integration point will be finalized when RFC 0002 merges. The encryption
module's internal design (crypto, key management, stream format) is unaffected.

### Parquet Modular Encryption

For Parquet data files, encryption is handled natively by the Parquet reader/writer using
`FileEncryptionProperties` and `FileDecryptionProperties` from `parquet-rs`.

**Write path** (`ParquetWriter`): When the output file is `NativeEncrypted`, the writer extracts
`NativeKeyMaterial` (plaintext DEK + AAD prefix) and configures `FileEncryptionProperties` on the
`AsyncArrowWriter`. The Parquet crate handles column/page-level encryption.

**Read path** (`ArrowReader`): When `FileScanTask.key_metadata` is present, the reader calls
`file_io.new_native_encrypted_input()` which deserializes `StandardKeyMetadata` to extract the
plaintext DEK and AAD prefix. These are used to build `FileDecryptionProperties` which are
passed to `ParquetRecordBatchStreamBuilder::new_with_options()`.

The `ArrowFileReader::get_metadata()` implementation forwards both `file_decryption_properties`
and `metadata_options` from `ArrowReaderOptions` to `ParquetMetaDataReader`, enabling encrypted
footer parsing.

### Catalog Integration

Encryption is configured at the catalog level. The `EncryptionManager` is attached to the
catalog's `FileIO`, and all tables loaded from that catalog inherit it automatically.

For catalogs that support `FileIOBuilder` extensions (e.g. `RestCatalog`), the encryption manager
can be added directly:

```rust
rest_catalog.with_file_io_extension(encryption_manager);
```

For catalogs that don't support extensions, an `EncryptedCatalog` wrapper implements the
`Catalog` trait by delegating to an inner catalog and attaching the `EncryptionManager` to
every `Table` returned:

```rust
let inner_catalog = MemoryCatalogBuilder::default().load("c", props).await?;
let encrypted = EncryptedCatalog::new(Arc::new(inner_catalog), encryption_manager);
```

The wrapper intercepts all `Table`-returning methods (`load_table`, `create_table`,
`register_table`, `update_table`) and calls `table.with_file_io(...)` to attach encryption.
All other catalog methods delegate directly.

`Table::with_file_io()` replaces the table's `FileIO` and rebuilds its `ObjectCache` (which
stores its own `FileIO` for manifest/manifest list loading).

### DataFusion Integration

The DataFusion integration requires **no encryption-specific code**. Encryption flows
transparently through the existing pipeline:

- **`IcebergTableProvider::scan()`** calls `catalog.load_table()` → table has encrypted FileIO → `IcebergTableScan` → `table.scan().to_arrow()` → `ArrowReader` decrypts via `key_metadata`
- **`IcebergTableProvider::insert_into()`** calls `catalog.load_table()` → table has encrypted FileIO → `IcebergWriteExec` uses `table.file_io()` → `RollingFileWriterBuilder` detects encryption → PME-encrypted data files + AGS1-encrypted manifests
- **`IcebergCommitExec`** uses `Transaction::fast_append()` → `SnapshotProducer` writes encrypted manifests/manifest list → `AddEncryptionKey` updates persisted in table metadata

### Format Version Requirement

Snapshot-level `encryption_key_id` is serialized only in the **V3** snapshot format. V2 snapshots
do not include this field, so encrypted manifest lists cannot be read back after a V2 round-trip.
Tables using encryption must use format version V3.

---

## Compatibility

### Java Interoperability

Cross-language compatibility is a hard requirement:

- **AGS1 format**: Byte-level compatible with Java's `AesGcmInputStream`/`AesGcmOutputStream`
  (same header, block size, nonce/tag layout, AAD construction)
- **StandardKeyMetadata**: Avro-serialized with the same schema as Java, enabling Rust to
  read tables encrypted by Java/Spark and vice versa
- **Parquet native encryption**: Uses `FileDecryptionProperties`/`FileEncryptionProperties`
  from `parquet-rs`, which follows the Parquet spec

---

## Future Work

- **Production KMS implementations**: AWS KMS, Azure Key Vault, GCP KMS
- **Column-level encryption policies**: Encrypt specific columns with different keys
- **Key rotation support**: Re-encrypt DEKs with new KEKs without re-encrypting data
- **AES-256-GCM support**: Depends on `parquet-rs` support
- **Storage Trait adaptation**: Replace `Extensions`-based `EncryptionManager` injection
  with the pattern from RFC 0002 (catalog-level or `EncryptingStorageFactory` wrapper)
