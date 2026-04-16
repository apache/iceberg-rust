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

The [Iceberg table spec](https://iceberg.apache.org/docs/nightly/encryption/) defines encryption
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

The encryption system uses envelope encryption with a chained key hierarchy, adapted from the
Java implementation to Rust's ownership and async model. KMS-managed master keys wrap KEKs,
which encrypt only manifest list key metadata. All other DEKs are protected by being stored
inside their encrypted parent files.

### Key Hierarchy

```
Master Key (in KMS)
  └── wraps → KEK (Key Encryption Key) — stored KMS-wrapped in table metadata
        └── encrypts → manifest list StandardKeyMetadata (AES-GCM, KEY_TIMESTAMP as AAD)
              │
              ├── manifest list DEK → encrypts manifest list file (AGS1)
              │     └── manifest key_metadata (plaintext StandardKeyMetadata) stored in manifest list entries
              │           └── manifest DEK → encrypts manifest file (AGS1)
              │                 └── data file key_metadata (plaintext StandardKeyMetadata) stored in manifest entries
              │                       └── data file DEK → encrypts data file (AGS1 or Parquet native)
```

- **Master keys** live in the KMS and never leave it
- **KEKs** are wrapped by the master key via KMS (`kmsClient.wrapKey()`) and stored in
  `TableMetadata.encryption_keys` with a `KEY_TIMESTAMP` property for rotation tracking
- **DEKs** are generated as plaintext random bytes and stored in `StandardKeyMetadata` per file.
  DEKs are **not** individually wrapped by a KEK. Instead, they are protected by being stored
  inside their encrypted parent file:
  - **Manifest list DEKs**: Their `StandardKeyMetadata` is AES-GCM encrypted by a KEK
    (using `KEY_TIMESTAMP` as AAD) and stored as an `EncryptedKey` in table metadata
  - **Manifest DEKs**: Their `StandardKeyMetadata` is stored as plaintext `key_metadata` bytes
    in manifest list entries — protected because the manifest list file itself is encrypted
  - **Data file DEKs**: Their `StandardKeyMetadata` is stored as plaintext `key_metadata` bytes
    in manifest entries — protected because the manifest file itself is encrypted
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
│                     EncryptionManager (concrete struct)                     │
│                                                                             │
│  EncryptionManager:                                                         │
│  - Envelope encryption: Master → KEK → manifest list StandardKeyMetadata    │
│  - DEKs are plaintext, protected by encrypted parent files                  │
│  - KEK cache (moka async, configurable TTL)                                 │
│  - Automatic KEK rotation (730 days, KEY_TIMESTAMP tracking)                │
│  - encrypt() / decrypt() for AGS1 stream files                              │
│  - encrypt_native() for Parquet Modular Encryption                          │
│  - wrap/unwrap_key_metadata() for manifest list keys (KEK + KMS)            │
│  - generate_dek() for per-file plaintext DEK generation                     │
│  - Constructed per-table by KmsClientFactory                                │
└─────────────────────────────────────────────────────────────────────────────┘
              │                              │
              ▼                              ▼
┌──────────────────────────┐   ┌──────────────────────────────────────────────┐
│   KeyManagementClient    │   │              KEK Cache                       │
│       (trait)            │   │                                              │
│                          │   │  - moka::future::Cache with configurable TTL │
│  wrap_key(key, key_id)   │   │  - Thread-safe async                         │
│  unwrap_key(wrapped, id) │   │  - Caches plaintext KEK bytes per key ID     │
│  generate_key(key_id)    │   │                                              │
└──────────────────────────┘   └──────────────────────────────────────────────┘
              │
              ▼
┌──────────────────────────┐
│    KMS Implementations   │
│                          │
│  - InMemoryKms (testing) │
│  - AWS KMS               │
│  - Azure KV (future)     │
│  - GCP KMS (future)      │
└──────────────────────────┘
              │
              ▲ created by
┌──────────────────────────┐
│   KmsClientFactory       │
│       (trait)            │
│                          │
│  create_kms_client(props)│
│                          │
│  - AwsKmsClientFactory   │
│  - Custom factories      │
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
       → get manifest list EncryptedKey
    2. Find the KEK via EncryptedKey.encrypted_by_id
       → unwrap KEK via KMS: kms_client.unwrap_key(kek.encrypted_key_metadata, table_key_id)
       (KEK is cached to avoid redundant KMS calls)
    3. AES-GCM decrypt the manifest list's StandardKeyMetadata using the
       unwrapped KEK, with KEY_TIMESTAMP as AAD
    4. Extract plaintext manifest list DEK from decrypted StandardKeyMetadata
    5. file_io.new_encrypted_input(path, key_metadata) → AGS1-decrypting InputFile
          │
          ▼
ManifestFile
  └── key_metadata: Option<Vec<u8>>  (plaintext StandardKeyMetadata, read from encrypted manifest list)
          │
  load_manifest(file_io)
    1. If key_metadata present:
       a. Parse StandardKeyMetadata → extract plaintext DEK + AAD prefix
       b. file_io.new_encrypted_input() → AGS1-decrypting InputFile
    2. If not: file_io.new_input()
          │
          ▼
FileScanTask
  └── key_metadata: Option<Vec<u8>>  (plaintext StandardKeyMetadata, read from encrypted manifest)
          │
  ArrowReader::create_parquet_record_batch_stream_builder_with_key_metadata()
    1. If key_metadata present:
       a. file_io.new_native_encrypted_input(path, key_metadata) → NativeEncrypted InputFile
       b. Parse StandardKeyMetadata → extract plaintext DEK + AAD prefix
       c. Build FileDecryptionProperties from NativeKeyMaterial (DEK + AAD prefix)
       d. Pass to ParquetRecordBatchStreamBuilder
    2. If not: standard Parquet read
```

#### Write Path (Encryption)

```
RollingFileWriter::new_output_file()
    1. If file_io.encryption_manager() is Some:
       a. file_io.new_native_encrypted_output(path) → EncryptedOutputFile
       b. EncryptionManager generates random plaintext DEK + AAD prefix
       c. OutputFile::NativeEncrypted carries NativeKeyMaterial for Parquet writer
       d. Store plaintext StandardKeyMetadata as key_metadata bytes on DataFile
          (protected by being stored inside the encrypted parent manifest)
    2. ParquetWriter detects NativeEncrypted, configures FileEncryptionProperties

SnapshotProducer::commit()
    1. Manifest writing:
       a. em.encrypt(output_file) → generates random plaintext DEK + AAD prefix
       b. Write manifest to AGS1-encrypting OutputFile
       c. Store plaintext StandardKeyMetadata as key_metadata on ManifestFile entry
          (protected by being stored inside the encrypted parent manifest list)
    2. Manifest list writing:
       a. em.encrypt(output_file) → generates random plaintext DEK + AAD prefix
       b. Write manifest list to AGS1-encrypting OutputFile
       c. Get or create KEK:
          - Find unexpired KEK (check KEY_TIMESTAMP, 730-day lifespan)
          - If none: generate new KEK, wrap via KMS: kms_client.wrap_key(kek, table_key_id)
       d. AES-GCM encrypt the manifest list's StandardKeyMetadata using the KEK,
          with KEY_TIMESTAMP as AAD
       e. Store as EncryptedKey (encrypted_by_id = kek_id) in encryption manager
       f. Store manifest list key_id on Snapshot.encryption_key_id
    3. Table commit includes AddEncryptionKey for all new entries:
       - New KEKs (encrypted_by_id = table_key_id, properties include KEY_TIMESTAMP)
       - New manifest list key metadata (encrypted_by_id = kek_id)
```

### Module Structure

```
crates/iceberg/src/
├── encryption/
│   ├── mod.rs                       # Module re-exports
│   ├── crypto.rs                    # AES-GCM primitives (SecureKey, AesGcmEncryptor)
│   ├── key_management.rs            # KeyManagementClient trait
│   ├── key_metadata.rs              # StandardKeyMetadata (Avro V1, Java-compatible)
│   ├── encryption_manager.rs        # EncryptionManager (concrete struct)
│   ├── file_encryptor.rs            # FileEncryptor (write-side AGS1 wrapper)
│   ├── file_decryptor.rs            # FileDecryptor (read-side AGS1 wrapper)
│   ├── encrypted_io.rs              # EncryptedInputFile / EncryptedOutputFile wrappers
│   ├── stream.rs                    # AesGcmFileRead / AesGcmFileWrite (AGS1 format)
│   ├── kms/
│   │   ├── mod.rs                   # KmsClientFactory trait
│   │   ├── in_memory.rs             # InMemoryKms (testing only)
│   │   └── aws.rs                   # AwsKeyManagementClient (feature-gated: kms-aws)
├── tests/
│   └── encryption_integration.rs    # End-to-end encryption round-trip tests
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
    /// Wrap (encrypt) a key using a wrapping key managed by the KMS.
    async fn wrap_key(&self, key: &[u8], wrapping_key_id: &str) -> Result<Vec<u8>>;

    /// Unwrap (decrypt) a previously wrapped key. Returns SensitiveBytes
    /// which zeroizes on drop and redacts in Debug output.
    async fn unwrap_key(&self, wrapped_key: &[u8], wrapping_key_id: &str) -> Result<SensitiveBytes>;

    /// Whether this KMS supports server-side key generation.
    /// If true, callers can use generate_key() for atomic key generation and wrapping.
    fn supports_key_generation(&self) -> bool { false }

    /// Generate a new key and wrap it atomically on the server side.
    /// Only supported when supports_key_generation() returns true.
    async fn generate_key(&self, wrapping_key_id: &str) -> Result<KeyGenerationResult> {
        Err(Error::new(ErrorKind::FeatureUnsupported, "..."))
    }
}
```

The `initialize()` method from the Java interface is intentionally omitted — in Rust,
KMS clients are constructed with their configuration already applied (via builder pattern
or constructor arguments), making a separate initialization step unnecessary.

The `SensitiveBytes` return type for `unwrap_key` ensures that plaintext key material
is automatically zeroized on drop and redacted in debug output.

`supports_key_generation()` and `generate_key()` support KMS backends like AWS KMS
that can atomically generate and wrap keys server-side via `GenerateDataKey`, which is
more secure than generating locally and wrapping separately.

Users implement this trait to integrate with their KMS of choice (AWS KMS, Azure Key Vault,
GCP KMS, HashiCorp Vault, etc.). An `InMemoryKms` is provided for testing.

#### StandardKeyMetadata

Avro-serialized metadata stored alongside encrypted files. Compatible with the Java
`StandardKeyMetadata` format for cross-language interoperability:

```rust
pub struct StandardKeyMetadata {
    encryption_key: Vec<u8>,  // Plaintext DEK (always plaintext — never individually wrapped)
    aad_prefix: Vec<u8>,      // Additional authenticated data prefix
    file_length: Option<i64>, // Optional encrypted file length
}
// Note: For manifest lists, the entire serialized StandardKeyMetadata is AES-GCM
// encrypted by a KEK before storage. For manifests and data files, the
// StandardKeyMetadata is stored as plaintext key_metadata in the parent
// encrypted file.
```

Wire format: `[version byte 0x01][Avro binary datum]` — byte-compatible with Java.

### EncryptionManager

`EncryptionManager` is a concrete struct (not a trait) that implements envelope encryption
with KMS-backed KEK management, KEK caching, and rotation. A trait is unnecessary here —
for unencrypted tables, the encryption manager is simply `None` rather than using a no-op
implementation:

```rust
pub struct EncryptionManager {
    kms_client: Arc<dyn KeyManagementClient>,
    kek_cache: moka::future::Cache<String, SensitiveBytes>,
    kek_lifespan_days: i64,
    algorithm: EncryptionAlgorithm,
    table_key_id: Option<String>,
    encryption_keys: HashMap<String, EncryptedKey>,
}

impl EncryptionManager {
    pub fn new(kms_client: Arc<dyn KeyManagementClient>) -> Self;

    /// Decrypt an AGS1 stream-encrypted file.
    pub async fn decrypt(&self, encrypted: EncryptedInputFile) -> Result<InputFile>;

    /// Encrypt a file with AGS1 stream encryption.
    pub async fn encrypt(&self, raw_output: OutputFile) -> Result<EncryptedOutputFile>;

    /// Encrypt for Parquet Modular Encryption (generates NativeKeyMaterial).
    pub async fn encrypt_native(&self, raw_output: OutputFile) -> Result<EncryptedOutputFile>;

    /// Unwrap key metadata for a manifest list.
    /// 1. Look up the manifest list's EncryptedKey by key ID
    /// 2. Find the KEK via encrypted_by_id
    /// 3. Unwrap the KEK via KMS
    /// 4. AES-GCM decrypt the manifest list's StandardKeyMetadata using the KEK,
    ///    with KEY_TIMESTAMP as AAD
    /// 5. Return the decrypted StandardKeyMetadata bytes (containing plaintext DEK)
    pub async fn unwrap_key_metadata(
        &self, encrypted_key: &EncryptedKey,
        encryption_keys: &HashMap<String, EncryptedKey>,
    ) -> Result<Vec<u8>>;

    /// Wrap key metadata for a manifest list with a KEK for storage in table metadata.
    pub async fn wrap_key_metadata(
        &self, key_metadata: &[u8],
    ) -> Result<(EncryptedKey, Option<EncryptedKey>)>;
}
```

`EncryptionManager` is typically not constructed directly by users. Instead,
`TableBuilder::build()` constructs it automatically from a `KmsClientFactory`
extension and the table's properties (see [Catalog Integration](#catalog-integration) below).
For manual construction in tests:

```rust
let em = EncryptionManager::new(Arc::new(kms_client))
    .with_table_key_id("master-key-1")   // Master key ID in KMS
    .with_encryption_keys(table_metadata.encryption_keys.clone());
```

### AGS1 Stream Encryption

Block-based stream encryption format compatible with Java's `AesGcmInputStream`/`AesGcmOutputStream`.

#### Format

```
┌──────────────────────────────────────────┐
│ Header (8 bytes)                         │
│   Magic: "AGS1" (4 bytes)                │
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

### EncryptedInputFile and EncryptedOutputFile Wrappers

Rather than adding encryption variants to `InputFile`/`OutputFile` (which would change the
public API of those core types), encryption uses dedicated wrapper types. `EncryptedInputFile`
and `EncryptedOutputFile` wrap a plain `InputFile`/`OutputFile` and add transparent
encryption/decryption. The plain `InputFile`/`OutputFile` types remain unchanged.

```rust
/// Wraps a plain InputFile with decryption capabilities.
pub enum EncryptedInputFile {
    /// AGS1 stream-encrypted file. Decrypted transparently on read.
    Encrypted {
        inner: InputFile,
        decryptor: Arc<AesGcmFileDecryptor>,
    },
    /// Parquet Modular Encryption. The Parquet reader handles decryption.
    NativeEncrypted {
        inner: InputFile,
        key_material: NativeKeyMaterial,
    },
}

/// Wraps a plain OutputFile with encryption capabilities.
pub enum EncryptedOutputFile {
    /// AGS1 stream-encrypted output. Encrypted transparently on write.
    Encrypted {
        inner: OutputFile,
        key_metadata: Box<[u8]>,
        encryptor: Arc<AesGcmFileEncryptor>,
    },
    /// Parquet Modular Encryption. The Parquet writer handles encryption.
    NativeEncrypted {
        inner: OutputFile,
        key_metadata: Box<[u8]>,
        key_material: NativeKeyMaterial,
    },
}
```

Both wrappers delegate standard operations (`location()`, `exists()`, `read()`, `reader()`,
`write()`, `writer()`) to the inner file, with `Encrypted` variants transparently
encrypting/decrypting via `AesGcmFileRead`/`AesGcmFileWrite`. `into_inner()` recovers
the underlying plain file.

`NativeKeyMaterial` carries the plaintext DEK and AAD prefix for Parquet's
`FileEncryptionProperties` / `FileDecryptionProperties`.

This wrapper approach means `ManifestReader` and `ManifestListWriter` accept the encrypted
wrapper types (or `Box<dyn FileWrite>`) where encryption is needed, rather than requiring
changes to the `InputFile`/`OutputFile` enums themselves.

### FileIO Integration

The `EncryptionManager` is attached to `FileIO` via a convenience method. The encryption
manager is typically created automatically by `TableBuilder::build()` using the catalog's
`KmsClientFactory`, but can also be attached directly for testing:

```rust
// Typically automatic via catalog + KmsClientFactory (see Catalog Integration).
// For manual/test setup:
let file_io = file_io.with_encryption_manager(Arc::new(encryption_manager));
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

RFC 0002 removes `Extensions` from `FileIOBuilder`. The `KmsClientFactory` will be
provided at the catalog level (Option A), which is consistent with the wrapper approach
for `EncryptedInputFile`/`EncryptedOutputFile` — encryption operates at the `FileIO` level
rather than wrapping storage:

```rust
let catalog = GlueCatalogBuilder::default()
    .with_storage_factory(Arc::new(OpenDalStorageFactory::S3))
    .with_kms_client_factory(Arc::new(AwsKmsClientFactory))
    .load("my_catalog", props)
    .await?;
```

The encryption module's internal design (crypto, key management, stream format) is unaffected
by the storage trait changes.

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

#### How Java Does It

In Java, encryption is configured through two catalog properties:

- `encryption.kms-impl` — fully qualified class name of the `KeyManagementClient` implementation
  (e.g. `org.apache.iceberg.aws.AwsKeyManagementClient`)
- `encryption.kms-type` — reserved for a future built-in registry of KMS types, but
  **not yet implemented** in Java (any value throws `"Unsupported KMS type"`)

These are mutually exclusive. `kms-impl` uses Java reflection to instantiate the KMS client
from a class name. The `table_key_id` is then read from the table's `encryption.key-id`
property, and a per-table `StandardEncryptionManager` is constructed in
`RESTTableOperations.encryption()`. The base `FileIO` is wrapped with `EncryptingFileIO.combine(io, encryption())`
to produce a per-table encrypting FileIO.

#### How Rust Does It

Rust does not have Java's reflection-based class loading, so `encryption.kms-impl` (a class name
string) is not useful. Instead, the user provides a `KmsClientFactory` to the catalog builder.
The factory creates per-table `KeyManagementClient` instances from table properties, and the
`table_key_id` and `encryption_keys` are inferred automatically from table metadata.

##### KmsClientFactory Trait

A `KmsClientFactory` abstracts the construction of KMS clients. This allows the catalog to
create a correctly configured KMS client for each table based on that table's properties,
without the user needing to know the details of per-table KMS configuration:

```rust
#[async_trait]
pub trait KmsClientFactory: Send + Sync + Debug {
    /// Create a KeyManagementClient from table properties.
    ///
    /// Called by TableBuilder::build() for each encrypted table. The factory
    /// receives the table's properties (which may include KMS endpoint, region,
    /// credentials, etc.) and returns a configured client.
    async fn create_kms_client(
        &self,
        properties: &HashMap<String, String>,
    ) -> Result<Arc<dyn KeyManagementClient>>;
}
```

Example implementations:

```rust
/// Factory for AWS KMS clients. Reads endpoint and region from table properties.
#[derive(Debug)]
pub struct AwsKmsClientFactory;

#[async_trait]
impl KmsClientFactory for AwsKmsClientFactory {
    async fn create_kms_client(
        &self,
        properties: &HashMap<String, String>,
    ) -> Result<Arc<dyn KeyManagementClient>> {
        let region = properties.get("kms.region").cloned();
        let endpoint = properties.get("kms.endpoint").cloned();
        let client = AwsKeyManagementClient::builder()
            .with_region(region)
            .with_endpoint(endpoint)
            .build()
            .await?;
        Ok(Arc::new(client))
    }
}
```

**Step 1: User provides the KMS client factory to the catalog builder.**

```rust
let catalog = RestCatalogBuilder::default()
    .with_kms_client_factory(Arc::new(AwsKmsClientFactory))
    .load("rest", props)
    .await?;
```

**Step 2: `TableBuilder::build()` auto-configures encryption per table.**

When building a `Table`, `TableBuilder::maybe_configure_encryption()` runs automatically.
This is the Rust equivalent of Java's `RESTTableOperations.io()` which calls
`EncryptingFileIO.combine(io, encryption())`. It checks:

1. Does the catalog have a `KmsClientFactory`? If not, return as-is.
2. Does the table metadata have an `encryption.key-id` property? If not, return as-is (unencrypted table).
3. If both are present:
   a. Call `kms_client_factory.create_kms_client(table_properties)` to get a per-table KMS client
   b. Construct an `EncryptionManager` with:
      - `table_key_id` from the `encryption.key-id` table property
      - `encryption_keys` from `TableMetadata.encryption_keys` (the KEK map)
      - The `KeyManagementClient` from the factory
4. Attach the `EncryptionManager` to the table's `FileIO`.

This runs on every `Table::builder().build()` call, so each table gets a correctly configured
per-table `EncryptionManager` even when a single catalog manages tables with different key IDs.

```rust
// User code — just provide the factory, everything else is automatic:
let table = catalog.load_table(&ident).await?;
// table.file_io() now has an EncryptionManager configured with
// the correct table_key_id and encryption_keys from this table's metadata.
```

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

- **Additional KMS implementations**: Azure Key Vault, GCP KMS (AWS KMS is implemented)
- **AES-256-GCM support**: Depends on `parquet-rs` support
