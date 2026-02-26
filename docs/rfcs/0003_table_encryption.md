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

### Problem Statement

The Rust implementation currently has no encryption support. Users reading encrypted Iceberg
tables created by Java/Spark cannot do so from `iceberg-rust`. Writing encrypted tables is
likewise impossible.

Additionally, the current `InputFile` type is a concrete struct tightly coupled to `opendal::Operator`.
This prevents cleanly representing encrypted input files -- in the Java implementation, `InputFile`
is an interface with encrypted variants (`EncryptedInputFile`, `NativeEncryptionInputFile`).

### Relationship to Storage Trait RFC

[RFC 0002 (Making Storage a Trait)](https://github.com/apache/iceberg-rust/pull/2116) proposes
converting `Storage` from an enum to a trait and removing the `Extensions` mechanism from
`FileIOBuilder`. This encryption RFC is designed to work both with the current `Extensions`-based
`FileIO` and with the future trait-based storage. Specific adaptation points are called out below.

---

## High-Level Architecture

The encryption system follows the same envelope encryption pattern as the Java implementation,
adapted to Rust's ownership and async model.

### Component Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              User / Table Scan                              │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                            EncryptionManager                                 │
│                                                                              │
│  - Orchestrates key unwrapping, caching, and encryptor creation              │
│  - Holds Arc<dyn KeyManagementClient> for KMS integration                    │
│  - Maintains KeyCache (LRU + TTL) to avoid redundant KMS calls               │
│  - Provides prepare_decryption() and bulk_prepare_decryption()               │
│  - Provides extract_aad_prefix() for Parquet native encryption               │
└─────────────────────────────────────────────────────────────────────────────┘
              │                              │
              ▼                              ▼
┌──────────────────────────┐   ┌──────────────────────────────────────────────┐
│   KeyManagementClient    │   │                 KeyCache                      │
│       (trait)            │   │                                               │
│                          │   │  - LRU cache with configurable TTL            │
│  wrap_key(dek, kek_id)   │   │  - Thread-safe         │
│  unwrap_key(wrapped_dek) │   │  - Caches Arc<AesGcmEncryptor> per metadata   │
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

### Decryption Data Flow

```
TableMetadata
  └── encryption_keys: {key_id → EncryptedKey(key_metadata bytes)}
          │
Snapshot  │
  └── encryption_key_id ──────┘
          │
          ▼
  load_manifest_list(file_io, table_metadata)
    1. Look up encryption_key_id in table_metadata.encryption_keys
    2. If found: file_io.new_encrypted_input(path, key_metadata) giving a new encrypted InputFile
    3. If not: file_io.new_input(path)
          │
          ▼
ManifestFile
  └── key_metadata: Option<Vec<u8>>
          │
  load_manifest(file_io)
    1. If key_metadata present: file_io.new_encrypted_input(path, key_metadata) giving a new encrypted InputFile
    2. If not: file_io.new_input(path)
          │
          ▼
FileScanTask
  └── key_metadata: Option<Vec<u8>>
          │
  ArrowReader::create_parquet_record_batch_stream_builder()
    1. If key_metadata present:
       * Parquet-native encrypted → FileDecryptionProperties with IcebergKeyRetriever
    2. If not: standard Parquet read
```

### Crate Structure

All encryption code lives in a new `encryption` module within the `iceberg` crate, gated
behind an `encryption` feature flag:

```
crates/iceberg/src/
├── encryption/
│   ├── mod.rs                  # Module re-exports
│   ├── crypto.rs               # AES-GCM primitives (SecureKey, AesGcmEncryptor)
│   ├── cache.rs                # KeyCache (LRU + TTL)
│   ├── key_management.rs       # KeyManagementClient trait + InMemoryKms
│   ├── key_metadata.rs         # EncryptionKeyMetadata trait + StandardKeyMetadata
│   ├── manager.rs              # EncryptionManager (orchestrator)
│   ├── parquet_key_retriever.rs  # Bridge to parquet-rs KeyRetriever
│   └── stream.rs               # AesGcmFileRead (AGS1 stream decryption)
├── io/
│   └── file_io.rs              # InputFile enum + EncryptedInputFile variant
└── arrow/
    └── reader.rs               # Parquet decryption integration
```

---

## Design

### Core Cryptographic Primitives

#### EncryptionAlgorithm

```rust
pub enum EncryptionAlgorithm {
    Aes128Gcm,
    // Future: Aes256Gcm
}

impl EncryptionAlgorithm {
    pub fn key_length(&self) -> usize;   // 16 for AES-128
    pub fn nonce_length(&self) -> usize; // 12 (96-bit)
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

Performs AES-GCM encrypt/decrypt operations. Ciphertext format matches the Java implementation:
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
pub trait KeyManagementClient: Send + Sync {
    /// Wraps a DEK using the master key identified by `master_key_id`.
    async fn wrap_key(&self, dek: &[u8], master_key_id: &str) -> Result<Vec<u8>>;

    /// Unwraps a previously wrapped DEK.
    async fn unwrap_key(&self, wrapped_dek: &[u8]) -> Result<Vec<u8>>;
}
```

Users implement this trait to integrate with their KMS of choice (AWS KMS, Azure Key Vault,
GCP KMS, HashiCorp Vault, etc.). An `InMemoryKms` is provided for testing.

#### StandardKeyMetadata

Avro-serialized metadata stored alongside encrypted files. Compatible with the Java
`StandardKeyMetadata` format for cross-language interoperability:

```rust
pub struct StandardKeyMetadata {
    encryption_key: Vec<u8>,  // Wrapped DEK
    aad_prefix: Vec<u8>,      // Additional authenticated data prefix
    file_length: Option<i64>, // Optional encrypted file length
}

impl StandardKeyMetadata {
    pub fn serialize(&self) -> Result<Vec<u8>>;
    pub fn deserialize(bytes: &[u8]) -> Result<Self>;
}
```

#### KeyCache

Thread-safe LRU cache with TTL to avoid redundant KMS round-trips:

```rust
pub struct KeyCache { /* ... */ }

impl KeyCache {
    pub fn new(capacity: usize, ttl: Duration) -> Self;
    pub async fn get(&self, key_metadata: &[u8]) -> Option<Arc<AesGcmEncryptor>>;
    pub async fn insert(&self, key_metadata: &[u8], encryptor: Arc<AesGcmEncryptor>);
    pub async fn evict_expired(&self);
}
```

### EncryptionManager

Central orchestrator that ties together KMS, caching, and encryptor creation:

```rust
pub struct EncryptionManager {
    kms_client: Arc<dyn KeyManagementClient>,
    algorithm: EncryptionAlgorithm,
    key_cache: Arc<KeyCache>,
}

impl EncryptionManager {
    pub fn new(
        kms_client: Arc<dyn KeyManagementClient>,
        algorithm: EncryptionAlgorithm,
        cache_ttl: Duration,
    ) -> Self;

    pub fn with_defaults(kms_client: Arc<dyn KeyManagementClient>) -> Self;

    /// Unwraps a DEK from key metadata and returns a cached encryptor.
    pub async fn prepare_decryption(
        &self,
        key_metadata: &[u8],
    ) -> Result<Arc<AesGcmEncryptor>>;

    /// Batch preparation for multiple files (parallel KMS calls).
    pub async fn bulk_prepare_decryption(
        &self,
        key_metadata_list: Vec<Vec<u8>>,
    ) -> Result<Vec<Arc<AesGcmEncryptor>>>;

    /// Extracts the AAD prefix from key metadata for Parquet native encryption.
    pub fn extract_aad_prefix(&self, key_metadata: &[u8]) -> Result<Vec<u8>>;
}
```

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
│   Nonce (12 bytes)                       │
│   Ciphertext (remaining bytes)           │
│   GCM Tag (16 bytes)                     │
└──────────────────────────────────────────┘

Cipher block size = plain_block_size + 12 (nonce) + 16 (tag) = 1,048,604
```

Each block's AAD is constructed as: `aad_prefix || block_index (4 bytes, little-endian)`.
This binds each block to its position in the stream, preventing block reordering attacks.

#### AesGcmFileRead

Implements the `FileRead` trait to provide transparent decryption of AGS1-encrypted files.
Supports random-access reads with an internal block cache (LRU, default 16 blocks):

```rust
pub struct AesGcmFileRead { /* ... */ }

impl AesGcmFileRead {
    pub async fn new(
        inner: Box<dyn FileRead>,
        encryptor: Arc<AesGcmEncryptor>,
        key_metadata: &StandardKeyMetadata,
        file_length: u64,
    ) -> Result<Self>;

    pub async fn calculate_plaintext_length_from_file(
        reader: &impl FileRead,
        file_length: u64,
    ) -> Result<u64>;
}

#[async_trait]
impl FileRead for AesGcmFileRead {
    async fn read(&self, range: Range<u64>) -> Result<Bytes>;
}
```

### InputFile: From Struct to Enum

**This is a key design change.** The current `InputFile` is a concrete struct. In the Java
implementation, `InputFile` is an interface with multiple implementations including encrypted
variants. We propose converting `InputFile` to an enum to support encrypted files without
requiring a separate type at every call site:

```rust
pub enum InputFile {
    /// Standard unencrypted input file.
    Plain {
        op: Operator,
        path: String,
        relative_path_pos: usize,
    },

    /// AGS1 stream-encrypted input file.
    /// The file is decrypted transparently on read.
    Encrypted {
        op: Operator,
        path: String,
        relative_path_pos: usize,
        encryptor: Arc<AesGcmEncryptor>,
        key_metadata: StandardKeyMetadata,
    },

    /// Parquet-native encrypted input file.
    /// Decryption is handled by the Parquet reader using FileDecryptionProperties.
    /// The InputFile itself reads raw (encrypted) bytes.
    NativeEncrypted {
        op: Operator,
        path: String,
        relative_path_pos: usize,
        key_metadata: Vec<u8>,
    },
}
```

This mirrors the Java hierarchy:

| Java                          | Rust                          |
|-------------------------------|-------------------------------|
| `InputFile` (interface)       | `InputFile` (enum)            |
| Regular `InputFile` impl      | `InputFile::Plain`            |
| `EncryptedInputFile` wrapper  | `InputFile::Encrypted`        |
| `NativeEncryptionInputFile`   | `InputFile::NativeEncrypted`  |

Common operations delegate to the appropriate variant:

```rust
impl InputFile {
    pub fn location(&self) -> &str;
    pub async fn exists(&self) -> Result<bool>;
    pub async fn metadata(&self) -> Result<FileMetadata>;
    pub async fn read(&self) -> Result<Bytes>;
    pub async fn reader(&self) -> Result<Box<dyn FileRead>>;
}
```

For the `Encrypted` variant, `read()` and `reader()` transparently decrypt via `AesGcmFileRead`.
For the `NativeEncrypted` variant, `read()` and `reader()` return raw bytes -- the Parquet
reader handles decryption using `FileDecryptionProperties`.

#### Adaptation for Storage Trait RFC

Once RFC 0002 merges, `InputFile` will hold `Arc<dyn Storage>` instead of `Operator`. The enum
structure remains the same -- only the inner storage handle type changes:

```rust
// After Storage Trait RFC merges:
pub enum InputFile {
    Plain {
        storage: Arc<dyn Storage>,
        path: String,
    },
    Encrypted {
        storage: Arc<dyn Storage>,
        path: String,
        encryptor: Arc<AesGcmEncryptor>,
        key_metadata: StandardKeyMetadata,
    },
    NativeEncrypted {
        storage: Arc<dyn Storage>,
        path: String,
        key_metadata: Vec<u8>,
    },
}
```

### FileIO Integration

#### Current Approach (with Extensions)

The `EncryptionManager` is injected into `FileIO` via the existing `Extensions` mechanism:

```rust
let encryption_manager = EncryptionManager::with_defaults(Arc::new(kms_client));

let file_io = FileIOBuilder::new("s3")
    .with_prop("s3.region", "us-east-1")
    .with_extension(encryption_manager)
    .build()?;

// Creates an encrypted InputFile
let input = file_io.new_input(path, key_metadata).await?;
let data = input.read().await?;
```

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

// Option B: Wrapping StorageFactory - I'm pretty sure this is more idomatic in the new trait world.
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

### Parquet Native Encryption Bridge

For files using Parquet Modular Encryption (where the Parquet file itself contains encrypted
column chunks), we bridge Iceberg's async key management with parquet-rs's synchronous
`KeyRetriever` trait:

```rust
pub struct IcebergKeyRetriever {
    encryption_manager: Arc<EncryptionManager>,
    runtime: tokio::runtime::Handle,
}

impl KeyRetriever for IcebergKeyRetriever {
    fn retrieve_key(&self, key_metadata: &[u8]) -> parquet::errors::Result<Vec<u8>> {
        // Bridge async → sync using the tokio runtime handle
        std::thread::scope(|s| {
            s.spawn(|| {
                self.runtime.block_on(async {
                    self.encryption_manager
                        .prepare_decryption(key_metadata)
                        .await
                })
            })
            .join()
        })
    }
}
```

The Arrow reader integrates this when `key_metadata` is present on a `FileScanTask`:

```rust
// In ArrowReader:
if let Some(key_metadata) = &task.key_metadata {
    let key_retriever = Arc::new(IcebergKeyRetriever::new(
        encryption_manager,
        runtime_handle,
    ));
    let decryption_properties = FileDecryptionProperties::with_key_retriever(
        key_retriever as Arc<dyn KeyRetriever>,
    )
    .build()?;
    builder = builder.with_file_decryption_properties(decryption_properties);
}
```

### Manifest & Snapshot Integration

#### ManifestFile

The `ManifestFile` struct gains an optional `key_metadata` field. When present,
`load_manifest()` uses encrypted I/O:

```rust
pub struct ManifestFile {
    // ... existing fields ...
    pub key_metadata: Option<Vec<u8>>,
}

impl ManifestFile {
    pub async fn load_manifest(&self, file_io: &FileIO) -> Result<Manifest> {
        let avro = match &self.key_metadata {
            Some(km) => {
                file_io
                    .new_encrypted_input(&self.manifest_path, km)
                    .await?
                    .read()
                    .await?
            }
            None => {
                file_io.new_input(&self.manifest_path)?.read().await?
            }
        };
        // Deserialize Avro manifest...
    }
}
```

#### Snapshot

Snapshots reference an `encryption_key_id` that maps to a key in `TableMetadata.encryption_keys`:

```rust
pub struct Snapshot {
    // ... existing fields ...
    pub encryption_key_id: Option<String>,
}

impl Snapshot {
    pub async fn load_manifest_list(
        &self,
        file_io: &FileIO,
        table_metadata: &TableMetadata,
    ) -> Result<ManifestList> {
        let bytes = match &self.encryption_key_id {
            Some(key_id) => {
                let encrypted_key = table_metadata
                    .encryption_keys
                    .get(key_id)
                    .ok_or_else(|| /* error */)?;
                file_io
                    .new_encrypted_input(&self.manifest_list, &encrypted_key.key_metadata)
                    .await?
                    .read()
                    .await?
            }
            None => file_io.new_input(&self.manifest_list)?.read().await?,
        };
        ManifestList::parse(bytes, /* ... */)
    }
}
```

#### FileScanTask

Propagates per-file encryption metadata through the scan pipeline:

```rust
pub struct FileScanTask {
    // ... existing fields ...
    pub key_metadata: Option<Vec<u8>>,
}
```

---

## Implementation Plan

### Phase 1: Core Encryption (Read Path)

- Cryptographic primitives: `EncryptionAlgorithm`, `SecureKey`, `AesGcmEncryptor`
- `KeyManagementClient` trait and `InMemoryKms`
- `StandardKeyMetadata` with Avro serialization (Java-compatible)
- `KeyCache` with LRU + TTL
- `EncryptionManager` with `prepare_decryption()` and `bulk_prepare_decryption()`
- `AesGcmFileRead` (AGS1 stream decryption implementing `FileRead`)
- `InputFile` enum conversion (`Plain`, `Encrypted`, `NativeEncrypted`)
- `FileIO::new_encrypted_input()` integration
- Manifest and snapshot decryption
- `FileScanTask.key_metadata` propagation
- `IcebergKeyRetriever` for Parquet native encryption
- Arrow reader integration with `FileDecryptionProperties`
- Feature-gated behind `encryption` feature flag
- Integration tests with `InMemoryKms`

### Phase 2: Write Path

- `OutputFile` enum conversion (mirroring `InputFile`)
- `AesGcmFileWrite` (AGS1 stream encryption implementing `FileWrite`)
- `EncryptionManager::prepare_encryption()` (generate DEK, wrap with KMS, create metadata)
- `FileIO::new_encrypted_output()` integration
- Parquet writer encryption support (`FileEncryptionProperties`)
- Encrypted manifest and manifest list writing
- Encrypted snapshot commit flow

### Phase 3: Production KMS Implementations

- AWS KMS `KeyManagementClient` implementation
- Azure Key Vault `KeyManagementClient` implementation
- GCP KMS `KeyManagementClient` implementation


### Phase 4: Storage Trait Adaptation

- Adapt to RFC 0002 when it merges:
  - Replace `Operator` with `Arc<dyn Storage>` in `InputFile`/`OutputFile` enum variants
  - Replace `Extensions`-based `EncryptionManager` injection with the new pattern
    (catalog-level or `EncryptingStorageFactory` wrapper)
  - Remove any `Extensions`-specific code

### Future Work

- Column-level encryption policies (encrypt specific columns with different keys)
- Key rotation support (re-encrypt DEKs with new KEKs without re-encrypting data)
- Encryption metadata in `TableMetadata` write path
- AES-256-GCM support (depends on apache/arrow-rs#9203)

---

## Compatibility

### Java Interoperability

Cross-language compatibility is a hard requirement:

- **AGS1 format**: Byte-level compatible with Java's `AesGcmInputStream`/`AesGcmOutputStream`
  (same header, block size, nonce/tag layout, AAD construction)
- **StandardKeyMetadata**: Avro-serialized with the same schema as Java, enabling Rust to
  read tables encrypted by Java/Spark and vice versa
- **Parquet native encryption**: Uses the same `KeyRetriever` interface from `parquet-rs`,
  which follows the Parquet spec

### Feature Flag

All encryption code is gated behind `--features encryption` to avoid adding cryptographic
dependencies for users who don't need encryption. The `aes-gcm` and `zeroize` crates are only compiled when enabled.

When the `encryption` feature is not enabled and an encrypted file is encountered, clear
error messages are returned indicating that the feature must be enabled.

---

## Risks and Mitigations

| Risk | Description | Mitigation |
| ---- | ----------- | ---------- |
| Storage trait churn | RFC 0002 may change `InputFile`/`FileIO` significantly | Design encryption module with clean boundaries; crypto/key management/stream code is independent of storage abstraction |
| Parquet async/sync bridge | `KeyRetriever` is sync but KMS calls are async | Use `std::thread::scope` + `runtime.block_on()` to bridge; document the requirement for a tokio runtime handle |
| Key metadata format drift | Java may evolve `StandardKeyMetadata` | Pin to Avro schema version; add schema version detection for forward compatibility |
| Performance: KMS latency | KMS round-trips add latency to file opens | `KeyCache` with TTL; `bulk_prepare_decryption()` for parallel unwrapping |
| `InputFile` enum breaking change | Converting from struct to enum breaks existing code | Not sure, I think we have to break this |

## Open Questions

1. **KMS crate structure**: Should KMS implementations live in `iceberg-encryption-{provider}`
   crates, or in the existing catalog crates (since AWS KMS is often used with Glue catalog)?
2. **Write path priority**: Should Phase 2 (write path) block on Phase 3 (storage trait
   adaptation), or proceed independently?

## Conclusion

This RFC introduces encryption support for `iceberg-rust`, following the Iceberg spec and
maintaining byte-level compatibility with the Java reference implementation. The design
separates concerns into pluggable components (KMS client, key cache, stream cipher, encryption
manager) and integrates with the existing read path through `FileIO`, manifest loading, and
the Arrow/Parquet reader. The `InputFile` type is evolved from a concrete struct to an enum
to cleanly represent encrypted file variants, mirroring Java's interface hierarchy. The
implementation is feature-gated and designed to adapt cleanly to the upcoming storage trait
refactoring from RFC 0002.
