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

# Making Storage a Trait

## Background

### Existing Implementation

The existing code implements storage functionality through a concrete `Storage` enum that handles different storage backends (S3, local filesystem, GCS, etc.). This implementation is tightly coupled with OpenDAL as the underlying storage layer. The `FileIO` struct wraps this `Storage` enum and provides a high-level API for file operations.

```rust
// Current: Concrete enum with variants for each backend
pub(crate) enum Storage {
    #[cfg(feature = "storage-memory")]
    Memory(Operator),
    #[cfg(feature = "storage-fs")]
    LocalFs,
    #[cfg(feature = "storage-s3")]
    S3 {
        configured_scheme: String,
        config: Arc<S3Config>,
        customized_credential_load: Option<CustomAwsCredentialLoader>,
    },
    #[cfg(feature = "storage-gcs")]
    Gcs { config: Arc<GcsConfig> },
    // ... other variants
}
```

Current structure:

- **FileIO:** Main interface for file operations, wraps `Arc<Storage>`
- **Storage:** Enum with variants for different storage backends
- **InputFile / OutputFile:** Concrete structs that hold an `Operator` and path

### Problem Statement

The original design has several limitations:

- **Tight Coupling** – All storage logic depends on OpenDAL, limiting flexibility. Users cannot easily opt in for other storage implementations like `object_store`
- **Customization Barriers** – Users cannot easily add custom behaviors or optimizations
- **No Extensibility** – Adding new backends requires modifying the core enum in the `iceberg` crate

As discussed in Issue #1314, making Storage a trait would allow pluggable storage and better integration with existing systems.

---

## High-Level Architecture

The new design introduces a trait-based storage abstraction with a factory pattern for creating storage instances. This enables pluggable storage backends while maintaining a clean separation between the core Iceberg library and storage implementations.

### Component Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              User Application                                │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                                 Catalog                                      │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  CatalogBuilder::with_file_io(file_io)                              │    │
│  │  - Accepts optional FileIO injection                                │    │
│  │  - Falls back to default_storage_factory() if not provided          │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                                 FileIO                                       │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  - config: StorageConfig (scheme + properties)                      │    │
│  │  - factory: Arc<dyn StorageFactory>                                 │    │
│  │  - storage: OnceCell<Arc<dyn Storage>> (lazy initialization)        │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
│  Methods: new_input(), new_output(), delete(), exists(), delete_prefix()    │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                    ┌─────────────────┴─────────────────┐
                    ▼                                   ▼
┌───────────────────────────────┐     ┌───────────────────────────────────────┐
│        StorageFactory         │     │              Storage                   │
│  (trait in iceberg crate)     │     │        (trait in iceberg crate)        │
│                               │     │                                        │
│  fn build(&self, config)      │────▶│  async fn exists(&self, path)          │
│     -> Arc<dyn Storage>       │     │  async fn read(&self, path)            │
│                               │     │  async fn write(&self, path, bytes)    │
│                               │     │  async fn delete(&self, path)          │
│                               │     │  fn new_input(&self, path)             │
│                               │     │  fn new_output(&self, path)            │
└───────────────────────────────┘     └───────────────────────────────────────┘
            │                                           ▲
            │                                           │
            ▼                                           │
┌─────────────────────────────────────────────────────────────────────────────┐
│                        Storage Implementations                               │
│                                                                              │
│  ┌─────────────────────┐  ┌─────────────────────┐  ┌─────────────────────┐  │
│  │   MemoryStorage     │  │   LocalFsStorage    │  │   OpenDalStorage    │  │
│  │   (iceberg crate)   │  │   (iceberg crate)   │  │ (iceberg-storage-   │  │
│  │                     │  │                     │  │      opendal)       │  │
│  │  - In-memory HashMap│  │  - std::fs ops      │  │  - S3, GCS, Azure   │  │
│  │  - For testing      │  │  - For local files  │  │  - OSS, filesystem  │  │
│  └─────────────────────┘  └─────────────────────┘  └─────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Data Flow

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                           FileIO Creation Flow                                │
└──────────────────────────────────────────────────────────────────────────────┘

  User Code                    FileIO                     StorageFactory
      │                          │                              │
      │  FileIO::from_path()     │                              │
      │─────────────────────────▶│                              │
      │                          │                              │
      │  .with_storage_factory() │                              │
      │─────────────────────────▶│                              │
      │                          │                              │
      │  .with_props()           │                              │
      │─────────────────────────▶│                              │
      │                          │                              │
      │  new_input(path)         │                              │
      │─────────────────────────▶│                              │
      │                          │  (lazy) factory.build()      │
      │                          │─────────────────────────────▶│
      │                          │                              │
      │                          │◀─────────────────────────────│
      │                          │  Arc<dyn Storage>            │
      │                          │                              │
      │◀─────────────────────────│                              │
      │  InputFile               │                              │


┌──────────────────────────────────────────────────────────────────────────────┐
│                        Catalog with FileIO Injection                          │
└──────────────────────────────────────────────────────────────────────────────┘

  User Code                  CatalogBuilder                  Catalog
      │                          │                              │
      │  ::default()             │                              │
      │─────────────────────────▶│                              │
      │                          │                              │
      │  .with_file_io(file_io)  │                              │
      │─────────────────────────▶│                              │
      │                          │                              │
      │  .load(name, props)      │                              │
      │─────────────────────────▶│                              │
      │                          │  new(config, Some(file_io))  │
      │                          │─────────────────────────────▶│
      │                          │                              │
      │◀─────────────────────────│◀─────────────────────────────│
      │  Catalog                 │                              │
```

### Crate Structure

```
crates/
├── iceberg/                         # Core Iceberg functionality
│   └── src/
│       └── io/
│           ├── mod.rs               # Re-exports
│           ├── storage.rs           # Storage + StorageFactory traits
│           ├── file_io.rs           # FileIO, InputFile, OutputFile
│           ├── config/              # StorageConfig and backend configs
│           │   ├── mod.rs           # StorageConfig
│           │   ├── s3.rs            # S3Config constants
│           │   ├── gcs.rs           # GcsConfig constants
│           │   ├── oss.rs           # OssConfig constants
│           │   └── azdls.rs         # AzdlsConfig constants
│           ├── memory.rs            # MemoryStorage (built-in)
│           └── local_fs.rs          # LocalFsStorage (built-in)
│
├── storage/
│   ├── opendal/                     # OpenDAL-based implementations
│   │   └── src/
│   │       ├── lib.rs               # Re-exports
│   │       ├── storage.rs           # OpenDalStorage + OpenDalStorageFactory
│   │       ├── storage_s3.rs        # S3 support
│   │       ├── storage_gcs.rs       # GCS support
│   │       ├── storage_oss.rs       # OSS support
│   │       ├── storage_azdls.rs     # Azure support
│   │       └── storage_fs.rs        # Filesystem support
│   │
│   └── utils/                       # Storage utilities
│       └── src/
│           └── lib.rs               # ResolvingStorageFactory, default_storage_factory()
│
└── catalog/                         # Catalog implementations
    ├── rest/                        # Uses with_file_io injection
    ├── glue/                        # Uses with_file_io injection
    ├── hms/                         # Uses with_file_io injection
    ├── s3tables/                    # Uses with_file_io injection
    └── sql/                         # Uses with_file_io injection
```

---

## Design Phase 1: Storage Trait and Core Types

Phase 1 focuses on converting Storage from an enum to a trait, introducing `StorageFactory` and `StorageConfig`, and updating `FileIO`, `InputFile`, and `OutputFile` to use the trait-based abstraction.

### Storage Trait

The `Storage` trait is defined in the `iceberg` crate and provides the interface for all storage operations. It uses `typetag` for serialization support across process boundaries.

```rust
#[async_trait]
#[typetag::serde(tag = "type")]
pub trait Storage: Debug + Send + Sync {
    /// Check if a file exists at the given path
    async fn exists(&self, path: &str) -> Result<bool>;

    /// Get metadata from an input path
    async fn metadata(&self, path: &str) -> Result<FileMetadata>;

    /// Read bytes from a path
    async fn read(&self, path: &str) -> Result<Bytes>;

    /// Get FileRead from a path
    async fn reader(&self, path: &str) -> Result<Box<dyn FileRead>>;

    /// Write bytes to an output path
    async fn write(&self, path: &str, bs: Bytes) -> Result<()>;

    /// Get FileWrite from a path
    async fn writer(&self, path: &str) -> Result<Box<dyn FileWrite>>;

    /// Delete a file at the given path
    async fn delete(&self, path: &str) -> Result<()>;

    /// Delete all files with the given prefix
    async fn delete_prefix(&self, path: &str) -> Result<()>;

    /// Create a new input file for reading
    fn new_input(&self, path: &str) -> Result<InputFile>;

    /// Create a new output file for writing
    fn new_output(&self, path: &str) -> Result<OutputFile>;
}
```

### StorageFactory Trait

The `StorageFactory` trait creates `Storage` instances from configuration. This enables lazy initialization and custom storage injection.

```rust
#[typetag::serde(tag = "type")]
pub trait StorageFactory: Debug + Send + Sync {
    /// Build a new Storage instance from the given configuration.
    fn build(&self, config: &StorageConfig) -> Result<Arc<dyn Storage>>;
}
```

### StorageConfig

`StorageConfig` replaces `FileIOBuilder` and `Extensions`, providing a simpler configuration model:

```rust
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageConfig {
    /// URL scheme (e.g., "s3", "gs", "file", "memory")
    scheme: String,
    /// Configuration properties for the storage backend
    props: HashMap<String, String>,
}

impl StorageConfig {
    pub fn new(scheme: impl Into<String>, props: HashMap<String, String>) -> Self;
    pub fn from_path(path: impl AsRef<str>) -> Result<Self>;
    pub fn scheme(&self) -> &str;
    pub fn props(&self) -> &HashMap<String, String>;
    pub fn get(&self, key: &str) -> Option<&String>;
    pub fn with_prop(self, key: impl Into<String>, value: impl Into<String>) -> Self;
    pub fn with_props(self, props: impl IntoIterator<Item = (impl Into<String>, impl Into<String>)>) -> Self;
}
```

#### Backend-Specific Configuration Types

In addition to `StorageConfig`, we provide typed configuration structs for each storage backend.
These can be constructed from `StorageConfig` and provide a structured way to access backend-specific settings:

- `S3Config` - Amazon S3 configuration
- `GcsConfig` - Google Cloud Storage configuration
- `OssConfig` - Alibaba Cloud OSS configuration
- `AzdlsConfig` - Azure Data Lake Storage configuration

Example of `S3Config`:

```rust
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct S3Config {
    pub endpoint: Option<String>,
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
    pub session_token: Option<String>,
    pub region: Option<String>,
    pub role_arn: Option<String>,
    pub allow_anonymous: bool,
    // ... other S3-specific fields
}

// Can be constructed from StorageConfig
impl From<&StorageConfig> for S3Config {
    fn from(config: &StorageConfig) -> Self { /* ... */ }
}
```

These typed configs are used internally by storage implementations (e.g., `OpenDalStorage`) to
parse properties from `StorageConfig` into strongly-typed configuration.

### FileIO Changes

`FileIO` is redesigned to use lazy storage initialization with a factory pattern:

```rust
#[derive(Clone)]
pub struct FileIO {
    /// Storage configuration containing scheme and properties
    config: StorageConfig,
    /// Factory for creating storage instances
    factory: Arc<dyn StorageFactory>,
    /// Cached storage instance (lazily initialized)
    storage: Arc<OnceCell<Arc<dyn Storage>>>,
}

impl FileIO {
    /// Create a new FileIO with the given configuration.
    pub fn new(config: StorageConfig) -> Self;

    /// Create a new FileIO backed by in-memory storage.
    pub fn new_with_memory() -> Self;

    /// Create FileIO from a path, inferring the scheme.
    pub fn from_path(path: impl AsRef<str>) -> Result<Self>;

    /// Set a custom storage factory.
    pub fn with_storage_factory(self, factory: Arc<dyn StorageFactory>) -> Self;

    /// Add a configuration property.
    pub fn with_prop(self, key: impl Into<String>, value: impl Into<String>) -> Self;

    /// Add multiple configuration properties.
    pub fn with_props(self, props: impl IntoIterator<Item = (impl Into<String>, impl Into<String>)>) -> Self;

    /// Get the storage configuration.
    pub fn config(&self) -> &StorageConfig;

    // File operations delegate to the lazily-initialized storage
    pub async fn delete(&self, path: impl AsRef<str>) -> Result<()>;
    pub async fn delete_prefix(&self, path: impl AsRef<str>) -> Result<()>;
    pub async fn exists(&self, path: impl AsRef<str>) -> Result<bool>;
    pub fn new_input(&self, path: impl AsRef<str>) -> Result<InputFile>;
    pub fn new_output(&self, path: impl AsRef<str>) -> Result<OutputFile>;
}
```

Key changes from the old design:
- Removed `FileIOBuilder` - configuration is now done via builder methods on `FileIO` itself
- Removed `Extensions` - custom behavior is now provided via `StorageFactory`
- Storage is lazily initialized on first use via `OnceCell`
- `DefaultStorageFactory` supports only "memory" and "file" schemes by default

### InputFile and OutputFile Changes

`InputFile` and `OutputFile` now hold a reference to `Arc<dyn Storage>` instead of an `Operator`:

```rust
pub struct InputFile {
    storage: Arc<dyn Storage>,
    path: String,
}

impl InputFile {
    pub fn new(storage: Arc<dyn Storage>, path: String) -> Self;
    pub fn location(&self) -> &str;
    pub async fn exists(&self) -> Result<bool>;
    pub async fn metadata(&self) -> Result<FileMetadata>;
    pub async fn read(&self) -> Result<Bytes>;
    pub async fn reader(&self) -> Result<Box<dyn FileRead>>;
}

pub struct OutputFile {
    storage: Arc<dyn Storage>,
    path: String,
}

impl OutputFile {
    pub fn new(storage: Arc<dyn Storage>, path: String) -> Self;
    pub fn location(&self) -> &str;
    pub async fn exists(&self) -> Result<bool>;
    pub async fn delete(&self) -> Result<()>;
    pub fn to_input_file(self) -> InputFile;
    pub async fn write(&self, bs: Bytes) -> Result<()>;
    pub async fn writer(&self) -> Result<Box<dyn FileWrite>>;
}
```

### Built-in Storage Implementations

The `iceberg` crate includes two built-in storage implementations for testing and basic use cases:

#### MemoryStorage

In-memory storage using a thread-safe `HashMap`, primarily for testing:

```rust
#[derive(Debug, Clone, Default)]
pub struct MemoryStorage {
    data: Arc<RwLock<HashMap<String, Bytes>>>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct MemoryStorageFactory;

#[typetag::serde]
impl StorageFactory for MemoryStorageFactory {
    fn build(&self, config: &StorageConfig) -> Result<Arc<dyn Storage>> {
        if config.scheme() != "memory" {
            return Err(/* error */);
        }
        Ok(Arc::new(MemoryStorage::new()))
    }
}
```

#### LocalFsStorage

Local filesystem storage using standard Rust `std::fs` operations:

```rust
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LocalFsStorage;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct LocalFsStorageFactory;

#[typetag::serde]
impl StorageFactory for LocalFsStorageFactory {
    fn build(&self, config: &StorageConfig) -> Result<Arc<dyn Storage>> {
        if config.scheme() != "file" {
            return Err(/* error */);
        }
        Ok(Arc::new(LocalFsStorage::new()))
    }
}
```

### CatalogBuilder Changes

The `CatalogBuilder` trait is extended with `with_file_io()` to allow FileIO injection:

```rust
pub trait CatalogBuilder: Default + Debug + Send + Sync {
    type C: Catalog;

    /// Set a custom FileIO to use for storage operations.
    fn with_file_io(self, file_io: FileIO) -> Self;

    /// Create a new catalog instance.
    fn load(
        self,
        name: impl Into<String>,
        props: HashMap<String, String>,
    ) -> impl Future<Output = Result<Self::C>> + Send;
}
```

Catalog implementations store the optional `FileIO` and use it when provided:

```rust
pub struct GlueCatalogBuilder {
    config: GlueCatalogConfig,
    file_io: Option<FileIO>,  // New field
}

impl CatalogBuilder for GlueCatalogBuilder {
    fn with_file_io(mut self, file_io: FileIO) -> Self {
        self.file_io = Some(file_io);
        self
    }

    // In load():
    // Use provided FileIO if Some, otherwise construct default
    let file_io = match file_io {
        Some(io) => io,
        None => FileIO::from_path(&config.warehouse)?
            .with_props(file_io_props)
            .with_storage_factory(default_storage_factory()),
    };
}
```

---

## Design Part 2: Separate Storage Crates

Phase 2 moves concrete OpenDAL-based implementations to a separate crate (`iceberg-storage-opendal`) and introduces `iceberg-storage-utils` as an adapter for catalog implementations.

### iceberg-storage-opendal Crate

This crate provides OpenDAL-based storage implementations for cloud storage backends:

```rust
// crates/storage/opendal/src/storage.rs

/// Unified OpenDAL-based storage implementation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OpenDalStorage {
    #[cfg(feature = "storage-fs")]
    LocalFs,

    #[cfg(feature = "storage-s3")]
    S3 {
        configured_scheme: String,
        config: Arc<S3Config>,
        #[serde(skip)]
        customized_credential_load: Option<CustomAwsCredentialLoader>,
    },

    #[cfg(feature = "storage-gcs")]
    Gcs { config: Arc<GcsConfig> },

    #[cfg(feature = "storage-oss")]
    Oss { config: Arc<OssConfig> },

    #[cfg(feature = "storage-azdls")]
    Azdls {
        configured_scheme: AzureStorageScheme,
        config: Arc<AzdlsConfig>,
    },
}

impl OpenDalStorage {
    /// Build storage from StorageConfig.
    pub fn build_from_config(config: &StorageConfig) -> Result<Self>;

    /// Creates operator from path.
    fn create_operator<'a>(&self, path: &'a str) -> Result<(Operator, &'a str)>;
}

#[async_trait]
#[typetag::serde]
impl Storage for OpenDalStorage {
    // Delegates all operations to the appropriate OpenDAL operator
}

/// Factory for creating OpenDalStorage instances.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct OpenDalStorageFactory;

impl OpenDalStorageFactory {
    /// Check if this factory supports the given scheme.
    pub fn supports_scheme(&self, scheme: &str) -> bool {
        matches!(scheme, "file" | "" | "s3" | "s3a" | "gs" | "gcs" | "oss" | "abfss" | "abfs" | "wasbs" | "wasb")
    }
}

#[typetag::serde]
impl StorageFactory for OpenDalStorageFactory {
    fn build(&self, config: &StorageConfig) -> Result<Arc<dyn Storage>> {
        let storage = OpenDalStorage::build_from_config(config)?;
        Ok(Arc::new(storage))
    }
}
```

Feature flags control which backends are available:
- `storage-fs`: Local filesystem
- `storage-s3`: Amazon S3
- `storage-gcs`: Google Cloud Storage
- `storage-oss`: Alibaba Cloud OSS
- `storage-azdls`: Azure Data Lake Storage

### iceberg-storage-utils Crate

This crate provides utilities for catalog implementations, including a resolving factory that delegates to the appropriate backend:

```rust
// crates/storage/utils/src/lib.rs

/// Returns the default storage factory based on enabled features.
pub fn default_storage_factory() -> Arc<dyn StorageFactory> {
    Arc::new(ResolvingStorageFactory::new())
}

/// A composite storage factory that delegates to the appropriate
/// backend based on the scheme.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ResolvingStorageFactory {
    #[cfg(feature = "opendal")]
    #[serde(skip)]
    opendal: OpenDalStorageFactory,
}

impl ResolvingStorageFactory {
    pub fn new() -> Self;

    /// Check if this factory supports the given scheme.
    pub fn supports_scheme(&self, scheme: &str) -> bool {
        #[cfg(feature = "opendal")]
        if self.opendal.supports_scheme(scheme) {
            return true;
        }
        scheme == "memory"
    }
}

#[typetag::serde]
impl StorageFactory for ResolvingStorageFactory {
    fn build(&self, config: &StorageConfig) -> Result<Arc<dyn Storage>> {
        let scheme = config.scheme();

        #[cfg(feature = "opendal")]
        {
            if self.opendal.supports_scheme(scheme) {
                return self.opendal.build(config);
            }
        }

        Err(Error::new(
            ErrorKind::FeatureUnsupported,
            format!("No storage backend available for scheme '{}'", scheme),
        ))
    }
}
```

### Catalog Integration

Catalog implementations depend on `iceberg-storage-utils` and use `default_storage_factory()` when no FileIO is injected:

```rust
// In catalog implementation (e.g., GlueCatalog)
use iceberg_storage_utils::default_storage_factory;

impl GlueCatalog {
    async fn new(config: GlueCatalogConfig, file_io: Option<FileIO>) -> Result<Self> {
        let file_io = match file_io {
            Some(io) => io,
            None => {
                // Build default FileIO with OpenDAL support
                FileIO::from_path(&config.warehouse)?
                    .with_props(file_io_props)
                    .with_storage_factory(default_storage_factory())
            }
        };
        // ...
    }
}
```

---

## Example Usage

### Basic Usage with Memory Storage (Testing)

```rust
use iceberg::io::FileIO;

// Create in-memory FileIO for testing
let file_io = FileIO::new_with_memory();

// Write and read files
let output = file_io.new_output("memory://test/file.txt")?;
output.write("Hello, World!".into()).await?;

let input = file_io.new_input("memory://test/file.txt")?;
let content = input.read().await?;
assert_eq!(content, bytes::Bytes::from("Hello, World!"));
```

### Using OpenDAL Storage Factory

```rust
use std::sync::Arc;
use iceberg::io::FileIO;
use iceberg_storage_opendal::OpenDalStorageFactory;

// Create FileIO with OpenDAL for S3
let file_io = FileIO::from_path("s3://my-bucket/warehouse")?
    .with_prop("s3.region", "us-east-1")
    .with_prop("s3.access-key-id", "my-access-key")
    .with_prop("s3.secret-access-key", "my-secret-key")
    .with_storage_factory(Arc::new(OpenDalStorageFactory));

// Use the FileIO
let input = file_io.new_input("s3://my-bucket/warehouse/table/metadata.json")?;
let metadata = input.read().await?;
```

### Using Catalogs with Default Storage

When using a catalog without injecting a custom `FileIO`, the catalog automatically uses
`default_storage_factory()` which includes all storage backends enabled by feature flags.

```rust
use std::collections::HashMap;
use iceberg::CatalogBuilder;
use iceberg_catalog_glue::GlueCatalogBuilder;

// No custom FileIO needed - catalog uses default_storage_factory() internally
// Storage backends are determined by enabled features (e.g., opendal)
let catalog = GlueCatalogBuilder::default()
    .load("my_catalog", HashMap::from([
        ("warehouse".to_string(), "s3://my-bucket/warehouse".to_string()),
        ("s3.region".to_string(), "us-east-1".to_string()),
    ]))
    .await?;

// Load and scan a table - storage is handled automatically
let table = catalog.load_table(&TableIdent::from_strs(["db", "my_table"])?).await?;
let scan = table.scan().build()?;
```

### Injecting Custom FileIO into Catalogs

For advanced use cases, you can inject a custom `FileIO` with specific storage configuration:

```rust
use std::collections::HashMap;
use std::sync::Arc;
use iceberg::CatalogBuilder;
use iceberg::io::FileIO;
use iceberg_catalog_glue::GlueCatalogBuilder;
use iceberg_storage_opendal::OpenDalStorageFactory;

// Create a custom FileIO with specific configuration
let file_io = FileIO::from_path("s3://my-bucket/warehouse")?
    .with_prop("s3.region", "us-east-1")
    .with_storage_factory(Arc::new(OpenDalStorageFactory));

// Inject FileIO into catalog
let catalog = GlueCatalogBuilder::default()
    .with_file_io(file_io)
    .load("my_catalog", HashMap::from([
        ("warehouse".to_string(), "s3://my-bucket/warehouse".to_string()),
    ]))
    .await?;
```

### Implementing Custom Storage

To implement a custom storage backend, implement the `Storage` trait with `#[typetag::serde]`:

```rust
use std::sync::Arc;
use async_trait::async_trait;
use iceberg::io::{Storage, StorageFactory, StorageConfig, InputFile, OutputFile};

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MyCustomStorage { /* fields */ }

#[async_trait]
#[typetag::serde]
impl Storage for MyCustomStorage {
    // Implement all required methods: exists, metadata, read, reader,
    // write, writer, delete, delete_prefix, new_input, new_output
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MyCustomStorageFactory;

#[typetag::serde]
impl StorageFactory for MyCustomStorageFactory {
    fn build(&self, config: &StorageConfig) -> iceberg::Result<Arc<dyn Storage>> {
        Ok(Arc::new(MyCustomStorage { /* ... */ }))
    }
}
```

### Routing to Multiple Storage Backends

To use different storage implementations for different schemes (e.g., a native S3 client
for S3 and OpenDAL for other schemes), implement routing at the `Storage` level:

```rust
use std::sync::Arc;
use async_trait::async_trait;
use iceberg::io::{Storage, StorageFactory, StorageConfig, InputFile, OutputFile};

/// A storage that routes to different backends based on path scheme
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RoutingStorage {
    s3_storage: NativeS3Storage,      // Custom S3 implementation
    opendal_storage: OpenDalStorage,  // OpenDAL for other schemes
}

#[async_trait]
#[typetag::serde]
impl Storage for RoutingStorage {
    async fn read(&self, path: &str) -> iceberg::Result<bytes::Bytes> {
        if path.starts_with("s3://") || path.starts_with("s3a://") {
            self.s3_storage.read(path).await
        } else {
            self.opendal_storage.read(path).await
        }
    }

    // Route other methods similarly...
}

/// Factory that creates RoutingStorage
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RoutingStorageFactory;

#[typetag::serde]
impl StorageFactory for RoutingStorageFactory {
    fn build(&self, config: &StorageConfig) -> iceberg::Result<Arc<dyn Storage>> {
        Ok(Arc::new(RoutingStorage {
            s3_storage: NativeS3Storage::new(config)?,
            opendal_storage: OpenDalStorage::build_from_config(config)?,
        }))
    }
}
```

---


## Implementation Plan

### Phase 1: Storage Trait
- Define `Storage` trait in `iceberg` crate
- Define `StorageFactory` trait in `iceberg` crate
- Introduce `StorageConfig` to replace `FileIOBuilder` configuration
- Update `FileIO` to use lazy storage initialization with factory pattern
- Update `InputFile`/`OutputFile` to use `Arc<dyn Storage>`
- Implement `MemoryStorage` and `LocalFsStorage` in `iceberg` crate
- Add `with_file_io()` to `CatalogBuilder` trait
- Update all catalog implementations to support FileIO injection

### Phase 2: Separate Storage Crates (Completed)
- Create `iceberg-storage-opendal` crate with `OpenDalStorage` and `OpenDalStorageFactory`
- Move S3, GCS, OSS, Azure implementations to `iceberg-storage-opendal`
- Create `iceberg-storage-utils` crate with `ResolvingStorageFactory` and `default_storage_factory()`
- Update catalog crates to depend on `iceberg-storage-utils`
- Remove storage feature flags from `iceberg` crate

### Future Work
- Add `object_store`-based storage implementations
- Consider introducing `IoErrorKind` for storage-specific error handling
