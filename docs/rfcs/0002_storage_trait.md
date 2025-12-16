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

# Making Storage a Trait in Iceberg-rust

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

impl Storage {
    pub(crate) fn create_operator<'a>(&self, path: &'a impl AsRef<str>) 
        -> crate::Result<(Operator, &'a str)> {
        match self {
            #[cfg(feature = "storage-s3")]
            Storage::S3 { configured_scheme, config, customized_credential_load } => {
                // S3-specific operator creation
            }
            // ... other match arms
        }
    }
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

### New Trait-Based Design

The new design replaces the concrete `Storage` enum with a `Storage` trait:

```rust
// New: Trait-based abstraction (defined in iceberg crate)
#[async_trait]
pub trait Storage: Debug + Send + Sync {
    ...
}
```

This enables:
- **Pluggable backends** – Users can implement custom storage backends
- **Multiple libraries** – Support for OpenDAL, object_store, or custom implementations
- **Separate crates** – Storage implementations can live in separate crates

---

## Design (Phase 1): Storage Trait and Core Types

Phase 1 focuses on defining the `Storage` trait and updating `InputFile`/`OutputFile` to use trait-based storage.

### Storage Trait

The `Storage` trait is defined in the `iceberg` crate and defines the interface for all storage operations:

```rust
#[async_trait]
pub trait Storage: Debug + Send + Sync {
    /// Check if a file exists at the given path
    async fn exists(&self, path: &str) -> Result<bool>;
    
    /// Get metadata for a file
    async fn metadata(&self, path: &str) -> Result<FileMetadata>;

    /// Read entire file content
    async fn read(&self, path: &str) -> Result<Bytes>;
    
    /// Create a reader for streaming reads
    async fn reader(&self, path: &str) -> Result<Box<dyn FileRead>>;

    /// Write bytes to a file (overwrites if exists)
    async fn write(&self, path: &str, bs: Bytes) -> Result<()>;
    
    /// Create a writer for streaming writes
    async fn writer(&self, path: &str) -> Result<Box<dyn FileWrite>>;

    /// Delete a single file
    async fn delete(&self, path: &str) -> Result<()>;
    
    /// Delete all files under a prefix (directory)
    async fn delete_prefix(&self, path: &str) -> Result<()>;

    /// Create an InputFile handle for the given path
    fn new_input(&self, path: &str) -> Result<InputFile>;
    
    /// Create an OutputFile handle for the given path
    fn new_output(&self, path: &str) -> Result<OutputFile>;
}
```

Note:
- All paths are absolute paths with scheme (e.g., `s3://bucket/path`)

### InputFile and OutputFile Changes

`InputFile` and `OutputFile` change from holding an `Operator` directly to holding a reference to the `Storage` trait:

**Before (current implementation):**
```rust
pub struct InputFile {
    op: Operator,
    path: String,
    relative_path_pos: usize,
}

impl InputFile {
    pub async fn read(&self) -> crate::Result<Bytes> {
        Ok(self.op.read(&self.path[self.relative_path_pos..]).await?.to_bytes())
    }
}
```

**After (new design):**
```rust
pub struct InputFile {
    storage: Arc<dyn Storage>,
    path: String,
}

impl InputFile {
    pub fn location(&self) -> &str { 
        &self.path 
    }
    
    pub async fn exists(&self) -> Result<bool> {
        self.storage.exists(&self.path).await
    }

    pub async fn read(&self) -> Result<Bytes> {
        self.storage.read(&self.path).await
    }
    
    pub async fn metadata(&self) -> Result<FileMetadata> {
        self.storage.metadata(&self.path).await
    }
    
    pub async fn reader(&self) -> Result<Box<dyn FileRead>> {
        self.storage.reader(&self.path).await
    }
}
```

Similarly for `OutputFile`:

```rust
pub struct OutputFile {
    storage: Arc<dyn Storage>,
    path: String,
}

impl OutputFile {
    pub fn location(&self) -> &str { &self.path }
    
    pub async fn exists(&self) -> Result<bool> {
        self.storage.exists(&self.path).await
    }
    
    pub async fn write(&self, bs: Bytes) -> Result<()> {
        self.storage.write(&self.path, bs).await
    }
    
    pub async fn writer(&self) -> Result<Box<dyn FileWrite>> {
        self.storage.writer(&self.path).await
    }
    
    pub fn to_input_file(self) -> InputFile {
        InputFile { storage: self.storage, path: self.path }
    }
}
```

### FileIO Changes

`FileIO` wraps an `Arc<dyn Storage>` instead of `Arc<Storage>` enum:

```rust
#[derive(Clone, Debug)]
pub struct FileIO {
    inner: Arc<dyn Storage>,
}

impl FileIO {
    /// Create a new FileIO with the given storage implementation
    pub fn new(storage: Arc<dyn Storage>) -> Self {
        Self { inner: storage }
    }

    pub async fn delete(&self, path: impl AsRef<str>) -> Result<()> {
        self.inner.delete(path.as_ref()).await
    }

    pub async fn exists(&self, path: impl AsRef<str>) -> Result<bool> {
        self.inner.exists(path.as_ref()).await
    }

    pub fn new_input(&self, path: impl AsRef<str>) -> Result<InputFile> {
        self.inner.new_input(path.as_ref())
    }

    pub fn new_output(&self, path: impl AsRef<str>) -> Result<OutputFile> {
        self.inner.new_output(path.as_ref())
    }
}
```

---

## Design (Phase 2): Storage Architecture Options

Phase 2 addresses how storage implementations are organized and how users interact with them. There are two architectural options under consideration.

### Crate Structure (Common to Both Options)

Both options use the same crate structure. The `Storage` trait remains in the `iceberg` crate (as it's an API/interface), while concrete implementations move to `iceberg-storage`:

```
crates/
├── iceberg/                    # Core Iceberg functionality (APIs/interfaces)
│   └── src/
│       └── io/
│           ├── mod.rs
│           ├── storage.rs      # Storage trait definition (stays here)
│           └── file_io.rs      # FileIO, InputFile, OutputFile
│
└── iceberg-storage/            # Concrete storage implementations
    └── src/
        ├── lib.rs              # Re-exports
        └── opendal/            # OpenDAL-based implementations
            └── ...
```

The `iceberg-storage` crate depends on the `iceberg` crate (for `Storage` trait, `Result`, `Error`, etc.).

---

### Option 1: Unified Storage (Multi-Scheme)

In this option, a single `Storage` implementation (e.g., `OpenDalStorage`) handles multiple URL schemes internally. There is no need for a registry since scheme routing happens within the storage implementation itself.

#### OpenDalStorage Implementation

```rust
/// Unified OpenDAL-based storage implementation.
///
/// This storage handles all supported schemes (S3, GCS, Azure, filesystem, memory)
/// through OpenDAL, creating operators on-demand based on the path scheme.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OpenDalStorage {
    /// In-memory storage, useful for testing
    #[cfg(feature = "storage-memory")]
    Memory(#[serde(skip, default = "default_memory_op")] Operator),
    
    /// Local filesystem storage
    #[cfg(feature = "storage-fs")]
    LocalFs,
    
    /// Amazon S3 storage
    /// Expects paths of the form `s3[a]://<bucket>/<path>`.
    #[cfg(feature = "storage-s3")]
    S3 {
        configured_scheme: String,
        config: Arc<S3Config>,
        #[serde(skip)]
        customized_credential_load: Option<CustomAwsCredentialLoader>,
    },
    
    /// Google Cloud Storage
    #[cfg(feature = "storage-gcs")]
    Gcs { config: Arc<GcsConfig> },
    
    /// Alibaba Cloud OSS
    #[cfg(feature = "storage-oss")]
    Oss { config: Arc<OssConfig> },
    
    /// Azure Data Lake Storage
    #[cfg(feature = "storage-azdls")]
    Azdls {
        configured_scheme: AzureStorageScheme,
        config: Arc<AzdlsConfig>,
    },
}

impl OpenDalStorage {
    /// Build storage from FileIOBuilder
    pub fn build(file_io_builder: FileIOBuilder) -> Result<Self> {
        let (scheme_str, props, extensions) = file_io_builder.into_parts();
        let scheme = Self::parse_scheme(&scheme_str)?;
        
        match scheme {
            #[cfg(feature = "storage-memory")]
            Scheme::Memory => Ok(Self::Memory(memory_config_build()?)),
            #[cfg(feature = "storage-fs")]
            Scheme::Fs => Ok(Self::LocalFs),
            #[cfg(feature = "storage-s3")]
            Scheme::S3 => Ok(Self::S3 {
                configured_scheme: scheme_str,
                config: s3_config_parse(props)?.into(),
                customized_credential_load: extensions
                    .get::<CustomAwsCredentialLoader>()
                    .map(Arc::unwrap_or_clone),
            }),
            // ... other schemes
        }
    }
    
    fn create_operator<'a>(&self, path: &'a str) -> Result<(Operator, &'a str)> {
        match self {
            #[cfg(feature = "storage-memory")]
            Self::Memory(op) => { /* ... */ }
            #[cfg(feature = "storage-fs")]
            Self::LocalFs => { /* ... */ }
            #[cfg(feature = "storage-s3")]
            Self::S3 { configured_scheme, config, customized_credential_load } => { /* ... */ }
            // ... other variants
        }
    }
}

#[async_trait]
impl Storage for OpenDalStorage {
    async fn exists(&self, path: &str) -> Result<bool> {
        let (op, rel_path) = self.create_operator(path)?;
        Ok(op.exists(rel_path).await?)
    }
    
    async fn read(&self, path: &str) -> Result<Bytes> {
        let (op, rel_path) = self.create_operator(path)?;
        Ok(op.read(rel_path).await?.to_bytes())
    }
    
    // ... implement other Storage trait methods
}
```

#### StorageRouter Helper

For users who want to compose multiple storage implementations (e.g., use OpenDAL for S3 but a custom storage for a proprietary system), we provide a `StorageRouter` helper. Note: `StorageRouter` does NOT implement `Storage` trait; it's a helper for resolving paths within custom storage implementations.

```rust
/// Helper for routing paths to different storage implementations.
/// This is NOT a Storage implementation itself, but a utility for building custom storages.
#[derive(Debug, Clone)]
pub struct StorageRouter {
    routes: DashMap<String, Arc<dyn Storage>>,  // prefix -> storage
}

impl StorageRouter {
    pub fn new() -> Self {
        Self { routes: DashMap::new() }
    }
    
    /// Register a storage for a URL prefix (e.g., "s3://", "s3://my-bucket/", "gs://")
    pub fn register(&self, prefix: impl Into<String>, storage: Arc<dyn Storage>) {
        self.routes.insert(prefix.into(), storage);
    }
    
    /// Resolve which storage should handle the given path.
    /// Returns the storage registered with the longest matching prefix.
    pub fn resolve(&self, path: &str) -> Result<Arc<dyn Storage>> {
        let mut best_match: Option<(usize, Arc<dyn Storage>)> = None;
        
        for entry in self.routes.iter() {
            if path.starts_with(entry.key()) {
                let prefix_len = entry.key().len();
                if best_match.is_none() || prefix_len > best_match.as_ref().unwrap().0 {
                    best_match = Some((prefix_len, entry.value().clone()));
                }
            }
        }
        
        best_match
            .map(|(_, storage)| storage)
            .ok_or_else(|| Error::new(
                ErrorKind::FeatureUnsupported,
                format!("No storage configured for path: {}", path),
            ))
    }
}
```

#### Example: Custom Storage Using StorageRouter

```rust
use iceberg::io::{Storage, FileMetadata, FileRead, FileWrite, InputFile, OutputFile};
use iceberg::{Result, Error, ErrorKind};
use iceberg_storage::{StorageRouter, OpenDalStorage};
use async_trait::async_trait;
use bytes::Bytes;
use std::sync::Arc;

/// Custom storage that routes S3 to OpenDAL and proprietary URLs to custom backend
#[derive(Debug, Clone)]
pub struct MyCompanyStorage {
    router: StorageRouter,
}

impl MyCompanyStorage {
    pub fn new(s3_config: S3Config, prop_client: ProprietaryClient) -> Result<Self> {
        let router = StorageRouter::new();
        
        // Route S3 URLs to OpenDAL
        let opendal = Arc::new(OpenDalStorage::S3 {
            configured_scheme: "s3".to_string(),
            config: Arc::new(s3_config),
            customized_credential_load: None,
        });
        router.register("s3://", opendal.clone());
        router.register("s3a://", opendal);
        
        // Route proprietary URLs to custom storage
        let prop_storage = Arc::new(ProprietaryStorage::new(prop_client));
        router.register("prop://", prop_storage);
        
        Ok(Self { router })
    }
}

#[async_trait]
impl Storage for MyCompanyStorage {
    async fn exists(&self, path: &str) -> Result<bool> {
        self.router.resolve(path)?.exists(path).await
    }
    
    async fn read(&self, path: &str) -> Result<Bytes> {
        self.router.resolve(path)?.read(path).await
    }
    
    // ... delegate all methods to router.resolve(path)?
    
    fn new_input(&self, path: &str) -> Result<InputFile> {
        Ok(InputFile::new(Arc::new(self.clone()), path.to_string()))
    }
    
    fn new_output(&self, path: &str) -> Result<OutputFile> {
        Ok(OutputFile::new(Arc::new(self.clone()), path.to_string()))
    }
}

// Usage
fn create_file_io() -> Result<FileIO> {
    let storage = MyCompanyStorage::new(s3_config, prop_client)?;
    Ok(FileIO::new(Arc::new(storage)))
}
```

---

### Option 2: Scheme-Specific Storage with Registry

In this option, each `Storage` implementation handles only one scheme (or a small set of related schemes like `s3`/`s3a`). A `StorageRegistry` maps URL schemes/prefixes to their corresponding storage implementations.

#### Crate Structure for Option 2

```
crates/iceberg-storage/
└── src/
    ├── lib.rs
    ├── registry.rs         # StorageRegistry
    └── opendal/
        ├── mod.rs
        ├── s3.rs           # OpenDalS3Storage
        ├── gcs.rs          # OpenDalGcsStorage
        ├── fs.rs           # OpenDalFsStorage
        ├── memory.rs       # OpenDalMemoryStorage
        ├── oss.rs          # OpenDalOssStorage
        └── azdls.rs        # OpenDalAzdlsStorage
```

#### StorageRegistry

The registry maps URL prefixes to storage implementations using `DashMap` for thread-safe concurrent access. Unlike `StorageRouter` which is a helper for custom `Storage` implementations, `StorageRegistry` is designed to be passed directly to `Catalog` implementations.

```rust
/// Registry that maps URL prefixes to storage implementations.
/// Can be passed directly to Catalog implementations.
#[derive(Debug, Clone, Default)]
pub struct StorageRegistry {
    storages: DashMap<String, Arc<dyn Storage>>,
}

impl StorageRegistry {
    pub fn new() -> Self {
        Self::default()
    }
    
    /// Register a storage for a URL prefix (e.g., "s3://", "s3://my-bucket/", "gs://")
    pub fn register(&self, prefix: impl Into<String>, storage: Arc<dyn Storage>) {
        self.storages.insert(prefix.into(), storage);
    }
    
    /// Get storage for a path.
    /// Returns the storage registered with the longest matching prefix.
    pub fn get(&self, path: &str) -> Result<Arc<dyn Storage>> {
        let mut best_match: Option<(usize, Arc<dyn Storage>)> = None;
        
        for entry in self.storages.iter() {
            if path.starts_with(entry.key()) {
                let prefix_len = entry.key().len();
                if best_match.is_none() || prefix_len > best_match.as_ref().unwrap().0 {
                    best_match = Some((prefix_len, entry.value().clone()));
                }
            }
        }
        
        best_match
            .map(|(_, storage)| storage)
            .ok_or_else(|| Error::new(
                ErrorKind::FeatureUnsupported,
                format!("No storage registered for path '{}'. Registered prefixes: {:?}", 
                    path, self.list_prefixes()),
            ))
    }
    
    /// List all registered prefixes
    pub fn list_prefixes(&self) -> Vec<String> {
        self.storages.iter().map(|e| e.key().clone()).collect()
    }
}
```

#### Scheme-Specific Storage Implementation

```rust
/// S3-specific storage implementation using OpenDAL
#[derive(Debug, Clone)]
pub struct OpenDalS3Storage {
    config: Arc<S3Config>,
    credential_loader: Option<Arc<dyn CredentialLoader>>,
}

impl OpenDalS3Storage {
    pub fn new(props: HashMap<String, String>) -> Result<Self> {
        let config = parse_s3_config(&props)?;
        Ok(Self {
            config: Arc::new(config),
            credential_loader: None,
        })
    }
    
    pub fn with_credential_loader(mut self, loader: Arc<dyn CredentialLoader>) -> Self {
        self.credential_loader = Some(loader);
        self
    }
    
    fn create_operator(&self, path: &str) -> Result<(Operator, &str)> {
        // Validate scheme
        if !path.starts_with("s3://") && !path.starts_with("s3a://") {
            return Err(Error::new(ErrorKind::DataInvalid,
                format!("OpenDalS3Storage only handles s3:// URLs, got: {}", path)));
        }
        
        let bucket = extract_bucket(path)?;
        let mut builder = S3::from_config((*self.config).clone());
        builder.bucket(&bucket);
        
        let op = Operator::new(builder)?.layer(RetryLayer::new()).finish();
        let rel_path = extract_relative_path(path)?;
        Ok((op, rel_path))
    }
}

#[async_trait]
impl Storage for OpenDalS3Storage {
    async fn exists(&self, path: &str) -> Result<bool> {
        let (op, rel_path) = self.create_operator(path)?;
        Ok(op.exists(rel_path).await?)
    }
    
    async fn read(&self, path: &str) -> Result<Bytes> {
        let (op, rel_path) = self.create_operator(path)?;
        Ok(op.read(rel_path).await?.to_bytes())
    }
    
    // ... implement other Storage trait methods
    
    fn new_input(&self, path: &str) -> Result<InputFile> {
        if !path.starts_with("s3://") && !path.starts_with("s3a://") {
            return Err(Error::new(ErrorKind::DataInvalid,
                format!("Invalid S3 path: {}", path)));
        }
        Ok(InputFile::new(Arc::new(self.clone()), path.to_string()))
    }
    
    fn new_output(&self, path: &str) -> Result<OutputFile> {
        if !path.starts_with("s3://") && !path.starts_with("s3a://") {
            return Err(Error::new(ErrorKind::DataInvalid,
                format!("Invalid S3 path: {}", path)));
        }
        Ok(OutputFile::new(Arc::new(self.clone()), path.to_string()))
    }
}
```

#### Example: Using StorageRegistry with Catalog

```rust
use iceberg_storage::{StorageRegistry, OpenDalS3Storage, OpenDalGcsStorage, OpenDalFsStorage};
use std::sync::Arc;

fn create_catalog_with_registry() -> Result<GlueCatalog> {
    let registry = StorageRegistry::new();
    
    // Register scheme-specific storages
    let s3_storage = Arc::new(OpenDalS3Storage::new(s3_props)?);
    registry.register("s3://", s3_storage.clone());
    registry.register("s3a://", s3_storage);
    
    let gcs_storage = Arc::new(OpenDalGcsStorage::new(gcs_props)?);
    registry.register("gs://", gcs_storage.clone());
    registry.register("gcs://", gcs_storage);
    
    let fs_storage = Arc::new(OpenDalFsStorage::new()?);
    registry.register("file://", fs_storage);
    
    // Pass registry directly to catalog
    GlueCatalog::new(config, registry).await
}
```

#### Example: Per-Bucket Configuration

```rust
fn create_multi_account_catalog() -> Result<GlueCatalog> {
    let registry = StorageRegistry::new();
    
    // Different S3 configurations for different buckets
    let prod_s3 = Arc::new(OpenDalS3Storage::new(prod_s3_props)?);
    let staging_s3 = Arc::new(OpenDalS3Storage::new(staging_s3_props)?);
    
    // Register specific bucket prefixes (longest match wins)
    registry.register("s3://prod-bucket/", prod_s3.clone());
    registry.register("s3://staging-bucket/", staging_s3);
    
    // Fallback for other S3 URLs
    registry.register("s3://", prod_s3);
    
    // Pass registry directly to catalog
    GlueCatalog::new(config, registry).await
}
```

---

### Comparison of Options

| Aspect | Option 1: Unified Storage | Option 2: Scheme-Specific + Registry |
|--------|---------------------------|--------------------------------------|
| **Crate Structure** | Single `OpenDalStorage` enum | Multiple `OpenDalXxxStorage` structs |
| **Routing** | Internal (via `StorageRouter`) | External (via `StorageRegistry`) |
| **Custom Storage** | Implement all schemes in one type | Implement one scheme per type |
| **Composition** | Use `StorageRouter` helper within Storage impl | Pass `StorageRegistry` to Catalog |
| **Type Safety** | Less (runtime scheme checking) | More (compile-time per storage) |
| **Code Organization** | Centralized | Modular |

---

## Catalog API Changes

The `Catalog` trait currently does not expose storage directly. However, catalogs internally use `FileIO` to read/write table metadata. With the storage trait abstraction, catalogs will need to be updated to work with the new `FileIO` that wraps `Arc<dyn Storage>`.

### Current Catalog Usage

```rust
// Current: Catalog implementations create FileIO internally
impl GlueCatalog {
    pub async fn new(config: GlueCatalogConfig) -> Result<Self> {
        let file_io = FileIOBuilder::new(&config.warehouse)
            .with_props(config.props.clone())
            .build()?;
        // ...
    }
}
```

### Updated Catalog Usage

The catalog API changes differ between the two architecture options:

#### For Option 1 (Unified Storage)

Catalogs accept `Arc<dyn Storage>` directly:

```rust
// Catalog accepts Storage directly
impl GlueCatalog {
    pub async fn new(config: GlueCatalogConfig, storage: Arc<dyn Storage>) -> Result<Self> {
        let file_io = FileIO::new(storage);
        // ...
    }
}

// Usage
let storage = Arc::new(OpenDalStorage::build(builder)?);
let catalog = GlueCatalog::new(config, storage).await?;
```

#### For Option 2 (Scheme-Specific + Registry)

Catalogs accept `StorageRegistry` directly, which allows the catalog to resolve the appropriate storage for each table location:

```rust
// Catalog accepts StorageRegistry
impl GlueCatalog {
    pub async fn new(config: GlueCatalogConfig, registry: StorageRegistry) -> Result<Self> {
        // Registry is stored and used to resolve storage for each table location
        // ...
    }
    
    pub async fn load_table(&self, table: &TableIdent) -> Result<Table> {
        let metadata_location = self.get_metadata_location(table)?;
        // Use registry to get the right storage for this table's location
        let storage = self.registry.get(&metadata_location)?;
        let file_io = FileIO::new(storage);
        // ...
    }
}

// Usage
let registry = StorageRegistry::new();
registry.register("s3://", Arc::new(OpenDalS3Storage::new(s3_props)?));
registry.register("gs://", Arc::new(OpenDalGcsStorage::new(gcs_props)?));
let catalog = GlueCatalog::new(config, registry).await?;
```

### CatalogBuilder Changes

The `CatalogBuilder` trait may need to accept storage configuration:

```rust
pub trait CatalogBuilder: Default + Debug + Send + Sync {
    type C: Catalog;
    
    /// Build catalog with default storage (from props)
    fn build(self, name: impl Into<String>, props: HashMap<String, String>) 
        -> impl Future<Output = Result<Self::C>> + Send;
    
    /// Build catalog with custom storage (Option 1)
    fn build_with_storage(
        self, 
        name: impl Into<String>, 
        props: HashMap<String, String>,
        storage: Arc<dyn Storage>,
    ) -> impl Future<Output = Result<Self::C>> + Send;
    
    /// Build catalog with storage registry (Option 2)
    fn build_with_registry(
        self, 
        name: impl Into<String>, 
        props: HashMap<String, String>,
        registry: StorageRegistry,
    ) -> impl Future<Output = Result<Self::C>> + Send;
}
```

This allows users to:
1. Use default storage behavior (backward compatible)
2. Inject custom storage implementations for testing or custom backends

---

## Open Questions

1. **Which architecture option should we adopt?** 
   - Option 1 (Unified Storage) is simpler and closer to current implementation
   - Option 2 (Scheme-Specific + Registry) is more modular and type-safe

2. **How should catalogs receive storage?**
   - Accept `Arc<dyn Storage>` directly?
   - Accept `FileIO`?
   - Accept `StorageRegistry` (Option 2)?
   - Keep current `FileIOBuilder` approach for backward compatibility?

3. **Should `StorageRouter`/`StorageRegistry` support prefix-based routing?**
   - e.g., different storage for `s3://bucket-a/` vs `s3://bucket-b/`
   - Current design supports this, but is it needed?

4. **How should credentials/extensions be passed?**
   - Current design uses `Extensions` type in `FileIOBuilder`
   - Should this be part of storage construction or a separate concern?

5. **Should we provide default implementations?**
   - e.g., `OpenDalStorage` that handles all schemes out of the box
   - For users who don't need customization

---

## Other Considerations

### Storage Error Handling

After the storage trait is stabilized, we may want to introduce more specific error handling for storage operations. Currently, storage errors are wrapped in the general `Error` type with `ErrorKind`. A future enhancement could add storage-specific error kinds to better handle different failure scenarios from various backends.

```rust
/// Storage-specific error kinds
pub enum IoErrorKind {
    /// File or object not found
    FileNotFound,
    /// Credentials expired or invalid
    CredentialExpired,
    /// Permission denied
    PermissionDenied,
    /// Network or connectivity error
    NetworkError,
    /// Storage service unavailable
    ServiceUnavailable,
    /// Rate limit exceeded
    RateLimitExceeded,
}

pub enum ErrorKind {
    // Existing variants...
    DataInvalid,
    FeatureUnsupported,
    // ...
    
    /// Storage I/O error with specific kind
    Io(IoErrorKind),
}
```

This would allow users to handle specific storage errors more precisely:

```rust
match result {
    Err(e) if matches!(e.kind(), ErrorKind::Io(IoErrorKind::FileNotFound)) => {
        // Handle missing file
    }
    Err(e) if matches!(e.kind(), ErrorKind::Io(IoErrorKind::CredentialExpired)) => {
        // Refresh credentials and retry
    }
    Err(e) => {
        // Handle other errors
    }
    Ok(data) => { /* ... */ }
}
```

---

## Implementation Plan

### Phase 1: Storage Trait (Current Branch)
- Define `Storage` trait in `iceberg` crate
- Update `InputFile`/`OutputFile` to use `Arc<dyn Storage>`
- Update `FileIO` to wrap `Arc<dyn Storage>`
- Implement `Storage` for existing `Storage` enum (backward compatibility)

### Phase 2: Concrete Implementations
- Create `iceberg-storage` crate
- Move/implement `OpenDalStorage` (Option 1) or scheme-specific storages (Option 2)
- Update catalog implementations to accept storage
- Consider introducing `IoErrorKind` for storage-specific error handling

### Phase 3: Additional Backends
- Add `object_store`-based implementations
- Document custom storage implementation guide
