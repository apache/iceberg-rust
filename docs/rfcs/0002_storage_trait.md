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
The existing code implements storage functionality through a concrete Storage enum that handles different storage backends (S3, local filesystem, GCS, etc.). This implementation is tightly coupled with OpenDAL as the underlying storage layer. The FileIO struct wraps this Storage enum and provides a high-level API for file operations.

Original structure:

- **FileIO:** Main interface for file operations
- **Storage:** Enum with variants for different storage backends
- **InputFile / OutputFile:** Concrete structs for reading and writing files

All storage operations are implemented directly in these concrete types, making it hard to extend or customize the storage layer without modifying the core codebase.

### Problem Statement
The original design has several limitations:

- **Tight Coupling** – All storage logic depends on OpenDAL, limiting flexibility. Users cannot easily opt in for other storage implementations like `object_store`
- **Customization Barriers** – Users cannot easily add custom behaviors or optimizations

As discussed in Issue #1314, making Storage a trait would allow pluggable storage and better integration with existing systems.

## Design

### New Architecture

The new architecture uses trait-based abstractions with a registry pattern and separate storage crates:

```
┌─────────────────────────────────────────────────────┐
│              crates/iceberg/src/io/                 │
│  ┌───────────────────────────────────────────────┐  │
│  │         Storage Trait & Registry              │  │
│  │  - pub trait Storage                          │  │
│  │  - pub trait StorageFactory                   │  │
│  │  - pub struct StorageRegistry                 │  │
│  │  - pub struct InputFile                       │  │
│  │  - pub struct OutputFile                      │  │
│  └───────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────┘
                        ▲
                        │
        ┌───────────────┼───────────────┬───────────────┐
        │               │               │               │
┌───────┴────────────┐  │  ┌────────────┴──────┐  ┌────┴──────────┐
│ crates/storage/    │  │  │ crates/storage/   │  │  Third-Party  │
│    opendal/        │  │  │  object_store/    │  │    Crates     │
│ ┌────────────────┐ │  │  │ ┌───────────────┐ │  │ ┌───────────┐ │
│ │ opendal-s3     │ │  │  │ │ objstore-s3   │ │  │ │  custom   │ │
│ │ impl Storage   │ │  │  │ │ impl Storage  │ │  │ │  storage  │ │
│ │ impl Factory   │ │  │  │ │ impl Factory  │ │  │ │impl traits│ │
│ └────────────────┘ │  │  │ └───────────────┘ │  │ └───────────┘ │
│ ┌────────────────┐ │  │  │ ┌───────────────┐ │  └───────────────┘
│ │ opendal-fs     │ │  │  │ │ objstore-gcs  │ │
│ │ impl Storage   │ │  │  │ │ impl Storage  │ │
│ │ impl Factory   │ │  │  │ │ impl Factory  │ │
│ └────────────────┘ │  │  │ └───────────────┘ │
│ ┌────────────────┐ │  │  │ ┌───────────────┐ │
│ │ opendal-gcs    │ │  │  │ │ objstore-azure│ │
│ │ impl Storage   │ │  │  │ │ impl Storage  │ │
│ │ impl Factory   │ │  │  │ │ impl Factory  │ │
│ └────────────────┘ │  │  │ └───────────────┘ │
│ ... (oss, azure,   │  │  └───────────────────┘
│      memory)       │  │
└────────────────────┘  │
```

### Storage Trait
The Storage trait defines the interface for all storage operations. This implementation uses Option 2 from the initial design: Storage as a trait with concrete `InputFile` and `OutputFile` structs.

```rust
#[async_trait]
pub trait Storage: Debug + Send + Sync {
    // File existence and metadata
    async fn exists(&self, path: &str) -> Result<bool>;
    async fn metadata(&self, path: &str) -> Result<FileMetadata>;

    // Reading operations
    async fn read(&self, path: &str) -> Result<Bytes>;
    async fn reader(&self, path: &str) -> Result<Box<dyn FileRead>>;

    // Writing operations
    async fn write(&self, path: &str, bs: Bytes) -> Result<()>;
    async fn writer(&self, path: &str) -> Result<Box<dyn FileWrite>>;

    // Deletion operations
    async fn delete(&self, path: &str) -> Result<()>;
    async fn delete_prefix(&self, path: &str) -> Result<()>;

    // File object creation
    fn new_input(&self, path: &str) -> Result<InputFile>;
    fn new_output(&self, path: &str) -> Result<OutputFile>;
}
```

### InputFile and OutputFile Structs

`InputFile` and `OutputFile` are concrete structs that contain a reference to the storage:

```rust

pub struct InputFile {
    pub storage: Arc<dyn Storage>,
    pub path: String,
}

pub struct OutputFile {
    pub storage: Arc<dyn Storage>,
    pub path: String,
}
```

Functions in `InputFile` and `OutputFile` delegate operations to the underlying Storage:

```rust

impl InputFile {
    pub async fn exists(&self) -> Result<bool> {
        self.storage.exists(&self.path).await
    }

    pub async fn read(&self) -> Result<Bytes> {
        self.storage.read(&self.path).await
    }
    // ... other methods
}
```

Benefits of this approach:

- Simpler and easier to maintain
- Less trait object overhead
- Clear delegation pattern
- Sufficient flexibility for most use cases

## StorageFactory and Registry

### StorageFactory Trait
The `StorageFactory` trait defines how storage backends are constructed:

```rust
pub trait StorageFactory: Debug + Send + Sync {
    fn build(
        &self,
        props: HashMap<String, String>,
        extensions: Extensions,
    ) -> Result<Arc<dyn Storage>>;
}
```

Key design decisions:

- Uses `&self` instead of `self` - factories are reusable
- No associated type - returns `Arc<dyn Storage>` directly
- `Send + Sync` for thread safety

### StorageRegistry
The registry manages storage factories and provides lookup by scheme:

```rust
#[derive(Debug, Clone)]
pub struct StorageRegistry {
    factories: HashMap<String, Arc<dyn StorageFactory>>,
}

impl StorageRegistry {
    pub fn new() -> Self { /* ... */ }
    pub fn register(&mut self, scheme: impl Into<String>, factory: Arc<dyn StorageFactory>);
    pub fn get_factory(&self, scheme: &str) -> Result<Arc<dyn StorageFactory>>;
    pub fn supported_types(&self) -> impl Iterator<Item = &str>;
}
```

Features:

- Automatic registration based on enabled cargo features
- Runtime registration of custom builders
- Case-insensitive scheme lookup
- Thread-safe and cloneable

## Example Usage

```rust
use iceberg::io::FileIOBuilder;

// Basic usage (same as the existing code)
let file_io = FileIOBuilder::new("s3")
    .with_prop("s3.region", "us-west-2")
    .build()?;

// Custom storage registration
use iceberg::io::{StorageRegistry, StorageFactory};

let mut registry = StorageRegistry::new();
registry.register("custom", Arc::new(MyCustomStorageFactory));

// Check supported types
let supported: Vec<_> = registry.supported_types().collect();
println!("Supported: {:?}", supported);
```

## Storage Implementations
Each storage backend has its own implementation:

| Storage | File | Struct | Factory | Schemes |
|---------|------|--------|---------|---------|
| S3 | `storage_s3.rs` | `OpenDALS3Storage` | `OpenDALS3StorageFactory` | `s3`, `s3a` |
| GCS | `storage_gcs.rs` | `OpenDALGcsStorage` | `OpenDALGcsStorageFactory` | `gs`, `gcs` |
| OSS | `storage_oss.rs` | `OpenDALOssStorage` | `OpenDALOssStorageFactory` | `oss` |
| Azure | `storage_azdls.rs` | `OpenDALAzdlsStorage` | `OpenDALAzdlsStorageFactory` | `abfs`, `abfss`, `wasb`, `wasbs` |
| Filesystem | `storage_fs.rs` | `OpenDALFsStorage` | `OpenDALFsStorageFactory` | `file`, `""` |
| Memory | `storage_memory.rs` | `OpenDALMemoryStorage` | `OpenDALMemoryStorageFactory` | `memory` |

### Implementation Pattern
Each storage follows this consistent pattern:

```rust
// 1. Storage struct with configuration
#[derive(Debug, Clone)]
pub struct OpenDALXxxStorage {
    config: Arc<XxxConfig>,
}

// 2. Helper method to create operator
impl OpenDALXxxStorage {
    fn create_operator<'a>(&self, path: &'a str) -> Result<(Operator, &'a str)> {
        // Create OpenDAL operator with retry layer
    }
}

// 3. Storage trait implementation
#[async_trait]
impl Storage for OpenDALXxxStorage {
    async fn exists(&self, path: &str) -> Result<bool> { /* ... */ }
    async fn metadata(&self, path: &str) -> Result<FileMetadata> { /* ... */ }

    // ... implement all 10 methods
}

// 4. Factory struct
#[derive(Debug)]
pub struct OpenDALXxxStorageFactory;

// 5. Factory trait implementation
impl StorageFactory for OpenDALXxxStorageFactory {
    fn build(&self, props, extensions) -> Result<Arc<dyn Storage>> {
        // Parse configuration and create storage
    }
}
```

### Component Flow

```
User Code
    ↓
FileIOBuilder::new("s3")
    ↓
FileIOBuilder::build()
    ↓
StorageRegistry::new()
    ↓
registry.get_factory("s3")
    ↓
OpenDALS3StorageFactory
    ↓
factory.build(props, extensions)
    ↓
OpenDALS3Storage (implements Storage)
    ↓
FileIO { inner: Arc<dyn Storage> }
```

## Custom Storage Implementation
To implement a custom storage backend:

```rust
use std::collections::HashMap;
use std::sync::Arc;
use async_trait::async_trait;
use bytes::Bytes;
use iceberg::io::{
    Storage, StorageFactory, Extensions, FileMetadata, 
    FileRead, FileWrite, InputFile, OutputFile
};

use iceberg::Result;

// 1. Define your storage struct
#[derive(Debug, Clone)]
struct MyCustomStorage {
    // Your configuration
}

// 2. Implement the Storage trait
#[async_trait]
impl Storage for MyCustomStorage {
    async fn exists(&self, path: &str) -> Result<bool> {
        // Your implementation
    }
    // ... implement all 10 methods
}

// 3. Define your factory
#[derive(Debug)]
struct MyCustomStorageFactory;

// 4. Implement StorageFactory
impl StorageFactory for MyCustomStorageFactory {
    fn build(
        &self,
        props: HashMap<String, String>,
        extensions: Extensions,
    ) -> Result<Arc<dyn Storage>> {
        // Parse configuration from props
        Ok(Arc::new(MyCustomStorage::new(props)?))
    }

}

// 5. Register your factory
let mut registry = StorageRegistry::new();
registry.register("custom", Arc::new(MyCustomStorageFactory));
```

## Open Questions

These design decisions need to be resolved during implementation:

### 1. Storage Granularity (Phase 1)

**Question:** Should we have a general `Storage` implementation that works with multiple schemes, or specific implementations per scheme?

**Option A: General Storage (e.g., `OpenDALStorage`)**
- Single implementation handles all schemes (s3, gcs, fs, etc.)
- Scheme detection happens at runtime
- Simpler codebase with less duplication

```rust
#[derive(Debug, Clone)]
pub struct OpenDALStorage {
    operator: Operator,
    scheme: String,
}
```

**Option B: Scheme-Specific Storage (e.g., `OpenDALS3Storage`, `OpenDALGcsStorage`)**
- Each storage backend has its own implementation
- Type-safe configuration per backend
- Better compile-time guarantees
- More explicit and easier to optimize per backend

```rust
#[derive(Debug, Clone)]
pub struct OpenDALS3Storage {
    config: Arc<S3Config>,
}

#[derive(Debug, Clone)]
pub struct OpenDALGcsStorage {
    config: Arc<GcsConfig>,
}
```

**Current Implementation:** The RFC describes Option B (scheme-specific)

### 2. Registry Location (Phase 1)

**Question:** Where should the `StorageRegistry` live?

This question depends on the answer to Question 1:

**If Option A (General Storage):**
- Do we even need a registry? A single `OpenDALStorage` could handle all schemes internally
- Registration should happen when the crate is loaded and a registry will not be necessary

**If Option B (Scheme-Specific Storage):**
- **Option 2a: Global Static Registry** - Single process-wide registry with lazy initialization
  - Pros: Simple to use, no need to pass registry around
  - Cons: Global state, harder to test, potential initialization ordering issues
  
- **Option 2b: Catalog-Owned Registry** - Each catalog instance owns its registry
  - Pros: Better encapsulation, easier testing, no global state
  - Cons: More complex API, need to pass registry through layers

### 3. Error Handling Strategy (Phase 2)

**Question:** How should storage errors be represented?

**Option A: Enum-Based Errors**
```rust
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("File not found: {path}")]
    NotFound { path: String },
    
    #[error("Permission denied: {path}")]
    PermissionDenied { path: String },
    
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    
    #[error("Backend error: {0}")]
    Backend(Box<dyn std::error::Error + Send + Sync>),
}
```
- Pros: Type-safe, pattern matching, clear error categories
- Cons: Need to map all backend errors to enum variants

**Option B: Trait-Based Errors with Into/From**
```rust
pub trait StorageError: std::error::Error + Send + Sync + 'static {
    fn is_not_found(&self) -> bool;
    fn is_permission_denied(&self) -> bool;
}

// Each backend implements its own error type
impl StorageError for OpenDALError { /* ... */ }
impl StorageError for ObjectStoreError { /* ... */ }
```
- Pros: Flexible, preserves original error information, easier for backends
- Cons: Less type-safe, harder to handle errors uniformly

**Option C: Hybrid Approach**
```rust
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("File not found: {path}")]
    NotFound { path: String },
    
    #[error(transparent)]
    Other(#[from] Box<dyn std::error::Error + Send + Sync>),
}
```
- Pros: Common errors are typed, uncommon errors are wrapped
- Cons: Some loss of type information for wrapped errors

**Recommendation:** Option C (Hybrid) provides a good balance, with common errors like `NotFound` being strongly typed while allowing backends to preserve their specific error details.

## Implementation Plan

- **Phase 1 (Initial Implementation):**
  - Define core traits (Storage, optionally InputFile/OutputFile)
  - Implement StorageFactory + StorageRegistry
  - Refactor OpenDAL to use traits
  - **Resolve:** Question 1 (Storage granularity) and Question 2 (Registry location)
  - **Decision needed:** Choose between general vs. scheme-specific storage implementations
  
- **Phase 2 (Stabilize Storage Trait):**
  - Move concrete storage implementations to a separate crate `iceberg-storage`
  - Add new API: `delete_iter` to provide batch delete operations
  - Remove `FileIOBuilder` and `Extensions` - serializable custom Storage implementations should handle their use cases
  - **Resolve:** Question 3 (Error handling strategy)
  - **Decision needed:** Define `StorageError` type and conversion strategy if needed
  - Stabilize the Storage trait API for long-term compatibility
  
- **Phase 3:** Add object_store + other backends
  - Implement `object_store`-based storage backends in separate crates
  - Validate error handling works across different backend implementations
  - Ensure the stabilized Storage trait works well with alternative implementations
