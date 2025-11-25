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
│  │  - pub trait StorageBuilder                   │  │
│  │  - pub struct StorageBuilderRegistry          │  │
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
│ │ impl Builder   │ │  │  │ │ impl Builder  │ │  │ │impl traits│ │
│ └────────────────┘ │  │  │ └───────────────┘ │  │ └───────────┘ │
│ ┌────────────────┐ │  │  │ ┌───────────────┐ │  └───────────────┘
│ │ opendal-fs     │ │  │  │ │ objstore-gcs  │ │
│ │ impl Storage   │ │  │  │ │ impl Storage  │ │
│ │ impl Builder   │ │  │  │ │ impl Builder  │ │
│ └────────────────┘ │  │  │ └───────────────┘ │
│ ┌────────────────┐ │  │  │ ┌───────────────┐ │
│ │ opendal-gcs    │ │  │  │ │ objstore-azure│ │
│ │ impl Storage   │ │  │  │ │ impl Storage  │ │
│ │ impl Builder   │ │  │  │ │ impl Builder  │ │
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
    async fn remove_dir_all(&self, path: &str) -> Result<()>;

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

## StorageBuilder and Registry

### StorageBuilder Trait
The `StorageBuilder` trait defines how storage backends are constructed:

```rust
pub trait StorageBuilder: Debug + Send + Sync {
    fn build(
        &self,
        props: HashMap<String, String>,
        extensions: Extensions,
    ) -> Result<Arc<dyn Storage>>;
}
```

Key design decisions:

- Uses `&self` instead of `self` - builders are reusable
- No associated type - returns `Arc<dyn Storage>` directly
- `Send + Sync` for thread safety

### StorageBuilderRegistry
The registry manages storage builders and provides lookup by scheme:

```rust
#[derive(Debug, Clone)]
pub struct StorageBuilderRegistry {
    builders: HashMap<String, Arc<dyn StorageBuilder>>,
}

impl StorageBuilderRegistry {
    pub fn new() -> Self { /* ... */ }
    pub fn register(&mut self, scheme: impl Into<String>, builder: Arc<dyn StorageBuilder>);
    pub fn get_builder(&self, scheme: &str) -> Result<Arc<dyn StorageBuilder>>;
    pub fn supported_types(&self) -> Vec<String>;
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
use iceberg::io::{StorageBuilderRegistry, StorageBuilder};

let mut registry = StorageBuilderRegistry::new();
registry.register("custom", Arc::new(MyCustomStorageBuilder));

// Check supported types
println!("Supported: {:?}", registry.supported_types());
```

## Storage Implementations
Each storage backend has its own implementation:

| Storage | File | Struct | Builder | Schemes |
|---------|------|--------|---------|---------|
| S3 | `storage_s3.rs` | `OpenDALS3Storage` | `OpenDALS3StorageBuilder` | `s3`, `s3a` |
| GCS | `storage_gcs.rs` | `OpenDALGcsStorage` | `OpenDALGcsStorageBuilder` | `gs`, `gcs` |
| OSS | `storage_oss.rs` | `OpenDALOssStorage` | `OpenDALOssStorageBuilder` | `oss` |
| Azure | `storage_azdls.rs` | `OpenDALAzdlsStorage` | `OpenDALAzdlsStorageBuilder` | `abfs`, `abfss`, `wasb`, `wasbs` |
| Filesystem | `storage_fs.rs` | `OpenDALFsStorage` | `OpenDALFsStorageBuilder` | `file`, `""` |
| Memory | `storage_memory.rs` | `OpenDALMemoryStorage` | `OpenDALMemoryStorageBuilder` | `memory` |

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

// 4. Builder struct
#[derive(Debug)]
pub struct OpenDALXxxStorageBuilder;

// 5. Builder trait implementation
impl StorageBuilder for OpenDALXxxStorageBuilder {
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
StorageBuilderRegistry::new()
    ↓
registry.get_builder("s3")
    ↓
OpenDALS3StorageBuilder
    ↓
builder.build(props, extensions)
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
    Storage, StorageBuilder, Extensions, FileMetadata, 
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

// 3. Define your builder
#[derive(Debug)]
struct MyCustomStorageBuilder;

// 4. Implement StorageBuilder
impl StorageBuilder for MyCustomStorageBuilder {
    fn build(
        &self,
        props: HashMap<String, String>,
        extensions: Extensions,
    ) -> Result<Arc<dyn Storage>> {
        // Parse configuration from props
        Ok(Arc::new(MyCustomStorage::new(props)?))
    }

}

// 5. Register your builder
let mut registry = StorageBuilderRegistry::new();
registry.register("custom", Arc::new(MyCustomStorageBuilder));
```

## Implementation Plan

- **Phase 1 (Initial Implementation):**
  - Define core traits (Storage, optionally InputFile/OutputFile)
  - Implement StorageBuilder + StorageLoader
  - Refactor OpenDAL to use traits
- **Phase 2:** Split storage backends into separate crates (crates/storage/opendal)
- **Phase 3:** Replace FileIO with StorageBuilderRegistry in Catalog structs
- **Phase 4:** Add object_store + other backends
