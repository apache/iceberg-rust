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

//! Storage interfaces for Iceberg.

mod config;
mod local_fs;
mod memory;

use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
pub use config::*;
pub use local_fs::{LocalFsStorage, LocalFsStorageFactory};
pub use memory::{MemoryStorage, MemoryStorageFactory};

use super::{FileInfo, FileMetadata, FileRead, FileWrite, InputFile, OutputFile};
use crate::{Error, ErrorKind, Result};

/// Trait for storage operations in Iceberg.
///
/// The trait supports serialization via `typetag`, allowing storage instances to be
/// serialized and deserialized across process boundaries.
///
/// Third-party implementations can implement this trait to provide custom storage backends.
///
/// # Implementing Custom Storage
///
/// To implement a custom storage backend:
///
/// 1. Create a struct that implements this trait
/// 2. Add `#[typetag::serde]` attribute for serialization support
/// 3. Implement all required methods
///
/// # Example
///
/// ```rust,ignore
/// #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
/// struct MyStorage {
///     // custom fields
/// }
///
/// #[async_trait]
/// #[typetag::serde]
/// impl Storage for MyStorage {
///     async fn exists(&self, path: &str) -> Result<bool> {
///         // implementation
///         todo!()
///     }
///     // ... implement other methods
/// }
/// ```
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

    /// Recursively list all files under a prefix.
    ///
    /// Returns one [`FileInfo`] per file (never a directory) reachable under `prefix`,
    /// descending into nested subdirectories. This is the read-side sibling of
    /// [`Self::delete_prefix`] and mirrors Java's
    /// `SupportsPrefixOperations.listPrefix` (whose Hadoop implementation uses
    /// `FileSystem.listFiles(prefix, recursive = true)`).
    ///
    /// # Prefix semantics
    ///
    /// As Java's interface documents, hierarchical filesystems may require the prefix to
    /// name a directory while key/value object stores allow arbitrary string prefixes. Each
    /// implementation MUST document which it implements and MUST agree with its own
    /// [`Self::delete_prefix`] on what falls "under the prefix" — a `list`/`delete_prefix`
    /// disagreement would let an orphan-file sweep delete a file the listing never reported.
    ///
    /// # Default behavior
    ///
    /// The default body returns a [`FeatureUnsupported`](ErrorKind::FeatureUnsupported)
    /// error rather than an empty list. An empty list is a valid "no files under this
    /// prefix" answer; a backend that simply cannot list MUST fail loudly so a caller never
    /// mistakes "cannot enumerate" for "nothing here". The defaulted body also keeps
    /// external `#[typetag::serde]` implementors of this trait compiling without changes.
    async fn list(&self, prefix: &str) -> Result<Vec<FileInfo>> {
        Err(Error::new(
            ErrorKind::FeatureUnsupported,
            format!(
                "Storage::list (prefix listing) is not supported by this storage backend (prefix: {prefix})"
            ),
        ))
    }

    /// Create a new input file for reading
    fn new_input(&self, path: &str) -> Result<InputFile>;

    /// Create a new output file for writing
    fn new_output(&self, path: &str) -> Result<OutputFile>;
}

/// Factory for creating Storage instances from configuration.
///
/// Implement this trait to provide custom storage backends. The factory pattern
/// allows for lazy initialization of storage instances and enables users to
/// inject custom storage implementations into catalogs.
///
/// # Example
///
/// ```rust,ignore
/// #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
/// struct MyCustomStorageFactory {
///     // custom configuration
/// }
///
/// #[typetag::serde]
/// impl StorageFactory for MyCustomStorageFactory {
///     fn build(&self, config: &StorageConfig) -> Result<Arc<dyn Storage>> {
///         // Create and return custom storage implementation
///         todo!()
///     }
/// }
/// ```
#[typetag::serde(tag = "type")]
pub trait StorageFactory: Debug + Send + Sync {
    /// Build a new Storage instance from the given configuration.
    ///
    /// # Arguments
    ///
    /// * `config` - The storage configuration containing scheme and properties
    ///
    /// # Returns
    ///
    /// A `Result` containing an `Arc<dyn Storage>` on success, or an error
    /// if the storage could not be created.
    fn build(&self, config: &StorageConfig) -> Result<Arc<dyn Storage>>;
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use serde::{Deserialize, Serialize};

    use super::*;

    /// A storage backend that overrides every required method but deliberately does NOT
    /// override `list`, so it exercises the defaulted trait body.
    #[derive(Debug, Serialize, Deserialize)]
    struct NoListStorage;

    #[async_trait]
    #[typetag::serde]
    impl Storage for NoListStorage {
        async fn exists(&self, _path: &str) -> Result<bool> {
            Ok(false)
        }

        async fn metadata(&self, _path: &str) -> Result<FileMetadata> {
            Ok(FileMetadata { size: 0 })
        }

        async fn read(&self, _path: &str) -> Result<Bytes> {
            Ok(Bytes::new())
        }

        async fn reader(&self, _path: &str) -> Result<Box<dyn FileRead>> {
            Err(Error::new(
                ErrorKind::FeatureUnsupported,
                "reader unsupported",
            ))
        }

        async fn write(&self, _path: &str, _bs: Bytes) -> Result<()> {
            Ok(())
        }

        async fn writer(&self, _path: &str) -> Result<Box<dyn FileWrite>> {
            Err(Error::new(
                ErrorKind::FeatureUnsupported,
                "writer unsupported",
            ))
        }

        async fn delete(&self, _path: &str) -> Result<()> {
            Ok(())
        }

        async fn delete_prefix(&self, _path: &str) -> Result<()> {
            Ok(())
        }

        fn new_input(&self, path: &str) -> Result<InputFile> {
            Ok(InputFile::new(Arc::new(NoListStorage), path.to_string()))
        }

        fn new_output(&self, path: &str) -> Result<OutputFile> {
            Ok(OutputFile::new(Arc::new(NoListStorage), path.to_string()))
        }
    }

    /// Risk: a backend that cannot list must FAIL LOUDLY, not return an empty Vec. Downstream
    /// (A2) reads an empty listing as "no orphans / everything orphan"; a silent empty answer
    /// from a backend that simply can't enumerate would corrupt the orphan decision. The
    /// default trait body MUST error with `FeatureUnsupported`, naming the operation.
    #[tokio::test]
    async fn test_default_list_errors_loudly_not_empty() {
        let storage = NoListStorage;
        let result = storage.list("memory://anything").await;

        let error = result.expect_err("default list must error, not return an empty Vec");
        assert_eq!(error.kind(), ErrorKind::FeatureUnsupported);
        assert!(
            error.message().contains("list"),
            "error message must name the unsupported operation, got: {}",
            error.message()
        );
    }
}
