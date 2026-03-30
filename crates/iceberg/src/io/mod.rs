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

//! File io implementation.
//!
//! # How to build `FileIO`
//!
//! `FileIO` uses explicit `StorageFactory` injection for storage creation.
//! We provide convenience constructors for common use cases:
//!
//! ```rust,ignore
//! use iceberg::io::{FileIO, FileIOBuilder};
//! use iceberg::io::{LocalFsStorageFactory, MemoryStorageFactory};
//! use std::sync::Arc;
//!
//! // Build a memory file io for testing
//! let file_io = FileIO::new_with_memory();
//!
//! // Build a local filesystem file io
//! let file_io = FileIO::new_with_fs();
//!
//! // Build with explicit factory and configuration
//! let file_io = FileIOBuilder::new(Arc::new(LocalFsStorageFactory))
//!     .with_prop("key", "value")
//!     .build();
//! ```
//!
//! # How to use `FileIO`
//!
//! Currently `FileIO` provides simple methods for file operations:
//!
//! - `delete`: Delete file.
//! - `delete_prefix`: Delete all files with a given prefix.
//! - `exists`: Check if file exists.
//! - `new_input`: Create input file for reading.
//! - `new_output`: Create output file for writing.

mod file_io;
mod storage;

pub use file_io::*;
pub use storage::*;

pub(crate) mod object_cache;

pub(crate) fn is_truthy(value: &str) -> bool {
    ["true", "t", "1", "on"].contains(&value.to_lowercase().as_str())
}
