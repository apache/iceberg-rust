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

//! Storage harness and path utilities for shared integration testing.

use std::sync::Arc;

use iceberg::io::FileIO;
use iceberg_test_utils::normalize_test_name;
use tempfile::TempDir;

/// Test harness holding an initialized `FileIO` instance and a base test directory/prefix.
pub struct StorageHarness {
    pub file_io: FileIO,
    pub base_path: String,
    pub label: &'static str,
    pub _tempdir: Option<Arc<TempDir>>,
}

impl StorageHarness {
    /// Create a new storage harness for a given `FileIO` instance.
    pub fn new(file_io: FileIO, base_path: impl Into<String>, label: &'static str) -> Self {
        Self {
            file_io,
            base_path: base_path.into(),
            label,
            _tempdir: None,
        }
    }

    /// Attach a temporary directory to be held for the lifetime of this harness.
    pub fn with_tempdir(mut self, tempdir: TempDir) -> Self {
        self._tempdir = Some(Arc::new(tempdir));
        self
    }

    /// Generate a unique, normalized path under this harness's `base_path`.
    pub fn unique_path(&self, test_name: &str) -> String {
        format!("{}{}", self.base_path, normalize_test_name(test_name))
    }
}

/// Helper function to generate a unique, normalized path for a given storage harness.
pub fn unique_path(harness: &StorageHarness, test_name: &str) -> String {
    harness.unique_path(test_name)
}
