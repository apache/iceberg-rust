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

//! Shared storage test suite for Apache Iceberg.
//!
//! Provides reusable contract test suites, harnesses, and utilities that validate
//! [`iceberg::io::FileIO`] and storage backend implementations across the workspace.

pub mod endpoint_probe;
pub mod file_io;
pub mod harness;

pub use endpoint_probe::{handle_unreachable_endpoint, is_endpoint_reachable, wait_until_ready};
pub use file_io::*;
pub use harness::{StorageHarness, unique_path};
