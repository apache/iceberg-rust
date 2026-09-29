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

//! Test utilities for this crate's own tests.
//!
//! Compiled under `cfg(test)` only, so nothing here is public API. Fixtures
//! that other crates in the workspace need from their own tests live in the
//! `iceberg_test_utils` crate instead.

mod delete_vector;
mod encryption;
mod record_batch;
mod runtime;
pub(crate) mod scan;

pub(crate) use delete_vector::encode_dv_blob;
pub(crate) use encryption::{make_encrypted_table, make_encryption_manager};
pub(crate) use record_batch::check_record_batches;
pub(crate) use runtime::test_runtime;
