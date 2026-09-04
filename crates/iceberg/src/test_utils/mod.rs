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

//! Test utilities.
//!
//! Compiled under `cfg(test)`, or behind the `test-utils` feature for other
//! crates in this workspace that need these fixtures from their own tests.
//! Not public API: it is subject to change and is not intended to be used by
//! external users. Items only needed within this crate stay `cfg(test)`.

#[cfg(test)]
mod encryption;
mod record_batch;
mod runtime;
#[cfg(test)]
pub(crate) mod scan;

#[cfg(test)]
pub(crate) use encryption::{make_encrypted_table, make_encryption_manager};
pub use record_batch::check_record_batches;
pub use runtime::test_runtime;
