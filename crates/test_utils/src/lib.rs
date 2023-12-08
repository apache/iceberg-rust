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

//! This crate contains common utilities for testing.
//!
//! It's not intended for use outside of `iceberg-rust`.

#[cfg(feature = "tests")]
mod cmd;
#[cfg(feature = "tests")]
pub mod docker;

#[cfg(feature = "tests")]
pub use common::*;

#[cfg(feature = "tests")]
mod common {
    use std::sync::Once;

    static INIT: Once = Once::new();
    pub fn set_up() {
        INIT.call_once(env_logger::init);
    }
    pub fn normalize_test_name(s: impl ToString) -> String {
        s.to_string().replace("::", "__").replace('.', "_")
    }
}
