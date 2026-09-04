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

//! A shared tokio runtime for tests that build a [`Table`](crate::table::Table).

use std::sync::OnceLock;

use crate::runtime::Runtime;

/// Returns a process-wide [`Runtime`] suitable for tests that need to construct
/// a [`Table`](crate::table::Table) outside a tokio context.
///
/// The returned [`Runtime`] wraps a single shared multi-thread tokio runtime
/// that is lazily built on first call and lives until process exit. Cloning is
/// cheap, so test code can call this every time it needs a runtime to feed
/// into [`TableBuilder::runtime`](crate::table::TableBuilder::runtime).
pub fn test_runtime() -> Runtime {
    static TOKIO_RT: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    let tokio_rt = TOKIO_RT.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("failed to build test tokio runtime")
    });
    Runtime::new(tokio_rt)
}
