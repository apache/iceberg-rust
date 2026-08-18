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

mod error;
pub use error::*;
mod catalog_access;

mod catalog_provider;
pub use catalog_provider::*;
pub mod physical_plan;
mod schema_provider;
pub mod table;
pub use table::table_provider_factory::IcebergTableProviderFactory;
pub use table::*;

pub(crate) mod task_writer;
#[cfg(test)]
mod test_utils;

use std::fmt;

use datafusion::catalog::Session as DFSession;
use datafusion::error::Result as DFResult;
use iceberg::SessionContext;

/// Resolves an Iceberg [`SessionContext`] from a DataFusion session.
///
/// The DataFusion integration calls the resolver once while planning each
/// session-aware scan or insert. The returned context is bound to that
/// operation and, for inserts, is retained through transaction commit.
/// Implementations should therefore return a stable Iceberg session identity
/// for repeated operations from the same DataFusion session.
pub trait SessionContextResolver: fmt::Debug + Send + Sync {
    /// Returns the Iceberg context associated with `session`.
    ///
    /// Returning an error aborts planning before the catalog is accessed.
    fn resolve(&self, session: &dyn DFSession) -> DFResult<SessionContext>;
}
