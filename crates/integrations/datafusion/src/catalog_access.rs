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

use std::sync::Arc;

use iceberg::{Catalog, SessionCatalog, SessionContext};

use crate::SessionContextResolver;

/// Describes how the DataFusion integration accesses an Iceberg catalog.
///
/// A catalog can either be accessed directly through [`Catalog`] or through a
/// [`SessionCatalog`] bound to an Iceberg [`SessionContext`]. Operations that
/// receive a DataFusion session resolve and bind its context once. Operations
/// for which DataFusion provides no session reuse one anonymous fallback
/// context so session-scoped catalog state remains stable across those calls.
#[derive(Clone, Debug)]
pub(crate) enum CatalogAccess {
    /// A catalog accessed directly through the [`Catalog`] API.
    Direct(Arc<dyn Catalog>),

    /// A session-aware catalog, its resolver, and the stable context used by
    /// DataFusion APIs that do not expose a session.
    SessionAware {
        catalog: Arc<dyn SessionCatalog>,
        resolver: Arc<dyn SessionContextResolver>,
        fallback_context: SessionContext,
    },
}
