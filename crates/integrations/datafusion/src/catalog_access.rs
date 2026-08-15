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

use iceberg::{Catalog, SessionCatalog};

use crate::SessionContextResolver;

/// Describes how the DataFusion integration accesses an Iceberg catalog.
///
/// A catalog can either be accessed directly through [`Catalog`] or through
/// [`SessionCatalog`] with an Iceberg [`SessionContext`] derived from the
/// current DataFusion session. Operations for which DataFusion provides no
/// session use an empty Iceberg context.
#[derive(Clone, Debug)]
pub(crate) enum CatalogAccess {
    /// A catalog accessed directly through the [`Catalog`] API.
    Direct(Arc<dyn Catalog>),

    /// A session-aware catalog and the resolver used to derive its Iceberg
    /// context from the current DataFusion session.
    SessionAware(Arc<dyn SessionCatalog>, Arc<dyn SessionContextResolver>),
}
