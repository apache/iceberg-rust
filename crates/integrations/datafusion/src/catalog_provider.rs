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

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::catalog::{CatalogProvider, SchemaProvider};
use futures::future::try_join_all;
use iceberg::{Catalog, NamespaceIdent, Result, SessionCatalog, SessionContext};

use crate::SessionContextResolver;
use crate::catalog_access::CatalogAccess;
use crate::schema_provider::IcebergSchemaProvider;

/// Provides a DataFusion interface to schemas in an Iceberg [`Catalog`] or
/// [`SessionCatalog`].
///
/// Acts as a centralized catalog provider that aggregates
/// multiple [`SchemaProvider`], each associated with distinct namespaces.
#[derive(Debug)]
pub struct IcebergCatalogProvider {
    /// A `HashMap` where keys are namespace names
    /// and values are dynamic references to objects implementing the
    /// [`SchemaProvider`] trait.
    schemas: HashMap<String, Arc<dyn SchemaProvider>>,
}

impl IcebergCatalogProvider {
    /// Asynchronously constructs an [`IcebergCatalogProvider`] from a
    /// [`Catalog`], fetching and initializing a schema provider for each
    /// namespace.
    ///
    /// This method retrieves the namespace names and collects an initialized
    /// schema provider for each namespace into a `HashMap`.
    pub async fn try_new(catalog: Arc<dyn Catalog>) -> Result<Self> {
        let direct = CatalogAccess::Direct(catalog);
        Self::try_new_with_access(direct).await
    }

    /// Creates an [`IcebergCatalogProvider`] backed by a [`SessionCatalog`].
    ///
    /// The [`SessionContextResolver`] derives an Iceberg session context for
    /// scans and inserts. Provider initialization, metadata-table lookup,
    /// table registration, and table deregistration do not receive a
    /// DataFusion session; they share one anonymous fallback context instead.
    ///
    /// Namespace and table discovery is performed once during construction and
    /// shared by all DataFusion sessions. Catalogs with session-dependent
    /// visibility must make the intended discovery set available to the
    /// anonymous fallback; discovery is not repeated per DataFusion session.
    pub async fn try_new_with_session_catalog(
        catalog: Arc<dyn SessionCatalog>,
        resolver: Arc<dyn SessionContextResolver>,
    ) -> Result<Self> {
        let session_aware = CatalogAccess::SessionAware {
            catalog,
            resolver,
            // One session context that's shared for all query-unrelated catalog operations.
            fallback_context: SessionContext::empty(),
        };
        Self::try_new_with_access(session_aware).await
    }

    async fn try_new_with_access(catalog: CatalogAccess) -> Result<Self> {
        // TODO:
        // Schemas and providers should be cached and evicted based on time
        // As of right now; schemas might become stale.
        let schema_names: Vec<_> = catalog
            .without_session()
            .list_namespaces(None)
            .await?
            .iter()
            .flat_map(|ns| ns.as_ref().clone())
            .collect();

        Ok(IcebergCatalogProvider {
            schemas: load_schema_providers(catalog, schema_names).await?,
        })
    }
}

impl CatalogProvider for IcebergCatalogProvider {
    fn schema_names(&self) -> Vec<String> {
        self.schemas.keys().cloned().collect()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        self.schemas.get(name).cloned()
    }
}

async fn load_schema_providers(
    catalog_access: CatalogAccess,
    schema_names: Vec<String>,
) -> Result<HashMap<String, Arc<dyn SchemaProvider>>> {
    let iceberg_providers = try_join_all(
        schema_names
            .iter()
            .map(|name| {
                IcebergSchemaProvider::try_new(
                    catalog_access.clone(),
                    NamespaceIdent::new(name.clone()),
                )
            })
            .collect::<Vec<_>>(),
    )
    .await?;

    let provider_map = schema_names
        .into_iter()
        .zip(iceberg_providers)
        .map(|(name, iceberg_provider)| {
            let provider = Arc::new(iceberg_provider) as Arc<dyn SchemaProvider>;
            (name, provider)
        })
        .collect();

    Ok(provider_map)
}
