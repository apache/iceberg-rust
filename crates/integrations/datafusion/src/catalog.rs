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

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use datafusion::catalog::{CatalogProvider, SchemaProvider};
use futures::future::try_join_all;
use iceberg::{Catalog, NamespaceIdent, Result};

use crate::IcebergCatalogConfig;
use crate::schema::IcebergSchemaProvider;

/// Builds a schema provider, threading the catalog config through when present so
/// the schema's tables (and the plan nodes they produce) can be distributed.
async fn build_schema_provider(
    client: Arc<dyn Catalog>,
    config: Option<IcebergCatalogConfig>,
    namespace: NamespaceIdent,
) -> Result<IcebergSchemaProvider> {
    match config {
        Some(config) => {
            IcebergSchemaProvider::try_new_with_config(client, config, namespace).await
        }
        None => IcebergSchemaProvider::try_new(client, namespace).await,
    }
}

/// Provides an interface to manage and access multiple schemas
/// within an Iceberg [`Catalog`].
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
    /// Asynchronously tries to construct a new [`IcebergCatalogProvider`]
    /// using the given client to fetch and initialize schema providers for
    /// each namespace in the Iceberg [`Catalog`].
    ///
    /// This method retrieves the list of namespace names
    /// attempts to create a schema provider for each namespace, and
    /// collects these providers into a `HashMap`.
    pub async fn try_new(client: Arc<dyn Catalog>) -> Result<Self> {
        Self::try_new_impl(client, None).await
    }

    /// Like [`try_new`](Self::try_new), but threads a serializable
    /// [`IcebergCatalogConfig`] into every schema and table provider it creates,
    /// so the catalog's tables can be queried by a distributed engine such as
    /// Ballista. The `client` must already be built from the same `config`.
    pub async fn try_new_with_config(
        client: Arc<dyn Catalog>,
        config: IcebergCatalogConfig,
    ) -> Result<Self> {
        Self::try_new_impl(client, Some(config)).await
    }

    async fn try_new_impl(
        client: Arc<dyn Catalog>,
        config: Option<IcebergCatalogConfig>,
    ) -> Result<Self> {
        // TODO:
        // Schemas and providers should be cached and evicted based on time
        // As of right now; schemas might become stale.
        let schema_names: Vec<_> = client
            .list_namespaces(None)
            .await?
            .iter()
            .flat_map(|ns| ns.as_ref().clone())
            .collect();

        let providers = try_join_all(
            schema_names
                .iter()
                .map(|name| {
                    build_schema_provider(
                        client.clone(),
                        config.clone(),
                        NamespaceIdent::new(name.clone()),
                    )
                })
                .collect::<Vec<_>>(),
        )
        .await?;

        let schemas: HashMap<String, Arc<dyn SchemaProvider>> = schema_names
            .into_iter()
            .zip(providers.into_iter())
            .map(|(name, provider)| {
                let provider = Arc::new(provider) as Arc<dyn SchemaProvider>;
                (name, provider)
            })
            .collect();

        Ok(IcebergCatalogProvider { schemas })
    }
}

impl CatalogProvider for IcebergCatalogProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        self.schemas.keys().cloned().collect()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        self.schemas.get(name).cloned()
    }
}
