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
use iceberg::{Catalog, NamespaceIdent, Result};

use crate::IcebergCatalogConfig;
use crate::schema::IcebergSchemaProvider;

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
                    IcebergSchemaProvider::try_new(
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
            .zip(providers)
            .map(|(name, provider)| {
                let provider = Arc::new(provider) as Arc<dyn SchemaProvider>;
                (name, provider)
            })
            .collect();

        Ok(IcebergCatalogProvider { schemas })
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

#[cfg(test)]
mod tests {
    use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
    use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
    use iceberg::{CatalogBuilder, TableCreation};
    use tempfile::TempDir;

    use super::*;
    use crate::table::IcebergTableProvider;

    async fn catalog_with_one_table() -> (Arc<dyn Catalog>, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let warehouse_path = temp_dir.path().to_str().unwrap().to_string();
        let catalog = MemoryCatalogBuilder::default()
            .load(
                "memory",
                HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), warehouse_path.clone())]),
            )
            .await
            .unwrap();

        let namespace = NamespaceIdent::new("test_ns".to_string());
        catalog
            .create_namespace(&namespace, HashMap::new())
            .await
            .unwrap();

        let schema = Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            ])
            .build()
            .unwrap();
        catalog
            .create_table(
                &namespace,
                TableCreation::builder()
                    .name("t".to_string())
                    .location(format!("{warehouse_path}/t"))
                    .schema(schema)
                    .properties(HashMap::new())
                    .build(),
            )
            .await
            .unwrap();

        (Arc::new(catalog), temp_dir)
    }

    /// Downcasts the `t` table provider in the `test_ns` schema and returns
    /// whether it carries an [`IcebergCatalogConfig`].
    async fn table_provider_has_config(provider: &IcebergCatalogProvider) -> bool {
        let schema = provider.schema("test_ns").expect("test_ns schema");
        let table = schema.table("t").await.unwrap().expect("table provider");
        table
            .downcast_ref::<IcebergTableProvider>()
            .expect("IcebergTableProvider")
            .catalog_config()
            .is_some()
    }

    #[tokio::test]
    async fn test_try_new_with_config_propagates_to_tables() {
        let (catalog, _temp_dir) = catalog_with_one_table().await;
        let config = IcebergCatalogConfig::new("memory", "memory", HashMap::new());

        let provider = IcebergCatalogProvider::try_new_with_config(catalog, config)
            .await
            .unwrap();

        assert!(
            table_provider_has_config(&provider).await,
            "try_new_with_config should thread the config down to table providers"
        );
    }

    #[tokio::test]
    async fn test_try_new_leaves_tables_config_less() {
        let (catalog, _temp_dir) = catalog_with_one_table().await;

        let provider = IcebergCatalogProvider::try_new(catalog).await.unwrap();

        assert!(
            !table_provider_has_config(&provider).await,
            "try_new should leave table providers without a config"
        );
    }
}
