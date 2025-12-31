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

#[cfg(test)]
mod tests {
    use iceberg::memory::MemoryCatalogBuilder;
    use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
    use iceberg::{CatalogBuilder, NamespaceIdent, TableCreation};

    use super::*;

    async fn create_catalog() -> Arc<dyn Catalog> {
        let mut props = HashMap::new();
        props.insert("warehouse".to_string(), "memory://".to_string());

        let catalog = MemoryCatalogBuilder::default()
            .load("test", props)
            .await
            .unwrap();

        Arc::new(catalog)
    }

    #[tokio::test]
    async fn test_iceberg_catalog_provider_empty() {
        let catalog = create_catalog().await;

        let provider = IcebergCatalogProvider::try_new(catalog).await.unwrap();

        assert!(provider.schema_names().is_empty());
    }

    #[tokio::test]
    async fn test_iceberg_catalog_provider_single_namespace() {
        let catalog = create_catalog().await;
        let ns = NamespaceIdent::new("a".to_string());
        catalog.create_namespace(&ns, HashMap::new()).await.unwrap();

        let provider = IcebergCatalogProvider::try_new(catalog).await.unwrap();
        let schema_names = provider.schema_names();

        assert_eq!(schema_names.len(), 1);
        assert!(schema_names.contains(&"a".to_string()));
        assert!(provider.schema("a").is_some());
    }

    #[tokio::test]
    async fn test_iceberg_catalog_provider_with_table() {
        let catalog = create_catalog().await;
        let ns = NamespaceIdent::new("a".to_string());
        catalog.create_namespace(&ns, HashMap::new()).await.unwrap();

        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "foo", Type::Primitive(PrimitiveType::Int)).into(),
            ])
            .build()
            .unwrap();

        let table_creation = TableCreation::builder()
            .name("t".to_string())
            .schema(schema)
            .build();

        catalog.create_table(&ns, table_creation).await.unwrap();

        let provider = IcebergCatalogProvider::try_new(catalog).await.unwrap();

        let schema_provider = provider.schema("a").unwrap();
        let table_names = schema_provider.table_names();

        assert!(table_names.contains(&"t".to_string()));
        assert!(schema_provider.table("t").await.unwrap().is_some());
    }

    #[tokio::test]
    async fn test_iceberg_catalog_provider_nested_namespaces() {
        let catalog = create_catalog().await;
        let ns1 = NamespaceIdent::new("a".to_string());
        let ns2 = NamespaceIdent::from_vec(vec!["a".to_string(), "b".to_string()]).unwrap();
        catalog
            .create_namespace(&ns1, HashMap::new())
            .await
            .unwrap();
        catalog
            .create_namespace(&ns2, HashMap::new())
            .await
            .unwrap();

        let provider = IcebergCatalogProvider::try_new(catalog).await.unwrap();
        let schema_names = provider.schema_names();

        // This will fail since only list_namespaces(None) is used.
        assert!(schema_names.contains(&"a".to_string()));
        assert!(schema_names.contains(&"a.b".to_string()));
        assert_eq!(schema_names.len(), 2);

        assert!(provider.schema("a").is_some());
        assert!(provider.schema("a.b").is_some());
    }
}
