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

//! Async catalog providers for Iceberg tables in DataFusion.
//!
//! This module provides asynchronous catalog and schema providers that fetch
//! metadata on-demand, ensuring users always see the latest catalog state.
//!
//! # Overview
//!
//! Unlike the deprecated [`IcebergCatalogProvider`] which captures a snapshot
//! of catalog state at creation time, these async providers fetch metadata
//! fresh for each query using DataFusion's [`AsyncSchemaProvider`] and
//! [`AsyncCatalogProvider`] traits.
//!
//! # Usage
//!
//! ```ignore
//! use iceberg_datafusion::{IcebergAsyncCatalogProvider, IcebergAsyncSchemaProvider};
//! use datafusion::catalog::AsyncSchemaProvider;
//!
//! // Create async catalog provider
//! let async_catalog = IcebergAsyncCatalogProvider::new(catalog);
//!
//! // For a query, parse SQL and resolve references
//! let state = ctx.state();
//! let statement = state.sql_to_statement(sql, &dialect)?;
//! let references = state.resolve_table_references(&statement)?;
//!
//! // Resolve only needed tables asynchronously
//! let resolved = async_catalog.resolve(&references, state.config(), "iceberg").await?;
//!
//! // Register and execute
//! ctx.catalog("iceberg").unwrap().register_schema("ns", resolved)?;
//! let df = ctx.sql(sql).await?;
//! ```
//!
//! [`IcebergCatalogProvider`]: crate::IcebergCatalogProvider
//! [`AsyncSchemaProvider`]: datafusion::catalog::AsyncSchemaProvider
//! [`AsyncCatalogProvider`]: datafusion::catalog::AsyncCatalogProvider

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::{AsyncCatalogProvider, AsyncSchemaProvider, TableProvider};
use datafusion::common::Result as DFResult;
use iceberg::{Catalog, ErrorKind, NamespaceIdent, TableIdent};

use crate::table::IcebergTableProvider;
use crate::to_datafusion_error;

/// An async schema provider that fetches table metadata on-demand from an Iceberg catalog.
///
/// This provider implements [`AsyncSchemaProvider`] to support dynamic catalog access.
/// Tables are loaded fresh from the catalog each time they are requested, ensuring
/// that newly created tables are immediately visible and dropped tables are not.
///
/// # Per-Query Caching
///
/// When used with DataFusion's `resolve()` method, tables are cached only for the
/// duration of a single query. This provides a consistent view during query execution
/// while still reflecting catalog changes between queries.
#[derive(Debug, Clone)]
pub struct IcebergAsyncSchemaProvider {
    catalog: Arc<dyn Catalog>,
    namespace: NamespaceIdent,
}

impl IcebergAsyncSchemaProvider {
    /// Creates a new async schema provider for the given namespace.
    ///
    /// # Arguments
    ///
    /// * `catalog` - The Iceberg catalog to use for table lookups
    /// * `namespace` - The namespace this schema provider represents
    pub fn new(catalog: Arc<dyn Catalog>, namespace: NamespaceIdent) -> Self {
        Self { catalog, namespace }
    }

    /// Returns the namespace this schema provider represents.
    pub fn namespace(&self) -> &NamespaceIdent {
        &self.namespace
    }

    /// Returns a reference to the underlying Iceberg catalog.
    pub fn catalog(&self) -> &Arc<dyn Catalog> {
        &self.catalog
    }
}

#[async_trait]
impl AsyncSchemaProvider for IcebergAsyncSchemaProvider {
    async fn table(&self, name: &str) -> DFResult<Option<Arc<dyn TableProvider>>> {
        let table_ident = TableIdent::new(self.namespace.clone(), name.to_string());

        match self.catalog.load_table(&table_ident).await {
            Ok(_) => {
                let provider = IcebergTableProvider::try_new(
                    self.catalog.clone(),
                    self.namespace.clone(),
                    name,
                )
                .await
                .map_err(to_datafusion_error)?;

                Ok(Some(Arc::new(provider) as Arc<dyn TableProvider>))
            }
            Err(e) if e.kind() == ErrorKind::TableNotFound => {
                // Table not found
                Ok(None)
            }
            Err(e) => Err(to_datafusion_error(e)),
        }
    }
}

/// An async catalog provider that fetches schema metadata on-demand from an Iceberg catalog.
///
/// This provider implements [`AsyncCatalogProvider`] to support dynamic catalog access.
/// Namespaces (schemas) are checked for existence when requested, and async schema
/// providers are created on-demand.
///
/// # Per-Query Caching
///
/// When used with DataFusion's `resolve()` method, schemas and their tables are cached
/// only for the duration of a single query. This provides a consistent view during
/// query execution while still reflecting catalog changes between queries.
///
/// # Example
///
/// ```ignore
/// let async_catalog = IcebergAsyncCatalogProvider::new(catalog);
///
/// // Resolve references for a specific query
/// let resolved = async_catalog
///     .resolve(&table_references, config, "catalog_name")
///     .await?;
///
/// // The resolved catalog can now be used for planning
/// ```
#[derive(Debug, Clone)]
pub struct IcebergAsyncCatalogProvider {
    catalog: Arc<dyn Catalog>,
}

impl IcebergAsyncCatalogProvider {
    /// Creates a new async catalog provider.
    ///
    /// # Arguments
    ///
    /// * `catalog` - The Iceberg catalog to use for namespace and table lookups
    pub fn new(catalog: Arc<dyn Catalog>) -> Self {
        Self { catalog }
    }

    /// Returns a reference to the underlying Iceberg catalog.
    pub fn catalog(&self) -> &Arc<dyn Catalog> {
        &self.catalog
    }
}

#[async_trait]
impl AsyncCatalogProvider for IcebergAsyncCatalogProvider {
    async fn schema(&self, name: &str) -> DFResult<Option<Arc<dyn AsyncSchemaProvider>>> {
        let namespace = NamespaceIdent::new(name.to_string());

        match self.catalog.namespace_exists(&namespace).await {
            Ok(true) => Ok(Some(Arc::new(IcebergAsyncSchemaProvider::new(
                self.catalog.clone(),
                namespace,
            )))),
            Ok(false) => Ok(None),
            Err(e) => Err(to_datafusion_error(e)),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use datafusion::catalog::AsyncSchemaProvider;
    use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
    use iceberg::{CatalogBuilder, TableCreation};
    use tempfile::TempDir;

    use super::*;

    async fn create_test_catalog() -> (Arc<dyn Catalog>, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let warehouse_path = temp_dir.path().to_str().unwrap().to_string();

        let catalog = MemoryCatalogBuilder::default()
            .load(
                "test_catalog",
                HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), warehouse_path)]),
            )
            .await
            .unwrap();

        (Arc::new(catalog) as Arc<dyn Catalog>, temp_dir)
    }

    #[tokio::test]
    async fn test_async_schema_provider_table_not_found() {
        let (catalog, _temp_dir) = create_test_catalog().await;

        let namespace = NamespaceIdent::new("test_ns".to_string());
        catalog
            .create_namespace(&namespace, HashMap::new())
            .await
            .unwrap();

        let provider = IcebergAsyncSchemaProvider::new(catalog, namespace);

        // Table doesn't exist, should return None
        let result = provider.table("nonexistent_table").await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_async_catalog_provider_schema_not_found() {
        let (catalog, _temp_dir) = create_test_catalog().await;

        let provider = IcebergAsyncCatalogProvider::new(catalog);

        // Schema doesn't exist, should return None
        let result = provider.schema("nonexistent_schema").await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_async_catalog_provider_schema_exists() {
        let (catalog, _temp_dir) = create_test_catalog().await;

        let namespace = NamespaceIdent::new("existing_ns".to_string());
        catalog
            .create_namespace(&namespace, HashMap::new())
            .await
            .unwrap();

        let provider = IcebergAsyncCatalogProvider::new(catalog);

        // Schema exists, should return Some
        let result = provider.schema("existing_ns").await.unwrap();
        assert!(result.is_some());
    }

    #[tokio::test]
    async fn test_dynamic_table_visibility() {
        let (catalog, _temp_dir) = create_test_catalog().await;

        let namespace = NamespaceIdent::new("dynamic_ns".to_string());
        catalog
            .create_namespace(&namespace, HashMap::new())
            .await
            .unwrap();

        let provider = IcebergAsyncSchemaProvider::new(catalog.clone(), namespace.clone());

        // Table doesn't exist yet
        assert!(provider.table("new_table").await.unwrap().is_none());

        // Create table in catalog
        let schema = iceberg::spec::Schema::builder()
            .with_fields(vec![
                iceberg::spec::NestedField::required(
                    1,
                    "id",
                    iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Int),
                )
                .into(),
            ])
            .build()
            .unwrap();

        let table_creation = TableCreation::builder()
            .name("new_table".to_string())
            .schema(schema)
            .build();

        catalog
            .create_table(&namespace, table_creation)
            .await
            .unwrap();

        // Now table should be visible through the same provider instance
        let result = provider.table("new_table").await.unwrap();
        assert!(result.is_some());
    }

    #[tokio::test]
    async fn test_dynamic_namespace_visibility() {
        let (catalog, _temp_dir) = create_test_catalog().await;

        let provider = IcebergAsyncCatalogProvider::new(catalog.clone());

        // Namespace doesn't exist yet
        assert!(provider.schema("new_namespace").await.unwrap().is_none());

        // Create namespace in catalog
        let namespace = NamespaceIdent::new("new_namespace".to_string());
        catalog
            .create_namespace(&namespace, HashMap::new())
            .await
            .unwrap();

        // Now namespace should be visible through the same provider instance
        let result = provider.schema("new_namespace").await.unwrap();
        assert!(result.is_some());
    }
}
