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
use std::sync::Arc;

use async_trait::async_trait;
use dashmap::DashMap;
use datafusion::catalog::SchemaProvider;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result as DFResult};
use futures::future::try_join_all;
use iceberg::arrow::arrow_schema_to_schema_with_assigned_ids;
use iceberg::inspect::MetadataTableType;
use iceberg::{Catalog, NamespaceIdent, Result, TableCreation};

use crate::table::IcebergTableProvider;
use crate::to_datafusion_error;

/// Represents a [`SchemaProvider`] for the Iceberg [`Catalog`], managing
/// access to table providers within a specific namespace.
#[derive(Debug)]
pub(crate) struct IcebergSchemaProvider {
    /// Reference to the Iceberg catalog
    catalog: Arc<dyn Catalog>,
    /// The namespace this schema represents
    namespace: NamespaceIdent,
    /// A concurrent map where keys are table names
    /// and values are dynamic references to objects implementing the
    /// [`TableProvider`] trait.
    tables: Arc<DashMap<String, Arc<IcebergTableProvider>>>,
}

impl IcebergSchemaProvider {
    /// Asynchronously tries to construct a new [`IcebergSchemaProvider`]
    /// using the given client to fetch and initialize table providers for
    /// the provided namespace in the Iceberg [`Catalog`].
    ///
    /// This method retrieves a list of table names
    /// attempts to create a table provider for each table name, and
    /// collects these providers into a `HashMap`.
    pub(crate) async fn try_new(
        client: Arc<dyn Catalog>,
        namespace: NamespaceIdent,
    ) -> Result<Self> {
        // TODO:
        // Tables and providers should be cached based on table_name
        // if we have a cache miss; we update our internal cache & check again
        // As of right now; tables might become stale.
        let table_names: Vec<_> = client
            .list_tables(&namespace)
            .await?
            .iter()
            .map(|tbl| tbl.name().to_string())
            .collect();

        let providers = try_join_all(
            table_names
                .iter()
                .map(|name| IcebergTableProvider::try_new(client.clone(), namespace.clone(), name))
                .collect::<Vec<_>>(),
        )
        .await?;

        let tables = DashMap::new();
        for (name, provider) in table_names.into_iter().zip(providers.into_iter()) {
            tables.insert(name, Arc::new(provider));
        }

        Ok(IcebergSchemaProvider {
            catalog: client,
            namespace,
            tables: Arc::new(tables),
        })
    }
}

#[async_trait]
impl SchemaProvider for IcebergSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        self.tables
            .iter()
            .flat_map(|entry| {
                let table_name = entry.key().clone();
                [table_name.clone()]
                    .into_iter()
                    .chain(
                        MetadataTableType::all_types().map(move |metadata_table_name| {
                            format!("{}${}", table_name, metadata_table_name.as_str())
                        }),
                    )
            })
            .collect()
    }

    fn table_exist(&self, name: &str) -> bool {
        if let Some((table_name, metadata_table_name)) = name.split_once('$') {
            self.tables.contains_key(table_name)
                && MetadataTableType::try_from(metadata_table_name).is_ok()
        } else {
            self.tables.contains_key(name)
        }
    }

    async fn table(&self, name: &str) -> DFResult<Option<Arc<dyn TableProvider>>> {
        if let Some((table_name, metadata_table_name)) = name.split_once('$') {
            let metadata_table_type =
                MetadataTableType::try_from(metadata_table_name).map_err(DataFusionError::Plan)?;
            if let Some(table) = self.tables.get(table_name) {
                let metadata_table = table
                    .metadata_table(metadata_table_type)
                    .await
                    .map_err(to_datafusion_error)?;
                return Ok(Some(Arc::new(metadata_table)));
            } else {
                return Ok(None);
            }
        }

        Ok(self
            .tables
            .get(name)
            .map(|entry| entry.value().clone() as Arc<dyn TableProvider>))
    }

    fn register_table(
        &self,
        name: String,
        table: Arc<dyn TableProvider>,
    ) -> DFResult<Option<Arc<dyn TableProvider>>> {
        // Convert DataFusion schema to Iceberg schema
        // DataFusion schemas don't have field IDs, so we use the function that assigns them automatically
        let df_schema = table.schema();
        let iceberg_schema = arrow_schema_to_schema_with_assigned_ids(df_schema.as_ref())
            .map_err(to_datafusion_error)?;

        // Create the table in the Iceberg catalog
        let table_creation = TableCreation::builder()
            .name(name.clone())
            .schema(iceberg_schema)
            .build();

        let catalog = self.catalog.clone();
        let namespace = self.namespace.clone();
        let tables = self.tables.clone();
        let name_clone = name.clone();

        // Use tokio's spawn_blocking to handle the async work on a blocking thread pool
        // This avoids the "cannot block within async runtime" error
        let result = tokio::task::spawn_blocking(move || {
            // Create a new runtime handle to execute the async work
            let rt = tokio::runtime::Handle::current();
            rt.block_on(async move {
                catalog
                    .create_table(&namespace, table_creation)
                    .await
                    .map_err(to_datafusion_error)?;

                // Create a new table provider using the catalog reference
                let table_provider = IcebergTableProvider::try_new(
                    catalog.clone(),
                    namespace.clone(),
                    name_clone.clone(),
                )
                .await
                .map_err(to_datafusion_error)?;

                // Store the new table provider
                let old_table = tables.insert(name_clone, Arc::new(table_provider));

                Ok(old_table.map(|t| t as Arc<dyn TableProvider>))
            })
        });

        // Block on the spawned task to get the result
        // This is safe because spawn_blocking moves the blocking to a dedicated thread pool
        futures::executor::block_on(result)
            .map_err(|e| DataFusionError::Execution(format!("Failed to create Iceberg table: {}", e)))?
    }

    fn deregister_table(&self, name: &str) -> DFResult<Option<Arc<dyn TableProvider>>> {
        // Don't allow dropping metadata tables directly
        if name.contains('$') {
            return Err(DataFusionError::Plan(
                "Cannot drop metadata tables directly. Drop the parent table instead.".to_string(),
            ));
        }

        let catalog = self.catalog.clone();
        let namespace = self.namespace.clone();
        let tables = self.tables.clone();
        let name = name.to_string();

        let result = tokio::task::spawn_blocking(move || {
            let rt = tokio::runtime::Handle::current();
            rt.block_on(async move {
                let table_ident =
                    iceberg::TableIdent::new(namespace.clone(), name.clone());

                // Drop the table from the Iceberg catalog
                catalog
                    .drop_table(&table_ident)
                    .await
                    .map_err(to_datafusion_error)?;

                // Remove from local cache and return the old provider
                let old_table = tables.remove(&name);
                Ok(old_table.map(|(_, provider)| provider as Arc<dyn TableProvider>))
            })
        });

        futures::executor::block_on(result).map_err(|e| {
            DataFusionError::Execution(format!("Failed to drop Iceberg table: {}", e))
        })?
    }
}
