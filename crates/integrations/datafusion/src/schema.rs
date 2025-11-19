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

use async_trait::async_trait;
use datafusion::catalog::SchemaProvider;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result as DFResult};
use futures::future::try_join_all;
use iceberg::arrow::arrow_type_to_type;
use iceberg::inspect::MetadataTableType;
use iceberg::runtime::spawn_blocking;
use iceberg::spec::{NestedField, Schema as IcebergSchema};
use iceberg::{Catalog, NamespaceIdent, Result, TableCreation};
use tokio::sync::RwLock;

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
    /// A `HashMap` where keys are table names
    /// and values are dynamic references to objects implementing the
    /// [`TableProvider`] trait.
    tables: Arc<RwLock<HashMap<String, Arc<IcebergTableProvider>>>>,
}

impl IcebergSchemaProvider {
    /// Convert an Arrow schema to an Iceberg schema, assigning field IDs automatically.
    /// 
    /// This is needed because DataFusion's CREATE TABLE doesn't include field IDs in the
    /// Arrow schema metadata, but Iceberg requires them. We assign sequential IDs starting from 1.
    fn arrow_schema_to_iceberg_schema(
        arrow_schema: &datafusion::arrow::datatypes::Schema,
    ) -> Result<IcebergSchema> {
        let mut field_id = 1;
        let mut fields = Vec::new();

        for field in arrow_schema.fields() {
            // Use iceberg's arrow_type_to_type for conversion
            let iceberg_type = arrow_type_to_type(field.data_type())?;
            let nested_field = if field.is_nullable() {
                NestedField::optional(field_id, field.name(), iceberg_type)
            } else {
                NestedField::required(field_id, field.name(), iceberg_type)
            };
            fields.push(nested_field.into());
            field_id += 1;
        }

        IcebergSchema::builder().with_fields(fields).build()
    }

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

        let tables: HashMap<String, Arc<IcebergTableProvider>> = table_names
            .into_iter()
            .zip(providers.into_iter())
            .map(|(name, provider)| (name, Arc::new(provider)))
            .collect();

        Ok(IcebergSchemaProvider {
            catalog: client,
            namespace,
            tables: Arc::new(RwLock::new(tables)),
        })
    }
}

#[async_trait]
impl SchemaProvider for IcebergSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        // Try to get a read lock without blocking
        // If we can't get it immediately, return empty list
        // This is a limitation of the sync API
        match self.tables.try_read() {
            Ok(tables) => tables
                .keys()
                .flat_map(|table_name| {
                    [table_name.clone()]
                        .into_iter()
                        .chain(MetadataTableType::all_types().map(|metadata_table_name| {
                            format!("{}${}", table_name.clone(), metadata_table_name.as_str())
                        }))
                })
                .collect(),
            Err(_) => Vec::new(),
        }
    }

    fn table_exist(&self, name: &str) -> bool {
        // Try to get a read lock without blocking
        match self.tables.try_read() {
            Ok(tables) => {
                if let Some((table_name, metadata_table_name)) = name.split_once('$') {
                    tables.contains_key(table_name)
                        && MetadataTableType::try_from(metadata_table_name).is_ok()
                } else {
                    tables.contains_key(name)
                }
            }
            Err(_) => false,
        }
    }

    async fn table(&self, name: &str) -> DFResult<Option<Arc<dyn TableProvider>>> {
        let tables = self.tables.read().await;
        if let Some((table_name, metadata_table_name)) = name.split_once('$') {
            let metadata_table_type =
                MetadataTableType::try_from(metadata_table_name).map_err(DataFusionError::Plan)?;
            if let Some(table) = tables.get(table_name) {
                let metadata_table = table
                    .metadata_table(metadata_table_type)
                    .await
                    .map_err(to_datafusion_error)?;
                return Ok(Some(Arc::new(metadata_table)));
            } else {
                return Ok(None);
            }
        }

        Ok(tables
            .get(name)
            .cloned()
            .map(|t| t as Arc<dyn TableProvider>))
    }

    fn register_table(
        &self,
        name: String,
        table: Arc<dyn TableProvider>,
    ) -> DFResult<Option<Arc<dyn TableProvider>>> {
        // Convert DataFusion schema to Iceberg schema
        // DataFusion schemas don't have field IDs, so we need to build the schema manually
        let df_schema = table.schema();
        let iceberg_schema = Self::arrow_schema_to_iceberg_schema(df_schema.as_ref())
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

        // Use iceberg's spawn_blocking to handle the async work on a blocking thread pool
        // This avoids the "cannot block within async runtime" error and works with both
        // tokio and smol runtimes
        let result = spawn_blocking(move || {
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
                let mut tables_guard = tables.write().await;
                let old_table = tables_guard.insert(name_clone, Arc::new(table_provider));

                Ok(old_table.map(|t| t as Arc<dyn TableProvider>))
            })
        });

        // Block on the spawned task to get the result
        // This is safe because spawn_blocking moves the blocking to a dedicated thread pool
        futures::executor::block_on(result)
    }

    fn deregister_table(&self, _name: &str) -> DFResult<Option<Arc<dyn TableProvider>>> {
        Err(DataFusionError::NotImplemented(
            "deregister_table is not supported".to_string(),
        ))
    }
}
