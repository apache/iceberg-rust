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
    /// Each DataFusion session that has [`IcebergOptions`] configured will
    /// propagate an Iceberg [`SessionContext`] for scans and inserts. Provider
    /// initialization, metadata-table lookup, table registration, and table
    /// deregistration do not receive a DataFusion session; they share one
    /// anonymous fallback context instead.
    ///
    /// Namespace and table discovery is performed once during construction and
    /// shared by all DataFusion sessions. Catalogs with session-dependent
    /// visibility must make the intended discovery set available to the
    /// anonymous fallback; discovery is not repeated per DataFusion session.
    pub async fn try_new_with_session_catalog(catalog: Arc<dyn SessionCatalog>) -> Result<Self> {
        let session_aware = CatalogAccess::SessionAware {
            catalog,
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::catalog::CatalogProvider;
    use datafusion::datasource::MemTable;
    use datafusion::logical_expr::dml::InsertOp;
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::prelude::SessionContext as DFSessionContext;

    use super::*;
    use crate::test_utils::create_recording_catalog;

    #[tokio::test]
    async fn test_session_aware_scan_uses_resolved_context() {
        let (session_catalog, namespace, table_name, _temp_dir) = create_recording_catalog().await;
        let provider =
            IcebergCatalogProvider::try_new_with_session_catalog(session_catalog.clone())
                .await
                .unwrap();

        let bootstrap_calls = session_catalog.calls();
        assert_eq!(
            bootstrap_calls
                .iter()
                .map(|call| call.operation)
                .collect::<Vec<_>>(),
            vec!["list_namespaces", "list_tables", "load_table"]
        );
        session_catalog.clear_calls();

        let fallback_session_id = bootstrap_calls[0].session_id.as_str();
        assert!(bootstrap_calls.iter().all(|call| {
            call.session_id == fallback_session_id
                && call.identity.is_none()
                && call.properties.is_empty()
                && call.credential_keys.is_empty()
        }));

        let schema = provider.schema(namespace[0].as_str()).unwrap();
        let table = schema.table(&table_name).await.unwrap().unwrap();

        let df_context = DFSessionContext::new();
        table
            .scan(&df_context.state(), None, &[], None)
            .await
            .unwrap();

        let calls = session_catalog.calls();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].operation, "load_table");
        assert_eq!(calls[0].session_id, "resolved-session");
        assert_eq!(calls[0].identity.as_deref(), Some("test-user"));
        assert_eq!(
            calls[0].properties.get("test-property").map(String::as_str),
            Some("test-value")
        );
        assert_eq!(calls[0].credential_keys, vec!["test-token"]);
    }

    #[tokio::test]
    async fn test_session_resolution_errors_prevent_catalog_access() {
        let (session_catalog, namespace, table_name, _temp_dir) = create_recording_catalog().await;
        let provider =
            IcebergCatalogProvider::try_new_with_session_catalog(session_catalog.clone())
                .await
                .unwrap();
        let schema = provider.schema(namespace[0].as_str()).unwrap();
        let table = schema.table(&table_name).await.unwrap().unwrap();
        session_catalog.clear_calls();

        let df_context = DFSessionContext::new();
        let error = table
            .scan(&df_context.state(), None, &[], None)
            .await
            .err()
            .unwrap();

        assert!(error.to_string().contains("session resolution failed"));

        let input = Arc::new(EmptyExec::new(table.schema()));
        let error = table
            .insert_into(&df_context.state(), input, InsertOp::Append)
            .await
            .err()
            .unwrap();
        assert!(error.to_string().contains("session resolution failed"));
        assert!(session_catalog.calls().is_empty());
    }

    #[tokio::test]
    async fn test_session_aware_insert_reuses_resolved_context_for_commit() {
        let (session_catalog, namespace, table_name, _temp_dir) = create_recording_catalog().await;
        let provider =
            IcebergCatalogProvider::try_new_with_session_catalog(session_catalog.clone())
                .await
                .unwrap();
        let schema = provider.schema(namespace[0].as_str()).unwrap();
        let table = schema.table(&table_name).await.unwrap().unwrap();
        session_catalog.clear_calls();

        let df_context = DFSessionContext::new();
        df_context.register_table("test_table", table).unwrap();
        df_context
            .sql("INSERT INTO test_table VALUES (1, 'test')")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let calls = session_catalog.calls();
        let load = calls
            .iter()
            .find(|call| call.operation == "load_table")
            .unwrap();
        let update = calls
            .iter()
            .find(|call| call.operation == "update_table")
            .unwrap();
        assert_eq!(load.session_id, "resolved-session");
        assert_eq!(update.session_id, load.session_id);
        assert_eq!(update.identity, load.identity);
        assert_eq!(update.properties, load.properties);
        assert_eq!(update.credential_keys, load.credential_keys);
    }

    #[tokio::test]
    async fn test_sessionless_schema_operations_share_fallback_context() {
        let (session_catalog, namespace, table_name, _temp_dir) = create_recording_catalog().await;
        let provider =
            IcebergCatalogProvider::try_new_with_session_catalog(session_catalog.clone())
                .await
                .unwrap();
        let schema = provider.schema(namespace[0].as_str()).unwrap();
        session_catalog.clear_calls();

        schema
            .table(&format!("{table_name}$snapshots"))
            .await
            .unwrap()
            .unwrap();

        let metadata_calls = session_catalog.calls();
        assert_eq!(metadata_calls.len(), 1);
        assert_eq!(metadata_calls[0].operation, "load_table");
        let fallback_session_id = metadata_calls[0].session_id.clone();
        assert!(metadata_calls.iter().all(|call| {
            call.session_id == fallback_session_id
                && call.identity.is_none()
                && call.properties.is_empty()
                && call.credential_keys.is_empty()
        }));
        session_catalog.clear_calls();

        let arrow_schema = schema.table(&table_name).await.unwrap().unwrap().schema();
        let empty_batch = RecordBatch::new_empty(arrow_schema.clone());
        let empty_table = MemTable::try_new(arrow_schema, vec![vec![empty_batch]]).unwrap();
        schema
            .register_table("registered_table".to_string(), Arc::new(empty_table))
            .unwrap();
        schema.deregister_table("registered_table").unwrap();

        let calls = session_catalog.calls();
        assert!(calls.iter().any(|call| call.operation == "create_table"));
        assert!(calls.iter().any(|call| call.operation == "drop_table"));
        assert!(calls.iter().all(|call| {
            call.session_id == fallback_session_id
                && call.identity.is_none()
                && call.properties.is_empty()
                && call.credential_keys.is_empty()
        }));
    }
}
