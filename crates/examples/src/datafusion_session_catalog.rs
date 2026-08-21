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

//! Connects a session-aware Iceberg catalog to DataFusion.
//!
//! Run with:
//!
//! ```text
//! cargo run -p iceberg-examples --example datafusion-session-catalog
//! ```
//!
//! The adapter at the bottom only makes the example self-contained. Applications
//! should pass their own `SessionCatalog` implementation to the provider.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::prelude::{SessionConfig, SessionContext as DataFusionSessionContext};
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalog, MemoryCatalogBuilder};
use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use iceberg::table::Table;
use iceberg::{
    Catalog, CatalogBuilder, Namespace, NamespaceIdent, Result, SessionCatalog,
    SessionContext as IcebergSessionContext, TableCommit, TableCreation, TableIdent,
};
use iceberg_datafusion::{IcebergCatalogProvider, IcebergOptions};

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let catalog =
        init_catalog_with_table(TableIdent::from_strs(["datafusion", "example"])?).await?;

    let session_catalog = Arc::new(ExampleSessionCatalog::new(catalog));
    // Provider construction discovers namespaces and tables with one stable,
    // anonymous fallback context. Session-dependent catalogs must make that
    // discovery set available to the fallback context.
    let provider = IcebergCatalogProvider::try_new_with_session_catalog(session_catalog).await?;

    let mut iceberg_options = IcebergOptions::default();
    iceberg_options.identity = Some("user123".to_string());

    let config = SessionConfig::new().with_extension(Arc::new(iceberg_options));
    let datafusion = DataFusionSessionContext::new_with_config(config);
    datafusion.register_catalog("iceberg", Arc::new(provider));

    // Planning the scan derives an Iceberg context from the DataFusion session
    // and its IcebergOptions, then forwards it to the session catalog's
    // `load_table` operation.
    datafusion
        .sql("SELECT COUNT(*) AS event_count FROM iceberg.datafusion.example")
        .await?
        .show()
        .await?;

    Ok(())
}

/// A small session-aware wrapper around the in-memory catalog used by this
/// standalone example.
///
/// Real session catalogs can use the context for authorization, credentials,
/// configuration, and caching. This wrapper logs it, then delegates catalog
/// operations so the example needs no external service.
#[derive(Debug)]
struct ExampleSessionCatalog {
    inner: MemoryCatalog,
}

impl ExampleSessionCatalog {
    fn new(inner: MemoryCatalog) -> Self {
        Self { inner }
    }

    fn log_context(context: &IcebergSessionContext, operation: &str) {
        let identity = context.identity().unwrap_or("<anonymous>");
        println!(
            "{operation}: session_id={}, identity={identity}",
            context.session_id()
        );
    }
}

#[async_trait]
impl SessionCatalog for ExampleSessionCatalog {
    async fn list_namespaces(
        &self,
        context: &IcebergSessionContext,
        parent: Option<&NamespaceIdent>,
    ) -> Result<Vec<NamespaceIdent>> {
        Self::log_context(context, "list_namespaces");
        self.inner.list_namespaces(parent).await
    }

    async fn create_namespace(
        &self,
        context: &IcebergSessionContext,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> Result<Namespace> {
        Self::log_context(context, "create_namespace");
        self.inner.create_namespace(namespace, properties).await
    }

    async fn get_namespace(
        &self,
        context: &IcebergSessionContext,
        namespace: &NamespaceIdent,
    ) -> Result<Namespace> {
        Self::log_context(context, "get_namespace");
        self.inner.get_namespace(namespace).await
    }

    async fn namespace_exists(
        &self,
        context: &IcebergSessionContext,
        namespace: &NamespaceIdent,
    ) -> Result<bool> {
        Self::log_context(context, "namespace_exists");
        self.inner.namespace_exists(namespace).await
    }

    async fn update_namespace(
        &self,
        context: &IcebergSessionContext,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> Result<()> {
        Self::log_context(context, "update_namespace");
        self.inner.update_namespace(namespace, properties).await
    }

    async fn drop_namespace(
        &self,
        context: &IcebergSessionContext,
        namespace: &NamespaceIdent,
    ) -> Result<()> {
        Self::log_context(context, "drop_namespace");
        self.inner.drop_namespace(namespace).await
    }

    async fn list_tables(
        &self,
        context: &IcebergSessionContext,
        namespace: &NamespaceIdent,
    ) -> Result<Vec<TableIdent>> {
        Self::log_context(context, "list_tables");
        self.inner.list_tables(namespace).await
    }

    async fn create_table(
        &self,
        context: &IcebergSessionContext,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> Result<Table> {
        Self::log_context(context, "create_table");
        self.inner.create_table(namespace, creation).await
    }

    async fn load_table(
        &self,
        context: &IcebergSessionContext,
        table: &TableIdent,
    ) -> Result<Table> {
        Self::log_context(context, "load_table");
        self.inner.load_table(table).await
    }

    async fn drop_table(&self, context: &IcebergSessionContext, table: &TableIdent) -> Result<()> {
        Self::log_context(context, "drop_table");
        self.inner.drop_table(table).await
    }

    async fn purge_table(&self, context: &IcebergSessionContext, table: &TableIdent) -> Result<()> {
        Self::log_context(context, "purge_table");
        self.inner.purge_table(table).await
    }

    async fn table_exists(
        &self,
        context: &IcebergSessionContext,
        table: &TableIdent,
    ) -> Result<bool> {
        Self::log_context(context, "table_exists");
        self.inner.table_exists(table).await
    }

    async fn rename_table(
        &self,
        context: &IcebergSessionContext,
        src: &TableIdent,
        dest: &TableIdent,
    ) -> Result<()> {
        Self::log_context(context, "rename_table");
        self.inner.rename_table(src, dest).await
    }

    async fn register_table(
        &self,
        context: &IcebergSessionContext,
        table: &TableIdent,
        metadata_location: String,
    ) -> Result<Table> {
        Self::log_context(context, "register_table");
        self.inner.register_table(table, metadata_location).await
    }

    async fn update_table(
        &self,
        context: &IcebergSessionContext,
        commit: TableCommit,
    ) -> Result<Table> {
        Self::log_context(context, "update_table");
        self.inner.update_table(commit).await
    }
}

async fn init_catalog_with_table(table_ident: TableIdent) -> Result<MemoryCatalog> {
    let catalog = MemoryCatalogBuilder::default()
        .load(
            "memory",
            HashMap::from([(
                MEMORY_CATALOG_WAREHOUSE.to_string(),
                "memory://session-catalog-example".to_string(),
            )]),
        )
        .await?;

    catalog
        .create_namespace(table_ident.namespace(), HashMap::new())
        .await?;
    catalog
        .create_table(
            table_ident.namespace(),
            TableCreation::builder()
                .name(table_ident.name().to_string())
                .schema(
                    Schema::builder()
                        .with_fields(vec![
                            NestedField::required(
                                1,
                                "event_id",
                                Type::Primitive(PrimitiveType::Long),
                            )
                            .into(),
                        ])
                        .build()?,
                )
                .build(),
        )
        .await?;

    Ok(catalog)
}
