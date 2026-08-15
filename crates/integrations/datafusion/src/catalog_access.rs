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

use async_trait::async_trait;
use iceberg::table::Table;
use iceberg::{
    Catalog, Namespace, NamespaceIdent, Result, SessionCatalog, SessionContext, TableCommit,
    TableCreation, TableIdent,
};

use crate::SessionContextResolver;

/// Describes how the DataFusion integration accesses an Iceberg catalog.
///
/// A catalog can either be accessed directly through [`Catalog`] or through a
/// [`SessionCatalog`] bound to an Iceberg [`SessionContext`]. Operations that
/// receive a DataFusion session resolve and bind its context once. Operations
/// for which DataFusion provides no session reuse one anonymous fallback
/// context so session-scoped catalog state remains stable across those calls.
#[derive(Clone, Debug)]
pub(crate) enum CatalogAccess {
    /// A catalog accessed directly through the [`Catalog`] API.
    Direct(Arc<dyn Catalog>),

    /// A session-aware catalog, its resolver, and the stable context used by
    /// DataFusion APIs that do not expose a session.
    SessionAware {
        catalog: Arc<dyn SessionCatalog>,
        resolver: Arc<dyn SessionContextResolver>,
        fallback_context: SessionContext,
    },
}

/// Adapts a [`SessionCatalog`] to [`Catalog`] by binding one [`SessionContext`].
///
/// Every catalog operation is forwarded to the inner session-aware catalog
/// with the same context. The binding is fixed for the lifetime of this
/// adapter; create another adapter to use a different session context.
#[derive(Debug)]
pub(crate) struct SessionBoundCatalog {
    context: SessionContext,
    inner: Arc<dyn SessionCatalog>,
}

impl SessionBoundCatalog {
    /// Creates a catalog view of `inner` bound to `context`.
    ///
    /// The inner catalog receives this context for every operation performed
    /// through the returned adapter.
    pub fn new(context: SessionContext, inner: Arc<dyn SessionCatalog>) -> Self {
        Self { context, inner }
    }
}

#[async_trait]
impl Catalog for SessionBoundCatalog {
    async fn list_namespaces(
        &self,
        parent: Option<&NamespaceIdent>,
    ) -> Result<Vec<NamespaceIdent>> {
        self.inner.list_namespaces(&self.context, parent).await
    }

    async fn create_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> Result<Namespace> {
        self.inner
            .create_namespace(&self.context, namespace, properties)
            .await
    }

    async fn get_namespace(&self, namespace: &NamespaceIdent) -> Result<Namespace> {
        self.inner.get_namespace(&self.context, namespace).await
    }

    async fn namespace_exists(&self, ns: &NamespaceIdent) -> Result<bool> {
        self.inner.namespace_exists(&self.context, ns).await
    }

    async fn update_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> Result<()> {
        self.inner
            .update_namespace(&self.context, namespace, properties)
            .await
    }

    async fn drop_namespace(&self, namespace: &NamespaceIdent) -> Result<()> {
        self.inner.drop_namespace(&self.context, namespace).await
    }

    async fn list_tables(&self, namespace: &NamespaceIdent) -> Result<Vec<TableIdent>> {
        self.inner.list_tables(&self.context, namespace).await
    }

    async fn create_table(
        &self,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> Result<Table> {
        self.inner
            .create_table(&self.context, namespace, creation)
            .await
    }

    async fn load_table(&self, table_ident: &TableIdent) -> Result<Table> {
        self.inner.load_table(&self.context, table_ident).await
    }

    async fn drop_table(&self, table: &TableIdent) -> Result<()> {
        self.inner.drop_table(&self.context, table).await
    }

    async fn purge_table(&self, table: &TableIdent) -> Result<()> {
        self.inner.purge_table(&self.context, table).await
    }

    async fn table_exists(&self, table: &TableIdent) -> Result<bool> {
        self.inner.table_exists(&self.context, table).await
    }

    async fn rename_table(&self, src: &TableIdent, dest: &TableIdent) -> Result<()> {
        self.inner.rename_table(&self.context, src, dest).await
    }

    async fn register_table(
        &self,
        table_ident: &TableIdent,
        metadata_location: String,
    ) -> Result<Table> {
        self.inner
            .register_table(&self.context, table_ident, metadata_location)
            .await
    }

    async fn update_table(&self, commit: TableCommit) -> Result<Table> {
        self.inner.update_table(&self.context, commit).await
    }
}
