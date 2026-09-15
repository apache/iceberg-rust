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
use datafusion::catalog::Session;
use iceberg::table::Table;
use iceberg::{
    Catalog, Namespace, NamespaceIdent, Result, SessionCatalog, SessionContext, TableCommit,
    TableCreation, TableIdent,
};

use crate::options::resolve_session_context;

/// Adapts a [`SessionCatalog`] to [`Catalog`] by binding one [`SessionContext`].
///
/// Every catalog operation is forwarded to the inner session-aware catalog
/// with the same context. The binding is fixed for the lifetime of this
/// adapter; create another adapter to use a different session context.
#[derive(Clone, Debug)]
pub(crate) struct SessionBindingCatalogAdapter {
    context: SessionContext,
    inner: Arc<dyn SessionCatalog>,
}

impl SessionBindingCatalogAdapter {
    /// Creates a catalog view of `inner` bound to `context`.
    ///
    /// The inner catalog receives this context for every operation performed
    /// through the returned adapter.
    pub(crate) fn new(context: SessionContext, inner: Arc<dyn SessionCatalog>) -> Self {
        Self { context, inner }
    }

    /// Adapts a plain, session-unaware catalog to a [`SessionBindingCatalogAdapter`].
    ///
    /// The bound session context is never used, but simply ignored.
    pub(crate) fn new_without_context(catalog: Arc<dyn Catalog>) -> Self {
        let session_catalog = SessionDroppingCatalogAdapter::new(catalog);
        Self::new(SessionContext::empty(), Arc::new(session_catalog))
    }

    /// Overwrites the already bound, usually shared fallback session, with
    /// a provided session, usually from a DataFusion query.
    pub(crate) fn with_session(
        self: &Arc<Self>,
        session: &dyn Session,
    ) -> Arc<SessionBindingCatalogAdapter> {
        match resolve_session_context(session) {
            None => Arc::clone(self),
            Some(context) => Arc::new(SessionBindingCatalogAdapter::new(
                context,
                Arc::clone(&self.inner),
            )),
        }
    }
}

#[async_trait]
impl Catalog for SessionBindingCatalogAdapter {
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

/// A wrapper around a [`Catalog`] to provide a [`SessionCatalog`] API by
/// ignoring any passed [`SessionContext`].
#[derive(Debug)]
struct SessionDroppingCatalogAdapter {
    inner: Arc<dyn Catalog>,
}

impl SessionDroppingCatalogAdapter {
    fn new(inner: Arc<dyn Catalog>) -> Self {
        Self { inner }
    }
}

#[async_trait]
impl SessionCatalog for SessionDroppingCatalogAdapter {
    async fn list_namespaces(
        &self,
        _: &SessionContext,
        parent: Option<&NamespaceIdent>,
    ) -> Result<Vec<NamespaceIdent>> {
        self.inner.list_namespaces(parent).await
    }

    async fn create_namespace(
        &self,
        _: &SessionContext,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> Result<Namespace> {
        self.inner.create_namespace(namespace, properties).await
    }

    async fn get_namespace(
        &self,
        _: &SessionContext,
        namespace: &NamespaceIdent,
    ) -> Result<Namespace> {
        self.inner.get_namespace(namespace).await
    }

    async fn namespace_exists(&self, _: &SessionContext, ns: &NamespaceIdent) -> Result<bool> {
        self.inner.namespace_exists(ns).await
    }

    async fn update_namespace(
        &self,
        _: &SessionContext,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> Result<()> {
        self.inner.update_namespace(namespace, properties).await
    }

    async fn drop_namespace(&self, _: &SessionContext, namespace: &NamespaceIdent) -> Result<()> {
        self.inner.drop_namespace(namespace).await
    }

    async fn list_tables(
        &self,
        _: &SessionContext,
        namespace: &NamespaceIdent,
    ) -> Result<Vec<TableIdent>> {
        self.inner.list_tables(namespace).await
    }

    async fn create_table(
        &self,
        _: &SessionContext,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> Result<Table> {
        self.inner.create_table(namespace, creation).await
    }

    async fn load_table(&self, _: &SessionContext, table_ident: &TableIdent) -> Result<Table> {
        self.inner.load_table(table_ident).await
    }

    async fn drop_table(&self, _: &SessionContext, table: &TableIdent) -> Result<()> {
        self.inner.drop_table(table).await
    }

    async fn purge_table(&self, _: &SessionContext, table: &TableIdent) -> Result<()> {
        self.inner.purge_table(table).await
    }

    async fn table_exists(&self, _: &SessionContext, table: &TableIdent) -> Result<bool> {
        self.inner.table_exists(table).await
    }

    async fn rename_table(
        &self,
        _: &SessionContext,
        src: &TableIdent,
        dest: &TableIdent,
    ) -> Result<()> {
        self.inner.rename_table(src, dest).await
    }

    async fn register_table(
        &self,
        _: &SessionContext,
        table_ident: &TableIdent,
        metadata_location: String,
    ) -> Result<Table> {
        self.inner
            .register_table(table_ident, metadata_location)
            .await
    }

    async fn update_table(&self, _: &SessionContext, commit: TableCommit) -> Result<Table> {
        self.inner.update_table(commit).await
    }
}
