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
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use iceberg::table::Table;
use iceberg::{
    Catalog, CatalogBuilder, Namespace, NamespaceIdent, Result, SessionCatalog, SessionContext,
    TableCommit, TableCreation, TableIdent,
};
use tempfile::TempDir;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CatalogCall {
    pub(crate) operation: &'static str,
    pub(crate) session_id: String,
    pub(crate) identity: Option<String>,
    pub(crate) properties: HashMap<String, String>,
    pub(crate) credential_keys: Vec<String>,
}

impl CatalogCall {
    fn new(operation: &'static str, context: &SessionContext) -> Self {
        let mut credential_keys = context.credentials().keys().cloned().collect::<Vec<_>>();
        credential_keys.sort();
        Self {
            operation,
            session_id: context.session_id().to_string(),
            identity: context.identity().map(ToString::to_string),
            properties: context.properties().clone(),
            credential_keys,
        }
    }
}

#[derive(Debug)]
pub(crate) struct RecordingSessionCatalog {
    inner: Arc<dyn Catalog>,
    calls: Mutex<Vec<CatalogCall>>,
}

impl RecordingSessionCatalog {
    fn new(inner: Arc<dyn Catalog>) -> Self {
        Self {
            inner,
            calls: Mutex::new(Vec::new()),
        }
    }

    fn record(&self, operation: &'static str, context: &SessionContext) {
        self.calls
            .lock()
            .unwrap()
            .push(CatalogCall::new(operation, context));
    }

    pub(crate) fn calls(&self) -> Vec<CatalogCall> {
        self.calls.lock().unwrap().clone()
    }

    pub(crate) fn clear_calls(&self) {
        self.calls.lock().unwrap().clear();
    }
}

#[async_trait]
impl SessionCatalog for RecordingSessionCatalog {
    async fn list_namespaces(
        &self,
        context: &SessionContext,
        parent: Option<&NamespaceIdent>,
    ) -> Result<Vec<NamespaceIdent>> {
        self.record("list_namespaces", context);
        self.inner.list_namespaces(parent).await
    }

    async fn create_namespace(
        &self,
        context: &SessionContext,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> Result<Namespace> {
        self.record("create_namespace", context);
        self.inner.create_namespace(namespace, properties).await
    }

    async fn get_namespace(
        &self,
        context: &SessionContext,
        namespace: &NamespaceIdent,
    ) -> Result<Namespace> {
        self.record("get_namespace", context);
        self.inner.get_namespace(namespace).await
    }

    async fn namespace_exists(
        &self,
        context: &SessionContext,
        namespace: &NamespaceIdent,
    ) -> Result<bool> {
        self.record("namespace_exists", context);
        self.inner.namespace_exists(namespace).await
    }

    async fn update_namespace(
        &self,
        context: &SessionContext,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> Result<()> {
        self.record("update_namespace", context);
        self.inner.update_namespace(namespace, properties).await
    }

    async fn drop_namespace(
        &self,
        context: &SessionContext,
        namespace: &NamespaceIdent,
    ) -> Result<()> {
        self.record("drop_namespace", context);
        self.inner.drop_namespace(namespace).await
    }

    async fn list_tables(
        &self,
        context: &SessionContext,
        namespace: &NamespaceIdent,
    ) -> Result<Vec<TableIdent>> {
        self.record("list_tables", context);
        self.inner.list_tables(namespace).await
    }

    async fn create_table(
        &self,
        context: &SessionContext,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> Result<Table> {
        self.record("create_table", context);
        self.inner.create_table(namespace, creation).await
    }

    async fn load_table(&self, context: &SessionContext, table: &TableIdent) -> Result<Table> {
        self.record("load_table", context);
        self.inner.load_table(table).await
    }

    async fn drop_table(&self, context: &SessionContext, table: &TableIdent) -> Result<()> {
        self.record("drop_table", context);
        self.inner.drop_table(table).await
    }

    async fn purge_table(&self, context: &SessionContext, table: &TableIdent) -> Result<()> {
        self.record("purge_table", context);
        self.inner.purge_table(table).await
    }

    async fn table_exists(&self, context: &SessionContext, table: &TableIdent) -> Result<bool> {
        self.record("table_exists", context);
        self.inner.table_exists(table).await
    }

    async fn rename_table(
        &self,
        context: &SessionContext,
        src: &TableIdent,
        dest: &TableIdent,
    ) -> Result<()> {
        self.record("rename_table", context);
        self.inner.rename_table(src, dest).await
    }

    async fn register_table(
        &self,
        context: &SessionContext,
        table: &TableIdent,
        metadata_location: String,
    ) -> Result<Table> {
        self.record("register_table", context);
        self.inner.register_table(table, metadata_location).await
    }

    async fn update_table(&self, context: &SessionContext, commit: TableCommit) -> Result<Table> {
        self.record("update_table", context);
        self.inner.update_table(commit).await
    }
}

pub(crate) async fn create_recording_catalog() -> (
    Arc<RecordingSessionCatalog>,
    NamespaceIdent,
    String,
    TempDir,
) {
    let temp_dir = TempDir::new().unwrap();
    let warehouse_path = temp_dir.path().to_str().unwrap().to_string();
    let catalog = Arc::new(
        MemoryCatalogBuilder::default()
            .load(
                "memory",
                HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), warehouse_path.clone())]),
            )
            .await
            .unwrap(),
    );

    let namespace = NamespaceIdent::new("test_ns".to_string());
    catalog
        .create_namespace(&namespace, HashMap::new())
        .await
        .unwrap();

    let schema = Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::required(2, "name", Type::Primitive(PrimitiveType::String)).into(),
        ])
        .build()
        .unwrap();
    let table_name = "test_table".to_string();
    let creation = TableCreation::builder()
        .name(table_name.clone())
        .location(format!("{warehouse_path}/{table_name}"))
        .schema(schema)
        .build();
    catalog.create_table(&namespace, creation).await.unwrap();

    (
        Arc::new(RecordingSessionCatalog::new(catalog)),
        namespace,
        table_name,
        temp_dir,
    )
}
