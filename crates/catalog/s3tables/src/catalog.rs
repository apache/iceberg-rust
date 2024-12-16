use std::collections::HashMap;

use async_trait::async_trait;
use iceberg::table::Table;
use iceberg::{Catalog, Namespace, NamespaceIdent, Result, TableCommit, TableCreation, TableIdent};

/// S3Tables catalog implementation.
#[derive(Debug)]
pub struct S3TablesCatalog {}

impl S3TablesCatalog {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl Catalog for S3TablesCatalog {
    async fn list_namespaces(
        &self,
        parent: Option<&NamespaceIdent>,
    ) -> Result<Vec<NamespaceIdent>> {
        todo!()
    }

    async fn create_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> Result<Namespace> {
        todo!()
    }

    async fn get_namespace(&self, namespace: &NamespaceIdent) -> Result<Namespace> {
        todo!()
    }

    async fn namespace_exists(&self, namespace: &NamespaceIdent) -> Result<bool> {
        todo!()
    }

    async fn update_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> Result<()> {
        todo!()
    }

    async fn drop_namespace(&self, namespace: &NamespaceIdent) -> Result<()> {
        todo!()
    }

    async fn list_tables(&self, namespace: &NamespaceIdent) -> Result<Vec<TableIdent>> {
        todo!()
    }

    async fn create_table(
        &self,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> Result<Table> {
        todo!()
    }

    async fn load_table(&self, table: &TableIdent) -> Result<Table> {
        todo!()
    }

    async fn drop_table(&self, table: &TableIdent) -> Result<()> {
        todo!()
    }

    async fn table_exists(&self, table: &TableIdent) -> Result<bool> {
        todo!()
    }

    async fn rename_table(&self, src: &TableIdent, dest: &TableIdent) -> Result<()> {
        todo!()
    }

    async fn update_table(&self, commit: TableCommit) -> Result<Table> {
        todo!()
    }
}
