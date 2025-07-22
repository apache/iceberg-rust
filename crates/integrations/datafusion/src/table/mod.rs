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

pub mod metadata_table;
mod static_catalog;
pub mod table_provider_factory;

use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datafusion::catalog::Session;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::inspect::MetadataTableType;
use iceberg::table::Table;
use iceberg::{Catalog, Error, ErrorKind, NamespaceIdent, Result, TableIdent};
use metadata_table::IcebergMetadataTableProvider;
use tokio::sync::RwLock;

use crate::physical_plan::scan::IcebergTableScan;

/// Represents a [`TableProvider`] for the Iceberg [`Catalog`],
/// managing access to a [`Table`].
#[derive(Debug, Clone)]
pub struct IcebergTableProvider {
    /// A reference to the catalog that this table belongs to.
    catalog: Arc<dyn Catalog>,
    /// A table in the catalog.
    table: Arc<RwLock<Table>>,
    /// The identifier to the table in the catalog.
    table_identifier: TableIdent,
    /// Table snapshot id that will be queried via this provider.
    snapshot_id: Option<i64>,
    /// A reference-counted arrow `Schema`.
    schema: ArrowSchemaRef,
}

impl IcebergTableProvider {
    /// Asynchronously tries to construct a new [`IcebergTableProvider`]
    /// using the given client and table name to fetch an actual [`Table`]
    /// in the provided namespace.
    pub async fn try_new(client: Arc<dyn Catalog>, table_identifier: TableIdent) -> Result<Self> {
        let table = client.load_table(&table_identifier).await?;

        let schema = Arc::new(schema_to_arrow_schema(table.metadata().current_schema())?);

        Ok(IcebergTableProvider {
            table: Arc::new(RwLock::new(table)),
            table_identifier,
            snapshot_id: None,
            catalog: client,
            schema,
        })
    }

    /// Asynchronously tries to construct a new [`IcebergTableProvider`]
    /// using a specific snapshot of the given table. Can be used to create a table provider from an existing table regardless of the catalog implementation.
    pub async fn try_new_from_table_snapshot(
        client: Arc<dyn Catalog>,
        table_identifier: TableIdent,
        snapshot_id: i64,
    ) -> Result<Self> {
        let table = client.load_table(&table_identifier).await?;
        let snapshot = table
            .metadata()
            .snapshot_by_id(snapshot_id)
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::Unexpected,
                    format!(
                        "snapshot id {snapshot_id} not found in table {}",
                        table.identifier().name()
                    ),
                )
            })?;
        let schema = snapshot.schema(table.metadata())?;
        let schema = Arc::new(schema_to_arrow_schema(&schema)?);
        Ok(IcebergTableProvider {
            table: Arc::new(RwLock::new(table)),
            table_identifier,
            snapshot_id: Some(snapshot_id),
            catalog: client,
            schema,
        })
    }

    /// Refreshes the table metadata to the latest snapshot.
    pub async fn refresh_table_metadata(&self) -> Result<Table> {
        let updated_table = self.catalog.load_table(&self.table_identifier).await?;

        let mut table_guard = self.table.write().await;
        *table_guard = updated_table.clone();

        Ok(updated_table)
    }

    pub(crate) fn metadata_table(&self, r#type: MetadataTableType) -> IcebergMetadataTableProvider {
        IcebergMetadataTableProvider {
            table: self.table.clone(),
            r#type,
        }
    }
}

#[async_trait]
impl TableProvider for IcebergTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> ArrowSchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        // Get the latest table metadata from the catalog if it exists
        let table = match self.refresh_table_metadata().await.ok() {
            Some(table) => table,
            None => self.table.read().await.clone(),
        };
        Ok(Arc::new(IcebergTableScan::new(
            table,
            self.snapshot_id,
            self.schema.clone(),
            projection,
            filters,
        )))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> std::result::Result<Vec<TableProviderFilterPushDown>, datafusion::error::DataFusionError>
    {
        // Push down all filters, as a single source of truth, the scanner will drop the filters which couldn't be push down
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use datafusion::common::Column;
    use datafusion::prelude::SessionContext;
    use iceberg::io::FileIO;
    use iceberg::table::{StaticTable, Table};
    use iceberg::{Namespace, NamespaceIdent, TableCommit, TableCreation, TableIdent, TableIdent};

    use super::*;

    #[derive(Debug)]
    struct TestCatalog {
        table: Table,
    }

    impl TestCatalog {
        fn new(table: Table) -> Self {
            Self { table }
        }
    }

    #[async_trait]
    impl Catalog for TestCatalog {
        async fn load_table(&self, _table_identifier: &TableIdent) -> Result<Table> {
            Ok(self.table.clone())
        }

        async fn list_namespaces(
            &self,
            _parent: Option<&NamespaceIdent>,
        ) -> Result<Vec<NamespaceIdent>> {
            unimplemented!()
        }

        async fn create_namespace(
            &self,
            _namespace: &NamespaceIdent,
            _properties: HashMap<String, String>,
        ) -> Result<Namespace> {
            unimplemented!()
        }

        async fn get_namespace(&self, _namespace: &NamespaceIdent) -> Result<Namespace> {
            unimplemented!()
        }

        async fn namespace_exists(&self, _namespace: &NamespaceIdent) -> Result<bool> {
            unimplemented!()
        }

        async fn update_namespace(
            &self,
            _namespace: &NamespaceIdent,
            _properties: HashMap<String, String>,
        ) -> Result<()> {
            unimplemented!()
        }

        async fn drop_namespace(&self, _namespace: &NamespaceIdent) -> Result<()> {
            unimplemented!()
        }

        async fn list_tables(&self, _namespace: &NamespaceIdent) -> Result<Vec<TableIdent>> {
            unimplemented!()
        }

        async fn create_table(
            &self,
            _namespace: &NamespaceIdent,
            _creation: TableCreation,
        ) -> Result<Table> {
            unimplemented!()
        }

        async fn drop_table(&self, _table: &TableIdent) -> Result<()> {
            unimplemented!()
        }

        async fn table_exists(&self, _table: &TableIdent) -> Result<bool> {
            unimplemented!()
        }

        async fn rename_table(&self, _src: &TableIdent, _dest: &TableIdent) -> Result<()> {
            unimplemented!()
        }

        async fn update_table(&self, _commit: TableCommit) -> Result<Table> {
            unimplemented!()
        }
    }

    async fn get_test_table_from_metadata_file() -> Table {
        let metadata_file_name = "TableMetadataV2Valid.json";
        let metadata_file_path = format!(
            "{}/tests/test_data/{}",
            env!("CARGO_MANIFEST_DIR"),
            metadata_file_name
        );
        let file_io = FileIO::from_path(&metadata_file_path)
            .unwrap()
            .build()
            .unwrap();
        let static_identifier = TableIdent::from_strs(["static_ns", "static_table"]).unwrap();
        let static_table =
            StaticTable::from_metadata_file(&metadata_file_path, static_identifier, file_io)
                .await
                .unwrap();
        static_table.into_table()
    }

    #[tokio::test]
    async fn test_try_new_from_table() {
        let table = get_test_table_from_metadata_file().await;
        let catalog = Arc::new(TestCatalog::new(table.clone()));
        let table_provider = IcebergTableProvider::try_new(catalog, table.identifier().clone())
            .await
            .unwrap();
        let ctx = SessionContext::new();
        ctx.register_table("mytable", Arc::new(table_provider))
            .unwrap();
        let df = ctx.sql("SELECT * FROM mytable").await.unwrap();
        let df_schema = df.schema();
        let df_columns = df_schema.fields();
        assert_eq!(df_columns.len(), 3);
        let x_column = df_columns.first().unwrap();
        let column_data = format!(
            "{:?}:{:?}",
            x_column.name(),
            x_column.data_type().to_string()
        );
        assert_eq!(column_data, "\"x\":\"Int64\"");
        let has_column = df_schema.has_column(&Column::from_name("z"));
        assert!(has_column);
    }

    #[tokio::test]
    async fn test_try_new_from_table_snapshot() {
        let table = get_test_table_from_metadata_file().await;
        let snapshot_id = table.metadata().snapshots().next().unwrap().snapshot_id();
        let catalog = Arc::new(TestCatalog::new(table.clone()));
        let table_provider = IcebergTableProvider::try_new_from_table_snapshot(
            catalog,
            table.identifier().clone(),
            snapshot_id,
        )
        .await
        .unwrap();
        let ctx = SessionContext::new();
        ctx.register_table("mytable", Arc::new(table_provider))
            .unwrap();
        let df = ctx.sql("SELECT * FROM mytable").await.unwrap();
        let df_schema = df.schema();
        let df_columns = df_schema.fields();
        assert_eq!(df_columns.len(), 3);
        let x_column = df_columns.first().unwrap();
        let column_data = format!(
            "{:?}:{:?}",
            x_column.name(),
            x_column.data_type().to_string()
        );
        assert_eq!(column_data, "\"x\":\"Int64\"");
        let has_column = df_schema.has_column(&Column::from_name("z"));
        assert!(has_column);
    }

    #[tokio::test]
    async fn test_physical_input_schema_consistent_with_logical_input_schema() {
        let table = get_test_table_from_metadata_file().await;
        let catalog = Arc::new(TestCatalog::new(table.clone()));
        let table_provider = IcebergTableProvider::try_new(catalog, table.identifier().clone())
            .await
            .unwrap();
        let ctx = SessionContext::new();
        ctx.register_table("mytable", Arc::new(table_provider))
            .unwrap();
        let df = ctx.sql("SELECT count(*) FROM mytable").await.unwrap();
        let physical_plan = df.create_physical_plan().await;
        assert!(physical_plan.is_ok())
    }
}
