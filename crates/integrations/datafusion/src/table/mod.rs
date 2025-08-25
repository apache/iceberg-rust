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
pub mod table_provider_factory;

use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datafusion::catalog::Session;
use datafusion::common::DataFusionError;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::inspect::MetadataTableType;
use iceberg::table::Table;
use iceberg::{Catalog, Error, ErrorKind, NamespaceIdent, Result, TableIdent};
use metadata_table::IcebergMetadataTableProvider;

use crate::physical_plan::commit::IcebergCommitExec;
use crate::physical_plan::scan::IcebergTableScan;
use crate::physical_plan::write::IcebergWriteExec;

/// Represents a [`TableProvider`] for the Iceberg [`Catalog`],
/// managing access to a [`Table`].
#[derive(Debug, Clone)]
pub struct IcebergTableProvider {
    /// A table in the catalog.
    table: Table,
    /// Table snapshot id that will be queried via this provider.
    snapshot_id: Option<i64>,
    /// A reference-counted arrow `Schema`.
    schema: ArrowSchemaRef,
    /// The catalog that the table belongs to.
    catalog: Option<Arc<dyn Catalog>>,
}

impl IcebergTableProvider {
    pub(crate) fn new(table: Table, schema: ArrowSchemaRef) -> Self {
        IcebergTableProvider {
            table,
            snapshot_id: None,
            schema,
            catalog: None,
        }
    }
    /// Asynchronously tries to construct a new [`IcebergTableProvider`]
    /// using the given client and table name to fetch an actual [`Table`]
    /// in the provided namespace.
    pub(crate) async fn try_new(
        client: Arc<dyn Catalog>,
        namespace: NamespaceIdent,
        name: impl Into<String>,
    ) -> Result<Self> {
        let ident = TableIdent::new(namespace, name.into());
        let table = client.load_table(&ident).await?;

        let schema = Arc::new(schema_to_arrow_schema(table.metadata().current_schema())?);

        Ok(IcebergTableProvider {
            table,
            snapshot_id: None,
            schema,
            catalog: Some(client),
        })
    }

    /// Asynchronously tries to construct a new [`IcebergTableProvider`]
    /// using the given table. Can be used to create a table provider from an existing table regardless of the catalog implementation.
    pub async fn try_new_from_table(table: Table) -> Result<Self> {
        let schema = Arc::new(schema_to_arrow_schema(table.metadata().current_schema())?);
        Ok(IcebergTableProvider {
            table,
            snapshot_id: None,
            schema,
            catalog: None,
        })
    }

    /// Asynchronously tries to construct a new [`IcebergTableProvider`]
    /// using a specific snapshot of the given table. Can be used to create a table provider from an existing table regardless of the catalog implementation.
    pub async fn try_new_from_table_snapshot(table: Table, snapshot_id: i64) -> Result<Self> {
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
            table,
            snapshot_id: Some(snapshot_id),
            schema,
            catalog: None,
        })
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
        Ok(Arc::new(IcebergTableScan::new(
            self.table.clone(),
            self.snapshot_id,
            self.schema.clone(),
            projection,
            filters,
        )))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DFResult<Vec<TableProviderFilterPushDown>> {
        // Push down all filters, as a single source of truth, the scanner will drop the filters which couldn't be push down
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }

    async fn insert_into(
        &self,
        _state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        _insert_op: InsertOp,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if !self
            .table
            .metadata()
            .default_partition_spec()
            .is_unpartitioned()
        {
            // TODO add insert into support for partitioned tables
            return Err(DataFusionError::NotImplemented(
                "IcebergTableProvider::insert_into does not support partitioned tables yet"
                    .to_string(),
            ));
        }

        let Some(catalog) = self.catalog.clone() else {
            return Err(DataFusionError::Execution(
                "Catalog cannot be none for insert_into".to_string(),
            ));
        };

        let write_plan = Arc::new(IcebergWriteExec::new(
            self.table.clone(),
            input,
            self.schema.clone(),
        ));

        // Merge the outputs of write_plan into one so we can commit all files together
        let coalesce_partitions = Arc::new(CoalescePartitionsExec::new(write_plan));

        Ok(Arc::new(IcebergCommitExec::new(
            self.table.clone(),
            catalog,
            coalesce_partitions,
            self.schema.clone(),
        )))
    }
}

#[cfg(test)]
mod tests {
    use datafusion::common::Column;
    use datafusion::prelude::SessionContext;
    use iceberg::TableIdent;
    use iceberg::io::FileIO;
    use iceberg::table::{StaticTable, Table};

    use super::*;

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
        let table_provider = IcebergTableProvider::try_new_from_table(table.clone())
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
        let table_provider =
            IcebergTableProvider::try_new_from_table_snapshot(table.clone(), snapshot_id)
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
        let table_provider = IcebergTableProvider::try_new_from_table(table.clone())
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
