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
use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datafusion::catalog::Session;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{BinaryExpr, Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::table::Table;
use iceberg::{Catalog, NamespaceIdent, Result, TableIdent};

use crate::physical_plan::scan::IcebergTableScan;

/// Represents a [`TableProvider`] for the Iceberg [`Catalog`],
/// managing access to a [`Table`].
pub struct IcebergTableProvider {
    /// A table in the catalog.
    table: Table,
    /// A reference-counted arrow `Schema`.
    schema: ArrowSchemaRef,
}

impl IcebergTableProvider {
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

        Ok(IcebergTableProvider { table, schema })
    }

    /// Asynchronously tries to construct a new [`IcebergTableProvider`]
    /// using the given table. Can be used to create a table provider from an existing table regardless of the catalog implementation.
    pub async fn try_new_from_table(table: Table) -> Result<Self> {
        let schema = Arc::new(schema_to_arrow_schema(table.metadata().current_schema())?);
        Ok(IcebergTableProvider { table, schema })
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
        let filter_support = filters
            .iter()
            .map(|e| match e {
                Expr::BinaryExpr(BinaryExpr { .. }) => TableProviderFilterPushDown::Inexact,
                _ => TableProviderFilterPushDown::Unsupported,
            })
            .collect::<Vec<TableProviderFilterPushDown>>();

        Ok(filter_support)
    }
}

#[cfg(test)]
mod tests {
    use iceberg::io::FileIO;
    use iceberg::table::{StaticTable, Table};
    use iceberg::TableIdent;

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
        let index_of_z = table_provider.schema().index_of("z").unwrap();
        assert_eq!(index_of_z, 2);
        let fields_num = table_provider.schema().fields().len();
        assert_eq!(fields_num, 3);
    }
}
