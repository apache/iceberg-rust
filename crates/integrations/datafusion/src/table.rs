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

use std::{any::Any, sync::Arc};

use async_trait::async_trait;
use datafusion::error::Result as DFResult;
use datafusion::{
    arrow::datatypes::SchemaRef as ArrowSchemaRef,
    datasource::{TableProvider, TableType},
    execution::context,
    logical_expr::Expr,
    physical_plan::ExecutionPlan,
};
use iceberg::{
    arrow::schema_to_arrow_schema, table::Table, Catalog, NamespaceIdent, Result, TableIdent,
};

use crate::physical_plan::scan::IcebergTableScan;

/// Represents a [`TableProvider`] for the Iceberg [`Catalog`],
/// managing access to a [`Table`].
pub(crate) struct IcebergTableProvider {
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
        _state: &context::SessionState,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(IcebergTableScan::new(
            self.table.clone(),
            self.schema.clone(),
        )))
    }
}
