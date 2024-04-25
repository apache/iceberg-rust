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

use std::sync::Arc;

use datafusion::datasource::{TableProvider, TableType};
use iceberg::{table::Table, Catalog, NamespaceIdent, Result, TableIdent};

pub(crate) struct IcebergTableProvider {
    _inner: Table,
}

impl IcebergTableProvider {
    pub(crate) async fn try_new(
        client: Arc<dyn Catalog>,
        namespace: NamespaceIdent,
        name: impl Into<String>,
    ) -> Result<Self> {
        let name = name.into();
        let ident = TableIdent::new(namespace, name);
        let table = client.load_table(&ident).await?;

        Ok(IcebergTableProvider { _inner: table })
    }
}

impl TableProvider for IcebergTableProvider {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> datafusion::arrow::datatypes::SchemaRef {
        todo!()
    }

    fn table_type(&self) -> datafusion::datasource::TableType {
        TableType::Base
    }

    fn scan<'life0, 'life1, 'life2, 'life3, 'async_trait>(
        &'life0 self,
        _state: &'life1 datafusion::execution::context::SessionState,
        _projection: Option<&'life2 Vec<usize>>,
        _filters: &'life3 [datafusion::prelude::Expr],
        _limit: Option<usize>,
    ) -> core::pin::Pin<
        Box<
            dyn core::future::Future<
                    Output = datafusion::error::Result<
                        Arc<dyn datafusion::physical_plan::ExecutionPlan>,
                    >,
                > + core::marker::Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        'life2: 'async_trait,
        'life3: 'async_trait,
        Self: 'async_trait,
    {
        todo!()
    }
}
