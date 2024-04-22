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

use dashmap::DashMap;
use datafusion::{
    catalog::schema::SchemaProvider, datasource::TableProvider, error::DataFusionError,
};
use futures::{future::try_join_all, FutureExt};
use iceberg::{Catalog, NamespaceIdent, Result};

use crate::table::IcebergTableProvider;

pub(crate) struct IcebergSchemaProvider {
    tables: DashMap<String, Arc<dyn TableProvider>>,
}

impl IcebergSchemaProvider {
    pub(crate) async fn try_new(
        client: Arc<dyn Catalog>,
        namespace: NamespaceIdent,
    ) -> Result<Self> {
        let table_names: Vec<String> = client
            .list_tables(&namespace)
            .await?
            .iter()
            .map(|t| t.name().to_owned())
            .collect();

        let futures: Vec<_> = table_names
            .iter()
            .map(|name| IcebergTableProvider::try_new(client.clone(), namespace.clone(), name))
            .collect();

        let providers = try_join_all(futures).await?;

        let mut tables = Vec::new();
        for (name, provider) in table_names.into_iter().zip(providers.into_iter()) {
            let provider = Arc::new(provider) as Arc<dyn TableProvider>;
            tables.push((name, provider));
        }

        Ok(IcebergSchemaProvider {
            tables: tables.into_iter().collect(),
        })
    }
}

impl SchemaProvider for IcebergSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        self.tables.iter().map(|c| c.key().clone()).collect()
    }

    fn table_exist(&self, name: &str) -> bool {
        self.tables.get(name).is_some()
    }

    fn table<'life0, 'life1, 'async_trait>(
        &'life0 self,
        name: &'life1 str,
    ) -> core::pin::Pin<
        Box<
            dyn core::future::Future<
                    Output = datafusion::error::Result<
                        Option<Arc<dyn TableProvider>>,
                        DataFusionError,
                    >,
                > + core::marker::Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        async move {
            let table = self.tables.get(name).map(|c| c.value().clone());
            Ok(table)
        }
        .boxed()
    }
}
