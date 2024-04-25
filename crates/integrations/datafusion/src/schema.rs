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
use dashmap::DashMap;
use datafusion::{catalog::schema::SchemaProvider, datasource::TableProvider};
use futures::future::try_join_all;
use iceberg::{Catalog, NamespaceIdent, Result};

use crate::table::IcebergTableProvider;

/// Represents a [`SchemaProvider`] for the Iceberg [`Catalog`], managing
/// access to table providers within a specific namespace.
pub(crate) struct IcebergSchemaProvider {
    /// A concurrent `HashMap` where keys are table names
    /// and values are dynamic references to objects implementing the
    /// [`TableProvider`] trait.
    tables: DashMap<String, Arc<dyn TableProvider>>,
}

impl IcebergSchemaProvider {
    /// Asynchronously tries to construct a new [`IcebergSchemaProvider`]
    /// using the given client to fetch and initialize table providers for
    /// the provided namespace in the Iceberg [`Catalog`].
    ///
    /// This method retrieves a list of table names
    /// attempts to create a table provider for each table name, and
    /// collects these providers into a concurrent `HashMap`.
    pub(crate) async fn try_new(
        client: Arc<dyn Catalog>,
        namespace: NamespaceIdent,
    ) -> Result<Self> {
        let table_names: Vec<_> = client
            .list_tables(&namespace)
            .await?
            .iter()
            .map(|tbl| tbl.name().to_string())
            .collect();

        let providers = try_join_all(
            table_names
                .iter()
                .map(|name| IcebergTableProvider::try_new(client.clone(), namespace.clone(), name))
                .collect::<Vec<_>>(),
        )
        .await?;

        let tables: Vec<_> = table_names
            .into_iter()
            .zip(providers.into_iter())
            .map(|(name, provider)| {
                let provider = Arc::new(provider) as Arc<dyn TableProvider>;
                (name, provider)
            })
            .collect();

        Ok(IcebergSchemaProvider {
            tables: tables.into_iter().collect(),
        })
    }
}

#[async_trait]
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

    async fn table(&self, name: &str) -> datafusion::error::Result<Option<Arc<dyn TableProvider>>> {
        let table = self.tables.get(name).map(|c| c.value().clone());
        Ok(table)
    }
}
