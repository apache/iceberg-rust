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
use datafusion::catalog::{schema::SchemaProvider, CatalogProvider};
use iceberg::{Catalog, NamespaceIdent, Result};

use crate::schema::IcebergSchemaProvider;

pub struct IcebergCatalogProvider {
    schemas: DashMap<String, Arc<dyn SchemaProvider>>,
}

impl IcebergCatalogProvider {
    pub async fn try_new(client: Arc<dyn Catalog>) -> Result<Self> {
        let schema_names: Vec<String> = client
            .list_namespaces(None)
            .await?
            .iter()
            .flat_map(|ns| ns.as_ref().clone())
            .collect();

        let mut schemas = Vec::new();
        for name in schema_names {
            let provider =
                IcebergSchemaProvider::try_new(client.clone(), &NamespaceIdent::new(name.clone()))
                    .await?;
            let provider = Arc::new(provider) as Arc<dyn SchemaProvider>;

            schemas.push((name, provider))
        }

        Ok(IcebergCatalogProvider {
            schemas: schemas.into_iter().collect(),
        })
    }
}

impl CatalogProvider for IcebergCatalogProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        self.schemas.iter().map(|c| c.key().clone()).collect()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        self.schemas.get(name).map(|c| c.value().clone())
    }
}
