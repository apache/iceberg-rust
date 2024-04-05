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

use crate::datafusion::schema::IcebergSchemaProvider;

struct IcebergCatalogProvider {
    schemas: DashMap<String, Arc<dyn SchemaProvider>>,
}

impl IcebergCatalogProvider {
    async fn try_new(client: Arc<dyn Catalog>) -> Result<Self> {
        let schema_names: Vec<String> = client
            .list_namespaces(None)
            .await?
            .iter()
            .map(|ns| ns.as_ref().clone())
            .flatten()
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

#[cfg(test)]
mod tests {
    use super::*;

    use std::{collections::HashMap, sync::Arc};

    use datafusion::execution::context::SessionContext;
    use iceberg::{
        io::{S3_ACCESS_KEY_ID, S3_ENDPOINT, S3_REGION, S3_SECRET_ACCESS_KEY},
        Result,
    };
    use iceberg_catalog_hms::{HmsCatalog, HmsCatalogConfig, HmsThriftTransport};

    fn create_hive_client() -> Result<HmsCatalog> {
        let props = HashMap::from([
            (
                S3_ENDPOINT.to_string(),
                format!("http://{}:{}", "0.0.0.0", "9000"),
            ),
            (S3_ACCESS_KEY_ID.to_string(), "minioadmin".to_string()),
            (S3_SECRET_ACCESS_KEY.to_string(), "minioadmin".to_string()),
            (S3_REGION.to_string(), "us-east-1".to_string()),
        ]);

        let config = HmsCatalogConfig::builder()
            .address("0.0.0.0:9083".to_string())
            .warehouse("s3a://transformed/dwh".to_string())
            .thrift_transport(HmsThriftTransport::Buffered)
            .props(props)
            .build();

        HmsCatalog::new(config)
    }

    #[tokio::test]
    async fn test_schema_provider() -> Result<()> {
        let client = Arc::new(create_hive_client()?);
        let catalog = Arc::new(IcebergCatalogProvider::try_new(client).await?);

        let ctx = SessionContext::new();
        ctx.register_catalog("hive", catalog);

        let provider = ctx.catalog("hive").unwrap();
        let result = provider.schema_names();

        println!("{:?}", result);

        Ok(())
    }
}
