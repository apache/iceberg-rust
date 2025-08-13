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

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use iceberg::{Catalog, CatalogBuilder, Error, ErrorKind, Result};
use iceberg_catalog_rest::RestCatalogBuilder;

#[async_trait]
pub trait BoxedCatalogBuilder {
    async fn load(
        self: Box<Self>,
        name: String,
        props: HashMap<String, String>,
    ) -> Result<Arc<dyn Catalog>>;
}

#[async_trait]
impl<T: CatalogBuilder + 'static> BoxedCatalogBuilder for T {
    async fn load(
        self: Box<Self>,
        name: String,
        props: HashMap<String, String>,
    ) -> Result<Arc<dyn Catalog>> {
        let builder = *self;
        Ok(Arc::new(builder.load(name, props).await?) as Arc<dyn Catalog>)
    }
}

pub fn load(r#type: &str) -> Result<Box<dyn BoxedCatalogBuilder>> {
    match r#type {
        "rest" => Ok(Box::new(RestCatalogBuilder::default()) as Box<dyn BoxedCatalogBuilder>),
        // "glue" => Ok(Box::new(GlueCatalogBuilder::default()) as Box<dyn BoxedCatalogBuilder>),
        _ => Err(Error::new(
            ErrorKind::FeatureUnsupported,
            format!("Unsupported catalog type: {}", r#type),
        )),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use iceberg_catalog_rest::REST_CATALOG_PROP_URI;

    use crate::load;

    #[tokio::test]
    async fn test_load_rest_catalog() {
        let catalog_loader = load("rest").unwrap();
        let catalog = catalog_loader
            .load(
                "rest".to_string(),
                HashMap::from([
                    (
                        REST_CATALOG_PROP_URI.to_string(),
                        "http://localhost:8080".to_string(),
                    ),
                    ("key".to_string(), "value".to_string()),
                ]),
            )
            .await;

        assert!(catalog.is_ok());
    }
}
