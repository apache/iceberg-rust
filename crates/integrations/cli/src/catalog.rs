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
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use anyhow::{anyhow, Context};
use datafusion::catalog::{CatalogProvider, CatalogProviderList};
use fs_err::read_to_string;
use iceberg_catalog_rest::{RestCatalog, RestCatalogConfig};
use iceberg_datafusion::IcebergCatalogProvider;
use serde::Deserialize;
use toml::{Table as TomlTable, Value};

const CONFIG_NAME_CATALOGS: &str = "catalogs";

#[derive(Deserialize, Debug, PartialEq)]
#[serde(tag = "type")]
pub enum CatalogConfig {
    #[serde(rename = "rest")]
    Rest(RestCatalogConfig),
}

#[derive(Deserialize, Debug, PartialEq)]
pub struct CatalogConfigDef {
    name: String,
    #[serde(flatten)]
    config: CatalogConfig,
}

impl CatalogConfigDef {
    async fn try_into_catalog(self) -> anyhow::Result<Arc<IcebergCatalogProvider>> {
        match self.config {
            CatalogConfig::Rest(config) => {
                let catalog = RestCatalog::new(config);
                Ok(Arc::new(
                    IcebergCatalogProvider::try_new(Arc::new(catalog)).await?,
                ))
            }
        }
    }

    pub fn parse(root: &TomlTable) -> anyhow::Result<HashMap<String, Self>> {
        if let Value::Array(catalog_configs) = root.get(CONFIG_NAME_CATALOGS).ok_or_else(|| {
            anyhow::Error::msg(format!("{CONFIG_NAME_CATALOGS} entry not found in config"))
        })? {
            let mut catalogs = HashMap::with_capacity(catalog_configs.len());
            for value in catalog_configs {
                if let Value::Table(table) = value {
                    let catalog: CatalogConfigDef = table.clone().try_into()?;
                    if catalogs.contains_key(&catalog.name) {
                        return Err(anyhow!("Duplicate catalog name: {}", catalog.name));
                    }
                    catalogs.insert(catalog.name.clone(), catalog);
                } else {
                    return Err(anyhow!("{CONFIG_NAME_CATALOGS} entry must be a table"));
                }
            }
            Ok(catalogs)
        } else {
            Err(anyhow!("{CONFIG_NAME_CATALOGS} must be an array of table!"))
        }
    }
}

impl TryFrom<TomlTable> for CatalogConfigDef {
    type Error = anyhow::Error;

    fn try_from(table: TomlTable) -> Result<Self, Self::Error> {
        table
            .try_into::<CatalogConfigDef>()
            .with_context(|| "Failed to parse catalog config".to_string())
    }
}

#[derive(Debug)]
pub struct IcebergCatalogList {
    catalogs: HashMap<String, Arc<IcebergCatalogProvider>>,
}

impl IcebergCatalogList {
    pub async fn parse(path: &Path) -> anyhow::Result<Self> {
        let root_config: TomlTable = toml::from_str(&read_to_string(path)?)?;
        let catalog_configs = CatalogConfigDef::parse(&root_config)?;

        let mut catalogs = HashMap::with_capacity(catalog_configs.len());
        for (name, config) in catalog_configs {
            let catalog = config.try_into_catalog().await?;
            catalogs.insert(name, catalog);
        }

        Ok(Self { catalogs })
    }
}

impl CatalogProviderList for IcebergCatalogList {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn register_catalog(
        &self,
        _name: String,
        _catalog: Arc<dyn CatalogProvider>,
    ) -> Option<Arc<dyn CatalogProvider>> {
        tracing::error!("Registering catalog is not supported yet");
        None
    }

    fn catalog_names(&self) -> Vec<String> {
        self.catalogs.keys().cloned().collect()
    }

    fn catalog(&self, name: &str) -> Option<Arc<dyn CatalogProvider>> {
        self.catalogs
            .get(name)
            .map(|c| c.clone() as Arc<dyn CatalogProvider>)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use fs_err::read_to_string;
    use iceberg_catalog_rest::RestCatalogConfig;
    use toml::Table as TomlTable;

    use crate::{CatalogConfig, CatalogConfigDef};

    #[test]
    fn test_parse_config() {
        let config_file_path = format!("{}/testdata/catalogs.toml", env!("CARGO_MANIFEST_DIR"));

        let root_config: TomlTable =
            toml::from_str(&read_to_string(config_file_path).unwrap()).unwrap();

        let catalog_configs = CatalogConfigDef::parse(&root_config).unwrap();

        assert_eq!(catalog_configs.len(), 2);

        let catalog1 = catalog_configs.get("demo").unwrap();
        let expected_catalog1 = CatalogConfigDef {
            name: "demo".to_string(),
            config: CatalogConfig::Rest(
                RestCatalogConfig::builder()
                    .uri("http://localhost:8080".to_string())
                    .warehouse("s3://iceberg-demo".to_string())
                    .props(HashMap::from([
                        (
                            "s3.endpoint".to_string(),
                            "http://localhost:9000".to_string(),
                        ),
                        ("s3.access_key_id".to_string(), "admin".to_string()),
                    ]))
                    .build(),
            ),
        };

        assert_eq!(catalog1, &expected_catalog1);

        let catalog2 = catalog_configs.get("demo2").unwrap();
        let expected_catalog2 = CatalogConfigDef {
            name: "demo2".to_string(),
            config: CatalogConfig::Rest(
                RestCatalogConfig::builder()
                    .uri("http://localhost2:8080".to_string())
                    .warehouse("s3://iceberg-demo2".to_string())
                    .props(HashMap::from([
                        (
                            "s3.endpoint".to_string(),
                            "http://localhost2:9090".to_string(),
                        ),
                        ("s3.access_key_id".to_string(), "admin2".to_string()),
                    ]))
                    .build(),
            ),
        };

        assert_eq!(catalog2, &expected_catalog2);
    }
}
