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

#[derive(Deserialize, Debug)]
#[serde(tag = "type")]
pub enum CatalogConfig {
    #[serde(rename = "rest")]
    Rest(RestCatalogConfig),
}

#[derive(Deserialize, Debug)]
pub struct CatalogConfigDef {
    name: String,
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
}

#[derive(Debug)]
pub struct IcebergCatalogList {
    catalogs: HashMap<String, Arc<IcebergCatalogProvider>>,
}

impl IcebergCatalogList {
    pub async fn parse(path: &Path) -> anyhow::Result<Self> {
        let root_config: TomlTable = toml::from_str(&read_to_string(path)?)?;
        let catalog_configs = Self::parse_catalogs(&root_config)?;

        let mut catalogs = HashMap::with_capacity(catalog_configs.len());
        for (name, config) in catalog_configs {
            let catalog = config.try_into_catalog().await?;
            catalogs.insert(name, catalog);
        }

        Ok(Self { catalogs })
    }

    fn parse_catalog(table: TomlTable) -> anyhow::Result<CatalogConfigDef> {
        table
            .try_into::<CatalogConfigDef>()
            .with_context(|| "Failed to parse catalog config".to_string())
    }

    pub(crate) fn parse_catalogs(root: &TomlTable) -> anyhow::Result<HashMap<String,
        CatalogConfigDef>> {
        if let Value::Array(catalog_configs) = root.get(CONFIG_NAME_CATALOGS).ok_or_else(|| {
            anyhow::Error::msg(format!("{CONFIG_NAME_CATALOGS} entry not found in config"))
        })? {
            let mut catalogs = HashMap::with_capacity(catalog_configs.len());
            for value in catalog_configs {
                if let Value::Table(table) = value {
                    let catalog = Self::parse_catalog(table.clone())?;
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
    use fs_err::read_to_string;
    use toml::{Table as TomlTable};
    use iceberg_datafusion::IcebergCatalogProvider;

    #[test]
    fn test_parse_config() {
        let config_file_path = format!(
            "{}/testdata/catalogs.toml",
            env!("CARGO_MANIFEST_DIR"));

        let root_config: TomlTable = toml::from_str(&read_to_string(config_file_path).unwrap())
            .unwrap();

        let catalog_configs = IcebergCatalogProvider::parse_catalogs(&root_config).unwrap();
    }
}
