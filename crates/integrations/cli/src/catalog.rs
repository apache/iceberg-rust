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
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;

use anyhow::anyhow;
use datafusion::catalog::{CatalogProvider, CatalogProviderList};
use fs_err::read_to_string;
use iceberg_catalog_glue::{GlueCatalog, GlueCatalogConfig};
use iceberg_catalog_rest::{RestCatalog, RestCatalogConfig};
use iceberg_datafusion::IcebergCatalogProvider;
use toml::{Table as TomlTable, Value};

const CONFIG_NAME_CATALOGS: &str = "catalogs";

#[derive(Debug)]
pub struct IcebergCatalogList {
    catalogs: HashMap<String, Arc<IcebergCatalogProvider>>,
}

impl IcebergCatalogList {
    pub async fn parse(path: &Path) -> anyhow::Result<Self> {
        let toml_table: TomlTable = toml::from_str(&read_to_string(path)?)?;
        Self::parse_table(&toml_table).await
    }

    pub async fn parse_table(configs: &TomlTable) -> anyhow::Result<Self> {
        if let Value::Array(catalogs_config) =
            configs.get(CONFIG_NAME_CATALOGS).ok_or_else(|| {
                anyhow::Error::msg(format!("{CONFIG_NAME_CATALOGS} entry not found in config"))
            })?
        {
            let mut catalogs = HashMap::with_capacity(catalogs_config.len());
            for config in catalogs_config {
                if let Value::Table(table_config) = config {
                    let (name, catalog_provider) =
                        IcebergCatalogList::parse_one(table_config).await?;
                    catalogs.insert(name, catalog_provider);
                } else {
                    return Err(anyhow!("{CONFIG_NAME_CATALOGS} entry must be a table"));
                }
            }
            Ok(Self { catalogs })
        } else {
            Err(anyhow!("{CONFIG_NAME_CATALOGS} must be an array of table!"))
        }
    }

    async fn parse_one(
        config: &TomlTable,
    ) -> anyhow::Result<(String, Arc<IcebergCatalogProvider>)> {
        let name = config
            .get("name")
            .ok_or_else(|| anyhow::anyhow!("name not found for catalog"))?
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("name is not string"))?;

        let r#type = config
            .get("type")
            .ok_or_else(|| anyhow::anyhow!("type not found for catalog"))?
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("type is not string"))?;

        let catalog_config = config
            .get("config")
            .ok_or_else(|| anyhow::anyhow!("config not found for catalog {name}"))?
            .as_table()
            .ok_or_else(|| anyhow::anyhow!("config is not table for catalog {name}"))?;

        let uri = catalog_config
            .get("uri")
            .ok_or_else(|| anyhow::anyhow!("uri not found for catalog {name}"))?
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("uri is not string"))?;

        let warehouse = catalog_config
            .get("warehouse")
            .ok_or_else(|| anyhow::anyhow!("warehouse not found for catalog {name}"))?
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("warehouse is not string for catalog {name}"))?;
        
        let schemas: HashSet<String> = HashSet::from_iter(catalog_config
            .get("schemas")
            .and_then(|schemas| schemas.as_array())
            .into_iter()
            .flatten()
            .filter_map(|value| value.as_str())
            .map(String::from));

        let props_table = catalog_config
            .get("props")
            .ok_or_else(|| anyhow::anyhow!("props not found for catalog {name}"))?
            .as_table()
            .ok_or_else(|| anyhow::anyhow!("props is not table for catalog {name}"))?;

        let mut props = HashMap::with_capacity(props_table.len());
        for (key, value) in props_table {
            let value_str = value
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("props {key} is not string"))?;
            props.insert(key.to_string(), value_str.to_string());
        }
        
        match r#type {
            "rest" => Self::rest_catalog_provider(name.to_string(), uri.to_string(), warehouse.to_string(), props).await,
            "glue" => {
                let catalog_id = catalog_config
                    .get("catalog_id")
                    .ok_or_else(|| anyhow::anyhow!("catalog_id not found for catalog {name}"))?
                    .as_str()
                    .ok_or_else(|| anyhow::anyhow!("catalog_id is not string for catalog {name}"))?;
                Self::glue_catalog_provider(name.to_string(), uri.to_string(), catalog_id.to_string(), warehouse.to_string(), schemas, props).await
            },
            _ => Err(anyhow::anyhow!("Unsupported catalog type: {}", r#type)),
        }
    }
    
    async fn rest_catalog_provider(
        name: String, 
        uri: String,
        warehouse: String, 
        props: HashMap<String, String>
    ) -> anyhow::Result<(String, Arc<IcebergCatalogProvider>)> {
        let rest_catalog_config = RestCatalogConfig::builder()
            .uri(uri)
            .warehouse(warehouse)
            .props(props)
            .build();
        
        Ok((
            name.to_string(),
            Arc::new(
                IcebergCatalogProvider::try_new(Arc::new(RestCatalog::new(rest_catalog_config)))
                    .await?,
            ),
        ))
    }
    
    async fn glue_catalog_provider(
        name: String,
        uri: String,
        catalog_id: String,
        warehouse: String,
        schemas: HashSet<String>,
        props: HashMap<String, String>
    ) -> anyhow::Result<(String, Arc<IcebergCatalogProvider>)> {
        let glue_catalog_config = GlueCatalogConfig::builder()
            .uri(uri.to_string())
            .catalog_id(catalog_id.to_string())
            .warehouse(warehouse.to_string())
            .props(props)
            .build();

        Ok((
            name.to_string(),
            Arc::new(
                IcebergCatalogProvider::try_new_with_schemas(
                    Arc::new(GlueCatalog::new(glue_catalog_config).await?),
                    schemas)
                    .await?,
            ),
        ))
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
