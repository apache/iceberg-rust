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
use std::fs::read_to_string;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, anyhow};
use iceberg::Catalog;
use serde::{Deserialize, Serialize};
use toml::{Table as TomlTable, Value};
use tracing::info;

use crate::engine::{EngineRunner, load_engine_runner};

/// Catalog configuration parsed from TOML
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CatalogConfig {
    /// Catalog type (e.g., "memory", "rest", "glue", "sql")
    pub r#type: String,
    /// Additional properties passed to the catalog builder
    #[serde(flatten)]
    pub properties: HashMap<String, String>,
}

/// Registry for managing catalog instances with lazy loading and sharing
pub struct CatalogRegistry {
    /// Cached catalog instances by name
    catalogs: HashMap<String, Arc<dyn Catalog>>,
}

impl CatalogRegistry {
    /// Create a new empty catalog registry
    pub fn new() -> Self {
        Self {
            catalogs: HashMap::new(),
        }
    }

    /// Get or create a catalog instance
    pub async fn get_or_create_catalog(
        &mut self,
        name: &str,
        config: &CatalogConfig,
    ) -> anyhow::Result<Arc<dyn Catalog>> {
        if let Some(catalog) = self.catalogs.get(name) {
            return Ok(Arc::clone(catalog));
        }

        // Load catalog using catalog-loader
        use iceberg_catalog_loader::CatalogLoader;
        let catalog = CatalogLoader::from(config.r#type.as_str())
            .load(name.to_string(), config.properties.clone())
            .await
            .with_context(|| format!("Failed to load catalog '{}' of type '{}'", name, config.r#type))?;

        self.catalogs.insert(name.to_string(), Arc::clone(&catalog));
        Ok(catalog)
    }

    /// Get all catalog names
    pub fn catalog_names(&self) -> Vec<String> {
        self.catalogs.keys().cloned().collect()
    }
}

pub struct Schedule {
    /// Engine names to engine instances
    engines: HashMap<String, Box<dyn EngineRunner>>,
    /// List of test steps to run
    steps: Vec<Step>,
    /// Path of the schedule file
    schedule_file: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Step {
    /// Engine name
    engine: String,
    /// Stl file path
    slt: String,
}

impl Schedule {
    pub fn new(
        engines: HashMap<String, Box<dyn EngineRunner>>,
        steps: Vec<Step>,
        schedule_file: String,
    ) -> Self {
        Self {
            engines,
            steps,
            schedule_file,
        }
    }

    /// Parse catalogs from TOML table
    async fn parse_catalogs(
        table: &TomlTable,
    ) -> anyhow::Result<HashMap<String, CatalogConfig>> {
        let catalogs_tbl = match table.get("catalogs") {
            Some(val) => val
                .as_table()
                .ok_or_else(|| anyhow!("'catalogs' must be a table"))?,
            None => return Ok(HashMap::new()), // catalogs 是可选的
        };

        let mut catalogs = HashMap::new();

        for (name, catalog_val) in catalogs_tbl {
            let cfg: CatalogConfig = catalog_val
                .clone()
                .try_into()
                .with_context(|| format!("Failed to parse catalog '{name}'"))?;

            if catalogs.insert(name.clone(), cfg).is_some() {
                return Err(anyhow!("Duplicate catalog '{name}'"));
            }
        }

        Ok(catalogs)
    }

    pub async fn from_file<P: AsRef<Path>>(path: P) -> anyhow::Result<Self> {
        let path_str = path.as_ref().to_string_lossy().to_string();
        let content = read_to_string(path)?;
        let toml_value = content.parse::<Value>()?;
        let toml_table = toml_value
            .as_table()
            .ok_or_else(|| anyhow!("Schedule file must be a TOML table"))?;

        // 先解析 catalogs
        let catalogs = Schedule::parse_catalogs(toml_table).await?;

        // 再解析 engines，传入 catalogs
        let engines = Schedule::parse_engines(toml_table, &catalogs).await?;

        let steps = Schedule::parse_steps(toml_table)?;

        Ok(Self::new(engines, steps, path_str))
    }

    pub async fn run(mut self) -> anyhow::Result<()> {
        info!("Starting test run with schedule: {}", self.schedule_file);

        for (idx, step) in self.steps.iter().enumerate() {
            info!(
                "Running step {}/{}, using engine {}, slt file path: {}",
                idx + 1,
                self.steps.len(),
                &step.engine,
                &step.slt
            );

            let engine = self
                .engines
                .get_mut(&step.engine)
                .ok_or_else(|| anyhow!("Engine {} not found", step.engine))?;

            let step_sql_path = PathBuf::from(format!(
                "{}/testdata/slts/{}",
                env!("CARGO_MANIFEST_DIR"),
                &step.slt
            ));

            engine.run_slt_file(&step_sql_path).await?;

            info!(
                "Completed step {}/{}, engine {}, slt file path: {}",
                idx + 1,
                self.steps.len(),
                &step.engine,
                &step.slt
            );
        }
        Ok(())
    }

    async fn parse_engines(
        table: &TomlTable,
        catalog_configs: &HashMap<String, CatalogConfig>,
    ) -> anyhow::Result<HashMap<String, Box<dyn EngineRunner>>> {
        let engines_tbl = table
            .get("engines")
            .with_context(|| "Schedule file must have an 'engines' table")?
            .as_table()
            .ok_or_else(|| anyhow!("'engines' must be a table"))?;

        let mut engines = HashMap::new();
        let mut catalog_registry = CatalogRegistry::new();

        for (name, engine_val) in engines_tbl {
            let cfg_tbl = engine_val
                .as_table()
                .ok_or_else(|| anyhow!("Config of engine '{name}' is not a table"))?
                .clone();

            let engine_type = cfg_tbl
                .get("type")
                .ok_or_else(|| anyhow::anyhow!("Engine {name} doesn't have a 'type' field"))?
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("Engine {name} type must be a string"))?;

            // 获取 catalog 引用（如果配置了）
            let catalog = if let Some(catalog_name) = cfg_tbl.get("catalog") {
                let catalog_name = catalog_name
                    .as_str()
                    .ok_or_else(|| anyhow!("Engine {name} catalog must be a string"))?;

                let catalog_config = catalog_configs
                    .get(catalog_name)
                    .ok_or_else(|| anyhow!("Catalog '{catalog_name}' not found"))?;

                Some(catalog_registry.get_or_create_catalog(catalog_name, catalog_config).await?)
            } else {
                None
            };

            let engine = load_engine_runner(engine_type, cfg_tbl.clone(), catalog).await?;

            if engines.insert(name.clone(), engine).is_some() {
                return Err(anyhow!("Duplicate engine '{name}'"));
            }
        }

        Ok(engines)
    }

    fn parse_steps(table: &TomlTable) -> anyhow::Result<Vec<Step>> {
        let steps_val = table
            .get("steps")
            .with_context(|| "Schedule file must have a 'steps' array")?;

        let steps: Vec<Step> = steps_val
            .clone()
            .try_into()
            .with_context(|| "Failed to deserialize steps")?;

        Ok(steps)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use toml::Table as TomlTable;

    use crate::schedule::Schedule;

    #[test]
    fn test_parse_steps() {
        let input = r#"
            [[steps]]
            engine = "datafusion"
            slt = "test.slt"

            [[steps]]
            engine = "spark"
            slt = "test2.slt"
        "#;

        let tbl: TomlTable = toml::from_str(input).unwrap();
        let steps = Schedule::parse_steps(&tbl).unwrap();

        assert_eq!(steps.len(), 2);
        assert_eq!(steps[0].engine, "datafusion");
        assert_eq!(steps[0].slt, "test.slt");
        assert_eq!(steps[1].engine, "spark");
        assert_eq!(steps[1].slt, "test2.slt");
    }

    #[test]
    fn test_parse_steps_empty() {
        let input = r#"
            [[steps]]
        "#;

        let tbl: TomlTable = toml::from_str(input).unwrap();
        let steps = Schedule::parse_steps(&tbl);

        assert!(steps.is_err());
    }

    #[tokio::test]
    async fn test_parse_engines_invalid_table() {
        let toml_content = r#"
            engines = "not_a_table"
        "#;

        let table: TomlTable = toml::from_str(toml_content).unwrap();
        let catalogs = HashMap::new();
        let result = Schedule::parse_engines(&table, &catalogs).await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_parse_catalogs() {
        let input = r#"
            [catalogs.memory_catalog]
            type = "memory"
            warehouse = "memory://test"

            [catalogs.rest_catalog]
            type = "rest"
            uri = "http://localhost:8181"
            warehouse = "s3://my-bucket/warehouse"
            credential = "client_credentials"
            token = "xxx"

            [catalogs.sql_catalog]
            type = "sql"
            uri = "postgresql://user:pass@localhost/iceberg"
            warehouse = "s3://my-bucket/warehouse"
            sql_bind_style = "DollarNumeric"
        "#;

        let tbl: TomlTable = toml::from_str(input).unwrap();
        let catalogs = Schedule::parse_catalogs(&tbl).await.unwrap();

        assert_eq!(catalogs.len(), 3);
        assert!(catalogs.contains_key("memory_catalog"));
        assert!(catalogs.contains_key("rest_catalog"));
        assert!(catalogs.contains_key("sql_catalog"));

        // 验证memory catalog配置
        let memory_cfg = &catalogs["memory_catalog"];
        assert_eq!(memory_cfg.r#type, "memory");
        assert_eq!(memory_cfg.properties.get("warehouse").unwrap(), "memory://test");

        // 验证rest catalog配置
        let rest_cfg = &catalogs["rest_catalog"];
        assert_eq!(rest_cfg.r#type, "rest");
        assert_eq!(rest_cfg.properties.get("uri").unwrap(), "http://localhost:8181");
        assert_eq!(rest_cfg.properties.get("warehouse").unwrap(), "s3://my-bucket/warehouse");
        assert_eq!(rest_cfg.properties.get("credential").unwrap(), "client_credentials");
        assert_eq!(rest_cfg.properties.get("token").unwrap(), "xxx");
    }

    #[tokio::test]
    async fn test_parse_catalogs_optional() {
        let input = r#"
            [engines.df]
            type = "datafusion"
        "#;

        let tbl: TomlTable = toml::from_str(input).unwrap();
        let catalogs = Schedule::parse_catalogs(&tbl).await.unwrap();

        assert_eq!(catalogs.len(), 0);
    }


    #[tokio::test]
    async fn test_parse_catalogs_invalid_type() {
        let input = r#"
            [catalogs.invalid]
            warehouse = "memory://test"
        "#;

        let tbl: TomlTable = toml::from_str(input).unwrap();
        let result = Schedule::parse_catalogs(&tbl).await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_engine_with_catalog_reference() {
        let input = r#"
            [catalogs.shared_catalog]
            type = "memory"
            warehouse = "memory://shared"

            [engines.df1]
            type = "datafusion"
            catalog = "shared_catalog"

            [engines.df2]
            type = "datafusion"
            catalog = "shared_catalog"
        "#;

        let tbl: TomlTable = toml::from_str(input).unwrap();
        let catalog_configs = Schedule::parse_catalogs(&tbl).await.unwrap();
        let engines = Schedule::parse_engines(&tbl, &catalog_configs).await.unwrap();

        assert_eq!(engines.len(), 2);
        assert!(engines.contains_key("df1"));
        assert!(engines.contains_key("df2"));
    }

    #[tokio::test]
    async fn test_engine_with_missing_catalog() {
        let input = r#"
            [engines.df]
            type = "datafusion"
            catalog = "nonexistent"
        "#;

        let tbl: TomlTable = toml::from_str(input).unwrap();
        let catalog_configs = Schedule::parse_catalogs(&tbl).await.unwrap();
        let result = Schedule::parse_engines(&tbl, &catalog_configs).await;

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Catalog 'nonexistent' not found"));
    }

    #[tokio::test]
    async fn test_engine_without_catalog() {
        let input = r#"
            [engines.df]
            type = "datafusion"
        "#;

        let tbl: TomlTable = toml::from_str(input).unwrap();
        let catalog_configs = Schedule::parse_catalogs(&tbl).await.unwrap();
        let engines = Schedule::parse_engines(&tbl, &catalog_configs).await.unwrap();

        assert_eq!(engines.len(), 1);
        assert!(engines.contains_key("df"));
    }

    #[tokio::test]
    async fn test_catalog_config_validation() {
        // 测试catalog配置必须有type字段
        let input = r#"
            [catalogs.invalid]
            warehouse = "memory://test"
        "#;

        let tbl: TomlTable = toml::from_str(input).unwrap();
        let result = Schedule::parse_catalogs(&tbl).await;

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Failed to parse catalog"));
    }

    #[tokio::test]
    async fn test_catalog_sharing_across_engines() {
        let input = r#"
            [catalogs.shared]
            type = "memory"
            warehouse = "memory://shared"

            [engines.engine1]
            type = "datafusion"
            catalog = "shared"

            [engines.engine2]
            type = "datafusion"
            catalog = "shared"

            [engines.engine3]
            type = "datafusion"
            catalog = "shared"
        "#;

        let tbl: TomlTable = toml::from_str(input).unwrap();
        let catalog_configs = Schedule::parse_catalogs(&tbl).await.unwrap();
        let engines = Schedule::parse_engines(&tbl, &catalog_configs).await.unwrap();

        assert_eq!(engines.len(), 3);
        assert!(engines.contains_key("engine1"));
        assert!(engines.contains_key("engine2"));
        assert!(engines.contains_key("engine3"));
    }

    #[tokio::test]
    async fn test_mixed_catalog_usage() {
        // 测试部分引擎使用自定义catalog，部分使用默认catalog
        let input = r#"
            [catalogs.custom]
            type = "memory"
            warehouse = "memory://custom"

            [engines.with_catalog]
            type = "datafusion"
            catalog = "custom"

            [engines.without_catalog]
            type = "datafusion"
        "#;

        let tbl: TomlTable = toml::from_str(input).unwrap();
        let catalog_configs = Schedule::parse_catalogs(&tbl).await.unwrap();
        let engines = Schedule::parse_engines(&tbl, &catalog_configs).await.unwrap();

        assert_eq!(engines.len(), 2);
        assert!(engines.contains_key("with_catalog"));
        assert!(engines.contains_key("without_catalog"));
    }

    #[tokio::test]
    async fn test_catalog_properties_preservation() {
        let input = r#"
            [catalogs.complex]
            type = "memory"
            warehouse = "memory://complex"
            some_property = "value"
            another_property = "42"
        "#;

        let tbl: TomlTable = toml::from_str(input).unwrap();
        let catalogs = Schedule::parse_catalogs(&tbl).await.unwrap();

        assert_eq!(catalogs.len(), 1);
        let config = &catalogs["complex"];
        assert_eq!(config.r#type, "memory");
        assert_eq!(config.properties.get("warehouse").unwrap(), "memory://complex");
        assert_eq!(config.properties.get("some_property").unwrap(), "value");
        assert_eq!(config.properties.get("another_property").unwrap(), "42");
    }
}
