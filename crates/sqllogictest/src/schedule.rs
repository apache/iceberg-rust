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

use anyhow::{Context, anyhow};
use serde::{Deserialize, Serialize};
use toml::{Table as TomlTable, Value};
use tracing::info;

use crate::engine::{EngineRunner, load_engine_runner};

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

    pub async fn from_file<P: AsRef<Path>>(path: P) -> anyhow::Result<Self> {
        let path_str = path.as_ref().to_string_lossy().to_string();
        let content = read_to_string(path)?;
        let toml_value = content.parse::<Value>()?;
        let toml_table = toml_value
            .as_table()
            .ok_or_else(|| anyhow!("Schedule file must be a TOML table"))?;

        let engines = Schedule::parse_engines(toml_table).await?;
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
    ) -> anyhow::Result<HashMap<String, Box<dyn EngineRunner>>> {
        let engines_tbl = table
            .get("engines")
            .with_context(|| "Schedule file must have an 'engines' table")?
            .as_table()
            .ok_or_else(|| anyhow!("'engines' must be a table"))?;

        let mut engines = HashMap::new();

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

            let engine = load_engine_runner(engine_type, cfg_tbl.clone()).await?;

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
        let result = Schedule::parse_engines(&table).await;

        assert!(result.is_err());
    }
}
