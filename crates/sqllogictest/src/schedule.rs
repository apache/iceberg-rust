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

use anyhow::anyhow;
use itertools::Itertools;
use toml::{Table, Value};

use crate::engine::{EngineRunner, load_engine};

/// Schedule of engines to run tests.
/// Controls the engine, storage, and catalog being used for the test steps
pub struct Schedule {
    // Map of engine names to engine instances.
    engines: HashMap<String, Box<dyn EngineRunner>>,
    // List of steps to run, each step is a sql file.
    steps: Vec<Step>,
    // catalog: Box<dyn Catalog>,
}

pub struct Step {
    /// Name of engine to execute.
    engine_name: String,
    /// Name of sql file.
    sql: String,
}

impl Schedule {
    pub async fn parse<P: AsRef<Path>>(schedule_def_file: P) -> anyhow::Result<Self> {
        let content = read_to_string(schedule_def_file)?;
        let toml_value = content.parse::<Value>()?;
        let toml_table = toml_value
            .as_table()
            .ok_or_else(|| anyhow::anyhow!("Schedule file must be a TOML table"))?;

        let engines = Schedule::parse_engines(toml_table).await?;
        let steps = Schedule::parse_steps(toml_table).await?;

        Ok(Self { engines, steps })
    }

    async fn parse_engines(
        table: &Table,
    ) -> anyhow::Result<HashMap<String, Box<dyn EngineRunner>>> {
        println!("parsing engine...");
        let engines = table
            .get("engines")
            .ok_or_else(|| anyhow::anyhow!("Schedule file must have an 'engines' table"))?
            .as_table()
            .ok_or_else(|| anyhow::anyhow!("'engines' must be a table"))?;

        let mut result = HashMap::new();
        for (name, engine_config) in engines {
            println!("engine: {name}, config: {engine_config}");
            let engine_configs = engine_config
                .as_table()
                .ok_or_else(|| anyhow::anyhow!("Config of engine {name} is not a table"))?;

            println!("name {name}, engine config {engine_configs}");

            let engine_type = engine_configs
                .get("type")
                .ok_or_else(|| anyhow::anyhow!("Engine {name} doesn't have a 'type' field"))?
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("Engine {name} type must be a string"))?;

            let engine = load_engine(engine_type, engine_configs.clone()).await?;

            result.insert(name.clone(), engine);
        }

        Ok(result)
    }

    async fn parse_steps(table: &Table) -> anyhow::Result<Vec<Step>> {
        let steps = table
            .get("steps")
            .ok_or_else(|| anyhow!("steps not found"))?
            .as_array()
            .ok_or_else(|| anyhow!("steps is not array"))?;

        steps.iter().map(Schedule::parse_step).try_collect()
    }

    fn parse_step(value: &Value) -> anyhow::Result<Step> {
        let t = value
            .as_table()
            .ok_or_else(|| anyhow!("Step must be a table!"))?;

        let engine_name = t
            .get("engine")
            .ok_or_else(|| anyhow!("Property engine is missing in step"))?
            .as_str()
            .ok_or_else(|| anyhow!("Property engine is not a string in step"))?
            .to_string();

        let sql = t
            .get("sql")
            .ok_or_else(|| anyhow!("Property sql is missing in step"))?
            .as_str()
            .ok_or_else(|| anyhow!("Property sqlis not a string in step"))?
            .to_string();

        println!("engine: {engine_name}, sql: {sql}");
        Ok(Step { engine_name, sql })
    }

    pub async fn run(mut self) -> anyhow::Result<()> {
        println!("running steps");

        for step_idx in 0..self.steps.len() {
            self.run_step(step_idx).await?;
        }

        Ok(())
    }

    async fn run_step(&mut self, step_index: usize) -> anyhow::Result<()> {
        println!("running step: {step_index}");

        let step = &self.steps[step_index];

        let engine = self
            .engines
            .get_mut(&step.engine_name)
            .ok_or_else(|| anyhow!("Engine {} not found!", step.engine_name))?;

        let step_sql_path = PathBuf::from(format!(
            "{}/testdata/slts/{}",
            env!("CARGO_MANIFEST_DIR"),
            &step.sql
        ));
        engine.run_slt_file(step_sql_path.as_path()).await?;
        Ok(())
    }
}
