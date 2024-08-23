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
use std::path::Path;
use anyhow::anyhow;
use itertools::Itertools;
use toml::{Table, Value};
use toml::value::Array;
use crate::engine::Engine;

/// Schedule of engines to run tests.
pub struct Schedule {
    /// Map of engine names to engine instances.
    engines: HashMap<String, Engine>,
    /// List of steps to run, each step is a sql file.
    steps: Vec<Step>,
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
        let toml_value = content.parse::<Value>()?.as_table()
            .ok_or_else(|| anyhow::anyhow!("Schedule file must be a TOML table"))?;

        let engines = Schedule::parse_engines(toml_value).await?;
        let steps  = Schedule::parse_steps(toml_value).await?;

        Ok(Self {
            engines,
            steps
        })
    }

    async fn parse_engines(table: &Table) -> anyhow::Result<HashMap<String, Engine>> {
        let engines = table.get("engines")
            .ok_or_else(|| anyhow::anyhow!("Schedule file must have an 'engines' table"))?
            .as_table()
            .ok_or_else(|| anyhow::anyhow!("'engines' must be a table"))?;

        let mut result = HashMap::new();
        for (name, engine_config) in engines {
            let engine_configs = engine_config.as_table()
                .ok_or_else(|| anyhow::anyhow!("Config of engine {name} is not a table"))?;

            let typ = engine_configs.get("type")
                .ok_or_else(|| anyhow::anyhow!("Engine {name} doesn't have a 'type' field"))?
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("Engine {name} type must be a string"))?;

            let engine = Engine::build(typ, engine_configs).await?;

            result.insert(name.clone(), engine);
        }

        Ok(result)
    }

    async fn parse_steps(table: &Table) -> anyhow::Result<Vec<Step>> {
        let steps = table.get("steps")
            .ok_or_else(|| anyhow!("steps not found"))?
            .as_array()
            .ok_or_else(|| anyhow!("steps is not array"))?;

        steps.iter().map(Schedule::parse_step)
            .try_collect()
    }

    fn parse_step(value: &Value) -> anyhow::Result<Step> {
        let t = value
            .as_table()
            .ok_or_else(|| anyhow!("Step must be a table!"))?;

        let engine_name = t.get("engine")
            .ok_or_else(|| anyhow!("Property engine is missing in step"))?
            .as_str()
            .ok_or_else(|| anyhow!("Property engine is not a string in step"))?
            .to_string();

        let sql = t.get("sql")
            .ok_or_else(|| anyhow!("Property sql is missing in step"))?
            .as_str()
            .ok_or_else(|| anyhow!("Property sqlis not a string in step"))?
            .to_string();

        Ok(Step {
            engine_name,
            sql,
        })
    }

    pub async fn run(mut self) -> anyhow::Result<()> {
        for step_idx in 0..self.steps.len() {
            self.run_step(step_idx).await?;
        }

        Ok(())
    }

    async fn run_step(&self, step_index: usize) -> anyhow::Result<()> {
        let step = &self.steps[step_index];

        let engine = self.engines.get(&step.engine_name)
            .ok_or_else(|| anyhow!("Engine {} not found!", step.engine_name))?
            .clone();

        engine.run_slt_file(&step.sql).await
    }
}
