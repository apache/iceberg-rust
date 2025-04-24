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
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::engine::Engine;

pub struct Schedule {
    engines: HashMap<String, Engine>,
    steps: Vec<Step>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Step {
    /// Engine name
    engine: String,
    /// Stl file path
    slt: String,
}

impl Schedule {
    pub fn new(engines: HashMap<String, Engine>, steps: Vec<Step>) -> Self {
        Self { engines, steps }
    }

    pub async fn run(mut self) -> anyhow::Result<()> {
        for (idx, step) in self.steps.iter().enumerate() {
            tracing::info!(
                "Running step {}/{}, using engine {}, slt file path: {}",
                idx + 1,
                self.steps.len(),
                &step.engine,
                &step.slt
            );

            let engine = self
                .engines
                .get_mut(&step.engine)
                .ok_or_else(|| anyhow::anyhow!("Engine {} not found", step.engine))?;

            engine
                .run_slt_file(&PathBuf::from(step.slt.clone()))
                .await?;
            tracing::info!(
                "Step {}/{}, engine {}, slt file path: {} finished",
                idx + 1,
                self.steps.len(),
                &step.engine,
                &step.slt
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use toml::Table as TomlTable;

    use crate::schedule::Step;

    #[test]
    fn test_parse_steps() {
        let steps = r#"
            [[steps]]
            engine = "datafusion"
            slt = "test.slt"

            [[steps]]
            engine = "spark"
            slt = "test2.slt"
        "#;

        let steps: Vec<Step> = toml::from_str::<TomlTable>(steps)
            .unwrap()
            .get("steps")
            .unwrap()
            .clone()
            .try_into()
            .unwrap();

        assert_eq!(steps.len(), 2);
        assert_eq!(steps[0].engine, "datafusion");
        assert_eq!(steps[0].slt, "test.slt");
        assert_eq!(steps[1].engine, "spark");
        assert_eq!(steps[1].slt, "test2.slt");
    }
}
