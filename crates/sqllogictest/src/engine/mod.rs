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

mod datafusion;

use std::path::Path;

use toml::Table as TomlTable;

use crate::engine::datafusion::DataFusionEngine;
use crate::error::Result;

const KEY_TYPE: &str = "type";
const TYPE_DATAFUSION: &str = "datafusion";

#[async_trait::async_trait]
pub trait EngineRunner: Sized {
    async fn run_slt_file(&mut self, path: &Path) -> Result<()>;
}

pub enum Engine {
    DataFusion(DataFusionEngine),
}

impl Engine {
    pub async fn new(config: TomlTable) -> Result<Self> {
        let engine_type = config
            .get(KEY_TYPE)
            .ok_or_else(|| anyhow::anyhow!("Missing required key: {KEY_TYPE}"))?
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("Config value for {KEY_TYPE} must be a string"))?;

        match engine_type {
            TYPE_DATAFUSION => {
                let engine = DataFusionEngine::new(config).await?;
                Ok(Engine::DataFusion(engine))
            }
            _ => Err(anyhow::anyhow!("Unsupported engine type: {engine_type}").into()),
        }
    }

    pub async fn run_slt_file(&mut self, path: &Path) -> Result<()> {
        match self {
            Engine::DataFusion(engine) => engine.run_slt_file(path).await,
        }
    }
}
