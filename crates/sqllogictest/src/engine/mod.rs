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
mod spark;

use std::path::Path;

use toml::Table as TomlTable;

use crate::engine::datafusion::DataFusionEngine;
use crate::engine::spark::SparkEngine;
use crate::error::Result;

#[async_trait::async_trait]
pub trait EngineRunner {
    async fn run_slt_file(&mut self, path: &Path) -> Result<()>;
}

pub async fn load_engine(engine_type: &str, cfg: TomlTable) -> Result<Box<dyn EngineRunner>> {
    match engine_type {
        "datafusion" => Ok(Box::new(DataFusionEngine::new(cfg).await?)),
        "spark-connect" => Ok(Box::new(SparkEngine::new(cfg).await?)),
        _ => Err(anyhow::anyhow!("Unsupported engine type: {}", engine_type).into()),
    }
}
