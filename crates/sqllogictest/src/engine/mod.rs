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

use std::collections::HashMap;
use std::path::Path;

use anyhow::anyhow;
use serde::Deserialize;
use sqllogictest::{AsyncDB, MakeConnection, Runner, parse_file};

use crate::engine::datafusion::DataFusionEngine;
use crate::error::{Error, Result};

/// Supported engine types for sqllogictest
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum EngineType {
    Datafusion,
}

/// Configuration for a single engine instance
#[derive(Debug, Clone, Deserialize)]
pub struct EngineConfig {
    /// The type of engine
    #[serde(rename = "type")]
    pub engine_type: EngineType,

    /// Additional configuration fields for extensibility
    /// This allows forward-compatibility with future fields like catalog_type, catalog_properties
    #[serde(flatten)]
    pub extra: HashMap<String, toml::Value>,
}

#[async_trait::async_trait]
pub trait EngineRunner: Send {
    async fn run_slt_file(&mut self, path: &Path) -> Result<()>;
}

pub async fn load_engine_runner(name: &str, config: EngineConfig) -> Result<Box<dyn EngineRunner>> {
    match config.engine_type {
        EngineType::Datafusion => Ok(Box::new(DataFusionEngine::new(name, &config).await?)),
    }
}

pub async fn run_slt_with_runner<D, M>(
    mut runner: Runner<D, M>,
    step_slt_file: impl AsRef<Path>,
) -> Result<()>
where
    D: AsyncDB + Send + 'static,
    M: MakeConnection<Conn = D> + Send + 'static,
{
    let path = step_slt_file.as_ref().canonicalize()?;
    let records = parse_file(&path).map_err(|e| Error(anyhow!("parsing slt file failed: {e}")))?;

    for record in records {
        if let Err(err) = runner.run_async(record).await {
            return Err(Error(anyhow!("SLT record execution failed: {err}")));
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::engine::{EngineConfig, EngineType, load_engine_runner};

    #[test]
    fn test_deserialize_engine_config() {
        let input = r#"
            type = "datafusion"
        "#;

        let config: EngineConfig = toml::from_str(input).unwrap();
        assert_eq!(config.engine_type, EngineType::Datafusion);
        assert!(config.extra.is_empty());
    }

    #[test]
    fn test_deserialize_engine_config_with_extras() {
        let input = r#"
            type = "datafusion"
            catalog_type = "rest"

            [catalog_properties]
            uri = "http://localhost:8181"
        "#;

        let config: EngineConfig = toml::from_str(input).unwrap();
        assert_eq!(config.engine_type, EngineType::Datafusion);
        assert!(config.extra.contains_key("catalog_type"));
        assert!(config.extra.contains_key("catalog_properties"));
    }

    #[tokio::test]
    async fn test_load_datafusion() {
        let config = EngineConfig {
            engine_type: EngineType::Datafusion,
            extra: HashMap::new(),
        };

        let result = load_engine_runner("df", config).await;
        assert!(result.is_ok());
    }
}
