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

use anyhow::anyhow;
use sqllogictest::{AsyncDB, MakeConnection, Runner, parse_file};
use toml::Table as TomlTable;

use crate::engine::datafusion::DataFusionEngine;
use crate::error::{Error, Result};

const TYPE_DATAFUSION: &str = "datafusion";

#[async_trait::async_trait]
pub trait EngineRunner: Send {
    async fn run_slt_file(&mut self, path: &Path) -> Result<()>;
}

pub async fn load_engine_runner(
    engine_type: &str,
    cfg: TomlTable,
) -> Result<Box<dyn EngineRunner>> {
    match engine_type {
        TYPE_DATAFUSION => Ok(Box::new(DataFusionEngine::new(cfg).await?)),
        _ => Err(anyhow::anyhow!("Unsupported engine type: {}", engine_type).into()),
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
    use crate::engine::{TYPE_DATAFUSION, load_engine_runner};

    #[tokio::test]
    async fn test_engine_invalid_type() {
        let input = r#"
            [engines]
            random = { type = "random_engine", url = "http://localhost:8181" }
        "#;
        let tbl = toml::from_str(input).unwrap();
        let result = load_engine_runner("random_engine", tbl).await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_load_datafusion() {
        let input = r#"
            [engines]
            df = { type = "datafusion" }
        "#;
        let tbl = toml::from_str(input).unwrap();
        let result = load_engine_runner(TYPE_DATAFUSION, tbl).await;

        assert!(result.is_ok());
    }
}
