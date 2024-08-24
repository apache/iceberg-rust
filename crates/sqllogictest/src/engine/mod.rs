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

use anyhow::anyhow;
pub use datafusion::*;
use sqllogictest::{strict_column_validator, AsyncDB, MakeConnection, Runner};
use std::sync::Arc;
use toml::Table;

mod conversion;
mod output;
mod normalize;

mod spark;
pub use spark::*;

mod datafusion;
pub use datafusion::*;


#[derive(Clone)]
pub enum Engine {
    DataFusion(Arc<Table>),
    SparkSQL(Arc<Table>),
}

impl Engine {
    pub async fn new(typ: &str, configs: &Table) -> anyhow::Result<Self> {
        let configs = Arc::new(configs.clone());
        match typ {
            "spark" => {
                Ok(Engine::SparkSQL(configs))
            }
            "datafusion" => {
                Ok(Engine::DataFusion(configs))
            }
            other => Err(anyhow!("Unknown engine type: {other}"))
        }
    }

    pub async fn run_slt_file(self, slt_file: impl Into<String>) -> anyhow::Result<()> {
        let absolute_file = format!("{}/testdata/slts/{}", env!("CARGO_MANIFEST_DIR"), slt_file);

        match self {
            Engine::DataFusion(configs) => {
                let configs = configs.clone();
                let runner = Runner::new(async || DataFusionEngine::new(&*configs).await);
                Self::run_with_runner(runner, absolute_file).await
            }
            Engine::SparkSQL(configs) => {
                let configs = configs.clone();
                let runner = Runner::new(async || {
                    SparkSqlEngine::new(&*configs).await
                });
                Self::run_with_runner(runner, absolute_file).await
            }
        }
    }

    async fn run_with_runner<D: AsyncDB, M: MakeConnection>(mut runner: Runner<D, M>,
                                                            slt_file: String) -> anyhow::Result<()> {
        runner.with_column_validator(strict_column_validator);
        Ok(runner
            .run_file_async(slt_file)
            .await?)
    }
}