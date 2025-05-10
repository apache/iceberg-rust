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

use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{anyhow, Context};
use datafusion::catalog::CatalogProvider;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_sqllogictest::DataFusion;
use indicatif::ProgressBar;
use sqllogictest::AsyncDB;
use toml::Table as TomlTable;

use crate::engine::Engine;
use crate::error::Result;

pub struct DataFusionEngine {
    datafusion: DataFusion,
}

#[async_trait::async_trait]
impl Engine for DataFusionEngine {
    async fn new(config: TomlTable) -> Result<Self> {
        let session_config = SessionConfig::new().with_target_partitions(4);
        let ctx = SessionContext::new_with_config(session_config);
        ctx.register_catalog("default", Self::create_catalog(&config).await?);

        Ok(Self {
            datafusion: DataFusion::new(ctx, PathBuf::from("testdata"), ProgressBar::new(100)),
        })
    }

    async fn run_slt_file(&mut self, path: &Path) -> Result<()> {
        let content = std::fs::read_to_string(path)
            .with_context(|| format!("Failed to read slt file {:?}", path))
            .map_err(|e| anyhow!(e))?;

        self.datafusion
            .run(content.as_str())
            .await
            .with_context(|| format!("Failed to run slt file {:?}", path))
            .map_err(|e| anyhow!(e))?;

        Ok(())
    }
}

impl DataFusionEngine {
    async fn create_catalog(_: &TomlTable) -> anyhow::Result<Arc<dyn CatalogProvider>> {
        todo!()
    }
}
