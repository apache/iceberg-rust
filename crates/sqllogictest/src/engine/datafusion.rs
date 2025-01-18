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

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context};
use arrow_array::RecordBatch;
use async_trait::async_trait;
use datafusion::catalog::CatalogProvider;
use datafusion::physical_plan::common::collect;
use datafusion::physical_plan::execute_stream;
use datafusion::prelude::{SessionConfig, SessionContext};
use sqllogictest::{AsyncDB, DBOutput};
use toml::Table as TomlTable;

use crate::display::normalize;
use crate::engine::output::{DFColumnType, DFOutput};
use crate::engine::Engine;
use crate::error::{Error, Result};

pub struct DataFusionEngine {
    config: TomlTable,
}

#[async_trait::async_trait]
impl Engine for DataFusionEngine {
    async fn new(config: TomlTable) -> Result<Self> {
        Ok(Self { config })
    }

    async fn run_slt_file(&self, path: &Path) -> Result<()> {
        let content = std::fs::read_to_string(path)
            .with_context(|| format!("Failed to read slt file {path}"))
            .map_err(|e| anyhow!(e))?;
        let mut db = DataFusionDB::new(&self.config).await?;

        db.run(content.as_str()).await?;

        Ok(())
    }
}

struct DataFusionDB {
    ctx: SessionContext,
}

impl Default for DataFusionDB {
    fn default() -> Self {
        let config = SessionConfig::new().with_target_partitions(4);

        let ctx = SessionContext::new_with_config(config);

        Self { ctx }
    }
}

#[async_trait]
impl AsyncDB for DataFusionDB {
    type Error = Error;
    type ColumnType = DFColumnType;

    async fn run(&mut self, sql: &str) -> Result<DFOutput> {
        Ok(run_query(&self.ctx, sql).await?)
    }

    /// Engine name of current database.
    fn engine_name(&self) -> &str {
        "DataFusion"
    }

    /// [`DataFusionDB`] calls this function to perform sleep.
    ///
    /// The default implementation is `std::thread::sleep`, which is universal to any async runtime
    /// but would block the current thread. If you are running in tokio runtime, you should override
    /// this by `tokio::time::sleep`.
    async fn sleep(dur: Duration) {
        tokio::time::sleep(dur).await;
    }
}

async fn run_query(ctx: &SessionContext, sql: impl Into<String>) -> anyhow::Result<DFOutput> {
    let df = ctx.sql(sql.into().as_str()).await?;
    let task_ctx = Arc::new(df.task_ctx());
    let plan = df.create_physical_plan().await?;

    let stream = execute_stream(plan, task_ctx)?;
    let types = normalize::convert_schema_to_types(stream.schema().fields());
    let results: Vec<RecordBatch> = collect(stream).await?;
    let rows = normalize::convert_batches(results)?;

    if rows.is_empty() && types.is_empty() {
        Ok(DBOutput::StatementComplete(0))
    } else {
        Ok(DBOutput::Rows { types, rows })
    }
}

impl DataFusionDB {
    pub async fn new(configs: &TomlTable) -> Result<Self> {
        let config = SessionConfig::new().with_target_partitions(4);

        let ctx = SessionContext::new_with_config(config);
        ctx.register_catalog("demo", Self::create_catalog(configs).await?);

        Ok(Self { ctx })
    }

    async fn create_catalog(_configs: &TomlTable) -> anyhow::Result<Arc<dyn CatalogProvider>> {
        todo!()
    }
}
