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

use crate::engine::output::DFColumnType;
use crate::engine::{normalize, DataFusionEngine};
use anyhow::anyhow;
use itertools::Itertools;
use spark_connect_rs::{SparkSession, SparkSessionBuilder};
use sqllogictest::{AsyncDB, DBOutput};
use std::time::Duration;
use toml::Table;

/// SparkSql engine implementation for sqllogictest.
pub struct SparkSqlEngine {
    session: SparkSession,
}

impl AsyncDB for SparkSqlEngine {
    type Error = anyhow::Error;
    type ColumnType = DFColumnType;

    async fn run(&mut self, sql: &str) -> anyhow::Result<DBOutput<DFColumnType>> {
        let results = self.session.sql(sql).await?.collect()?;
        let types = normalize::convert_schema_to_types(results.schema().fields());
        let rows = crate::engine::normalize::convert_batches(results)?;

        if rows.is_empty() && types.is_empty() {
            Ok(DBOutput::StatementComplete(0))
        } else {
            Ok(DBOutput::Rows { types, rows })
        }
    }

    /// Engine name of current database.
    fn engine_name(&self) -> &str {
        "SparkConnect"
    }

    /// [`DataFusionEngine`] calls this function to perform sleep.
    ///
    /// The default implementation is `std::thread::sleep`, which is universal to any async runtime
    /// but would block the current thread. If you are running in tokio runtime, you should override
    /// this by `tokio::time::sleep`.
    async fn sleep(dur: Duration) {
        tokio::time::sleep(dur).await;
    }
}

impl SparkSqlEngine {
    pub async fn new(configs: &Table) -> anyhow::Result<Self> {
        let url = configs.get("url")
            .ok_or_else(|| anyhow!("url property doesn't exist for spark engine"))?
            .as_str()
            .ok_or_else(|| anyhow!("url property is not a string for spark engine"))?;

        let session = SparkSessionBuilder::remote(url)
            .app_name("SparkConnect")
            .build()
            .await?;

        Ok(Self { session })
    }
}
