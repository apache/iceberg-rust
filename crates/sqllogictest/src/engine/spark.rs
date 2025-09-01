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
use std::time::Duration;

use anyhow::{Context, anyhow};
use datafusion_sqllogictest::{DFColumnType, DFOutput, convert_batches, convert_schema_to_types};
use spark_connect_rs::{SparkSession, SparkSessionBuilder};
use sqllogictest::{AsyncDB, DBOutput, Record, parse_file};
use toml::Table as TomlTable;

use crate::engine::EngineRunner;
use crate::error::{Error, Result};

pub struct SparkEngine {
    session: SparkSession,
}

#[async_trait::async_trait]
impl AsyncDB for SparkEngine {
    type Error = Error;
    type ColumnType = DFColumnType;

    async fn run(&mut self, sql: &str) -> Result<DBOutput<DFColumnType>> {
        Self::run_query(&self.session, sql).await
    }

    async fn shutdown(&mut self) {}

    fn engine_name(&self) -> &str {
        "SparkConnect"
    }

    async fn sleep(dur: Duration) {
        tokio::time::sleep(dur).await;
    }
}

#[async_trait::async_trait]
impl EngineRunner for SparkEngine {
    async fn run_slt_file(&mut self, path: &Path) -> crate::error::Result<()> {
        let path_dir = path.to_str().unwrap();
        println!("engine running slt file on path: {path_dir}");

        let session = self.session.clone();
        let runner = sqllogictest::Runner::new(move || {
            let session = session.clone();
            async move { Ok(SparkEngine { session }) }
        });

        let result: std::result::Result<(), Error> = Self::run_file_in_runner(path, runner).await;

        result
    }
}

impl SparkEngine {
    pub async fn new(configs: TomlTable) -> Result<Self> {
        let url = configs
            .get("url")
            .ok_or_else(|| anyhow!("url property doesn't exist for spark engine"))?
            .as_str()
            .ok_or_else(|| anyhow!("url property is not a string for spark engine"))?;

        let session = SparkSessionBuilder::remote(url)
            .app_name("SparkConnect")
            .build()
            .await
            .map_err(|e| anyhow!(e))?;

        Ok(Self { session })
    }

    pub async fn run_query(session: &SparkSession, sql: impl Into<String>) -> Result<DFOutput> {
        let df = session.sql(sql.into().as_str()).await.unwrap();
        let batches = df.collect().await.unwrap();
        let schema = batches.schema();
        let types = convert_schema_to_types(schema.fields());

        // Convert batches to rows of strings
        let rows = convert_batches(vec![batches]).unwrap();

        Ok(DBOutput::Rows { types, rows })
    }

    async fn run_file_in_runner<D: AsyncDB, M>(
        path: &Path,
        mut runner: sqllogictest::Runner<D, M>,
    ) -> Result<()>
    where
        M: sqllogictest::MakeConnection<Conn = D>,
    {
        println!("run file in runner");

        let records = parse_file(path).context("Failed to parse slt file")?;

        let mut errs = vec![];
        for record in records.into_iter() {
            if let Record::Halt { .. } = record {
                break;
            }
            if let Err(err) = runner.run_async(record).await {
                errs.push(format!("{err}"));
            }
        }

        if !errs.is_empty() {
            let mut msg = format!("{} errors in file {}\n\n", errs.len(), path.display());
            for (i, err) in errs.iter().enumerate() {
                msg.push_str(&format!("{}. {err}\n\n", i + 1));
            }
            return Err(Error(anyhow!(msg)));
        }

        Ok(())
    }
}
