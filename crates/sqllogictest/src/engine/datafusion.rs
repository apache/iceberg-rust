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
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, anyhow};
use datafusion::catalog::CatalogProvider;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_sqllogictest::DataFusion;
use iceberg::CatalogBuilder;
use iceberg_catalog_rest::{REST_CATALOG_PROP_URI, RestCatalogBuilder};
use iceberg_datafusion::IcebergCatalogProvider;
use indicatif::ProgressBar;
use sqllogictest::runner::AsyncDB;
use sqllogictest::{MakeConnection, Record, parse_file};
use toml::Table as TomlTable;

use crate::engine::EngineRunner;
use crate::error::{Error, Result};

pub struct DataFusionEngine {
    relative_path: PathBuf,
    pb: ProgressBar,
    config: TomlTable,
}

#[async_trait::async_trait]
impl EngineRunner for DataFusionEngine {
    async fn run_slt_file(&mut self, path: &Path) -> Result<()> {
        let session_config = SessionConfig::new().with_target_partitions(4);
        let ctx = SessionContext::new_with_config(session_config);
        ctx.register_catalog("default", Self::create_catalog(&self.config).await?);

        let runner = sqllogictest::Runner::new(|| async {
            Ok(DataFusion::new(
                ctx.clone(),
                self.relative_path.clone(),
                self.pb.clone(),
            ))
        });

        let result: std::result::Result<(), Error> = Self::run_file_in_runner(path, runner).await;
        self.pb.finish_and_clear();

        result
    }
}

impl DataFusionEngine {
    pub async fn new(config: TomlTable) -> Result<Self> {
        Ok(Self {
            relative_path: PathBuf::from("testdata"),
            pb: ProgressBar::new(100),
            config,
        })
    }

    async fn create_catalog(_: &TomlTable) -> anyhow::Result<Arc<dyn CatalogProvider>> {
        let catalog = RestCatalogBuilder::default()
            .load(
                "rest",
                HashMap::from([
                    (
                        REST_CATALOG_PROP_URI.to_string(),
                        "http://localhost:8181".to_string(),
                    ),
                    (
                        "s3.endpoint".to_string(),
                        "http://localhost:9000".to_string(),
                    ),
                    ("s3.access-key-id".to_string(), "admin".to_string()),
                    ("s3.secret-access-key".to_string(), "password".to_string()),
                    ("s3.region".to_string(), "us-east-1".to_string()),
                    ("s3.disable-config-load".to_string(), "true".to_string()),
                ]),
            )
            .await?;

        Ok(Arc::new(
            IcebergCatalogProvider::try_new(Arc::new(catalog)).await?,
        ))
    }

    async fn run_file_in_runner<D: AsyncDB, M: MakeConnection<Conn = D>>(
        path: &Path,
        mut runner: sqllogictest::Runner<D, M>,
    ) -> Result<()> {
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
