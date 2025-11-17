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

use datafusion::catalog::CatalogProvider;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_sqllogictest::DataFusion;
use iceberg::{Catalog, CatalogBuilder};
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg_datafusion::IcebergCatalogProvider;
use indicatif::ProgressBar;
use toml::Table as TomlTable;

use crate::engine::{EngineRunner, run_slt_with_runner};
use crate::error::Result;

pub struct DataFusionEngine {
    test_data_path: PathBuf,
    session_context: SessionContext,
}

impl std::fmt::Debug for DataFusionEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DataFusionEngine")
            .field("test_data_path", &self.test_data_path)
            .field("session_context", &"<SessionContext>")
            .finish()
    }
}

#[async_trait::async_trait]
impl EngineRunner for DataFusionEngine {
    async fn run_slt_file(&mut self, path: &Path) -> Result<()> {
        let ctx = self.session_context.clone();
        let testdata = self.test_data_path.clone();

        let runner = sqllogictest::Runner::new({
            move || {
                let ctx = ctx.clone();
                let testdata = testdata.clone();
                async move {
                    // Everything here is owned; no `self` capture.
                    Ok(DataFusion::new(ctx, testdata, ProgressBar::new(100)))
                }
            }
        });

        run_slt_with_runner(runner, path).await
    }
}

impl DataFusionEngine {
    pub async fn new(
        config: TomlTable,
        catalog: Option<Arc<dyn Catalog>>,
    ) -> Result<Self> {
        let session_config = SessionConfig::new()
            .with_target_partitions(4)
            .with_information_schema(true);
        let ctx = SessionContext::new_with_config(session_config);

        let catalog_provider = match catalog {
            Some(cat) => {
                // Use the provided catalog
                Arc::new(IcebergCatalogProvider::try_new(cat).await?)
            }
            None => {
                // Fallback: create default MemoryCatalog
                Self::create_default_catalog(&config).await?
            }
        };

        ctx.register_catalog("default", catalog_provider);

        Ok(Self {
            test_data_path: PathBuf::from("testdata"),
            session_context: ctx,
        })
    }

    async fn create_default_catalog(
        _: &TomlTable,
    ) -> anyhow::Result<Arc<dyn CatalogProvider>> {
        // Create default MemoryCatalog as fallback
        let catalog = MemoryCatalogBuilder::default()
            .load(
                "memory",
                HashMap::from([(
                    MEMORY_CATALOG_WAREHOUSE.to_string(),
                    "memory://".to_string(),
                )]),
            )
            .await?;

        Ok(Arc::new(
            IcebergCatalogProvider::try_new(Arc::new(catalog)).await?,
        ))
    }
}
