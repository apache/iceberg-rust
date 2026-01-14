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
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_sqllogictest::DataFusion;
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::spec::{NestedField, PrimitiveType, Schema, Transform, Type, UnboundPartitionSpec};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation};
use iceberg_datafusion::{IcebergCatalogProvider, IcebergTableProviderFactory};
use indicatif::ProgressBar;

use crate::engine::{DatafusionCatalogConfig, EngineRunner, run_slt_with_runner};
use crate::error::Result;

pub struct DataFusionEngine {
    test_data_path: PathBuf,
    session_context: SessionContext,
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
    pub async fn new(catalog_config: Option<DatafusionCatalogConfig>) -> Result<Self> {
        let session_config = SessionConfig::new()
            .with_target_partitions(4)
            .with_information_schema(true);

        // Create the catalog first so we can share it with the factory
        let (catalog_provider, iceberg_catalog) =
            Self::create_catalog(catalog_config.as_ref()).await?;

        // Build session state with the IcebergTableProviderFactory registered
        let mut state = SessionStateBuilder::new()
            .with_config(session_config)
            .with_default_features()
            .build();

        // Register the IcebergTableProviderFactory with the injected catalog
        // This enables CREATE EXTERNAL TABLE ... STORED AS ICEBERG to load tables from the catalog
        state.table_factories_mut().insert(
            "ICEBERG".to_string(),
            Arc::new(IcebergTableProviderFactory::new_with_catalog(
                iceberg_catalog,
            )),
        );

        let ctx = SessionContext::new_with_state(state);
        ctx.register_catalog("default", catalog_provider);

        Ok(Self {
            test_data_path: PathBuf::from("testdata"),
            session_context: ctx,
        })
    }

    async fn create_catalog(
        _catalog_config: Option<&DatafusionCatalogConfig>,
    ) -> anyhow::Result<(Arc<dyn CatalogProvider>, Arc<dyn Catalog>)> {
        // TODO: Use catalog_config to load different catalog types via iceberg-catalog-loader
        // See: https://github.com/apache/iceberg-rust/issues/1780
        let catalog = MemoryCatalogBuilder::default()
            .load(
                "memory",
                HashMap::from([(
                    MEMORY_CATALOG_WAREHOUSE.to_string(),
                    "memory://".to_string(),
                )]),
            )
            .await?;

        // Create a test namespace for INSERT INTO tests
        let namespace = NamespaceIdent::new("default".to_string());
        catalog.create_namespace(&namespace, HashMap::new()).await?;

        // Create partitioned test table (unpartitioned tables are now created via SQL)
        Self::create_partitioned_table(&catalog, &namespace).await?;

        let catalog_arc = Arc::new(catalog);
        let catalog_provider =
            Arc::new(IcebergCatalogProvider::try_new(catalog_arc.clone()).await?);

        Ok((catalog_provider, catalog_arc))
    }

    /// Create a partitioned test table with id, category, and value columns
    /// Partitioned by category using identity transform
    ///
    /// This table is created in the Iceberg catalog and can be accessed via:
    /// 1. `default.default.test_partitioned_table` - auto-registered by IcebergCatalogProvider
    /// 2. `CREATE EXTERNAL TABLE test_partitioned_table STORED AS ICEBERG LOCATION ''` - loaded via factory
    ///
    /// The insert_into.slt tests use approach #2 to demonstrate the CREATE EXTERNAL TABLE feature.
    async fn create_partitioned_table(
        catalog: &impl Catalog,
        namespace: &NamespaceIdent,
    ) -> anyhow::Result<()> {
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::required(2, "category", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::optional(3, "value", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()?;

        let partition_spec = UnboundPartitionSpec::builder()
            .with_spec_id(0)
            .add_partition_field(2, "category", Transform::Identity)?
            .build();

        catalog
            .create_table(
                namespace,
                TableCreation::builder()
                    .name("test_partitioned_table".to_string())
                    .schema(schema)
                    .partition_spec(partition_spec)
                    .build(),
            )
            .await?;

        Ok(())
    }
}
