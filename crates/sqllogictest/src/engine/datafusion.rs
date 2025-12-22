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

use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_sqllogictest::DataFusion;
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::spec::{NestedField, PrimitiveType, Schema, Transform, Type, UnboundPartitionSpec};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation};
use iceberg_catalog_loader::CatalogLoader;
use iceberg_datafusion::IcebergCatalogProvider;
use indicatif::ProgressBar;
use toml::Table as TomlTable;

use crate::engine::{EngineRunner, run_slt_with_runner};
use crate::error::Result;

const DEFAULT_CATALOG_TYPE: &str = "memory";

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
    /// Create a new DataFusion engine with catalog configuration from the TOML config.
    ///
    /// # Configuration
    ///
    /// The engine reads catalog configuration from the TOML config:
    /// - `catalog_type`: The type of catalog to use (e.g., "memory", "rest"). Defaults to "memory".
    /// - `catalog_properties`: Additional properties for the catalog (optional).
    ///
    /// # Example configuration
    ///
    /// ```toml
    /// [engines]
    /// df = { type = "datafusion", catalog_type = "rest", catalog_properties = { uri = "http://localhost:8181" } }
    /// ```
    pub async fn new(config: TomlTable) -> Result<Self> {
        let catalog = Self::create_catalog(&config).await?;

        let session_config = SessionConfig::new()
            .with_target_partitions(4)
            .with_information_schema(true);
        let ctx = SessionContext::new_with_config(session_config);

        // Create test namespace and tables in the catalog
        Self::setup_test_data(&catalog).await?;

        // Register the catalog with DataFusion
        let catalog_provider = IcebergCatalogProvider::try_new(catalog)
            .await
            .map_err(|e| {
                crate::error::Error(anyhow::anyhow!("Failed to create catalog provider: {e}"))
            })?;
        ctx.register_catalog("default", Arc::new(catalog_provider));

        Ok(Self {
            test_data_path: PathBuf::from("testdata"),
            session_context: ctx,
        })
    }

    /// Create a catalog from the engine configuration.
    ///
    /// Supported catalog types:
    /// - "memory": In-memory catalog (default), useful for testing
    /// - "rest": REST catalog
    /// - "glue": AWS Glue catalog
    /// - "hms": Hive Metastore catalog
    /// - "s3tables": S3 Tables catalog
    /// - "sql": SQL catalog
    async fn create_catalog(config: &TomlTable) -> Result<Arc<dyn Catalog>> {
        let catalog_type = config
            .get("catalog_type")
            .and_then(|v| v.as_str())
            .unwrap_or(DEFAULT_CATALOG_TYPE);

        let catalog_properties: HashMap<String, String> = config
            .get("catalog_properties")
            .and_then(|v| v.as_table())
            .map(|t| {
                t.iter()
                    .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                    .collect()
            })
            .unwrap_or_default();

        if catalog_type == "memory" {
            // Memory catalog is built-in to iceberg crate, not in catalog-loader
            // Ensure warehouse is set for memory catalog
            let mut props = catalog_properties;
            if !props.contains_key(MEMORY_CATALOG_WAREHOUSE) {
                // Use a temp directory as default warehouse for testing
                props.insert(
                    MEMORY_CATALOG_WAREHOUSE.to_string(),
                    std::env::temp_dir()
                        .join("iceberg-sqllogictest")
                        .to_string_lossy()
                        .to_string(),
                );
            }
            let catalog = MemoryCatalogBuilder::default()
                .load("default", props)
                .await
                .map_err(|e| {
                    crate::error::Error(anyhow::anyhow!("Failed to load memory catalog: {e}"))
                })?;
            Ok(Arc::new(catalog))
        } else {
            // Use catalog-loader for other catalog types
            let catalog = CatalogLoader::from(catalog_type)
                .load("default".to_string(), catalog_properties)
                .await
                .map_err(|e| crate::error::Error(anyhow::anyhow!("Failed to load catalog: {e}")))?;
            Ok(catalog)
        }
    }

    /// Set up the test namespace and tables in the catalog.
    async fn setup_test_data(catalog: &Arc<dyn Catalog>) -> anyhow::Result<()> {
        // Create a test namespace for INSERT INTO tests
        let namespace = NamespaceIdent::new("default".to_string());

        // Try to create the namespace, ignore if it already exists
        if catalog
            .create_namespace(&namespace, HashMap::new())
            .await
            .is_err()
        {
            // Namespace might already exist, that's ok
        }

        // Create test tables (ignore errors if they already exist)
        let _ = Self::create_unpartitioned_table(catalog, &namespace).await;
        let _ = Self::create_partitioned_table(catalog, &namespace).await;

        Ok(())
    }

    /// Create an unpartitioned test table with id and name columns
    /// TODO: this can be removed when we support CREATE TABLE
    async fn create_unpartitioned_table(
        catalog: &Arc<dyn Catalog>,
        namespace: &NamespaceIdent,
    ) -> anyhow::Result<()> {
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::optional(2, "name", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()?;

        catalog
            .create_table(
                namespace,
                TableCreation::builder()
                    .name("test_unpartitioned_table".to_string())
                    .schema(schema)
                    .build(),
            )
            .await?;

        Ok(())
    }

    /// Create a partitioned test table with id, category, and value columns
    /// Partitioned by category using identity transform
    /// TODO: this can be removed when we support CREATE TABLE
    async fn create_partitioned_table(
        catalog: &Arc<dyn Catalog>,
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
