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

//! Iceberg table providers for DataFusion.
//!
//! This module provides two table provider implementations:
//!
//! - [`IcebergTableProvider`]: Catalog-backed provider with automatic metadata refresh.
//!   Use for write operations and when you need to see the latest table state.
//!
//! - [`IcebergStaticTableProvider`]: Static provider for read-only access to a specific
//!   table snapshot. Use for consistent analytical queries or time-travel scenarios.

mod bucketing;
pub mod metadata_table;
pub mod table_provider_factory;

use std::any::Any;
use std::num::NonZeroUsize;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datafusion::catalog::Session;
use datafusion::common::DataFusionError;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::{ExecutionPlan, Partitioning};
use futures::TryStreamExt;
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::inspect::MetadataTableType;
use iceberg::scan::FileScanTask;
use iceberg::spec::TableProperties;
use iceberg::table::Table;
use iceberg::{Catalog, Error, ErrorKind, NamespaceIdent, Result, TableIdent};
use metadata_table::IcebergMetadataTableProvider;

use crate::error::to_datafusion_error;
use crate::physical_plan::commit::IcebergCommitExec;
use crate::physical_plan::project::project_with_partition;
use crate::physical_plan::repartition::repartition;
use crate::physical_plan::scan::IcebergTableScanBuilder;
use crate::physical_plan::sort::sort_by_partition;
use crate::physical_plan::write::IcebergWriteExec;

/// Catalog-backed table provider with automatic metadata refresh.
///
/// This provider loads fresh table metadata from the catalog on every scan and write
/// operation, ensuring you always see the latest table state. Use this when you need
/// write operations or want to see the most up-to-date data.
///
/// For read-only access to a specific snapshot without catalog overhead, use
/// [`IcebergStaticTableProvider`] instead.
#[derive(Debug, Clone)]
pub struct IcebergTableProvider {
    /// The catalog that manages this table
    catalog: Arc<dyn Catalog>,
    /// The table identifier (namespace + name)
    table_ident: TableIdent,
    /// A reference-counted arrow `Schema` (cached at construction)
    schema: ArrowSchemaRef,
}

impl IcebergTableProvider {
    /// Creates a new catalog-backed table provider.
    ///
    /// Loads the table once to get the initial schema, then stores the catalog
    /// reference for future metadata refreshes on each operation.
    pub(crate) async fn try_new(
        catalog: Arc<dyn Catalog>,
        namespace: NamespaceIdent,
        name: impl Into<String>,
    ) -> Result<Self> {
        let table_ident = TableIdent::new(namespace, name.into());

        let table = catalog.load_table(&table_ident).await?;
        let schema = Arc::new(schema_to_arrow_schema(table.metadata().current_schema())?);

        Ok(IcebergTableProvider {
            catalog,
            table_ident,
            schema,
        })
    }

    pub(crate) async fn metadata_table(
        &self,
        r#type: MetadataTableType,
    ) -> Result<IcebergMetadataTableProvider> {
        let table = self.catalog.load_table(&self.table_ident).await?;
        Ok(IcebergMetadataTableProvider { table, r#type })
    }
}

#[async_trait]
impl TableProvider for IcebergTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> ArrowSchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        // Second load: fetch the latest snapshot so scans always reflect current table state.
        let table = self
            .catalog
            .load_table(&self.table_ident)
            .await
            .map_err(to_datafusion_error)?;

        // Use the same builder path for eager file planning and execution so
        // snapshot, projection, and filter handling cannot drift.
        let scan_builder = IcebergTableScanBuilder::new(table.clone(), self.schema.clone())
            // Always use current snapshot for catalog-backed provider.
            .with_snapshot_id(None)
            .with_projection(projection)
            .with_filters(filters)
            .with_limit(limit);
        let table_scan_config = scan_builder.table_scan_config();

        let tasks: Vec<FileScanTask> = scan_builder
            .build_iceberg_table_scan(&table_scan_config)?
            .plan_files()
            .await
            .map_err(to_datafusion_error)?
            .try_collect::<Vec<_>>()
            .await
            .map_err(to_datafusion_error)?;

        // Output schema after projection: column indices in `Hash` exprs and any
        // Arrow array we hash must reference this schema, not the full table schema.
        let output_schema = scan_builder.output_schema()?;

        let target_partitions = state.config().target_partitions();
        // Always produce at least 1 partition so that DataFusion can schedule
        // the plan normally and callers can safely call execute(0). An empty
        // bucket simply yields an empty record-batch stream.
        let n_partitions = target_partitions.min(tasks.len()).max(1);

        // identity_cols is Some(non-empty) iff every condition for declaring
        // Partitioning::Hash is met: the table's default spec has identity-transform
        // fields, every such source column is present in the output projection, and
        // every column type is supported by the identity hash materialization path.
        // Any miss collapses to None, which forces UnknownPartitioning regardless
        // of bucketing strategy.
        let identity_cols = bucketing::compute_identity_cols(&table, &output_schema);

        let (buckets, all_had_full_key) =
            bucketing::bucket_tasks(tasks, n_partitions, identity_cols.as_deref());

        let partitioning = match identity_cols {
            Some(cols) if !cols.is_empty() && all_had_full_key && n_partitions > 0 => {
                let exprs: Vec<Arc<dyn PhysicalExpr>> = cols
                    .iter()
                    .map(|c| Arc::new(Column::new(&c.name, c.output_idx)) as Arc<dyn PhysicalExpr>)
                    .collect();
                // This declaration is only sound if the Arrow arrays built from
                // partition literals hash identically to the column arrays the
                // reader emits at scan time. DataFusion's hash dispatch is
                // dtype-specific, so any drift in the reader output type (for
                // example Utf8 vs Utf8View) must either update the bucketing
                // path to materialize that exact dtype or fall back to
                // UnknownPartitioning.
                Partitioning::Hash(exprs, n_partitions)
            }
            _ => Partitioning::UnknownPartitioning(n_partitions),
        };

        Ok(Arc::new(
            scan_builder
                .with_task_buckets(buckets, partitioning)
                .build_with_table_scan_config(table_scan_config)?,
        ))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DFResult<Vec<TableProviderFilterPushDown>> {
        // Push down all filters, as a single source of truth, the scanner will drop the filters which couldn't be push down
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }

    async fn insert_into(
        &self,
        state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        _insert_op: InsertOp,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let table = self
            .catalog
            .load_table(&self.table_ident)
            .await
            .map_err(to_datafusion_error)?;

        let partition_spec = table.metadata().default_partition_spec();

        // Step 1: Project partition values for partitioned tables
        let plan_with_partition = if !partition_spec.is_unpartitioned() {
            project_with_partition(input, &table)?
        } else {
            input
        };

        // Step 2: Repartition for parallel processing
        let target_partitions =
            NonZeroUsize::new(state.config().target_partitions()).ok_or_else(|| {
                DataFusionError::Configuration(
                    "target_partitions must be greater than 0".to_string(),
                )
            })?;

        let repartitioned_plan =
            repartition(plan_with_partition, table.metadata_ref(), target_partitions)?;

        let fanout_enabled = table
            .metadata()
            .properties()
            .get(TableProperties::PROPERTY_DATAFUSION_WRITE_FANOUT_ENABLED)
            .map(|value| {
                value
                    .parse::<bool>()
                    .map_err(|e| {
                        Error::new(
                            ErrorKind::DataInvalid,
                            format!(
                                "Invalid value for {}, expected 'true' or 'false'",
                                TableProperties::PROPERTY_DATAFUSION_WRITE_FANOUT_ENABLED
                            ),
                        )
                        .with_source(e)
                    })
                    .map_err(to_datafusion_error)
            })
            .transpose()?
            .unwrap_or(TableProperties::PROPERTY_DATAFUSION_WRITE_FANOUT_ENABLED_DEFAULT);

        let write_input = if fanout_enabled {
            repartitioned_plan
        } else {
            sort_by_partition(repartitioned_plan)?
        };

        let write_plan = Arc::new(IcebergWriteExec::new(
            table.clone(),
            write_input,
            self.schema.clone(),
        ));

        // Merge the outputs of write_plan into one so we can commit all files together
        let coalesce_partitions = Arc::new(CoalescePartitionsExec::new(write_plan));

        Ok(Arc::new(IcebergCommitExec::new(
            table,
            self.catalog.clone(),
            coalesce_partitions,
            self.schema.clone(),
        )))
    }
}

/// Static table provider for read-only snapshot access.
///
/// This provider holds a cached table instance and does not refresh metadata or support
/// write operations. Use this for consistent analytical queries, time-travel scenarios,
/// or when you want to avoid catalog overhead.
///
/// For catalog-backed tables with write support and automatic refresh, use
/// [`IcebergTableProvider`] instead.
#[derive(Debug, Clone)]
pub struct IcebergStaticTableProvider {
    /// The static table instance (never refreshed)
    table: Table,
    /// Optional snapshot ID for this static view
    snapshot_id: Option<i64>,
    /// A reference-counted arrow `Schema`
    schema: ArrowSchemaRef,
}

impl IcebergStaticTableProvider {
    /// Creates a static provider from a table instance.
    ///
    /// Uses the table's current snapshot for all queries. Does not support write operations.
    pub async fn try_new_from_table(table: Table) -> Result<Self> {
        let schema = Arc::new(schema_to_arrow_schema(table.metadata().current_schema())?);
        Ok(IcebergStaticTableProvider {
            table,
            snapshot_id: None,
            schema,
        })
    }

    /// Creates a static provider for a specific table snapshot.
    ///
    /// Queries the specified snapshot for all operations. Useful for time-travel queries.
    /// Does not support write operations.
    pub async fn try_new_from_table_snapshot(table: Table, snapshot_id: i64) -> Result<Self> {
        let snapshot = table
            .metadata()
            .snapshot_by_id(snapshot_id)
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::Unexpected,
                    format!(
                        "snapshot id {snapshot_id} not found in table {}",
                        table.identifier().name()
                    ),
                )
            })?;
        let table_schema = snapshot.schema(table.metadata())?;
        let schema = Arc::new(schema_to_arrow_schema(&table_schema)?);
        Ok(IcebergStaticTableProvider {
            table,
            snapshot_id: Some(snapshot_id),
            schema,
        })
    }
}

#[async_trait]
impl TableProvider for IcebergStaticTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> ArrowSchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(
            IcebergTableScanBuilder::new(self.table.clone(), self.schema.clone())
                .with_snapshot_id(self.snapshot_id)
                .with_projection(projection)
                .with_filters(filters)
                .with_limit(limit)
                .build()?,
        ))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DFResult<Vec<TableProviderFilterPushDown>> {
        // Push down all filters, as a single source of truth, the scanner will drop the filters which couldn't be push down
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }

    async fn insert_into(
        &self,
        _state: &dyn Session,
        _input: Arc<dyn ExecutionPlan>,
        _insert_op: InsertOp,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Err(to_datafusion_error(Error::new(
            ErrorKind::FeatureUnsupported,
            "Write operations are not supported on IcebergStaticTableProvider. \
             Use IcebergTableProvider with a catalog for write support."
                .to_string(),
        )))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use datafusion::common::Column;
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::prelude::SessionContext;
    use iceberg::io::FileIO;
    use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
    use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
    use iceberg::table::{StaticTable, Table};
    use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};
    use tempfile::TempDir;

    use super::*;
    use crate::physical_plan::scan::IcebergTableScan;

    async fn get_test_table_from_metadata_file() -> Table {
        let metadata_file_name = "TableMetadataV2Valid.json";
        let metadata_file_path = format!(
            "{}/tests/test_data/{}",
            env!("CARGO_MANIFEST_DIR"),
            metadata_file_name
        );
        let file_io = FileIO::new_with_fs();
        let static_identifier = TableIdent::from_strs(["static_ns", "static_table"]).unwrap();
        let static_table =
            StaticTable::from_metadata_file(&metadata_file_path, static_identifier, file_io)
                .await
                .unwrap();
        static_table.into_table()
    }

    async fn get_test_catalog_and_table() -> (Arc<dyn Catalog>, NamespaceIdent, String, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let warehouse_path = temp_dir.path().to_str().unwrap().to_string();

        let catalog = MemoryCatalogBuilder::default()
            .load(
                "memory",
                HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), warehouse_path.clone())]),
            )
            .await
            .unwrap();

        let namespace = NamespaceIdent::new("test_ns".to_string());
        catalog
            .create_namespace(&namespace, HashMap::new())
            .await
            .unwrap();

        let schema = Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::required(2, "name", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
            .unwrap();

        let table_creation = TableCreation::builder()
            .name("test_table".to_string())
            .location(format!("{warehouse_path}/test_table"))
            .schema(schema)
            .properties(HashMap::new())
            .build();

        catalog
            .create_table(&namespace, table_creation)
            .await
            .unwrap();

        (
            Arc::new(catalog),
            namespace,
            "test_table".to_string(),
            temp_dir,
        )
    }

    // Tests for IcebergStaticTableProvider

    #[tokio::test]
    async fn test_static_provider_from_table() {
        let table = get_test_table_from_metadata_file().await;
        let table_provider = IcebergStaticTableProvider::try_new_from_table(table.clone())
            .await
            .unwrap();
        let ctx = SessionContext::new();
        ctx.register_table("mytable", Arc::new(table_provider))
            .unwrap();
        let df = ctx.sql("SELECT * FROM mytable").await.unwrap();
        let df_schema = df.schema();
        let df_columns = df_schema.fields();
        assert_eq!(df_columns.len(), 3);
        let x_column = df_columns.first().unwrap();
        let column_data = format!(
            "{:?}:{:?}",
            x_column.name(),
            x_column.data_type().to_string()
        );
        assert_eq!(column_data, "\"x\":\"Int64\"");
        let has_column = df_schema.has_column(&Column::from_name("z"));
        assert!(has_column);
    }

    #[tokio::test]
    async fn test_static_provider_from_snapshot() {
        let table = get_test_table_from_metadata_file().await;
        let snapshot_id = table.metadata().snapshots().next().unwrap().snapshot_id();
        let table_provider =
            IcebergStaticTableProvider::try_new_from_table_snapshot(table.clone(), snapshot_id)
                .await
                .unwrap();
        let ctx = SessionContext::new();
        ctx.register_table("mytable", Arc::new(table_provider))
            .unwrap();
        let df = ctx.sql("SELECT * FROM mytable").await.unwrap();
        let df_schema = df.schema();
        let df_columns = df_schema.fields();
        assert_eq!(df_columns.len(), 3);
        let x_column = df_columns.first().unwrap();
        let column_data = format!(
            "{:?}:{:?}",
            x_column.name(),
            x_column.data_type().to_string()
        );
        assert_eq!(column_data, "\"x\":\"Int64\"");
        let has_column = df_schema.has_column(&Column::from_name("z"));
        assert!(has_column);
    }

    #[tokio::test]
    async fn test_static_provider_rejects_writes() {
        let table = get_test_table_from_metadata_file().await;
        let table_provider = IcebergStaticTableProvider::try_new_from_table(table.clone())
            .await
            .unwrap();
        let ctx = SessionContext::new();
        ctx.register_table("mytable", Arc::new(table_provider))
            .unwrap();

        // Attempt to insert into the static provider should fail
        let result = ctx.sql("INSERT INTO mytable VALUES (1, 2, 3)").await;

        // The error should occur during planning or execution
        // We expect an error indicating write operations are not supported
        assert!(
            result.is_err() || {
                let df = result.unwrap();
                df.collect().await.is_err()
            }
        );
    }

    #[tokio::test]
    async fn test_static_provider_scan() {
        let table = get_test_table_from_metadata_file().await;
        let table_provider = IcebergStaticTableProvider::try_new_from_table(table.clone())
            .await
            .unwrap();
        let ctx = SessionContext::new();
        ctx.register_table("mytable", Arc::new(table_provider))
            .unwrap();

        // Test that scan operations work correctly
        let df = ctx.sql("SELECT count(*) FROM mytable").await.unwrap();
        let physical_plan = df.create_physical_plan().await;
        assert!(physical_plan.is_ok());
    }

    // Tests for IcebergTableProvider

    #[tokio::test]
    async fn test_catalog_backed_provider_creation() {
        let (catalog, namespace, table_name, _temp_dir) = get_test_catalog_and_table().await;

        // Test creating a catalog-backed provider
        let provider =
            IcebergTableProvider::try_new(catalog.clone(), namespace.clone(), table_name.clone())
                .await
                .unwrap();

        // Verify the schema is loaded correctly
        let schema = provider.schema();
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(1).name(), "name");
    }

    #[tokio::test]
    async fn test_catalog_backed_provider_scan() {
        let (catalog, namespace, table_name, _temp_dir) = get_test_catalog_and_table().await;

        let provider =
            IcebergTableProvider::try_new(catalog.clone(), namespace.clone(), table_name.clone())
                .await
                .unwrap();

        let ctx = SessionContext::new();
        ctx.register_table("test_table", Arc::new(provider))
            .unwrap();

        // Test that scan operations work correctly
        let df = ctx.sql("SELECT * FROM test_table").await.unwrap();

        // Verify the schema in the query result
        let df_schema = df.schema();
        assert_eq!(df_schema.fields().len(), 2);
        assert_eq!(df_schema.field(0).name(), "id");
        assert_eq!(df_schema.field(1).name(), "name");

        let physical_plan = df.create_physical_plan().await;
        assert!(physical_plan.is_ok());
    }

    #[tokio::test]
    async fn test_catalog_backed_provider_insert() {
        let (catalog, namespace, table_name, _temp_dir) = get_test_catalog_and_table().await;

        let provider =
            IcebergTableProvider::try_new(catalog.clone(), namespace.clone(), table_name.clone())
                .await
                .unwrap();

        let ctx = SessionContext::new();
        ctx.register_table("test_table", Arc::new(provider))
            .unwrap();

        // Test that insert operations work correctly
        let result = ctx.sql("INSERT INTO test_table VALUES (1, 'test')").await;

        // Insert should succeed (or at least not fail during planning)
        assert!(result.is_ok());

        // Try to execute the insert plan
        let df = result.unwrap();
        let execution_result = df.collect().await;

        // The execution should succeed
        assert!(execution_result.is_ok());
    }

    #[tokio::test]
    async fn test_physical_input_schema_consistent_with_logical_input_schema() {
        let (catalog, namespace, table_name, _temp_dir) = get_test_catalog_and_table().await;

        let provider =
            IcebergTableProvider::try_new(catalog.clone(), namespace.clone(), table_name.clone())
                .await
                .unwrap();

        let ctx = SessionContext::new();
        ctx.register_table("test_table", Arc::new(provider))
            .unwrap();

        // Create a query plan
        let df = ctx.sql("SELECT id, name FROM test_table").await.unwrap();

        // Get logical schema before consuming df
        let logical_schema = df.schema().clone();

        // Get physical plan (this consumes df)
        let physical_plan = df.create_physical_plan().await.unwrap();
        let physical_schema = physical_plan.schema();

        // Verify that logical and physical schemas are consistent
        assert_eq!(
            logical_schema.fields().len(),
            physical_schema.fields().len()
        );

        for (logical_field, physical_field) in logical_schema
            .fields()
            .iter()
            .zip(physical_schema.fields().iter())
        {
            assert_eq!(logical_field.name(), physical_field.name());
            assert_eq!(logical_field.data_type(), physical_field.data_type());
        }
    }

    async fn get_partitioned_test_catalog_and_table(
        fanout_enabled: Option<bool>,
    ) -> (Arc<dyn Catalog>, NamespaceIdent, String, TempDir) {
        use iceberg::spec::{Transform, UnboundPartitionSpec};

        let temp_dir = TempDir::new().unwrap();
        let warehouse_path = temp_dir.path().to_str().unwrap().to_string();

        let catalog = MemoryCatalogBuilder::default()
            .load(
                "memory",
                HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), warehouse_path.clone())]),
            )
            .await
            .unwrap();

        let namespace = NamespaceIdent::new("test_ns".to_string());
        catalog
            .create_namespace(&namespace, HashMap::new())
            .await
            .unwrap();

        let schema = Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::required(2, "category", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
            .unwrap();

        let partition_spec = UnboundPartitionSpec::builder()
            .with_spec_id(0)
            .add_partition_field(2, "category", Transform::Identity)
            .unwrap()
            .build();

        let mut properties = HashMap::new();
        if let Some(enabled) = fanout_enabled {
            properties.insert(
                iceberg::spec::TableProperties::PROPERTY_DATAFUSION_WRITE_FANOUT_ENABLED
                    .to_string(),
                enabled.to_string(),
            );
        }

        let table_creation = TableCreation::builder()
            .name("partitioned_table".to_string())
            .location(format!("{warehouse_path}/partitioned_table"))
            .schema(schema)
            .partition_spec(partition_spec)
            .properties(properties)
            .build();

        catalog
            .create_table(&namespace, table_creation)
            .await
            .unwrap();

        (
            Arc::new(catalog),
            namespace,
            "partitioned_table".to_string(),
            temp_dir,
        )
    }

    /// Helper to check if a plan contains a SortExec node
    fn plan_contains_sort(plan: &Arc<dyn ExecutionPlan>) -> bool {
        if plan.name() == "SortExec" {
            return true;
        }
        for child in plan.children() {
            if plan_contains_sort(child) {
                return true;
            }
        }
        false
    }

    #[tokio::test]
    async fn test_insert_plan_fanout_enabled_no_sort() {
        use datafusion::datasource::TableProvider;
        use datafusion::logical_expr::dml::InsertOp;
        use datafusion::physical_plan::empty::EmptyExec;

        // When fanout is enabled (default), no sort node should be added
        let (catalog, namespace, table_name, _temp_dir) =
            get_partitioned_test_catalog_and_table(Some(true)).await;

        let provider =
            IcebergTableProvider::try_new(catalog.clone(), namespace.clone(), table_name.clone())
                .await
                .unwrap();

        let ctx = SessionContext::new();
        let input_schema = provider.schema();
        let input = Arc::new(EmptyExec::new(input_schema)) as Arc<dyn ExecutionPlan>;

        let state = ctx.state();
        let insert_plan = provider
            .insert_into(&state, input, InsertOp::Append)
            .await
            .unwrap();

        // With fanout enabled, there should be no SortExec in the plan
        assert!(
            !plan_contains_sort(&insert_plan),
            "Plan should NOT contain SortExec when fanout is enabled"
        );
    }

    #[tokio::test]
    async fn test_insert_plan_fanout_disabled_has_sort() {
        use datafusion::datasource::TableProvider;
        use datafusion::logical_expr::dml::InsertOp;
        use datafusion::physical_plan::empty::EmptyExec;

        // When fanout is disabled, a sort node should be added
        let (catalog, namespace, table_name, _temp_dir) =
            get_partitioned_test_catalog_and_table(Some(false)).await;

        let provider =
            IcebergTableProvider::try_new(catalog.clone(), namespace.clone(), table_name.clone())
                .await
                .unwrap();

        let ctx = SessionContext::new();
        let input_schema = provider.schema();
        let input = Arc::new(EmptyExec::new(input_schema)) as Arc<dyn ExecutionPlan>;

        let state = ctx.state();
        let insert_plan = provider
            .insert_into(&state, input, InsertOp::Append)
            .await
            .unwrap();

        // With fanout disabled, there should be a SortExec in the plan
        assert!(
            plan_contains_sort(&insert_plan),
            "Plan should contain SortExec when fanout is disabled"
        );
    }

    #[tokio::test]
    async fn test_limit_pushdown_static_provider() {
        use datafusion::datasource::TableProvider;

        let table = get_test_table_from_metadata_file().await;
        let table_provider = IcebergStaticTableProvider::try_new_from_table(table.clone())
            .await
            .unwrap();

        let ctx = SessionContext::new();
        let state = ctx.state();

        // Test scan with limit
        let scan_plan = table_provider
            .scan(&state, None, &[], Some(10))
            .await
            .unwrap();

        // Verify that the scan plan is an IcebergTableScan
        let iceberg_scan = scan_plan
            .as_any()
            .downcast_ref::<IcebergTableScan>()
            .expect("Expected IcebergTableScan");

        // Verify the limit is set
        assert_eq!(
            iceberg_scan.limit(),
            Some(10),
            "Limit should be set to 10 in the scan plan"
        );
    }

    #[tokio::test]
    async fn test_limit_pushdown_catalog_backed_provider() {
        use datafusion::datasource::TableProvider;

        let (catalog, namespace, table_name, _temp_dir) = get_test_catalog_and_table().await;

        let provider =
            IcebergTableProvider::try_new(catalog.clone(), namespace.clone(), table_name.clone())
                .await
                .unwrap();

        let ctx = SessionContext::new();
        let state = ctx.state();

        // Test scan with limit
        let scan_plan = provider.scan(&state, None, &[], Some(5)).await.unwrap();

        // Verify that the scan plan is an IcebergTableScan
        let iceberg_scan = scan_plan
            .as_any()
            .downcast_ref::<IcebergTableScan>()
            .expect("Expected IcebergTableScan");

        // Verify the limit is set
        assert_eq!(
            iceberg_scan.limit(),
            Some(5),
            "Limit should be set to 5 in the scan plan"
        );
    }

    #[tokio::test]
    async fn test_no_limit_pushdown() {
        use datafusion::datasource::TableProvider;

        let table = get_test_table_from_metadata_file().await;
        let table_provider = IcebergStaticTableProvider::try_new_from_table(table.clone())
            .await
            .unwrap();

        let ctx = SessionContext::new();
        let state = ctx.state();

        // Test scan without limit
        let scan_plan = table_provider.scan(&state, None, &[], None).await.unwrap();

        // Verify that the scan plan is an IcebergTableScan
        let iceberg_scan = scan_plan
            .as_any()
            .downcast_ref::<IcebergTableScan>()
            .expect("Expected IcebergTableScan");

        // Verify the limit is None
        assert_eq!(
            iceberg_scan.limit(),
            None,
            "Limit should be None when not specified"
        );
    }

    // ── Bucketed scan tests ──────────────────────────────────────────────────

    async fn make_catalog_and_table_for_bucketing()
    -> (Arc<dyn Catalog>, NamespaceIdent, String, tempfile::TempDir) {
        use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
        use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
        use iceberg::{CatalogBuilder, TableCreation};

        let temp_dir = tempfile::TempDir::new().unwrap();
        let warehouse = temp_dir.path().to_str().unwrap().to_string();

        let catalog = Arc::new(
            MemoryCatalogBuilder::default()
                .load(
                    "memory",
                    std::collections::HashMap::from([(
                        MEMORY_CATALOG_WAREHOUSE.to_string(),
                        warehouse.clone(),
                    )]),
                )
                .await
                .unwrap(),
        );

        let namespace = NamespaceIdent::new("ns".to_string());
        catalog
            .create_namespace(&namespace, std::collections::HashMap::new())
            .await
            .unwrap();

        let schema = Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::required(2, "name", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
            .unwrap();

        catalog
            .create_table(
                &namespace,
                TableCreation::builder()
                    .name("t".to_string())
                    .location(format!("{warehouse}/t"))
                    .schema(schema)
                    .properties(std::collections::HashMap::new())
                    .build(),
            )
            .await
            .unwrap();

        (catalog, namespace, "t".to_string(), temp_dir)
    }

    /// Registers `n` synthetic data files in the table metadata via the iceberg
    /// transaction API. No actual parquet files are written, only the metadata
    /// entries that `plan_files()` reads are created.
    async fn append_fake_data_files(
        catalog: &Arc<dyn Catalog>,
        namespace: &NamespaceIdent,
        table_name: &str,
        n: usize,
    ) {
        use iceberg::spec::{DataContentType, DataFileBuilder, DataFileFormat};
        use iceberg::transaction::{ApplyTransactionAction, Transaction};

        let table = catalog
            .load_table(&TableIdent::new(namespace.clone(), table_name.to_string()))
            .await
            .unwrap();

        let data_files = (0..n)
            .map(|i| {
                DataFileBuilder::default()
                    .content(DataContentType::Data)
                    .file_path(format!(
                        "{}/data/fake_{i}.parquet",
                        table.metadata().location()
                    ))
                    .file_format(DataFileFormat::Parquet)
                    .file_size_in_bytes(128)
                    .record_count(1)
                    .partition_spec_id(table.metadata().default_partition_spec_id())
                    .build()
                    .unwrap()
            })
            .collect::<Vec<_>>();

        let tx = Transaction::new(&table);
        let action = tx.fast_append().add_data_files(data_files);
        action
            .apply(tx)
            .unwrap()
            .commit(catalog.as_ref())
            .await
            .unwrap();
    }

    fn ctx_with_target_partitions(n: usize) -> SessionContext {
        use datafusion::prelude::SessionConfig;
        SessionContext::new_with_config(SessionConfig::new().with_target_partitions(n))
    }

    /// An empty table must produce a single empty-bucket scan so that DataFusion
    /// can schedule the plan normally. execute(0) on an empty bucket simply
    /// returns an empty record-batch stream.
    #[tokio::test]
    async fn test_empty_table_single_empty_bucket() {
        let (catalog, namespace, table_name, _temp_dir) =
            make_catalog_and_table_for_bucketing().await;
        // no files appended
        let provider = IcebergTableProvider::try_new(catalog, namespace, table_name)
            .await
            .unwrap();
        let plan = provider
            .scan(&ctx_with_target_partitions(8).state(), None, &[], None)
            .await
            .unwrap();
        let scan = plan.as_any().downcast_ref::<IcebergTableScan>().unwrap();
        let buckets = scan.buckets().expect("expected eager scan buckets");

        assert_eq!(buckets.len(), 1);
        assert_eq!(buckets[0].len(), 0);
        assert_eq!(scan.properties().partitioning.partition_count(), 1);
    }

    /// When the table has no identity-partition columns, every task takes the
    /// fallback (file_path) bucket path, so the declaration must drop to
    /// `UnknownPartitioning`. The bucket count should still equal
    /// min(target_partitions, num_files).
    #[tokio::test]
    async fn test_unpartitioned_falls_back_to_unknown() {
        use datafusion::physical_plan::Partitioning;

        let (catalog, namespace, table_name, _temp_dir) =
            make_catalog_and_table_for_bucketing().await;
        append_fake_data_files(&catalog, &namespace, &table_name, 5).await;

        let provider = IcebergTableProvider::try_new(catalog, namespace, table_name)
            .await
            .unwrap();
        let plan = provider
            .scan(&ctx_with_target_partitions(3).state(), None, &[], None)
            .await
            .unwrap();
        let scan = plan.as_any().downcast_ref::<IcebergTableScan>().unwrap();
        let buckets = scan.buckets().expect("expected eager scan buckets");

        let total_files: usize = buckets.iter().map(|b| b.len()).sum();
        assert_eq!(total_files, 5);
        assert_eq!(buckets.len(), 3);
        assert!(matches!(
            scan.properties().partitioning,
            Partitioning::UnknownPartitioning(3)
        ));
    }

    /// Bucket count must be capped at the number of files: spinning up more
    /// DataFusion partitions than there are tasks would just leave empty
    /// streams, wasting scheduler slots.
    #[tokio::test]
    async fn test_bucket_count_capped_at_file_count() {
        let (catalog, namespace, table_name, _temp_dir) =
            make_catalog_and_table_for_bucketing().await;
        append_fake_data_files(&catalog, &namespace, &table_name, 2).await;

        let provider = IcebergTableProvider::try_new(catalog, namespace, table_name)
            .await
            .unwrap();
        let plan = provider
            .scan(&ctx_with_target_partitions(16).state(), None, &[], None)
            .await
            .unwrap();
        let scan = plan.as_any().downcast_ref::<IcebergTableScan>().unwrap();
        let buckets = scan.buckets().expect("expected eager scan buckets");

        assert_eq!(buckets.len(), 2);
    }

    /// target_partitions = 1 collapses every task into a single bucket, giving
    /// the same execution profile as a single-partition scan.
    #[tokio::test]
    async fn test_single_target_partition_single_bucket() {
        let (catalog, namespace, table_name, _temp_dir) =
            make_catalog_and_table_for_bucketing().await;
        append_fake_data_files(&catalog, &namespace, &table_name, 4).await;

        let provider = IcebergTableProvider::try_new(catalog, namespace, table_name)
            .await
            .unwrap();
        let plan = provider
            .scan(&ctx_with_target_partitions(1).state(), None, &[], None)
            .await
            .unwrap();
        let scan = plan.as_any().downcast_ref::<IcebergTableScan>().unwrap();
        let buckets = scan.buckets().expect("expected eager scan buckets");

        assert_eq!(buckets.len(), 1);
        assert_eq!(buckets[0].len(), 4);
    }

    #[tokio::test]
    async fn test_catalog_backed_eager_scan_uses_builder_projection_and_predicate() {
        use datafusion::prelude::{col, lit};
        use iceberg::expr::Reference;
        use iceberg::spec::Datum;

        let (catalog, namespace, table_name, _temp_dir) =
            make_catalog_and_table_for_bucketing().await;
        append_fake_data_files(&catalog, &namespace, &table_name, 2).await;

        let provider = IcebergTableProvider::try_new(catalog, namespace, table_name)
            .await
            .unwrap();
        let projection = vec![1_usize];
        let filters = vec![col("id").eq(lit(1_i32))];

        let plan = provider
            .scan(
                &ctx_with_target_partitions(2).state(),
                Some(&projection),
                &filters,
                None,
            )
            .await
            .unwrap();
        let scan = plan.as_any().downcast_ref::<IcebergTableScan>().unwrap();

        assert!(scan.buckets().is_some(), "expected eager scan buckets");
        assert_eq!(scan.projection().unwrap(), &["name".to_string()]);
        assert_eq!(
            scan.predicates(),
            Some(&Reference::new("id").equal_to(Datum::int(1)))
        );
    }

    async fn make_partitioned_catalog_and_table_for_bucketing()
    -> (Arc<dyn Catalog>, NamespaceIdent, String, tempfile::TempDir) {
        use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
        use iceberg::spec::{
            NestedField, PrimitiveType, Schema, Transform, Type, UnboundPartitionSpec,
        };
        use iceberg::{CatalogBuilder, TableCreation};

        let temp_dir = tempfile::TempDir::new().unwrap();
        let warehouse = temp_dir.path().to_str().unwrap().to_string();

        let catalog = Arc::new(
            MemoryCatalogBuilder::default()
                .load(
                    "memory",
                    std::collections::HashMap::from([(
                        MEMORY_CATALOG_WAREHOUSE.to_string(),
                        warehouse.clone(),
                    )]),
                )
                .await
                .unwrap(),
        );

        let namespace = NamespaceIdent::new("ns".to_string());
        catalog
            .create_namespace(&namespace, std::collections::HashMap::new())
            .await
            .unwrap();

        let schema = Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::required(2, "name", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
            .unwrap();

        let partition_spec = UnboundPartitionSpec::builder()
            .with_spec_id(0)
            .add_partition_field(2, "name_part", Transform::Identity)
            .unwrap()
            .build();

        catalog
            .create_table(
                &namespace,
                TableCreation::builder()
                    .name("t".to_string())
                    .location(format!("{warehouse}/t"))
                    .schema(schema)
                    .partition_spec(partition_spec)
                    .properties(std::collections::HashMap::new())
                    .build(),
            )
            .await
            .unwrap();

        (catalog, namespace, "t".to_string(), temp_dir)
    }

    /// Like [`append_fake_data_files`] but each file carries a partition tuple
    /// matching the table's identity-partition spec on `name`.
    async fn append_partitioned_fake_data_files(
        catalog: &Arc<dyn Catalog>,
        namespace: &NamespaceIdent,
        table_name: &str,
        partition_values: Vec<&str>,
    ) {
        use iceberg::spec::{DataContentType, DataFileBuilder, DataFileFormat, Literal, Struct};
        use iceberg::transaction::{ApplyTransactionAction, Transaction};

        let table = catalog
            .load_table(&TableIdent::new(namespace.clone(), table_name.to_string()))
            .await
            .unwrap();

        let data_files = partition_values
            .iter()
            .enumerate()
            .map(|(i, value)| {
                DataFileBuilder::default()
                    .content(DataContentType::Data)
                    .file_path(format!(
                        "{}/data/fake_{i}.parquet",
                        table.metadata().location()
                    ))
                    .file_format(DataFileFormat::Parquet)
                    .file_size_in_bytes(128)
                    .record_count(1)
                    .partition_spec_id(table.metadata().default_partition_spec_id())
                    .partition(Struct::from_iter(vec![Some(Literal::string(*value))]))
                    .build()
                    .unwrap()
            })
            .collect::<Vec<_>>();

        let tx = Transaction::new(&table);
        let action = tx.fast_append().add_data_files(data_files);
        action
            .apply(tx)
            .unwrap()
            .commit(catalog.as_ref())
            .await
            .unwrap();
    }

    /// Identity-partitioned table whose source column is in the projection
    /// must produce `Partitioning::Hash` referencing that column.
    #[tokio::test]
    async fn test_identity_partitioned_declares_hash() {
        use datafusion::physical_expr::expressions::Column;
        use datafusion::physical_plan::Partitioning;

        let (catalog, namespace, table_name, _temp_dir) =
            make_partitioned_catalog_and_table_for_bucketing().await;
        append_partitioned_fake_data_files(&catalog, &namespace, &table_name, vec![
            "a", "b", "c", "a", "b", "c",
        ])
        .await;

        let provider = IcebergTableProvider::try_new(catalog, namespace, table_name)
            .await
            .unwrap();
        let plan = provider
            .scan(&ctx_with_target_partitions(3).state(), None, &[], None)
            .await
            .unwrap();
        let scan = plan.as_any().downcast_ref::<IcebergTableScan>().unwrap();
        let buckets = scan.buckets().expect("expected eager scan buckets");

        let total_files: usize = buckets.iter().map(|b| b.len()).sum();
        assert_eq!(total_files, 6);

        match &scan.properties().partitioning {
            Partitioning::Hash(exprs, n) => {
                assert_eq!(*n, 3);
                assert_eq!(exprs.len(), 1);
                let col = exprs[0]
                    .as_any()
                    .downcast_ref::<Column>()
                    .expect("expected Column expr");
                assert_eq!(col.name(), "name");
            }
            other => panic!("expected Partitioning::Hash, got {other:?}"),
        }
    }

    /// Identity partition task buckets must match DataFusion's own hash
    /// repartition bucket calculation for the same concrete Arrow array type.
    #[tokio::test]
    async fn test_identity_partitioned_hash_buckets_match_datafusion_repartition() {
        use datafusion::arrow::array::{ArrayRef, StringArray};
        use datafusion::common::hash_utils::create_hashes;
        use datafusion::physical_plan::Partitioning;
        use datafusion::physical_plan::repartition::REPARTITION_RANDOM_STATE;

        let partition_values = vec!["a", "b", "c", "a", "b", "c", "z"];
        let n_partitions = 4_usize;

        let (catalog, namespace, table_name, _temp_dir) =
            make_partitioned_catalog_and_table_for_bucketing().await;
        append_partitioned_fake_data_files(
            &catalog,
            &namespace,
            &table_name,
            partition_values.clone(),
        )
        .await;

        let provider = IcebergTableProvider::try_new(catalog, namespace, table_name)
            .await
            .unwrap();
        let plan = provider
            .scan(
                &ctx_with_target_partitions(n_partitions).state(),
                None,
                &[],
                None,
            )
            .await
            .unwrap();
        let scan = plan.as_any().downcast_ref::<IcebergTableScan>().unwrap();
        let buckets = scan.buckets().expect("expected eager scan buckets");

        assert!(matches!(
            scan.properties().partitioning,
            Partitioning::Hash(_, 4)
        ));

        let arrays: Vec<ArrayRef> = vec![Arc::new(StringArray::from(partition_values))];
        let mut hashes = vec![0_u64; arrays[0].len()];
        create_hashes(
            &arrays,
            REPARTITION_RANDOM_STATE.random_state(),
            &mut hashes,
        )
        .unwrap();

        let mut actual_bucket_by_file = vec![None; hashes.len()];
        for (bucket_idx, bucket) in buckets.iter().enumerate() {
            for task in bucket.iter() {
                let file_idx = task
                    .data_file_path()
                    .strip_suffix(".parquet")
                    .and_then(|path| path.rsplit_once("fake_").map(|(_, idx)| idx))
                    .and_then(|idx| idx.parse::<usize>().ok())
                    .expect("fake data file path should include its row index");
                actual_bucket_by_file[file_idx] = Some(bucket_idx);
            }
        }

        for (file_idx, hash) in hashes.iter().enumerate() {
            let expected_bucket = (hash % n_partitions as u64) as usize;
            assert_eq!(
                actual_bucket_by_file[file_idx],
                Some(expected_bucket),
                "file {file_idx} should be assigned to DataFusion hash bucket {expected_bucket}"
            );
        }
    }

    /// A projection that omits the partition source column drops
    /// `compute_identity_cols` to `None`, collapsing to `UnknownPartitioning`.
    #[tokio::test]
    async fn test_projection_without_partition_col_falls_back_to_unknown() {
        use datafusion::physical_plan::Partitioning;

        let (catalog, namespace, table_name, _temp_dir) =
            make_partitioned_catalog_and_table_for_bucketing().await;
        append_partitioned_fake_data_files(&catalog, &namespace, &table_name, vec!["a", "b"]).await;

        let provider = IcebergTableProvider::try_new(catalog, namespace, table_name)
            .await
            .unwrap();
        // Project only "id" (idx 0), excluding the partition column "name" (idx 1).
        let projection = vec![0_usize];
        let plan = provider
            .scan(
                &ctx_with_target_partitions(3).state(),
                Some(&projection),
                &[],
                None,
            )
            .await
            .unwrap();
        let scan = plan.as_any().downcast_ref::<IcebergTableScan>().unwrap();

        assert!(matches!(
            scan.properties().partitioning,
            Partitioning::UnknownPartitioning(_)
        ));
    }
}
