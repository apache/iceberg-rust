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

use std::any::Any;
use std::sync::Arc;

use datafusion::error::Result as DFResult;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::expressions::Column;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};
use iceberg::spec::{SchemaRef, TableMetadata, TableMetadataRef, Transform};

/// Iceberg-specific repartition execution plan that optimizes data distribution
/// for parallel processing while respecting Iceberg table partitioning semantics.
///
/// This execution plan automatically determines the optimal partitioning strategy based on
/// the table's partition specification and sort order:
///
/// ## Partitioning Strategies
///
/// - **Unpartitioned tables**: Uses round-robin distribution to ensure balanced load
///   across all workers, maximizing parallelism for write operations.
///
/// - **Hash partitioning**: Used for tables with identity transforms or bucket transforms:
///   - Identity partition columns (e.g., `PARTITIONED BY (user_id, category)`)
///   - Bucket columns from partition spec or sort order
///   - This ensures data co-location within partitions and buckets for optimal file clustering
///
/// - **Round-robin partitioning**: Used for:
///   - Range-only partitions (e.g., date/time partitions that concentrate data)
///   - Tables with only temporal/range transforms that don't provide good distribution
///   - Unpartitioned or non-bucketed tables
///
/// - **Mixed transforms**: Tables with both range and identity/bucket transforms use hash
///   partitioning on the identity/bucket columns for optimal distribution.
///
/// ## Performance notes
///
/// - Only repartitions when the input partitioning scheme differs from the desired strategy
/// - Only repartitions when the input partition count differs from the target
/// - Requires explicit target partition count for deterministic behavior
/// - Preserves column order (partitions first, then buckets) for consistent file layout
#[derive(Debug)]
pub struct IcebergRepartitionExec {
    /// Input execution plan
    input: Arc<dyn ExecutionPlan>,
    /// Iceberg table schema to determine partitioning strategy
    table_schema: SchemaRef,
    /// Iceberg table metadata to determine partitioning strategy
    table_metadata: TableMetadataRef,
    /// Target number of partitions for data distribution
    target_partitions: usize,
    /// Partitioning strategy
    partitioning_strategy: Partitioning,
    /// Plan properties for optimization
    plan_properties: PlanProperties,
}

impl IcebergRepartitionExec {
    /// Creates a new IcebergRepartitionExec with automatic partitioning strategy selection.
    ///
    /// This constructor analyzes the table's partition specification and sort order to determine
    /// the optimal repartitioning strategy for insert operations.
    ///
    /// # Arguments
    ///
    /// * `input` - The input execution plan providing data to be repartitioned
    /// * `table_schema` - The Iceberg table schema used to resolve column references
    /// * `table_metadata` - The Iceberg table metadata containing partition spec and sort order
    /// * `target_partitions` - Target number of partitions for parallel processing:
    ///   - Must be > 0 (explicit partition count for performance tuning)
    ///
    /// # Returns
    ///
    /// A configured repartition execution plan that will apply the optimal partitioning
    /// strategy during execution, or pass through unchanged data if no repartitioning
    /// is needed.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let repartition_exec = IcebergRepartitionExec::new(
    ///     input_plan,
    ///     table.schema_ref(),
    ///     table.metadata_ref(),
    ///     4, // Explicit partition count
    /// )?;
    /// ```
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        table_schema: SchemaRef,
        table_metadata: TableMetadataRef,
        target_partitions: usize,
    ) -> DFResult<Self> {
        if target_partitions == 0 {
            return Err(datafusion::error::DataFusionError::Plan(
                "IcebergRepartitionExec requires target_partitions > 0".to_string(),
            ));
        }

        let partitioning_strategy = Self::determine_partitioning_strategy(
            &input,
            &table_schema,
            &table_metadata,
            target_partitions,
        )?;

        let plan_properties = Self::compute_properties(&input, &partitioning_strategy)?;

        Ok(Self {
            input,
            table_schema,
            table_metadata,
            target_partitions,
            partitioning_strategy,
            plan_properties,
        })
    }

    /// Computes the plan properties based on the table partitioning strategy
    /// Selects the partitioning strategy based on the table partitioning strategy
    fn compute_properties(
        input: &Arc<dyn ExecutionPlan>,
        partitioning_strategy: &Partitioning,
    ) -> DFResult<PlanProperties> {
        let schema = input.schema();
        let equivalence_properties = EquivalenceProperties::new(schema);

        Ok(PlanProperties::new(
            equivalence_properties,
            partitioning_strategy.clone(),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ))
    }

    /// Determines the optimal partitioning strategy based on table metadata.
    ///
    /// This function analyzes the table's partition specification and sort order to select
    /// the most appropriate DataFusion partitioning strategy for insert operations.
    ///
    /// ## Partitioning Strategy Logic
    ///
    /// The strategy is determined by analyzing the table's partition transforms:
    ///
    /// - **Hash partitioning**: Used only when there are identity transforms (direct column partitioning)
    ///   or bucket transforms that provide good data distribution:
    ///   1. Identity partition columns (e.g., `PARTITIONED BY (user_id, category)`)
    ///   2. Bucket columns from partition spec (e.g., `bucket(16, user_id)`)
    ///   3. Bucket columns from sort order
    ///   
    ///   This ensures data co-location within partitions and buckets for optimal file clustering.
    ///
    /// - **Round-robin partitioning**: Used for:
    ///   - Unpartitioned tables
    ///   - Range-only partitions (e.g., date/time partitions that concentrate data)
    ///   - Tables with only temporal/range transforms that don't provide good distribution
    ///   - Tables with no suitable hash columns
    ///
    /// ## Column Priority and Deduplication
    ///
    /// When multiple column sources are available, they are combined in this order:
    /// 1. Partition identity columns (highest priority)
    /// 2. Bucket columns from partition spec  
    /// 3. Bucket columns from sort order
    ///
    /// Duplicate columns are automatically removed while preserving the priority order.
    ///
    /// ## Fallback Behavior
    ///
    /// If no suitable hash columns are found (e.g., unpartitioned, range-only, or non-bucketed table),
    /// falls back to round-robin batch partitioning for even load distribution.
    fn determine_partitioning_strategy(
        input: &Arc<dyn ExecutionPlan>,
        table_schema: &SchemaRef,
        table_metadata: &TableMetadata,
        target_partitions: usize,
    ) -> DFResult<Partitioning> {
        use std::collections::HashSet;

        let partition_spec = table_metadata.default_partition_spec();
        let sort_order = table_metadata.default_sort_order();

        // Column name iter for hashing depending on mode
        let names_iter: Box<dyn Iterator<Item = &str>> = {
            // Partition identity columns
            let part_names = partition_spec.fields().iter().filter_map(|pf| {
                if matches!(pf.transform, Transform::Identity) {
                    table_schema
                        .field_by_id(pf.source_id)
                        .map(|sf| sf.name.as_str())
                } else {
                    None
                }
            });
            // Bucket columns from partition spec
            let bucket_names_part = partition_spec.fields().iter().filter_map(|pf| {
                if let Transform::Bucket(_) = pf.transform {
                    table_schema
                        .field_by_id(pf.source_id)
                        .map(|sf| sf.name.as_str())
                } else {
                    None
                }
            });
            // Bucket columns from sort order
            let bucket_names_sort = sort_order.fields.iter().filter_map(|sf| {
                if let Transform::Bucket(_) = sf.transform {
                    table_schema
                        .field_by_id(sf.source_id)
                        .map(|field| field.name.as_str())
                } else {
                    None
                }
            });
            Box::new(part_names.chain(bucket_names_part).chain(bucket_names_sort))
        };

        // Order: partitions first, then buckets
        // Deduplicate while preserving order
        let input_schema = input.schema();
        let mut seen: HashSet<&str> = HashSet::new();
        let hash_exprs: Vec<Arc<dyn datafusion::physical_expr::PhysicalExpr>> = names_iter
            .filter(|name| seen.insert(*name))
            .map(|name| {
                let idx = input_schema
                    .index_of(name)
                    .map_err(|e| datafusion::error::DataFusionError::Plan(e.to_string()))?;
                Ok(Arc::new(Column::new(name, idx))
                    as Arc<dyn datafusion::physical_expr::PhysicalExpr>)
            })
            .collect::<DFResult<_>>()?;

        if !hash_exprs.is_empty() {
            return Ok(Partitioning::Hash(hash_exprs, target_partitions));
        }

        // Fallback to round-robin for unpartitioned, non-bucketed tables, and range-only partitions
        Ok(Partitioning::RoundRobinBatch(target_partitions))
    }

    /// Returns whether repartitioning is actually needed
    pub fn needs_repartitioning(&self) -> bool {
        let desired = self.plan_properties.output_partitioning();
        let input_p = self.input.properties().output_partitioning();
        match (input_p, desired) {
            (Partitioning::RoundRobinBatch(a), Partitioning::RoundRobinBatch(b)) => a != b,
            (Partitioning::Hash(a_exprs, a_n), Partitioning::Hash(b_exprs, b_n)) => {
                a_n != b_n || !self.same_columns(a_exprs, b_exprs)
            }
            _ => true,
        }
    }

    /// Helper function to check if two sets of column expressions are the same
    fn same_columns(
        &self,
        a_exprs: &[Arc<dyn PhysicalExpr>],
        b_exprs: &[Arc<dyn PhysicalExpr>],
    ) -> bool {
        if a_exprs.len() != b_exprs.len() {
            return false;
        }
        a_exprs.iter().zip(b_exprs.iter()).all(|(a, b)| {
            a.as_any().downcast_ref::<Column>() == b.as_any().downcast_ref::<Column>()
        })
    }
}

impl ExecutionPlan for IcebergRepartitionExec {
    fn name(&self) -> &str {
        "IcebergRepartitionExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(datafusion::error::DataFusionError::Internal(
                "IcebergRepartitionExec should have exactly one child".to_string(),
            ));
        }

        Ok(Arc::new(IcebergRepartitionExec::new(
            children[0].clone(),
            self.table_schema.clone(),
            self.table_metadata.clone(),
            self.target_partitions,
        )?))
    }

    fn properties(&self) -> &PlanProperties {
        &self.plan_properties
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        // If no repartitioning is needed, pass through the input stream
        if !self.needs_repartitioning() {
            return self.input.execute(partition, context);
        }

        let repartition_exec =
            RepartitionExec::try_new(self.input.clone(), self.partitioning_strategy.clone())?;

        repartition_exec.execute(partition, context)
    }
}

impl DisplayAs for IcebergRepartitionExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                let table_type = if self
                    .table_metadata
                    .default_partition_spec()
                    .is_unpartitioned()
                {
                    "unpartitioned"
                } else {
                    "partitioned"
                };

                write!(
                    f,
                    "IcebergRepartitionExec: target_partitions={}, table={}, needs_repartitioning={}",
                    self.target_partitions,
                    table_type,
                    self.needs_repartitioning()
                )
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::{
        DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema,
    };
    use datafusion::physical_plan::empty::EmptyExec;
    use iceberg::TableIdent;
    use iceberg::io::FileIO;
    use iceberg::spec::{
        NestedField, NullOrder, PrimitiveType, Schema, SortDirection, SortField, SortOrder,
        Transform, Type,
    };
    use iceberg::table::Table;

    use super::*;

    fn create_test_table() -> Table {
        let schema = Schema::builder()
            .with_fields(vec![
                Arc::new(NestedField::required(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Long),
                )),
                Arc::new(NestedField::required(
                    2,
                    "data",
                    Type::Primitive(PrimitiveType::String),
                )),
            ])
            .build()
            .unwrap();

        let partition_spec = iceberg::spec::PartitionSpec::builder(schema.clone())
            .build()
            .unwrap();
        let sort_order = iceberg::spec::SortOrder::builder().build(&schema).unwrap();
        let table_metadata_builder = iceberg::spec::TableMetadataBuilder::new(
            schema,
            partition_spec,
            sort_order,
            "/test/table".to_string(),
            iceberg::spec::FormatVersion::V2,
            std::collections::HashMap::new(),
        )
        .unwrap();

        let table_metadata = table_metadata_builder.build().unwrap();

        Table::builder()
            .metadata(table_metadata.metadata)
            .identifier(TableIdent::from_strs(["test", "table"]).unwrap())
            .file_io(FileIO::from_path("/tmp").unwrap().build().unwrap())
            .metadata_location("/test/metadata.json".to_string())
            .build()
            .unwrap()
    }

    fn create_test_arrow_schema() -> Arc<ArrowSchema> {
        Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", ArrowDataType::Int64, false),
            ArrowField::new("data", ArrowDataType::Utf8, false),
        ]))
    }

    #[tokio::test]
    async fn test_repartition_unpartitioned_table() {
        let table = create_test_table();
        let input = Arc::new(EmptyExec::new(create_test_arrow_schema()));

        let repartition_exec = IcebergRepartitionExec::new(
            input,
            table.metadata().current_schema().clone(),
            table.metadata_ref(),
            4,
        )
        .unwrap();

        assert_eq!(repartition_exec.target_partitions, 4);
        assert_eq!(repartition_exec.name(), "IcebergRepartitionExec");

        assert!(repartition_exec.needs_repartitioning());
    }

    #[tokio::test]
    async fn test_repartition_explicit_partitions() {
        let table = create_test_table();
        let input = Arc::new(EmptyExec::new(create_test_arrow_schema()));

        let repartition_exec = IcebergRepartitionExec::new(
            input,
            table.metadata().current_schema().clone(),
            table.metadata_ref(),
            8,
        )
        .unwrap();

        assert_eq!(repartition_exec.target_partitions, 8);
    }

    #[tokio::test]
    async fn test_repartition_zero_partitions_fails() {
        let table = create_test_table();
        let input = Arc::new(EmptyExec::new(create_test_arrow_schema()));

        let result = IcebergRepartitionExec::new(
            input,
            table.metadata().current_schema().clone(),
            table.metadata_ref(),
            0,
        );

        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("requires target_partitions > 0")
        );
    }

    #[tokio::test]
    async fn test_partition_count_validation() {
        let table = create_test_table();
        let input = Arc::new(EmptyExec::new(create_test_arrow_schema()));

        // Test that explicit partition counts work correctly
        let target_partitions = 16; // Fixed value for deterministic tests
        let repartition_exec = IcebergRepartitionExec::new(
            input,
            table.metadata().current_schema().clone(),
            table.metadata_ref(),
            target_partitions,
        )
        .unwrap();

        assert_eq!(repartition_exec.target_partitions, target_partitions);
    }

    #[tokio::test]
    async fn test_datafusion_repartitioning_integration() {
        let table = create_test_table();
        let input = Arc::new(EmptyExec::new(create_test_arrow_schema()));

        // Create repartition exec with 3 target partitions
        let repartition_exec = IcebergRepartitionExec::new(
            input,
            table.metadata().current_schema().clone(),
            table.metadata_ref(),
            3,
        )
        .unwrap();

        // Verify that our Iceberg-aware partitioning strategy is applied correctly
        let partitioning = repartition_exec.properties().output_partitioning();
        match partitioning {
            Partitioning::RoundRobinBatch(n) => {
                assert_eq!(*n, 3, "Should use round-robin for unpartitioned table");
            }
            _ => panic!("Expected RoundRobinBatch partitioning for unpartitioned table"),
        }

        // Test execution - verify DataFusion integration works
        let task_ctx = Arc::new(TaskContext::default());
        let stream = repartition_exec.execute(0, task_ctx.clone()).unwrap();

        // Verify the stream was created successfully
        assert!(!stream.schema().fields().is_empty());
    }

    #[tokio::test]
    async fn test_bucket_aware_partitioning() {
        let schema = Schema::builder()
            .with_fields(vec![
                Arc::new(NestedField::required(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Long),
                )),
                Arc::new(NestedField::required(
                    2,
                    "category",
                    Type::Primitive(PrimitiveType::String),
                )),
            ])
            .build()
            .unwrap();

        // Create a sort order with bucket transform
        let sort_order = SortOrder::builder()
            .with_order_id(1)
            .with_sort_field(SortField {
                source_id: 2, // category column
                transform: Transform::Bucket(4),
                direction: SortDirection::Ascending,
                null_order: NullOrder::First,
            })
            .build(&schema)
            .unwrap();

        let partition_spec = iceberg::spec::PartitionSpec::builder(schema.clone())
            .build()
            .unwrap();
        let table_metadata_builder = iceberg::spec::TableMetadataBuilder::new(
            schema,
            partition_spec,
            sort_order,
            "/test/bucketed_table".to_string(),
            iceberg::spec::FormatVersion::V2,
            std::collections::HashMap::new(),
        )
        .unwrap();

        let table_metadata = table_metadata_builder.build().unwrap();
        let table = Table::builder()
            .metadata(table_metadata.metadata)
            .identifier(TableIdent::from_strs(["test", "bucketed_table"]).unwrap())
            .file_io(FileIO::from_path("/tmp").unwrap().build().unwrap())
            .metadata_location("/test/bucketed_metadata.json".to_string())
            .build()
            .unwrap();

        // Create Arrow schema that matches the Iceberg schema for this test
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", ArrowDataType::Int64, false),
            ArrowField::new("category", ArrowDataType::Utf8, false),
        ]));
        let input = Arc::new(EmptyExec::new(arrow_schema));
        let repartition_exec = IcebergRepartitionExec::new(
            input,
            table.metadata().current_schema().clone(),
            table.metadata_ref(),
            4,
        )
        .unwrap();

        // Should use hash partitioning for bucketed table
        let partitioning = repartition_exec.properties().output_partitioning();
        assert!(
            matches!(partitioning, Partitioning::Hash(_, _)),
            "Should use hash partitioning for bucketed table"
        );
    }

    #[tokio::test]
    async fn test_combined_partition_and_bucket_strategy() {
        let schema = Schema::builder()
            .with_fields(vec![
                Arc::new(NestedField::required(
                    1,
                    "date",
                    Type::Primitive(PrimitiveType::Date),
                )),
                Arc::new(NestedField::required(
                    2,
                    "user_id",
                    Type::Primitive(PrimitiveType::Long),
                )),
                Arc::new(NestedField::required(
                    3,
                    "amount",
                    Type::Primitive(PrimitiveType::Long),
                )),
            ])
            .build()
            .unwrap();

        // Create partition spec on date column
        let partition_spec = iceberg::spec::PartitionSpec::builder(schema.clone())
            .add_partition_field("date", "date", Transform::Identity)
            .unwrap()
            .build()
            .unwrap();

        // Create sort order with bucket transform on user_id
        let sort_order = SortOrder::builder()
            .with_order_id(1)
            .with_sort_field(SortField {
                source_id: 2, // user_id column
                transform: Transform::Bucket(8),
                direction: SortDirection::Ascending,
                null_order: NullOrder::First,
            })
            .build(&schema)
            .unwrap();

        let table_metadata_builder = iceberg::spec::TableMetadataBuilder::new(
            schema,
            partition_spec,
            sort_order,
            "/test/partitioned_bucketed_table".to_string(),
            iceberg::spec::FormatVersion::V2,
            std::collections::HashMap::new(),
        )
        .unwrap();

        let table_metadata = table_metadata_builder.build().unwrap();
        let table = Table::builder()
            .metadata(table_metadata.metadata)
            .identifier(TableIdent::from_strs(["test", "partitioned_bucketed_table"]).unwrap())
            .file_io(FileIO::from_path("/tmp").unwrap().build().unwrap())
            .metadata_location("/test/partitioned_bucketed_metadata.json".to_string())
            .build()
            .unwrap();

        // Create Arrow schema that matches the Iceberg schema for this test
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("date", ArrowDataType::Date32, false),
            ArrowField::new("user_id", ArrowDataType::Int64, false),
            ArrowField::new("amount", ArrowDataType::Int64, false),
        ]));
        let input = Arc::new(EmptyExec::new(arrow_schema));
        let repartition_exec = IcebergRepartitionExec::new(
            input,
            table.metadata().current_schema().clone(),
            table.metadata_ref(),
            4,
        )
        .unwrap();

        // Should use hash partitioning with BOTH partition and bucket columns
        let partitioning = repartition_exec.properties().output_partitioning();
        match partitioning {
            Partitioning::Hash(exprs, _) => {
                assert_eq!(
                    exprs.len(),
                    2,
                    "Should have both partition and bucket columns"
                );

                let column_names: Vec<String> = exprs
                    .iter()
                    .filter_map(|expr| {
                        expr.as_any()
                            .downcast_ref::<Column>()
                            .map(|col| col.name().to_string())
                    })
                    .collect();

                assert!(
                    column_names.contains(&"date".to_string()),
                    "Should include partition column 'date'"
                );
                assert!(
                    column_names.contains(&"user_id".to_string()),
                    "Should include bucket column 'user_id'"
                );
            }
            _ => panic!("Expected Hash partitioning for partitioned+bucketed table"),
        }
    }

    #[tokio::test]
    async fn test_none_distribution_mode_fallback() {
        let schema = Schema::builder()
            .with_fields(vec![Arc::new(NestedField::required(
                1,
                "id",
                Type::Primitive(PrimitiveType::Long),
            ))])
            .build()
            .unwrap();

        let partition_spec = iceberg::spec::PartitionSpec::builder(schema.clone())
            .build()
            .unwrap();
        let sort_order = iceberg::spec::SortOrder::builder().build(&schema).unwrap();

        let mut properties = std::collections::HashMap::new();
        properties.insert("write.distribution-mode".to_string(), "none".to_string());

        let table_metadata_builder = iceberg::spec::TableMetadataBuilder::new(
            schema,
            partition_spec,
            sort_order,
            "/test/none_table".to_string(),
            iceberg::spec::FormatVersion::V2,
            properties,
        )
        .unwrap();

        let table_metadata = table_metadata_builder.build().unwrap();
        let table = Table::builder()
            .metadata(table_metadata.metadata)
            .identifier(TableIdent::from_strs(["test", "none_table"]).unwrap())
            .file_io(FileIO::from_path("/tmp").unwrap().build().unwrap())
            .metadata_location("/test/none_metadata.json".to_string())
            .build()
            .unwrap();

        let input = Arc::new(EmptyExec::new(create_test_arrow_schema()));
        let repartition_exec = IcebergRepartitionExec::new(
            input,
            table.metadata().current_schema().clone(),
            table.metadata_ref(),
            4,
        )
        .unwrap();

        // Should fallback to round-robin for "none" distribution mode
        let partitioning = repartition_exec.properties().output_partitioning();
        assert!(
            matches!(partitioning, Partitioning::RoundRobinBatch(_)),
            "Should use round-robin for 'none' distribution mode"
        );
    }

    #[tokio::test]
    async fn test_schema_ref_convenience_method() {
        let table = create_test_table();

        // Test that table.schema_ref() works correctly
        let schema_ref_1 = table.schema_ref();
        let schema_ref_2 = Arc::clone(table.metadata().current_schema());

        // Should be the same Arc
        assert!(
            Arc::ptr_eq(&schema_ref_1, &schema_ref_2),
            "schema_ref() should return the same Arc as manual approach"
        );
    }

    #[tokio::test]
    async fn test_range_only_partitions_use_round_robin() {
        let schema = Schema::builder()
            .with_fields(vec![
                Arc::new(NestedField::required(
                    1,
                    "date",
                    Type::Primitive(PrimitiveType::Date),
                )),
                Arc::new(NestedField::required(
                    2,
                    "amount",
                    Type::Primitive(PrimitiveType::Long),
                )),
            ])
            .build()
            .unwrap();

        let partition_spec = iceberg::spec::PartitionSpec::builder(schema.clone())
            .add_partition_field("date", "date_day", Transform::Day)
            .unwrap()
            .build()
            .unwrap();

        let sort_order = iceberg::spec::SortOrder::builder().build(&schema).unwrap();
        let table_metadata_builder = iceberg::spec::TableMetadataBuilder::new(
            schema,
            partition_spec,
            sort_order,
            "/test/range_only_table".to_string(),
            iceberg::spec::FormatVersion::V2,
            std::collections::HashMap::new(),
        )
        .unwrap();

        let table_metadata = table_metadata_builder.build().unwrap();
        let table = Table::builder()
            .metadata(table_metadata.metadata)
            .identifier(TableIdent::from_strs(["test", "range_only_table"]).unwrap())
            .file_io(FileIO::from_path("/tmp").unwrap().build().unwrap())
            .metadata_location("/test/range_only_metadata.json".to_string())
            .build()
            .unwrap();

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("date", ArrowDataType::Date32, false),
            ArrowField::new("amount", ArrowDataType::Int64, false),
        ]));
        let input = Arc::new(EmptyExec::new(arrow_schema));
        let repartition_exec = IcebergRepartitionExec::new(
            input,
            table.metadata().current_schema().clone(),
            table.metadata_ref(),
            4,
        )
        .unwrap();

        let partitioning = repartition_exec.properties().output_partitioning();
        assert!(
            matches!(partitioning, Partitioning::RoundRobinBatch(_)),
            "Should use round-robin for range-only partitions"
        );
    }

    #[tokio::test]
    async fn test_mixed_transforms_use_hash_partitioning() {
        let schema = Schema::builder()
            .with_fields(vec![
                Arc::new(NestedField::required(
                    1,
                    "date",
                    Type::Primitive(PrimitiveType::Date),
                )),
                Arc::new(NestedField::required(
                    2,
                    "user_id",
                    Type::Primitive(PrimitiveType::Long),
                )),
                Arc::new(NestedField::required(
                    3,
                    "amount",
                    Type::Primitive(PrimitiveType::Long),
                )),
            ])
            .build()
            .unwrap();

        // Create partition spec with both range (date) and identity (user_id) transforms
        let partition_spec = iceberg::spec::PartitionSpec::builder(schema.clone())
            .add_partition_field("date", "date_day", Transform::Day)
            .unwrap()
            .add_partition_field("user_id", "user_id", Transform::Identity)
            .unwrap()
            .build()
            .unwrap();

        let sort_order = iceberg::spec::SortOrder::builder().build(&schema).unwrap();
        let table_metadata_builder = iceberg::spec::TableMetadataBuilder::new(
            schema,
            partition_spec,
            sort_order,
            "/test/mixed_transforms_table".to_string(),
            iceberg::spec::FormatVersion::V2,
            std::collections::HashMap::new(),
        )
        .unwrap();

        let table_metadata = table_metadata_builder.build().unwrap();
        let table = Table::builder()
            .metadata(table_metadata.metadata)
            .identifier(TableIdent::from_strs(["test", "mixed_transforms_table"]).unwrap())
            .file_io(FileIO::from_path("/tmp").unwrap().build().unwrap())
            .metadata_location("/test/mixed_transforms_metadata.json".to_string())
            .build()
            .unwrap();

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("date", ArrowDataType::Date32, false),
            ArrowField::new("user_id", ArrowDataType::Int64, false),
            ArrowField::new("amount", ArrowDataType::Int64, false),
        ]));
        let input = Arc::new(EmptyExec::new(arrow_schema));
        let repartition_exec = IcebergRepartitionExec::new(
            input,
            table.metadata().current_schema().clone(),
            table.metadata_ref(),
            4,
        )
        .unwrap();

        let partitioning = repartition_exec.properties().output_partitioning();
        match partitioning {
            Partitioning::Hash(exprs, _) => {
                assert_eq!(
                    exprs.len(),
                    1,
                    "Should have one hash column (user_id identity transform)"
                );
                let column_names: Vec<String> = exprs
                    .iter()
                    .filter_map(|expr| {
                        expr.as_any()
                            .downcast_ref::<Column>()
                            .map(|col| col.name().to_string())
                    })
                    .collect();
                assert!(
                    column_names.contains(&"user_id".to_string()),
                    "Should include identity transform column 'user_id'"
                );
            }
            _ => panic!("Expected Hash partitioning for table with identity transforms"),
        }
    }
}
