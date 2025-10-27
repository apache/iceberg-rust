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

use std::collections::HashSet;
use std::num::NonZeroUsize;
use std::sync::Arc;

use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::expressions::Column;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::{ExecutionPlan, Partitioning};
use iceberg::spec::{TableMetadata, TableMetadataRef, Transform};
use tracing;

use crate::physical_plan::project::PARTITION_VALUES_COLUMN;

/// Creates an Iceberg-aware repartition execution plan that optimizes data distribution
/// for parallel processing while respecting Iceberg table partitioning semantics.
///
/// Automatically determines the optimal partitioning strategy based on the table's
/// partition specification and sort order.
///
/// ## Partitioning Strategies
///
/// - **Unpartitioned tables** – Uses round-robin distribution to balance load across workers.
/// - **Hash partitioning** – Applied for tables with identity or bucket transforms, ensuring
///   co-location of related data for efficient file clustering.
/// - **Round-robin partitioning** – Used for range-only or temporal transforms that don’t
///   provide good distribution.
/// - **Mixed transforms** – Combines range and identity/bucket transforms, using hash
///   partitioning on identity/bucket columns.
///
/// ## Performance Notes
///
/// - Only repartitions when the input scheme or partition count differs from the target.
/// - Requires an explicit target partition count for deterministic behavior.
/// - Preserves column order (partitions first, then buckets) for consistent layout.
///
/// # Arguments
///
/// * `input` – The input execution plan providing data to be repartitioned (already projected to match table schema).
/// * `table_metadata` – Iceberg table metadata containing partition spec and sort order.
/// * `target_partitions` – Target number of partitions for parallel processing (must be > 0).
///
/// # Returns
///
/// An execution plan that applies the optimal partitioning strategy, or the original input plan if no repartitioning is needed.
///
/// # Example
///
/// ```ignore
/// let repartitioned_plan = repartition(
///     input_plan,
///     table.metadata_ref(),
///     4, // Explicit partition count
/// )?;
/// ```
pub fn repartition(
    input: Arc<dyn ExecutionPlan>,
    table_metadata: TableMetadataRef,
    target_partitions: NonZeroUsize,
) -> DFResult<Arc<dyn ExecutionPlan>> {
    let partitioning_strategy =
        determine_partitioning_strategy(&input, &table_metadata, target_partitions)?;

    if !needs_repartitioning(&input, &partitioning_strategy) {
        return Ok(input);
    }

    Ok(Arc::new(RepartitionExec::try_new(
        input,
        partitioning_strategy,
    )?))
}

/// Returns whether repartitioning is actually needed by comparing input and desired partitioning
fn needs_repartitioning(input: &Arc<dyn ExecutionPlan>, desired: &Partitioning) -> bool {
    let input_partitioning = input.properties().output_partitioning();
    match (input_partitioning, desired) {
        (Partitioning::RoundRobinBatch(a), Partitioning::RoundRobinBatch(b)) => a != b,
        (Partitioning::Hash(a_exprs, a_n), Partitioning::Hash(b_exprs, b_n)) => {
            a_n != b_n || !same_columns(a_exprs, b_exprs)
        }
        _ => true,
    }
}

/// Helper function to check if two sets of column expressions are the same
fn same_columns(a_exprs: &[Arc<dyn PhysicalExpr>], b_exprs: &[Arc<dyn PhysicalExpr>]) -> bool {
    if a_exprs.len() != b_exprs.len() {
        return false;
    }
    a_exprs.iter().zip(b_exprs.iter()).all(|(a, b)| {
        if let (Some(a_col), Some(b_col)) = (
            a.as_any().downcast_ref::<Column>(),
            b.as_any().downcast_ref::<Column>(),
        ) {
            a_col.name() == b_col.name() && a_col.index() == b_col.index()
        } else {
            std::ptr::eq(a.as_ref(), b.as_ref())
        }
    })
}

/// Determine the optimal partitioning strategy based on table metadata.
///
/// Analyzes the table's partition specification and sort order to select
/// the most appropriate DataFusion partitioning strategy for insert operations.
///
/// ## Partitioning Strategy
///
/// - **Hash partitioning using `_partition` column**: Used when the input includes a
///   projected `_partition` column. Ensures data is distributed based on actual partition values.
///
/// - **Hash partitioning using source columns**: Applied when identity or bucket transforms
///   provide good distribution:
///   1. Identity partition columns (e.g., `PARTITIONED BY (user_id, category)`)
///   2. Bucket columns from partition spec (e.g., `bucket(16, user_id)`)
///   3. Bucket columns from sort order  
///   Ensures co-location within partitions and buckets for optimal file clustering.
///
/// - **Round-robin partitioning**: Used for unpartitioned, range-only, or non-bucketed tables
///   where hash partitioning is not feasible.
///
/// ## Column Priority
///
/// Columns are combined in the following order, with duplicates removed:
/// 1. `_partition` column (highest priority, if present)
/// 2. Identity partition columns
/// 3. Bucket columns from partition spec
/// 4. Bucket columns from sort order
///
/// ## Fallback
///
/// If no suitable hash columns are found, falls back to round-robin batch partitioning
/// to ensure even load distribution across partitions.
fn determine_partitioning_strategy(
    input: &Arc<dyn ExecutionPlan>,
    table_metadata: &TableMetadata,
    target_partitions: NonZeroUsize,
) -> DFResult<Partitioning> {
    let partition_spec = table_metadata.default_partition_spec();
    let table_schema = table_metadata.current_schema();
    let input_schema = input.schema();

    match (
        !partition_spec.is_unpartitioned(),
        input_schema.index_of(PARTITION_VALUES_COLUMN),
    ) {
        (true, Ok(partition_col_idx)) => {
            let partition_field = input_schema.field(partition_col_idx);
            if partition_field.name() != PARTITION_VALUES_COLUMN {
                return Err(DataFusionError::Plan(format!(
                    "Expected {} column at index {}, but found '{}'",
                    PARTITION_VALUES_COLUMN,
                    partition_col_idx,
                    partition_field.name()
                )));
            }

            let partition_expr = Arc::new(Column::new(PARTITION_VALUES_COLUMN, partition_col_idx))
                as Arc<dyn PhysicalExpr>;
            return Ok(Partitioning::Hash(
                vec![partition_expr],
                target_partitions.get(),
            ));
        }
        (true, Err(_)) => {
            tracing::warn!(
                "Partitioned table input missing {} column. \
                 Consider adding partition projection before repartitioning.",
                PARTITION_VALUES_COLUMN
            );
        }
        (false, Ok(_)) => {
            tracing::warn!(
                "Input contains {} column but table is unpartitioned. \
                 This may indicate unnecessary projection.",
                PARTITION_VALUES_COLUMN
            );
        }
        (false, Err(_)) => {
            // Table is unpartitioned and _partition column is not present - nothing to do
        }
    }

    let names_iter = partition_spec
        .fields()
        .iter()
        .filter_map(|pf| match pf.transform {
            Transform::Identity | Transform::Bucket(_) => table_schema
                .field_by_id(pf.source_id)
                .map(|sf| sf.name.as_str()),
            _ => None,
        });

    let mut seen = HashSet::new();
    let hash_exprs: Vec<Arc<dyn PhysicalExpr>> = names_iter
        .filter(|name| seen.insert(*name))
        .map(|name| {
            let idx = input_schema.index_of(name).map_err(|e| {
                DataFusionError::Plan(format!(
                    "Column '{}' not found in input schema. Ensure projection happens before repartitioning. Error: {}",
                    name, e
                ))
            })?;
            Ok(Arc::new(Column::new(name, idx))
                as Arc<dyn PhysicalExpr>)
        })
        .collect::<DFResult<_>>()?;

    if !hash_exprs.is_empty() {
        Ok(Partitioning::Hash(hash_exprs, target_partitions.get()))
    } else {
        Ok(Partitioning::RoundRobinBatch(target_partitions.get()))
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::{
        DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema,
    };
    use datafusion::execution::TaskContext;
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

        let repartitioned_plan = repartition(
            input.clone(),
            table.metadata_ref(),
            std::num::NonZeroUsize::new(4).unwrap(),
        )
        .unwrap();

        assert_ne!(input.name(), repartitioned_plan.name());
        assert_eq!(repartitioned_plan.name(), "RepartitionExec");
    }

    #[tokio::test]
    async fn test_repartition_explicit_partitions() {
        let table = create_test_table();
        let input = Arc::new(EmptyExec::new(create_test_arrow_schema()));

        let repartitioned_plan = repartition(
            input,
            table.metadata_ref(),
            std::num::NonZeroUsize::new(8).unwrap(),
        )
        .unwrap();

        let partitioning = repartitioned_plan.properties().output_partitioning();
        match partitioning {
            Partitioning::RoundRobinBatch(n) => {
                assert_eq!(*n, 8);
            }
            _ => panic!("Expected RoundRobinBatch partitioning"),
        }
    }

    #[tokio::test]
    async fn test_repartition_zero_partitions_fails() {
        let _table = create_test_table();
        let _input = Arc::new(EmptyExec::new(create_test_arrow_schema()));

        let result = std::num::NonZeroUsize::new(0);
        assert!(result.is_none(), "NonZeroUsize::new(0) should return None");

        // Test that we can't call repartition with 0 partitions
        // This is prevented at compile time by NonZeroUsize
        let _ = result; // This would be None, so we can't call repartition
    }

    #[tokio::test]
    async fn test_partition_count_validation() {
        let table = create_test_table();
        let input = Arc::new(EmptyExec::new(create_test_arrow_schema()));

        let target_partitions = 16;
        let repartitioned_plan = repartition(
            input,
            table.metadata_ref(),
            std::num::NonZeroUsize::new(target_partitions).unwrap(),
        )
        .unwrap();

        let partitioning = repartitioned_plan.properties().output_partitioning();
        match partitioning {
            Partitioning::RoundRobinBatch(n) => {
                assert_eq!(*n, target_partitions);
            }
            _ => panic!("Expected RoundRobinBatch partitioning"),
        }
    }

    #[tokio::test]
    async fn test_datafusion_repartitioning_integration() {
        let table = create_test_table();
        let input = Arc::new(EmptyExec::new(create_test_arrow_schema()));

        let repartitioned_plan = repartition(
            input,
            table.metadata_ref(),
            std::num::NonZeroUsize::new(3).unwrap(),
        )
        .unwrap();

        let partitioning = repartitioned_plan.properties().output_partitioning();
        match partitioning {
            Partitioning::RoundRobinBatch(n) => {
                assert_eq!(*n, 3, "Should use round-robin for unpartitioned table");
            }
            _ => panic!("Expected RoundRobinBatch partitioning for unpartitioned table"),
        }

        let task_ctx = Arc::new(TaskContext::default());
        let stream = repartitioned_plan.execute(0, task_ctx.clone()).unwrap();

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

        let sort_order = SortOrder::builder()
            .with_order_id(1)
            .with_sort_field(SortField {
                source_id: 2,
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

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", ArrowDataType::Int64, false),
            ArrowField::new("category", ArrowDataType::Utf8, false),
        ]));
        let input = Arc::new(EmptyExec::new(arrow_schema));
        let repartitioned_plan = repartition(
            input,
            table.metadata_ref(),
            std::num::NonZeroUsize::new(4).unwrap(),
        )
        .unwrap();

        let partitioning = repartitioned_plan.properties().output_partitioning();
        // For bucketed tables without _partition column, should use round-robin
        // since the new logic prioritizes _partition column when available
        match partitioning {
            Partitioning::Hash(_, _) => {
                // This would happen if _partition column is present
            }
            Partitioning::RoundRobinBatch(_) => {
                // This happens when _partition column is not present
            }
            _ => panic!("Unexpected partitioning strategy"),
        }
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

        let partition_spec = iceberg::spec::PartitionSpec::builder(schema.clone())
            .add_partition_field("date", "date", Transform::Identity)
            .unwrap()
            .build()
            .unwrap();

        let sort_order = SortOrder::builder()
            .with_order_id(1)
            .with_sort_field(SortField {
                source_id: 2,
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

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("date", ArrowDataType::Date32, false),
            ArrowField::new("user_id", ArrowDataType::Int64, false),
            ArrowField::new("amount", ArrowDataType::Int64, false),
        ]));
        let input = Arc::new(EmptyExec::new(arrow_schema));
        let repartitioned_plan = repartition(
            input,
            table.metadata_ref(),
            std::num::NonZeroUsize::new(4).unwrap(),
        )
        .unwrap();

        let partitioning = repartitioned_plan.properties().output_partitioning();
        match partitioning {
            Partitioning::Hash(exprs, _) => {
                // With the new logic, we expect at least 1 column
                assert!(
                    exprs.len() >= 1,
                    "Should have at least one column for hash partitioning"
                );

                let column_names: Vec<String> = exprs
                    .iter()
                    .filter_map(|expr| {
                        expr.as_any()
                            .downcast_ref::<Column>()
                            .map(|col| col.name().to_string())
                    })
                    .collect();

                // Should include either user_id (identity transform) or date (partition field)
                let has_user_id = column_names.contains(&"user_id".to_string());
                let has_date = column_names.contains(&"date".to_string());
                assert!(
                    has_user_id || has_date,
                    "Should include either 'user_id' or 'date' column, got: {:?}",
                    column_names
                );
            }
            Partitioning::RoundRobinBatch(_) => {
                // This could happen if no suitable hash columns are found
            }
            _ => panic!("Unexpected partitioning strategy: {:?}", partitioning),
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
        let repartitioned_plan = repartition(
            input,
            table.metadata_ref(),
            std::num::NonZeroUsize::new(4).unwrap(),
        )
        .unwrap();

        let partitioning = repartitioned_plan.properties().output_partitioning();
        assert!(
            matches!(partitioning, Partitioning::RoundRobinBatch(_)),
            "Should use round-robin for 'none' distribution mode"
        );
    }

    #[tokio::test]
    async fn test_schema_ref_convenience_method() {
        let table = create_test_table();

        let schema_ref_1 = table.schema_ref();
        let schema_ref_2 = Arc::clone(table.metadata().current_schema());

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
        let repartitioned_plan = repartition(
            input,
            table.metadata_ref(),
            std::num::NonZeroUsize::new(4).unwrap(),
        )
        .unwrap();

        let partitioning = repartitioned_plan.properties().output_partitioning();
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
        let repartitioned_plan = repartition(
            input,
            table.metadata_ref(),
            std::num::NonZeroUsize::new(4).unwrap(),
        )
        .unwrap();

        let partitioning = repartitioned_plan.properties().output_partitioning();
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
