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
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use datafusion::arrow::compute::SortOptions;
use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datafusion::common::Result as DFResult;
use datafusion::error::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::{LexOrdering, PhysicalExpr, PhysicalSortExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::expressions::Column;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use iceberg::spec::{PartitionSpecRef, Transform};
use iceberg::table::Table;

/// An execution plan that sorts incoming data by Iceberg partition values.
///
/// This execution plan takes input data that has been repartitioned and sorts it by
/// partition values within each partition. This ensures that data belonging to the
/// same Iceberg partition is grouped together, allowing a single writer to efficiently
/// process it in subsequent steps.
#[derive(Debug)]
pub struct IcebergPartitionSortExec {
    table: Table,
    input: Arc<dyn ExecutionPlan>,
    sort_exprs: Vec<PhysicalSortExpr>,
    cache: PlanProperties,
}

impl IcebergPartitionSortExec {
    /// Create a new IcebergPartitionSortExec
    pub fn new(input: Arc<dyn ExecutionPlan>, table: Table) -> DFResult<Self> {
        // Extract partition spec from table
        let partition_spec = table.metadata().default_partition_spec();

        // Generate sort expressions from partition spec
        let sort_exprs = Self::create_sort_expressions(partition_spec, input.schema())?;

        // Compute plan properties
        let cache = Self::compute_properties(&input);

        Ok(Self {
            input,
            table,
            sort_exprs,
            cache,
        })
    }

    /// Create sort expressions from partition spec
    fn create_sort_expressions(
        partition_spec: &PartitionSpecRef,
        schema: ArrowSchemaRef,
    ) -> DFResult<Vec<PhysicalSortExpr>> {
        if partition_spec.is_unpartitioned() {
            return Err(DataFusionError::Execution(
                "IcebergPartitionSortExec is expected to be used on partitioned table only!"
                    .to_string(),
            ));
        }

        let mut sort_exprs = Vec::new();

        // For each partition field, create a sort expression
        for field in partition_spec.fields() {
            // Skip void transforms as they don't contribute to sorting
            if matches!(field.transform, Transform::Void) {
                continue;
            }

            // todo revisit this part, the input schema may not have field ids, and using field ids as indices is wrong
            // Find the column in the schema that corresponds to the source_id
            // In a real implementation, we would need to map from Iceberg schema to Arrow schema
            let source_id_usize = field.source_id as usize;
            let schema_len = schema.fields().len();
            let column_index_and_name = if source_id_usize >= schema_len {
                None
            } else {
                let f = schema.field(source_id_usize);
                Some((source_id_usize, f.name().to_string()))
            };

            let column_index_and_name = column_index_and_name.ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "Could not find column for source_id: {}",
                    field.source_id
                ))
            })?;

            // todo need to handle partition value transform as well (how without field ids??)
            // Create a physical expression based on the transform
            // For now, we'll just use the column directly for all transforms
            let expr: Arc<dyn PhysicalExpr> = Arc::new(Column::new(
                &column_index_and_name.1,
                column_index_and_name.0,
            ));

            // Add to sort expressions
            sort_exprs.push(PhysicalSortExpr {
                expr,
                options: SortOptions::default(), // Ascending, nulls last
            });
        }

        Ok(sort_exprs)
    }

    /// Compute plan properties
    fn compute_properties(input: &Arc<dyn ExecutionPlan>) -> PlanProperties {
        PlanProperties::new(
            // Equivalence properties would be calculated in [`SortExec`] according to the sort expressions
            input.properties().equivalence_properties().clone(),
            input.properties().output_partitioning().clone(),
            EmissionType::Final,
            Boundedness::Bounded,
        )
    }
}

impl ExecutionPlan for IcebergPartitionSortExec {
    fn name(&self) -> &str {
        "IcebergPartitionSortExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(format!(
                "IcebergPartitionSortExec expects exactly one child, got {}",
                children.len()
            )));
        }

        // Create a new instance with the new child
        IcebergPartitionSortExec::new(Arc::clone(&children[0]), self.table.clone())
            .map(|exec| Arc::new(exec) as Arc<dyn ExecutionPlan>)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        // Convert Vec<PhysicalSortExpr> to LexOrdering
        let lex_ordering = LexOrdering::from(self.sort_exprs.clone());

        // We always set preserve_partitioning to true to ensure the output partitioning
        // is the same as the input partitioning
        let sort_exec = Arc::new(
            SortExec::new(lex_ordering, Arc::clone(&self.input)).with_preserve_partitioning(true),
        );

        // Execute the sort
        sort_exec.execute(partition, context)
    }
}

impl DisplayAs for IcebergPartitionSortExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => {
                write!(
                    f,
                    "IcebergPartitionSortExec: table={}",
                    self.table.identifier()
                )
            }
            DisplayFormatType::Verbose => {
                write!(
                    f,
                    "IcebergPartitionSortExec: table={}, sort_exprs=[{}]",
                    self.table.identifier(),
                    self.sort_exprs
                        .iter()
                        .map(|e| format!("{}", e))
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            }
            DisplayFormatType::TreeRender => {
                write!(
                    f,
                    "IcebergPartitionSortExec: table={}",
                    self.table.identifier()
                )
            }
        }
    }
}

#[cfg(test)]
mod tests {}
