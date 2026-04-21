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

use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, ExecutionPlan, Partitioning, PlanProperties};
use datafusion::prelude::Expr;
use futures::TryStreamExt;
use iceberg::expr::Predicate;
use iceberg::scan::FileScanTask;
use iceberg::table::Table;

use super::expr_to_predicate::convert_filters_to_predicate;
use super::scan::get_column_names;
use crate::to_datafusion_error;

/// A DataFusion [`ExecutionPlan`] that reads one [`FileScanTask`] per partition.
///
/// Arrow reader configuration (row-group filtering, row selection, concurrency
/// limit, batch size) matches [`IcebergTableScan`][super::scan::IcebergTableScan]:
/// it is sourced from the underlying [`TableScan`][iceberg::scan::TableScan]
/// rebuilt in [`execute`](ExecutionPlan::execute) and applied via
/// [`TableScan::to_arrow_with_tasks`][iceberg::scan::TableScan::to_arrow_with_tasks].
///
/// Note: the `TableScan` is rebuilt on every `execute(partition)` call rather
/// than cached as an `Arc<TableScan>` on the struct. Caching would avoid
/// redundant schema resolution and predicate binding per partition, but
/// `TableScan` carries a `PlanContext` with `Arc`-shared evaluator caches
/// which is awkward to serialize if this plan ever needs to be shipped across
/// workers. The per-build cost is bounded (no I/O), so the rebuild is kept
/// for now; revisit once the cross-worker story is clearer.
#[derive(Debug)]
pub struct IcebergPartitionedScan {
    /// A table in the catalog.
    table: Table,
    /// Snapshot of the table to scan.
    snapshot_id: Option<i64>,
    /// Stores certain, often expensive to compute,
    /// plan properties used in query optimization.
    plan_properties: Arc<PlanProperties>,
    /// Projection column names, None means all columns.
    projection: Option<Vec<String>>,
    /// Filters to apply to the table scan.
    predicates: Option<Predicate>,
    /// Pre-planned file scan tasks, one per DataFusion partition.
    tasks: Vec<FileScanTask>,
}

impl IcebergPartitionedScan {
    pub(crate) fn new(
        table: Table,
        snapshot_id: Option<i64>,
        schema: ArrowSchemaRef,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        tasks: Vec<FileScanTask>,
    ) -> Self {
        let output_schema = match projection {
            None => schema.clone(),
            Some(projection) => Arc::new(schema.project(projection).unwrap()),
        };
        let plan_properties = Self::compute_properties(output_schema, tasks.len());
        let projection = get_column_names(schema, projection);
        let predicates = convert_filters_to_predicate(filters);

        Self {
            table,
            snapshot_id,
            plan_properties,
            projection,
            predicates,
            tasks,
        }
    }

    pub fn table(&self) -> &Table {
        &self.table
    }

    pub fn snapshot_id(&self) -> Option<i64> {
        self.snapshot_id
    }

    pub fn projection(&self) -> Option<&[String]> {
        self.projection.as_deref()
    }

    pub fn predicates(&self) -> Option<&Predicate> {
        self.predicates.as_ref()
    }

    pub fn tasks(&self) -> &[FileScanTask] {
        &self.tasks
    }

    fn compute_properties(schema: ArrowSchemaRef, n_partitions: usize) -> Arc<PlanProperties> {
        Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(n_partitions),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ))
    }
}

impl ExecutionPlan for IcebergPartitionedScan {
    fn name(&self) -> &str {
        "IcebergPartitionedScan"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan + 'static>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return Err(DataFusionError::Internal(format!(
                "{} is a leaf node and expects no children, but {} were provided",
                self.name(),
                children.len()
            )));
        }
        Ok(self)
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.plan_properties
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let task = self.tasks.get(partition).cloned().ok_or_else(|| {
            DataFusionError::Internal(format!(
                "{}: partition index {partition} is out of bounds (total tasks: {})",
                self.name(),
                self.tasks.len()
            ))
        })?;

        let table = self.table.clone();
        let snapshot_id = self.snapshot_id;
        let column_names = self.projection.clone();
        let predicates = self.predicates.clone();

        let fut = async move {
            // Rebuild a TableScan mirroring IcebergTableScan::get_batch_stream so we
            // inherit the same defaults (row-group filtering, batch size, concurrency, ...).
            let scan_builder = match snapshot_id {
                Some(id) => table.scan().snapshot_id(id),
                None => table.scan(),
            };
            let mut scan_builder = match column_names {
                Some(names) => scan_builder.select(names),
                None => scan_builder.select_all(),
            };
            if let Some(pred) = predicates {
                scan_builder = scan_builder.with_filter(pred);
            }
            let table_scan = scan_builder.build().map_err(to_datafusion_error)?;

            let task_stream = Box::pin(futures::stream::once(futures::future::ready(Ok(task))));
            let record_batch_stream = table_scan
                .to_arrow_with_tasks(task_stream)
                .map_err(to_datafusion_error)?
                .map_err(to_datafusion_error);
            Ok::<_, DataFusionError>(record_batch_stream)
        };

        let stream = futures::stream::once(fut).try_flatten();

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }
}

impl DisplayAs for IcebergPartitionedScan {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        let projection = self
            .projection
            .clone()
            .map_or(String::new(), |v| v.join(","));
        let predicate = self
            .predicates
            .clone()
            .map_or(String::new(), |p| format!("{p}"));
        let file_count = self.tasks.len();
        write!(
            f,
            "{} projection:[{projection}] predicate:[{predicate}] file_count:[{file_count}]",
            self.name()
        )?;
        if file_count <= 5 {
            let files = self
                .tasks
                .iter()
                .map(|t| t.data_file_path())
                .collect::<Vec<_>>()
                .join(", ");
            write!(f, " files:[{files}]")?;
        }
        Ok(())
    }
}
