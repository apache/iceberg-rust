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
use std::pin::Pin;
use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datafusion::error::Result as DFResult;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, ExecutionPlan, Partitioning, PlanProperties};
use datafusion::prelude::Expr;
use futures::{Stream, TryStreamExt};
use iceberg::arrow::ArrowReaderBuilder;
use iceberg::expr::Predicate;
use iceberg::io::FileIO;
use iceberg::scan::CombinedScanTask;
use iceberg::table::Table;

use super::expr_to_predicate::convert_filters_to_predicate;
use crate::to_datafusion_error;

/// Manages the scanning process of an Iceberg [`Table`], encapsulating the
/// necessary details and computed properties required for execution planning.
#[derive(Debug)]
pub struct IcebergTableScan {
    /// A table in the catalog.
    table: Table,
    /// Snapshot of the table to scan.
    snapshot_id: Option<i64>,
    /// Stores certain, often expensive to compute,
    /// plan properties used in query optimization.
    plan_properties: Arc<PlanProperties>,
    /// Projection column names, None means all columns
    projection: Option<Vec<String>>,
    /// Filters to apply to the table scan
    predicates: Option<Predicate>,
    /// Optional limit on the number of rows to return
    limit: Option<usize>,
    /// Pre-planned combined scan tasks, one per DataFusion partition.
    /// When `None`, falls back to single-partition planning via `plan_files()`.
    combined_tasks: Option<Arc<Vec<CombinedScanTask>>>,
    /// FileIO for reading data files.
    file_io: FileIO,
}

impl IcebergTableScan {
    /// Creates a new [`IcebergTableScan`] without eagerly planning tasks.
    ///
    /// This produces a single-partition scan. Call [`plan`] after construction
    /// to enable multi-partition execution via `plan_tasks()`.
    pub(crate) fn new(
        table: Table,
        snapshot_id: Option<i64>,
        schema: ArrowSchemaRef,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Self {
        let output_schema = match projection {
            None => schema.clone(),
            Some(projection) => Arc::new(schema.project(projection).unwrap()),
        };
        let plan_properties = Self::compute_properties(output_schema, 1);
        let projection = get_column_names(schema.clone(), projection);
        let predicates = convert_filters_to_predicate(filters);
        let file_io = table.file_io().clone();

        Self {
            table,
            snapshot_id,
            plan_properties,
            projection,
            predicates,
            limit,
            combined_tasks: None,
            file_io,
        }
    }

    /// Eagerly plans scan tasks via `plan_tasks()`, enabling multi-partition
    /// parallel execution in DataFusion.
    ///
    /// If planning fails (e.g. manifests are unreachable), returns `self`
    /// unchanged in single-partition mode. Errors will surface at `execute()` time.
    pub(crate) async fn plan(mut self) -> Self {
        let combined_tasks = self.try_plan_tasks().await;
        if let Ok(tasks) = combined_tasks {
            let num_partitions = tasks.len().max(1);
            self.plan_properties = Self::compute_properties(self.schema(), num_partitions);
            self.combined_tasks = Some(Arc::new(tasks));
        }
        self
    }

    async fn try_plan_tasks(&self) -> Result<Vec<CombinedScanTask>, iceberg::Error> {
        let scan_builder = match self.snapshot_id {
            Some(snapshot_id) => self.table.scan().snapshot_id(snapshot_id),
            None => self.table.scan(),
        };
        let mut scan_builder = match &self.projection {
            Some(column_names) => scan_builder.select(column_names.clone()),
            None => scan_builder.select_all(),
        };
        if let Some(ref pred) = self.predicates {
            scan_builder = scan_builder.with_filter(pred.clone());
        }
        let table_scan = scan_builder.build()?;
        let combined_tasks: Vec<CombinedScanTask> = table_scan
            .plan_tasks()
            .await?
            .try_collect()
            .await?;
        Ok(combined_tasks)
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

    pub fn limit(&self) -> Option<usize> {
        self.limit
    }

    fn compute_properties(schema: ArrowSchemaRef, num_partitions: usize) -> Arc<PlanProperties> {
        Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(num_partitions),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ))
    }
}

impl ExecutionPlan for IcebergTableScan {
    fn name(&self) -> &str {
        "IcebergTableScan"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan + 'static>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
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
        let schema = self.schema();

        // If we have pre-planned tasks, use them for partition-aware execution
        if let Some(ref combined_tasks) = self.combined_tasks {
            let Some(combined_task) = combined_tasks.get(partition) else {
                return Ok(Box::pin(RecordBatchStreamAdapter::new(
                    schema,
                    futures::stream::empty(),
                )));
            };

            let tasks = combined_task.tasks().to_vec();
            let file_io = self.file_io.clone();

            let fut = async move {
                let task_stream = Box::pin(futures::stream::iter(tasks.into_iter().map(Ok)));
                let reader = ArrowReaderBuilder::new(file_io).build();
                let stream = reader
                    .read(task_stream)
                    .map_err(to_datafusion_error)?
                    .map_err(to_datafusion_error);
                Ok::<_, datafusion::error::DataFusionError>(stream)
            };

            let stream = futures::stream::once(fut).try_flatten();
            return Ok(Box::pin(RecordBatchStreamAdapter::new(
                schema,
                apply_limit(stream, self.limit),
            )));
        }

        // Fallback: single-partition mode using plan_files() + to_arrow()
        let fut = get_batch_stream(
            self.table.clone(),
            self.snapshot_id,
            self.projection.clone(),
            self.predicates.clone(),
        );
        let stream = futures::stream::once(fut).try_flatten();

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            apply_limit(stream, self.limit),
        )))
    }
}

/// Apply an optional row limit to a stream of record batches.
fn apply_limit(
    stream: impl Stream<Item = DFResult<RecordBatch>> + Send + 'static,
    limit: Option<usize>,
) -> Pin<Box<dyn Stream<Item = DFResult<RecordBatch>> + Send>> {
    if let Some(limit) = limit {
        let mut remaining = limit;
        Box::pin(stream.try_filter_map(move |batch| {
            futures::future::ready(if remaining == 0 {
                Ok(None)
            } else if batch.num_rows() <= remaining {
                remaining -= batch.num_rows();
                Ok(Some(batch))
            } else {
                let limited_batch = batch.slice(0, remaining);
                remaining = 0;
                Ok(Some(limited_batch))
            })
        }))
    } else {
        Box::pin(stream)
    }
}

impl DisplayAs for IcebergTableScan {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(
            f,
            "IcebergTableScan projection:[{}] predicate:[{}]",
            self.projection
                .clone()
                .map_or(String::new(), |v| v.join(",")),
            self.predicates
                .clone()
                .map_or(String::from(""), |p| format!("{p}")),
        )
    }
}

/// Asynchronously retrieves a stream of [`RecordBatch`] instances
/// from a given table.
///
/// This function initializes a [`TableScan`], builds it,
/// and then converts it into a stream of Arrow [`RecordBatch`]es.
async fn get_batch_stream(
    table: Table,
    snapshot_id: Option<i64>,
    column_names: Option<Vec<String>>,
    predicates: Option<Predicate>,
) -> DFResult<Pin<Box<dyn Stream<Item = DFResult<RecordBatch>> + Send>>> {
    let scan_builder = match snapshot_id {
        Some(snapshot_id) => table.scan().snapshot_id(snapshot_id),
        None => table.scan(),
    };

    let mut scan_builder = match column_names {
        Some(column_names) => scan_builder.select(column_names),
        None => scan_builder.select_all(),
    };
    if let Some(pred) = predicates {
        scan_builder = scan_builder.with_filter(pred);
    }
    let table_scan = scan_builder.build().map_err(to_datafusion_error)?;

    let stream = table_scan
        .to_arrow()
        .await
        .map_err(to_datafusion_error)?
        .map_err(to_datafusion_error);
    Ok(Box::pin(stream))
}

fn get_column_names(
    schema: ArrowSchemaRef,
    projection: Option<&Vec<usize>>,
) -> Option<Vec<String>> {
    projection.map(|v| {
        v.iter()
            .map(|p| schema.field(*p).name().clone())
            .collect::<Vec<String>>()
    })
}
