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
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, ExecutionPlan, Partitioning, PlanProperties};
use datafusion::prelude::Expr;
use futures::{Stream, TryStreamExt};
use iceberg::arrow::ArrowReaderBuilder;
use iceberg::expr::Predicate;
use iceberg::scan::{FileScanTask, TableScan};
use iceberg::table::Table;

use super::expr_to_predicate::convert_filters_to_predicate;
use crate::to_datafusion_error;
const DEFAULT_PARALLELISM: usize = 1;
fn available_parallelism() -> NonZeroUsize {
    std::thread::available_parallelism()
        .unwrap_or_else(|_err| NonZeroUsize::new(DEFAULT_PARALLELISM).unwrap())
}

/// Iceberg [`Table`] scan as a DataFusion [`ExecutionPlan`].
///
/// Has two construction modes: lazy single-partition scans that plan files
/// inside `execute(0)`, and eager multi-partition scans over pre-planned
/// [`FileScanTask`] buckets.
///
/// Note: in eager mode the underlying `TableScan` is rebuilt on every
/// `execute(partition)` call. The per-build cost is bounded (no I/O) and
/// keeps the plan free of `Arc`-shared evaluator caches that are awkward to
/// serialize across workers.
#[derive(Debug)]
pub struct IcebergTableScan {
    /// A table in the catalog.
    table: Table,
    /// Snapshot of the table to scan.
    snapshot_id: Option<i64>,
    /// Cached plan properties used by query optimization.
    plan_properties: Arc<PlanProperties>,
    /// Projection column names, None means all columns.
    projection: Option<Vec<String>>,
    /// Filters to apply to the table scan.
    predicates: Option<Predicate>,
    /// Pre-planned file scan tasks per partition (eager mode), or `None` (lazy mode).
    buckets: Option<Vec<Vec<FileScanTask>>>,
    /// Optional limit on the number of rows to return.
    limit: Option<usize>,
}

/// Builder to create an [`IcebergTableScan`].
pub struct IcebergTableScanBuilder {
    table: Table,
    snapshot_id: Option<i64>,
    schema: ArrowSchemaRef,
    projection: Option<Vec<usize>>,
    filters: Vec<Expr>,
    limit: Option<usize>,
    partitioning: Partitioning,
    buckets: Option<Vec<Vec<FileScanTask>>>,
}

impl IcebergTableScanBuilder {
    /// Creates a builder for a lazy single-partition scan.
    pub fn new(table: Table, schema: ArrowSchemaRef) -> Self {
        Self {
            table,
            schema,
            snapshot_id: None,
            projection: None,
            filters: vec![],
            limit: None,
            partitioning: Partitioning::UnknownPartitioning(1),
            buckets: None,
        }
    }

    /// Sets the snapshot to scan. When not set, it uses current snapshot.
    pub fn with_snapshot_id(mut self, snapshot_id: Option<i64>) -> Self {
        self.snapshot_id = snapshot_id;
        self
    }

    /// Sets the projected output columns.
    pub fn with_projection(mut self, projection: Option<&Vec<usize>>) -> Self {
        self.projection = projection.cloned();
        self
    }

    /// Sets the filters to apply to the table scan.
    pub fn with_filters(mut self, filters: &[Expr]) -> Self {
        self.filters = filters.to_vec();
        self
    }

    /// Sets the optional row limit.
    pub fn with_limit(mut self, limit: Option<usize>) -> Self {
        self.limit = limit;
        self
    }

    /// Sets pre-planned task buckets for eager multi-partition scans.
    pub fn with_task_buckets(
        mut self,
        buckets: Vec<Vec<FileScanTask>>,
        partitioning: Partitioning,
    ) -> Self {
        self.buckets = Some(buckets);
        self.partitioning = partitioning;
        self
    }

    /// Builds the [`IcebergTableScan`].
    pub fn build(self) -> DFResult<IcebergTableScan> {
        if let Some(buckets) = &self.buckets {
            let partition_count = self.partitioning.partition_count();
            if buckets.len() != partition_count {
                return Err(DataFusionError::Internal(format!(
                    "IcebergTableScan expected {} task buckets to match partitioning, got {}",
                    partition_count,
                    buckets.len()
                )));
            }
        }

        let output_schema = match &self.projection {
            None => self.schema.clone(),
            Some(projection) => Arc::new(self.schema.project(projection).map_err(|err| {
                DataFusionError::Plan(format!("Failed to project Iceberg table schema: {err}"))
            })?),
        };
        let plan_properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(output_schema),
            self.partitioning,
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        let projection = get_column_names(self.schema, self.projection.as_ref());
        let predicates = convert_filters_to_predicate(&self.filters);

        Ok(IcebergTableScan {
            table: self.table,
            snapshot_id: self.snapshot_id,
            plan_properties,
            projection,
            predicates,
            buckets: self.buckets,
            limit: self.limit,
        })
    }
}

impl IcebergTableScan {
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

    /// Returns the pre-planned file task buckets.
    ///
    /// `None` means lazy mode, where file tasks are planned inside `execute`;
    /// `Some` means eager mode, where `execute` reads from pre-planned buckets.
    pub fn buckets(&self) -> Option<&[Vec<FileScanTask>]> {
        self.buckets.as_deref()
    }

    pub fn limit(&self) -> Option<usize> {
        self.limit
    }

    fn total_file_count(&self) -> usize {
        self.buckets()
            .map_or(0, |buckets| buckets.iter().map(|b| b.len()).sum())
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
        let bucket = match &self.buckets {
            Some(buckets) => Some(buckets.get(partition).cloned().ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "{}: partition index {partition} is out of bounds (total buckets: {})",
                    self.name(),
                    buckets.len()
                ))
            })?),
            None => None,
        };

        let fut = build_record_batch_stream(
            self.table.clone(),
            self.snapshot_id,
            self.projection.clone(),
            self.predicates.clone(),
            bucket,
        );

        let stream = Box::pin(futures::stream::once(fut).try_flatten())
            as Pin<Box<dyn Stream<Item = DFResult<RecordBatch>> + Send>>;

        let limited_stream = match self.limit {
            Some(limit) => apply_limit(stream, limit),
            None => stream,
        };

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            limited_stream,
        )))
    }
}

impl DisplayAs for IcebergTableScan {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        let projection = self
            .projection
            .as_deref()
            .map_or(String::new(), |v| v.join(","));
        let predicate = self
            .predicates
            .as_ref()
            .map_or(String::new(), |p| p.to_string());

        write!(
            f,
            "{} projection:[{projection}] predicate:[{predicate}]",
            self.name()
        )?;
        if let Some(buckets) = &self.buckets {
            let file_count = self.total_file_count();
            let bucket_count = buckets.len();
            write!(f, " buckets:[{bucket_count}] file_count:[{file_count}]")?;
        }
        if let Some(limit) = self.limit {
            write!(f, " limit:[{limit}]")?;
        }
        Ok(())
    }
}

fn build_table_scan(
    table: Table,
    snapshot_id: Option<i64>,
    column_names: Option<Vec<String>>,
    predicates: Option<Predicate>,
) -> DFResult<TableScan> {
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
    scan_builder.build().map_err(to_datafusion_error)
}

/// Builds the `RecordBatch` stream for a single partition. When `bucket` is
/// `Some`, streams the pre-planned tasks directly through an `ArrowReader`;
/// when `None`, plans and reads the full scan via `to_arrow`.
async fn build_record_batch_stream(
    table: Table,
    snapshot_id: Option<i64>,
    column_names: Option<Vec<String>>,
    predicates: Option<Predicate>,
    bucket: Option<Vec<FileScanTask>>,
) -> DFResult<Pin<Box<dyn Stream<Item = DFResult<RecordBatch>> + Send>>> {
    let stream: Pin<Box<dyn Stream<Item = DFResult<RecordBatch>> + Send>> = match bucket {
        Some(bucket) => {
            let task_stream = Box::pin(futures::stream::iter(
                bucket.into_iter().map(Ok::<_, iceberg::Error>),
            ));
            let num_cpus = available_parallelism().get();
            let arrow_reader_builder = ArrowReaderBuilder::new(table.file_io().clone())
                .with_data_file_concurrency_limit(num_cpus)
                .with_row_group_filtering_enabled(true)
                .with_row_selection_enabled(true);

            Box::pin(
                arrow_reader_builder
                    .build()
                    .read(task_stream)
                    .map_err(to_datafusion_error)?
                    .stream()
                    .map_err(to_datafusion_error),
            )
        }
        None => {
            let table_scan = build_table_scan(table, snapshot_id, column_names, predicates)?;
            Box::pin(
                table_scan
                    .to_arrow()
                    .await
                    .map_err(to_datafusion_error)?
                    .map_err(to_datafusion_error),
            )
        }
    };
    Ok(stream)
}

/// Truncates a stream of `RecordBatch` to at most `limit` rows.
fn apply_limit(
    stream: Pin<Box<dyn Stream<Item = DFResult<RecordBatch>> + Send>>,
    limit: usize,
) -> Pin<Box<dyn Stream<Item = DFResult<RecordBatch>> + Send>> {
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
}

pub(super) fn get_column_names(
    schema: ArrowSchemaRef,
    projection: Option<&Vec<usize>>,
) -> Option<Vec<String>> {
    projection.map(|v| {
        v.iter()
            .map(|p| schema.field(*p).name().clone())
            .collect::<Vec<String>>()
    })
}
