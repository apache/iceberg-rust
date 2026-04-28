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
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, ExecutionPlan, Partitioning, PlanProperties};
use datafusion::prelude::Expr;
use futures::{Stream, TryStreamExt};
use iceberg::expr::Predicate;
use iceberg::scan::FileScanTask;
use iceberg::table::Table;

use super::expr_to_predicate::convert_filters_to_predicate;
use crate::to_datafusion_error;

/// Manages the scanning process of an Iceberg [`Table`], encapsulating the
/// necessary details and computed properties required for execution planning.
///
/// When constructed with pre-planned [`FileScanTask`] buckets via
/// [`IcebergTableScan::new_with_tasks`], each DataFusion partition `i` streams
/// every task in `buckets[i]` using
/// [`TableScan::to_arrow_with_tasks`][iceberg::scan::TableScan::to_arrow_with_tasks].
///
/// When constructed via [`IcebergTableScan::new`] (no pre-planned tasks), the
/// full table is scanned lazily in a single partition using
/// [`TableScan::to_arrow`][iceberg::scan::TableScan::to_arrow]. This mode is
/// used by [`IcebergStaticTableProvider`][crate::table::IcebergStaticTableProvider].
///
/// In both modes the optional `limit` field truncates the output stream to at
/// most that many rows.
///
/// Note: when using pre-planned tasks, the `TableScan` is rebuilt on every
/// `execute(partition)` call rather than cached. `TableScan` carries a
/// `PlanContext` with `Arc`-shared evaluator caches which is awkward to
/// serialize if this plan ever needs to be shipped across workers. The
/// per-build cost is bounded (no I/O), so the rebuild is kept for now;
/// revisit once the cross-worker story is clearer.
#[derive(Debug)]
pub struct IcebergTableScan {
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
    /// Pre-planned file scan tasks grouped by output DataFusion partition.
    /// `None` in lazy mode (single-partition scan via `to_arrow()`).
    /// `Some(buckets)` in eager mode: `buckets[i]` holds every task that
    /// `execute(i)` will read.
    buckets: Option<Vec<Vec<FileScanTask>>>,
    /// Optional limit on the number of rows to return.
    limit: Option<usize>,
}

impl IcebergTableScan {
    /// Creates a lazy single-partition scan.
    ///
    /// All file tasks are discovered and read inside `execute(0)` via
    /// [`TableScan::to_arrow`][iceberg::scan::TableScan::to_arrow].
    /// Used by [`IcebergStaticTableProvider`][crate::table::IcebergStaticTableProvider].
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
        let plan_properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(output_schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        let projection = get_column_names(schema, projection);
        let predicates = convert_filters_to_predicate(filters);

        Self {
            table,
            snapshot_id,
            plan_properties,
            projection,
            predicates,
            buckets: None,
            limit,
        }
    }

    /// Creates an eager multi-partition scan from pre-planned file task buckets.
    ///
    /// Each DataFusion partition `i` streams the tasks in `buckets[i]` via
    /// [`TableScan::to_arrow_with_tasks`][iceberg::scan::TableScan::to_arrow_with_tasks].
    /// The `partitioning` argument is used directly for [`PlanProperties`], so the
    /// caller is responsible for ensuring it matches the bucketing strategy.
    /// Used by [`IcebergTableProvider`][crate::table::IcebergTableProvider].
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new_with_tasks(
        table: Table,
        snapshot_id: Option<i64>,
        schema: ArrowSchemaRef,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
        buckets: Vec<Vec<FileScanTask>>,
        partitioning: Partitioning,
    ) -> Self {
        let output_schema = match projection {
            None => schema.clone(),
            Some(projection) => Arc::new(schema.project(projection).unwrap()),
        };
        let plan_properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(output_schema),
            partitioning,
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        let projection = get_column_names(schema, projection);
        let predicates = convert_filters_to_predicate(filters);

        Self {
            table,
            snapshot_id,
            plan_properties,
            projection,
            predicates,
            buckets: Some(buckets),
            limit,
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

    /// Returns the pre-planned file task buckets, or an empty slice in lazy mode.
    pub fn buckets(&self) -> &[Vec<FileScanTask>] {
        self.buckets.as_deref().unwrap_or(&[])
    }

    pub fn limit(&self) -> Option<usize> {
        self.limit
    }

    fn total_file_count(&self) -> usize {
        self.buckets().iter().map(|b| b.len()).sum()
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
        let table = self.table.clone();
        let snapshot_id = self.snapshot_id;
        let column_names = self.projection.clone();
        let predicates = self.predicates.clone();
        let limit = self.limit;

        let stream = match &self.buckets {
            Some(buckets) => {
                // Eager mode: stream the pre-planned bucket for this partition.
                let bucket = buckets.get(partition).cloned().ok_or_else(|| {
                    DataFusionError::Internal(format!(
                        "{}: partition index {partition} is out of bounds (total buckets: {})",
                        self.name(),
                        buckets.len()
                    ))
                })?;

                let fut = async move {
                    // Rebuild a TableScan so we inherit the same defaults
                    // (row-group filtering, batch size, concurrency, ...).
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

                    let task_stream = Box::pin(futures::stream::iter(
                        bucket.into_iter().map(Ok::<_, iceberg::Error>),
                    ));
                    let record_batch_stream = table_scan
                        .to_arrow_with_tasks(task_stream)
                        .map_err(to_datafusion_error)?
                        .map_err(to_datafusion_error);
                    Ok::<_, DataFusionError>(record_batch_stream)
                };

                let s = futures::stream::once(fut).try_flatten();
                Box::pin(s) as Pin<Box<dyn Stream<Item = DFResult<RecordBatch>> + Send>>
            }
            None => {
                // Lazy mode: discover and read all tasks inside execute().
                let fut = get_batch_stream(table, snapshot_id, column_names, predicates);
                let s = futures::stream::once(fut).try_flatten();
                Box::pin(s)
            }
        };

        // Apply limit if specified.
        let limited_stream: Pin<Box<dyn Stream<Item = DFResult<RecordBatch>> + Send>> =
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
                stream
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
            .clone()
            .map_or(String::new(), |v| v.join(","));
        let predicate = self
            .predicates
            .clone()
            .map_or(String::new(), |p| format!("{p}"));

        match &self.buckets {
            Some(buckets) => {
                let file_count = self.total_file_count();
                let bucket_count = buckets.len();
                write!(
                    f,
                    "{} projection:[{projection}] predicate:[{predicate}] \
                     buckets:[{bucket_count}] file_count:[{file_count}]",
                    self.name()
                )?;
                if file_count <= 5 {
                    let files = buckets
                        .iter()
                        .flat_map(|b| b.iter().map(|t| t.data_file_path()))
                        .collect::<Vec<_>>()
                        .join(", ");
                    write!(f, " files:[{files}]")?;
                }
            }
            None => write!(
                f,
                "{} projection:[{projection}] predicate:[{predicate}]",
                self.name()
            )?,
        }
        if let Some(limit) = self.limit {
            write!(f, " limit:[{limit}]")?;
        }
        Ok(())
    }
}

/// Asynchronously retrieves a stream of [`RecordBatch`] instances from a
/// given table. Used in lazy (single-partition) scan mode.
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
