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

use std::pin::Pin;
use std::sync::Arc;
use std::vec;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datafusion::error::Result as DFResult;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, ExecutionPlan, Partitioning, PlanProperties};
use futures::{Stream, TryStreamExt};
use iceberg::expr::Predicate;
use iceberg::scan::{FileScanTask, FileScanTaskStream};
use iceberg::table::Table;

use super::scan_planning::{IcebergScanConfig, build_table_scan};
use crate::to_datafusion_error;

/// Manages the scanning process of an Iceberg [`Table`], encapsulating the
/// necessary details and computed properties required for execution planning.
#[derive(Debug)]
pub struct IcebergTableScan {
    /// A table in the catalog.
    table: Table,
    /// Snapshot, projection, output schema, and pushed predicates for this scan.
    scan_config: IcebergScanConfig,
    /// Stores certain, often expensive to compute,
    /// plan properties used in query optimization.
    plan_properties: Arc<PlanProperties>,
    /// Optional limit on the number of rows to return
    limit: Option<usize>,
    /// Pre-planned file scan tasks, grouped by partition. `None` keeps planning lazy.
    file_task_groups: Option<Vec<Arc<[FileScanTask]>>>,
}

impl IcebergTableScan {
    pub(crate) fn new(
        table: Table,
        scan_config: IcebergScanConfig,
        limit: Option<usize>,
        file_task_groups: Option<Vec<Vec<FileScanTask>>>,
    ) -> Self {
        let partition_count = file_task_groups.as_ref().map_or(1, |groups| groups.len());
        let plan_properties =
            IcebergTableScan::compute_properties(scan_config.output_schema(), partition_count);
        let file_task_groups = file_task_groups.map(|groups| {
            groups
                .into_iter()
                .map(Arc::<[FileScanTask]>::from)
                .collect()
        });

        Self {
            table,
            scan_config,
            plan_properties,
            limit,
            file_task_groups,
        }
    }

    pub fn table(&self) -> &Table {
        &self.table
    }

    pub fn snapshot_id(&self) -> Option<i64> {
        self.scan_config.snapshot_id()
    }

    pub fn projection(&self) -> Option<&[String]> {
        self.scan_config.column_names()
    }

    pub fn predicates(&self) -> Option<&Predicate> {
        self.scan_config.predicates()
    }

    pub fn limit(&self) -> Option<usize> {
        self.limit
    }

    /// Computes [`PlanProperties`] used in query optimization.
    fn compute_properties(schema: ArrowSchemaRef, partition_count: usize) -> Arc<PlanProperties> {
        Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(partition_count),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ))
    }
}

impl ExecutionPlan for IcebergTableScan {
    fn name(&self) -> &str {
        "IcebergTableScan"
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
        let stream: Pin<Box<dyn Stream<Item = DFResult<RecordBatch>> + Send>> = match &self
            .file_task_groups
        {
            Some(file_task_groups) => {
                let Some(file_task_group) = file_task_groups.get(partition).cloned() else {
                    return Err(datafusion::common::DataFusionError::Internal(format!(
                        "IcebergTableScan partition {partition} does not exist; scan has {} partitions",
                        file_task_groups.len()
                    )));
                };

                let tasks: FileScanTaskStream = Box::pin(futures::stream::iter(
                    (0..file_task_group.len()).map(move |idx| Ok(file_task_group[idx].clone())),
                ));
                let stream = self
                    .table
                    .reader_builder()
                    // Eager planning lets DataFusion drive scan concurrency via output
                    // partitions. Match DataFusion's FileStream model, where each
                    // output partition owns one ScanState; keep one data file in
                    // flight per output partition here.
                    // https://github.com/apache/datafusion/blob/ad8e7b7f2babe3fcddc3a4f9b5cd1ac0d1b16ad9/datafusion/datasource/src/file_stream/scan_state.rs#L42-L43
                    .with_data_file_concurrency_limit(1)
                    .build()
                    // TODO: Avoid cloning FileScanTasks here once ArrowReader can accept shared tasks.
                    .read(tasks)
                    .map_err(to_datafusion_error)?
                    .stream()
                    .map_err(to_datafusion_error);

                Box::pin(stream)
            }
            None => {
                let table = self.table.clone();
                let scan_config = self.scan_config.clone();
                let fut = async move {
                    let table_scan = build_table_scan(&table, &scan_config)?;
                    let stream = table_scan
                        .to_arrow()
                        .await
                        .map_err(to_datafusion_error)?
                        .map_err(to_datafusion_error);
                    Ok::<_, datafusion::common::DataFusionError>(stream)
                };

                Box::pin(futures::stream::once(fut).try_flatten())
            }
        };

        // Apply limit if specified
        let limited_stream: Pin<Box<dyn Stream<Item = DFResult<RecordBatch>> + Send>> =
            if let Some(limit) = self.limit {
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
        write!(
            f,
            "IcebergTableScan projection:[{}] predicate:[{}]",
            self.projection().map_or(String::new(), |v| v.join(",")),
            self.predicates()
                .map_or(String::from(""), |p| format!("{p}")),
        )?;
        if let Some(file_task_groups) = &self.file_task_groups {
            let task_count: usize = file_task_groups.iter().map(|group| group.len()).sum();
            write!(
                f,
                " task_groups:[{}] tasks:[{}]",
                file_task_groups.len(),
                task_count,
            )?;
        }
        if let Some(limit) = self.limit {
            write!(f, " limit:[{limit}]")?;
        }
        Ok(())
    }
}
