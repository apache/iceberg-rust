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
use std::vec;

use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datafusion::catalog::Session;
use datafusion::config::ConfigOptions;
use datafusion::error::Result as DFResult;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, ExecutionPlan, Partitioning, PlanProperties};
use datafusion::prelude::Expr;
use futures::TryStreamExt;
use iceberg::arrow::{ArrowReaderBuilder, GroupPruner};
use iceberg::expr::Predicate;
use iceberg::scan::{FileScanTask, FileScanTaskStream};
use iceberg::table::Table;

use super::expr_to_predicate::convert_filters_to_predicate;
use crate::to_datafusion_error;

#[derive(Debug, Clone)]
struct PartitionTask {
    file_scan_tasks: Vec<FileScanTask>,
}

impl PartitionTask {
    pub fn to_file_scan_task_stream(&self) -> FileScanTaskStream {
        let file_scan_tasks = self.file_scan_tasks.clone();
        Box::pin(futures::stream::iter(file_scan_tasks.into_iter().map(Ok)))
    }
}

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
    plan_properties: PlanProperties,
    /// Projection column names, None means all columns
    projection: Option<Vec<String>>,
    /// Filters to apply to the table scan
    predicates: Option<Predicate>,

    /// The tasks to scan the table.
    partition_tasks: Vec<PartitionTask>,
}

impl IcebergTableScan {
    /// Creates a new [`IcebergTableScan`] object.
    pub(crate) async fn new(
        session_state: &dyn Session,
        table: Table,
        snapshot_id: Option<i64>,
        schema: ArrowSchemaRef,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
    ) -> DFResult<Self> {
        // Create the table scan
        let output_schema = match projection {
            None => schema.clone(),
            Some(projection) => Arc::new(schema.project(projection).unwrap()),
        };
        let projection = get_column_names(schema.clone(), projection);
        let predicates = convert_filters_to_predicate(filters);
        let scan_builder = match snapshot_id {
            Some(snapshot_id) => table.scan().snapshot_id(snapshot_id),
            None => table.scan(),
        };
        let mut scan_builder = match &projection {
            Some(column_names) => scan_builder.select(column_names.clone()),
            None => scan_builder.select_all(),
        };
        if let Some(pred) = &predicates {
            scan_builder = scan_builder.with_filter(pred.clone());
        }
        let table_scan = scan_builder.build().map_err(to_datafusion_error)?;

        // Create the file scan tasks and partition them
        let file_tasks_stream = table_scan.plan_files().await.map_err(to_datafusion_error)?;
        let pruner = GroupPruner::new(table.file_io().clone());
        let target_partitions = session_state.config().target_partitions();
        let pruned_file_tasks = pruner
            .prune(file_tasks_stream)
            .try_collect::<Vec<_>>()
            .await
            .map_err(to_datafusion_error)?;
        let partition_tasks = partition_file_tasks_stream(pruned_file_tasks, target_partitions);

        let plan_properties =
            Self::compute_properties(output_schema.clone(), partition_tasks.len());

        Ok(Self {
            table,
            snapshot_id,
            plan_properties,
            projection,
            predicates,
            partition_tasks,
        })
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

    /// Computes [`PlanProperties`] used in query optimization.
    fn compute_properties(schema: ArrowSchemaRef, target_partitions: usize) -> PlanProperties {
        // TODO:
        // This is more or less a placeholder, to be replaced
        // once we support output-partitioning
        PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(target_partitions),
            EmissionType::Incremental,
            Boundedness::Bounded,
        )
    }
}

impl ExecutionPlan for IcebergTableScan {
    fn name(&self) -> &str {
        "IcebergTableScan"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn children(&self) -> Vec<&Arc<(dyn ExecutionPlan + 'static)>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn properties(&self) -> &PlanProperties {
        &self.plan_properties
    }

    fn repartitioned(
        &self,
        target_partitions: usize,
        _config: &ConfigOptions,
    ) -> DFResult<Option<Arc<dyn ExecutionPlan>>> {
        let original_tasks = self
            .partition_tasks
            .clone()
            .into_iter()
            .flat_map(|p_tasks| p_tasks.file_scan_tasks);

        let merged_tasks = FileScanTask::merge(original_tasks).unwrap();

        let partition_tasks = partition_file_tasks_stream(merged_tasks, target_partitions);

        Ok(Some(Arc::new(IcebergTableScan {
            table: self.table.clone(),
            snapshot_id: self.snapshot_id,
            plan_properties: self.plan_properties.clone(),
            projection: self.projection.clone(),
            predicates: self.predicates.clone(),
            partition_tasks,
        })))
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let partition_task = &self.partition_tasks[partition];
        let file_scan_task_stream = partition_task.to_file_scan_task_stream();

        let arrow_reader = ArrowReaderBuilder::new(self.table.file_io().clone()).build();

        let stream = futures::stream::once(arrow_reader.read(file_scan_task_stream))
            .try_flatten()
            .map_err(to_datafusion_error);

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
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
            self.projection
                .clone()
                .map_or(String::new(), |v| v.join(",")),
            self.predicates
                .clone()
                .map_or(String::from(""), |p| format!("{}", p))
        )
    }
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

/// Partition the file scan tasks stream into multiple partitions.
fn partition_file_tasks_stream(
    file_tasks: Vec<FileScanTask>,
    target_partitions: usize,
) -> Vec<PartitionTask> {
    // If no file tasks, return empty vector
    if file_tasks.is_empty() {
        return Vec::new();
    }

    // Calculate total number of units (splittable ranges + non-splittable tasks)
    let sum_unit = file_tasks
        .iter()
        .map(|task| {
            task.file_range
                .as_ref()
                .map(|range| range.len())
                .unwrap_or(1)
        })
        .sum::<usize>();

    // Use the minimum of target_partitions and sum_unit to avoid empty partitions
    let num_partitions = target_partitions.min(sum_unit).max(1);

    // Calculate target units per partition
    let units_per_partition = sum_unit / num_partitions;
    let extra_units = sum_unit % num_partitions;

    // Initialize partition tasks vector
    let mut partition_tasks: Vec<PartitionTask> = (0..num_partitions)
        .map(|_| PartitionTask {
            file_scan_tasks: Vec::new(),
        })
        .collect();

    let mut current_partition = 0;
    let mut current_partition_units = 0;
    let mut target_units_for_current = units_per_partition
        + if current_partition < extra_units {
            1
        } else {
            0
        };

    for file_task in file_tasks {
        if let Some(mut file_range) = file_task.file_range.clone() {
            // Task has file_range, it can be split
            let range_len = file_range.len();
            let mut remaining_units = range_len;
            let mut current_task = file_task.clone();

            while remaining_units > 0 {
                let available_space = target_units_for_current - current_partition_units;

                if available_space == 0 {
                    // Move to next partition
                    current_partition += 1;
                    if current_partition >= num_partitions {
                        // If we've filled all partitions, put remaining in the last partition
                        current_partition = num_partitions - 1;
                        target_units_for_current = usize::MAX; // Allow unlimited units in last partition
                        current_partition_units = 0;
                    } else {
                        current_partition_units = 0;
                        target_units_for_current = units_per_partition
                            + if current_partition < extra_units {
                                1
                            } else {
                                0
                            };
                    }
                    continue;
                }

                let units_to_take = available_space.min(remaining_units);

                if units_to_take == remaining_units {
                    // Take all remaining units - add the current task
                    current_task.file_range = Some(file_range.clone());
                    partition_tasks[current_partition]
                        .file_scan_tasks
                        .push(current_task.clone());
                    current_partition_units += units_to_take;
                    remaining_units = 0;
                } else {
                    // Split the task - take part of the range
                    let split_range = file_range.split(units_to_take);
                    let mut split_task = current_task.clone();
                    split_task.file_range = Some(split_range);
                    partition_tasks[current_partition]
                        .file_scan_tasks
                        .push(split_task);
                    current_partition_units += units_to_take;
                    remaining_units -= units_to_take;
                }
            }
        } else {
            // Task has no file_range, it cannot be split - treat as 1 unit
            if current_partition_units >= target_units_for_current {
                // Move to next partition
                current_partition += 1;
                if current_partition >= num_partitions {
                    // If we've filled all partitions, put in the last partition
                    current_partition = num_partitions - 1;
                } else {
                    current_partition_units = 0;
                    target_units_for_current = units_per_partition
                        + if current_partition < extra_units {
                            1
                        } else {
                            0
                        };
                }
            }

            partition_tasks[current_partition]
                .file_scan_tasks
                .push(file_task);
            current_partition_units += 1;
        }
    }

    partition_tasks
}
