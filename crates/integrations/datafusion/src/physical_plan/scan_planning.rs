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

use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datafusion::error::Result as DFResult;
use datafusion::prelude::Expr;
use futures::TryStreamExt;
use iceberg::arrow::ArrowReaderBuilder;
use iceberg::expr::Predicate;
use iceberg::scan::{FileScanTask, TableScan};
use iceberg::table::Table;

use super::expr_to_predicate::convert_filters_to_predicate;
use crate::to_datafusion_error;

#[derive(Debug, Clone)]
pub(crate) struct IcebergScanConfig {
    /// Snapshot of the table to scan.
    snapshot_id: Option<i64>,
    /// Output schema after projection.
    output_schema: ArrowSchemaRef,
    /// Projection column names, None means all columns.
    column_names: Option<Vec<String>>,
    /// Filters to apply to the table scan.
    predicates: Option<Predicate>,
}

impl IcebergScanConfig {
    pub(crate) fn new(
        schema: ArrowSchemaRef,
        snapshot_id: Option<i64>,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
    ) -> Self {
        let output_schema = match projection {
            None => schema.clone(),
            Some(projection) => Arc::new(schema.project(projection).unwrap()),
        };

        Self {
            snapshot_id,
            output_schema,
            column_names: get_column_names(schema, projection),
            predicates: convert_filters_to_predicate(filters),
        }
    }

    pub(crate) fn snapshot_id(&self) -> Option<i64> {
        self.snapshot_id
    }

    pub(crate) fn output_schema(&self) -> ArrowSchemaRef {
        self.output_schema.clone()
    }

    pub(crate) fn column_names(&self) -> Option<&[String]> {
        self.column_names.as_deref()
    }

    pub(crate) fn predicates(&self) -> Option<&Predicate> {
        self.predicates.as_ref()
    }
}

/// Result of eager scan planning: the [`TableScan`] that planned the file scan
/// tasks, alongside those tasks grouped per output partition.
#[derive(Debug)]
pub(crate) struct EagerScanPlan {
    /// The [`TableScan`] used to plan `task_groups`. Retained so that every output
    /// partition builds its reader from this same scan, instead of rebuilding a
    /// throwaway `TableScan` on each `execute()` call.
    table_scan: Arc<TableScan>,
    /// Planned file scan tasks, one group per output partition.
    task_groups: Vec<Arc<[FileScanTask]>>,
}

impl EagerScanPlan {
    /// Number of output partitions, i.e. the number of task groups.
    pub(crate) fn partition_count(&self) -> usize {
        self.task_groups.len()
    }

    /// Total number of planned file scan tasks across all partitions.
    pub(crate) fn task_count(&self) -> usize {
        self.task_groups.iter().map(|group| group.len()).sum()
    }

    /// Returns the task group assigned to `partition`, or `None` if out of range.
    pub(crate) fn task_group(&self, partition: usize) -> Option<Arc<[FileScanTask]>> {
        self.task_groups.get(partition).cloned()
    }

    /// Returns an [`ArrowReaderBuilder`] configured for this scan.
    ///
    /// This deliberately routes through [`TableScan::arrow_reader_builder`] rather
    /// than constructing an [`ArrowReaderBuilder`] directly: it keeps the reader
    /// settings (batch size, row group filtering, row selection) sourced from the
    /// same place as the lazy path's `TableScan::to_arrow`, so the two scan paths
    /// cannot silently drift apart.
    pub(crate) fn arrow_reader_builder(&self) -> ArrowReaderBuilder {
        self.table_scan.arrow_reader_builder()
    }
}

pub(crate) async fn plan_eager_scan(
    table: &Table,
    scan_config: &IcebergScanConfig,
    target_partitions: usize,
) -> DFResult<EagerScanPlan> {
    // Do not cache planned FileScanTasks in the provider in v1. They are query-specific
    // because projection, predicate binding, snapshot schema, and delete planning can differ
    // between scans. Catalog-backed providers also need fresh metadata on each scan.
    // TODO: Revisit provider-level caching for static tables with a precise cache key.
    let table_scan = Arc::new(build_table_scan(table, scan_config)?);

    let tasks: Vec<FileScanTask> = table_scan
        .plan_files()
        .await
        .map_err(to_datafusion_error)?
        .try_collect::<Vec<_>>()
        .await
        .map_err(to_datafusion_error)?;

    let task_groups = group_file_scan_tasks_round_robin(tasks, target_partitions)
        .into_iter()
        .map(Arc::<[FileScanTask]>::from)
        .collect();

    Ok(EagerScanPlan {
        table_scan,
        task_groups,
    })
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

/// Groups file scan tasks into `target_partitions` groups using a naive
/// round-robin assignment. Non-empty groups are bounded by `tasks.len()`.
// TODO: Replace this naive round-robin grouping with size-based grouping once the
// first parallel scan path is stable. Keep this v1 simple and deterministic.
fn group_file_scan_tasks_round_robin(
    tasks: Vec<FileScanTask>,
    target_partitions: usize,
) -> Vec<Vec<FileScanTask>> {
    if tasks.is_empty() {
        return vec![vec![]];
    }

    let target_partitions = target_partitions.max(1).min(tasks.len());

    let mut groups: Vec<Vec<FileScanTask>> = vec![Vec::new(); target_partitions];
    for (i, task) in tasks.into_iter().enumerate() {
        groups[i % target_partitions].push(task);
    }

    groups
}

pub(crate) fn build_table_scan(
    table: &Table,
    scan_config: &IcebergScanConfig,
) -> DFResult<TableScan> {
    let builder = match scan_config.snapshot_id {
        Some(id) => table.scan().snapshot_id(id),
        None => table.scan(),
    };
    let mut builder = match scan_config.column_names.clone() {
        Some(names) => builder.select(names),
        None => builder.select_all(),
    };
    if let Some(pred) = scan_config.predicates.clone() {
        builder = builder.with_filter(pred);
    }
    builder.build().map_err(to_datafusion_error)
}
