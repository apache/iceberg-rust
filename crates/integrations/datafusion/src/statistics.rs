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

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::stats::Precision;
use datafusion::common::{ColumnStatistics, Statistics};
use datafusion::error::Result as DFResult;
use datafusion::physical_expr::{analyze, AnalysisContext, ExprBoundaries, PhysicalExpr};
use iceberg::spec::{DataContentType, ManifestStatus};
use iceberg::table::Table;
use iceberg::Result;

use crate::physical_plan::expr_to_predicate::datum_to_scalar_value;

// Compute DataFusion table statistics for a given table/snapshot
pub async fn compute_statistics(table: &Table, snapshot_id: Option<i64>) -> Result<Statistics> {
    let file_io = table.file_io();
    let metadata = table.metadata();
    let snapshot = table.snapshot(snapshot_id)?;

    let mut num_rows = 0;
    let mut lower_bounds = HashMap::new();
    let mut upper_bounds = HashMap::new();
    let mut null_counts = HashMap::new();

    let manifest_list = snapshot.load_manifest_list(file_io, metadata).await?;

    // For each existing/added manifest in the snapshot aggregate the row count, as well as null
    // count and min/max values.
    for manifest_file in manifest_list.entries() {
        let manifest = manifest_file.load_manifest(file_io).await?;
        manifest.entries().iter().for_each(|manifest_entry| {
            // Gather stats only for non-deleted data files
            if manifest_entry.status() != ManifestStatus::Deleted {
                let data_file = manifest_entry.data_file();
                if data_file.content_type() == DataContentType::Data {
                    num_rows += data_file.record_count();
                    data_file.lower_bounds().iter().for_each(|(col_id, min)| {
                        lower_bounds
                            .entry(*col_id)
                            .and_modify(|col_min| {
                                if min < col_min {
                                    *col_min = min.clone()
                                }
                            })
                            .or_insert(min.clone());
                    });
                    data_file.upper_bounds().iter().for_each(|(col_id, max)| {
                        upper_bounds
                            .entry(*col_id)
                            .and_modify(|col_max| {
                                if max > col_max {
                                    *col_max = max.clone()
                                }
                            })
                            .or_insert(max.clone());
                    });
                    data_file
                        .null_value_counts()
                        .iter()
                        .for_each(|(col_id, null_count)| {
                            null_counts
                                .entry(*col_id)
                                .and_modify(|col_null_count| *col_null_count += *null_count)
                                .or_insert(*null_count);
                        });
                }
            }
        })
    }

    // Construct the DataFusion `Statistics` object, leaving any missing info as `Precision::Absent`
    let schema = snapshot.schema(metadata)?;
    let col_stats = schema
        .as_struct()
        .fields()
        .iter()
        .map(|field| {
            ColumnStatistics {
                null_count: null_counts
                    .get(&field.id)
                    .map(|nc| Precision::Inexact(*nc as usize))
                    .unwrap_or(Precision::Absent),
                max_value: upper_bounds
                    .get(&field.id)
                    .and_then(|datum| datum_to_scalar_value(datum).map(Precision::Inexact))
                    .unwrap_or(Precision::Absent),
                min_value: lower_bounds
                    .get(&field.id)
                    .and_then(|datum| datum_to_scalar_value(datum).map(Precision::Inexact))
                    .unwrap_or(Precision::Absent),
                distinct_count: Precision::Absent, // will be picked up after #417
            }
        })
        .collect();

    Ok(Statistics {
        num_rows: Precision::Inexact(num_rows as usize),
        total_byte_size: Precision::Absent,
        column_statistics: col_stats,
    })
}

// Apply bounds to the provided input statistics.
//
// Adapted from `FilterExec::statistics_helper` in DataFusion.
pub fn apply_bounds(
    input_stats: Statistics,
    predicate: &Arc<dyn PhysicalExpr>,
    schema: SchemaRef,
) -> DFResult<Statistics> {
    let num_rows = input_stats.num_rows;
    let total_byte_size = input_stats.total_byte_size;
    let input_analysis_ctx =
        AnalysisContext::try_from_statistics(&schema, &input_stats.column_statistics)?;

    let analysis_ctx = analyze(predicate, input_analysis_ctx, &schema)?;

    // Estimate (inexact) selectivity of predicate
    let selectivity = analysis_ctx.selectivity.unwrap_or(1.0);
    let num_rows = num_rows.with_estimated_selectivity(selectivity);
    let total_byte_size = total_byte_size.with_estimated_selectivity(selectivity);

    let column_statistics = analysis_ctx
        .boundaries
        .into_iter()
        .enumerate()
        .map(
            |(
                idx,
                ExprBoundaries {
                    interval,
                    distinct_count,
                    ..
                },
            )| {
                let (lower, upper) = interval.into_bounds();
                let (min_value, max_value) = if lower.eq(&upper) {
                    (Precision::Exact(lower), Precision::Exact(upper))
                } else {
                    (Precision::Inexact(lower), Precision::Inexact(upper))
                };
                ColumnStatistics {
                    null_count: input_stats.column_statistics[idx].null_count.to_inexact(),
                    max_value,
                    min_value,
                    distinct_count: distinct_count.to_inexact(),
                }
            },
        )
        .collect();
    Ok(Statistics {
        num_rows,
        total_byte_size,
        column_statistics,
    })
}
