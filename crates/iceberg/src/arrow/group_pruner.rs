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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use futures::{TryStreamExt, try_join};
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use parquet::arrow::arrow_reader::ArrowReaderOptions;
use parquet::file::metadata::ParquetMetaData;
use parquet::schema::types::SchemaDescriptor;

use crate::Result;
use crate::arrow::{ArrowFileReader, CollectFieldIdVisitor, build_field_id_map};
use crate::expr::BoundPredicate;
use crate::expr::visitors::bound_predicate_visitor::visit;
use crate::expr::visitors::row_group_metrics_evaluator::RowGroupMetricsEvaluator;
use crate::io::FileIO;
use crate::scan::{FileScanGroup, FileScanTask, FileScanTaskStream, ParquetFileScanGroup};
use crate::spec::DataFileFormat;

/// Prune the row groups of the parquet files based on the predicate
#[derive(Clone)]
pub struct ParquetGroupPruner {
    file_io: FileIO,
}

impl ParquetGroupPruner {
    /// Create a new ParquetGroupPruner
    pub fn new(file_io: FileIO) -> Self {
        ParquetGroupPruner { file_io }
    }

    fn build_field_id_set_and_map(
        parquet_schema: &SchemaDescriptor,
        predicate: &BoundPredicate,
    ) -> Result<(HashSet<i32>, HashMap<i32, usize>)> {
        // Collects all Iceberg field IDs referenced in the filter predicate
        let mut collector = CollectFieldIdVisitor {
            field_ids: HashSet::default(),
        };
        visit(&mut collector, predicate)?;

        let iceberg_field_ids = collector.field_ids();
        let field_id_map = build_field_id_map(parquet_schema)?;

        Ok((iceberg_field_ids, field_id_map))
    }

    fn get_selected_row_group_indices(
        &self,
        task: &FileScanTask,
        parquet_schema: &SchemaDescriptor,
        parquet_metadata: &Arc<ParquetMetaData>,
    ) -> Result<Vec<usize>> {
        if let Some(predicate) = &task.predicate {
            let (_iceberg_field_ids, field_id_map) =
                Self::build_field_id_set_and_map(parquet_schema, predicate)?;

            let row_groups_metadata = parquet_metadata.row_groups();
            let mut results = Vec::with_capacity(parquet_metadata.row_groups().len());

            for (idx, row_group_metadata) in row_groups_metadata.iter().enumerate() {
                if RowGroupMetricsEvaluator::eval(
                    predicate,
                    row_group_metadata,
                    &field_id_map,
                    &task.schema,
                )? {
                    results.push(idx);
                }
            }

            Ok(results)
        } else {
            Ok((0..parquet_metadata.num_row_groups()).collect())
        }
    }

    /// Prune the row groups of the parquet files based on the predicate
    pub async fn prune(&self, mut task: FileScanTask) -> Result<FileScanTask> {
        // Create stream reader for parquet file meta
        let parquet_file = self.file_io.new_input(&task.data_file_path)?;
        let (parquet_metadata, parquet_reader) =
            try_join!(parquet_file.metadata(), parquet_file.reader())?;
        let parquet_file_reader = ArrowFileReader::new(parquet_metadata, parquet_reader)
            .with_preload_column_index(true)
            .with_preload_offset_index(true)
            .with_preload_page_index(false);
        let record_batch_stream_builder = ParquetRecordBatchStreamBuilder::new_with_options(
            parquet_file_reader,
            ArrowReaderOptions::new(),
        )
        .await?;

        let row_group_indices = self.get_selected_row_group_indices(
            &task,
            record_batch_stream_builder.parquet_schema(),
            record_batch_stream_builder.metadata(),
        )?;

        task.file_range = Some(FileScanGroup::Parquet(ParquetFileScanGroup {
            row_group_indexes: row_group_indices,
        }));

        Ok(task)
    }
}

/// Prune the row groups of the parquet files based on the predicate
pub struct GroupPruner {
    parquet_pruner: ParquetGroupPruner,
}

impl GroupPruner {
    /// Create a new GroupPruner
    pub fn new(file_io: FileIO) -> Self {
        GroupPruner {
            parquet_pruner: ParquetGroupPruner::new(file_io),
        }
    }

    /// Prune the groups of the files task based on the info of the file scan task.(E.g. predicate)
    pub fn prune(&self, file_tasks_stream: FileScanTaskStream) -> FileScanTaskStream {
        let parquet_pruner = self.parquet_pruner.clone();

        let pruned_stream = file_tasks_stream
            .map_ok(move |task| {
                let parquet_pruner = parquet_pruner.clone();
                async move {
                    match task.data_file_format {
                        DataFileFormat::Parquet => {
                            // Use the parquet pruner to prune row groups
                            parquet_pruner.prune(task).await
                        }
                        DataFileFormat::Avro | DataFileFormat::Orc | DataFileFormat::Puffin => {
                            // For other formats, return the task unchanged as no pruning is supported
                            Ok(task)
                        }
                    }
                }
            })
            .try_buffer_unordered(10); // Process up to 10 tasks concurrently

        Box::pin(pruned_stream)
    }
}
