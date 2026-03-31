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
use datafusion::error::Result as DFResult;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, ExecutionPlan, Partitioning, PlanProperties};
use futures::TryStreamExt;
use iceberg::arrow::ArrowReaderBuilder;
use iceberg::io::FileIO;
use iceberg::scan::FileScanTask;

use crate::to_datafusion_error;

/// A DataFusion [`ExecutionPlan`] that reads one [`FileScanTask`] per partition.
///
/// Display information (projection, predicate) is derived at runtime from the output schema and
/// the tasks rather than stored as dedicated struct fields.
#[derive(Debug, Clone)]
pub struct IcebergPartitionedScan {
    tasks: Vec<FileScanTask>,
    file_io: FileIO,
    plan_properties: Arc<PlanProperties>,
}

impl IcebergPartitionedScan {
    pub fn new(tasks: Vec<FileScanTask>, file_io: FileIO, schema: ArrowSchemaRef) -> Self {
        let n_partitions = tasks.len();
        let plan_properties = Self::compute_properties(schema, n_partitions);
        Self {
            tasks,
            file_io,
            plan_properties,
        }
    }

    pub fn tasks(&self) -> &[FileScanTask] {
        &self.tasks
    }

    pub fn file_io(&self) -> &FileIO {
        &self.file_io
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
        let task = self.tasks.get(partition).cloned().ok_or_else(|| {
            datafusion::error::DataFusionError::Internal(format!(
                "{}: partition index {partition} is out of bounds \
                 (total tasks: {})",
                self.name(),
                self.tasks.len()
            ))
        })?;

        let file_io = self.file_io.clone();

        let fut = async move {
            let task_stream = futures::stream::once(futures::future::ready(Ok(task)));
            let record_batch_stream = ArrowReaderBuilder::new(file_io)
                .build()
                .read(Box::pin(task_stream))
                .map_err(to_datafusion_error)?
                .map_err(to_datafusion_error);
            Ok::<_, datafusion::error::DataFusionError>(record_batch_stream)
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
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect::<Vec<_>>()
            .join(",");
        // All tasks share the same predicate (they come from a single scan plan build),
        // so reading it from the first task is sufficient.
        let predicate = self
            .tasks
            .first()
            .and_then(|t| t.predicate())
            .map_or(String::new(), |p| format!("{p}"));
        let file_count = self.tasks.len();
        write!(
            f,
            "{} projection:[{projection}] predicate:[{predicate}] file_count:[{file_count}]",
            self.name()
        )?;
        if self.tasks.len() <= 5 {
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
