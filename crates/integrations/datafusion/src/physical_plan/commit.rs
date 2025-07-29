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
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, RecordBatch, StringArray, UInt64Array};
use datafusion::arrow::datatypes::{
    DataType, Field, Schema as ArrowSchema, SchemaRef as ArrowSchemaRef,
};
use datafusion::common::{DataFusionError, Result as DFResult};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures::StreamExt;
use iceberg::Catalog;
use iceberg::spec::{DataFile, deserialize_data_file_from_json};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};

use crate::physical_plan::DATA_FILES_COL_NAME;
use crate::to_datafusion_error;

/// IcebergCommitExec is responsible for collecting the files written and use
/// [`Transaction::fast_append`] to commit the data files written.
#[derive(Debug)]
pub(crate) struct IcebergCommitExec {
    table: Table,
    catalog: Arc<dyn Catalog>,
    input: Arc<dyn ExecutionPlan>,
    schema: ArrowSchemaRef,
    count_schema: ArrowSchemaRef,
    plan_properties: PlanProperties,
}

impl IcebergCommitExec {
    pub fn new(
        table: Table,
        catalog: Arc<dyn Catalog>,
        input: Arc<dyn ExecutionPlan>,
        schema: ArrowSchemaRef,
    ) -> Self {
        let plan_properties = Self::compute_properties(schema.clone());

        Self {
            table,
            catalog,
            input,
            schema,
            count_schema: Self::make_count_schema(),
            plan_properties,
        }
    }

    // Compute the plan properties for this execution plan
    fn compute_properties(schema: ArrowSchemaRef) -> PlanProperties {
        PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        )
    }

    // Create a record batch with just the count of rows written
    fn make_count_batch(count: u64) -> DFResult<RecordBatch> {
        let count_array = Arc::new(UInt64Array::from(vec![count])) as ArrayRef;

        RecordBatch::try_from_iter_with_nullable(vec![("count", count_array, false)]).map_err(|e| {
            DataFusionError::ArrowError(e, Some("Failed to make count batch!".to_string()))
        })
    }

    fn make_count_schema() -> ArrowSchemaRef {
        // Define a schema.
        Arc::new(ArrowSchema::new(vec![Field::new(
            "count",
            DataType::UInt64,
            false,
        )]))
    }
}

impl DisplayAs for IcebergCommitExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => {
                write!(f, "IcebergCommitExec: table={}", self.table.identifier())
            }
            DisplayFormatType::Verbose => {
                write!(
                    f,
                    "IcebergCommitExec: table={}, schema={:?}",
                    self.table.identifier(),
                    self.schema
                )
            }
            DisplayFormatType::TreeRender => {
                write!(f, "IcebergCommitExec: table={}", self.table.identifier())
            }
        }
    }
}

impl ExecutionPlan for IcebergCommitExec {
    fn name(&self) -> &str {
        "IcebergCommitExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.plan_properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(format!(
                "IcebergCommitExec expects exactly one child, but provided {}",
                children.len()
            )));
        }

        Ok(Arc::new(IcebergCommitExec::new(
            self.table.clone(),
            self.catalog.clone(),
            children[0].clone(),
            self.schema.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        // IcebergCommitExec only has one partition (partition 0)
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "IcebergCommitExec only has one partition, but got partition {}",
                partition
            )));
        }

        let table = self.table.clone();
        let input_plan = self.input.clone();
        let count_schema = Arc::clone(&self.count_schema);

        // todo revisit this
        let spec_id = self.table.metadata().default_partition_spec_id();
        let partition_type = self.table.metadata().default_partition_type().clone();
        let current_schema = self.table.metadata().current_schema().clone();

        let catalog = Arc::clone(&self.catalog);

        // Process the input streams from all partitions and commit the data files
        let stream = futures::stream::once(async move {
            let mut data_files: Vec<DataFile> = Vec::new();
            let mut total_record_count: u64 = 0;

            // Execute and collect results from the input coalesced plan
            let mut batch_stream = input_plan.execute(0, context)?;

            while let Some(batch_result) = batch_stream.next().await {
                let batch = batch_result?;

                let files_array = batch
                    .column_by_name(DATA_FILES_COL_NAME)
                    .ok_or_else(|| {
                        DataFusionError::Internal(
                            "Expected 'data_files' column in input batch".to_string(),
                        )
                    })?
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| {
                        DataFusionError::Internal(
                            "Expected 'data_files' column to be StringArray".to_string(),
                        )
                    })?;

                // todo remove log
                println!("files_array to deserialize: {:?}", files_array);

                // Deserialize all data files from the StringArray
                let batch_files: Vec<DataFile> = files_array
                    .into_iter()
                    .flatten()
                    .map(|f| -> DFResult<DataFile> {
                        // Parse JSON to DataFileSerde and convert to DataFile
                        deserialize_data_file_from_json(
                            f,
                            spec_id,
                            &partition_type,
                            &current_schema,
                        )
                        .map_err(to_datafusion_error)
                    })
                    .collect::<datafusion::common::Result<_>>()?;

                // add record_counts from the current batch to total record count
                total_record_count += batch_files.iter().map(|f| f.record_count()).sum::<u64>();

                // Add all deserialized files to our collection
                data_files.extend(batch_files);
            }

            // If no data files were collected, return an empty result
            if data_files.is_empty() {
                return Ok(RecordBatch::new_empty(count_schema));
            }

            // Create a transaction and commit the data files
            let tx = Transaction::new(&table);
            let action = tx.fast_append().add_data_files(data_files);

            // Apply the action and commit the transaction
            let _updated_table = action
                .apply(tx)
                .map_err(to_datafusion_error)?
                .commit(catalog.as_ref())
                .await
                .map_err(to_datafusion_error)?;

            Self::make_count_batch(total_record_count)
        })
        .boxed();

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.count_schema),
            stream,
        )))
    }
}
