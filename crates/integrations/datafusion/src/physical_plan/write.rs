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
use std::str::FromStr;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{
    DataType, Field, Schema as ArrowSchema, SchemaRef as ArrowSchemaRef,
};
use datafusion::common::Result as DFResult;
use datafusion::error::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    execute_input_stream,
};
use futures::StreamExt;
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::spec::{
    DataFileFormat, FormatVersion, PROPERTY_DEFAULT_FILE_FORMAT,
    PROPERTY_DEFAULT_FILE_FORMAT_DEFAULT, serialize_data_file_to_json,
};
use iceberg::table::Table;
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{Error, ErrorKind};
use parquet::file::properties::WriterProperties;
use uuid::Uuid;

use crate::physical_plan::DATA_FILES_COL_NAME;
use crate::to_datafusion_error;

pub(crate) struct IcebergWriteExec {
    table: Table,
    input: Arc<dyn ExecutionPlan>,
    result_schema: ArrowSchemaRef,
    plan_properties: PlanProperties,
}

impl IcebergWriteExec {
    pub fn new(table: Table, input: Arc<dyn ExecutionPlan>, schema: ArrowSchemaRef) -> Self {
        let plan_properties = Self::compute_properties(&input, schema);

        Self {
            table,
            input,
            result_schema: Self::make_result_schema(),
            plan_properties,
        }
    }

    fn compute_properties(
        input: &Arc<dyn ExecutionPlan>,
        schema: ArrowSchemaRef,
    ) -> PlanProperties {
        PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(input.output_partitioning().partition_count()),
            EmissionType::Final,
            Boundedness::Bounded,
        )
    }

    // Create a record batch with serialized data files
    fn make_result_batch(data_files: Vec<String>) -> DFResult<RecordBatch> {
        let files_array = Arc::new(StringArray::from(data_files)) as ArrayRef;

        RecordBatch::try_new(Self::make_result_schema(), vec![files_array]).map_err(|e| {
            DataFusionError::ArrowError(e, Some("Failed to make result batch".to_string()))
        })
    }

    fn make_result_schema() -> ArrowSchemaRef {
        // Define a schema.
        Arc::new(ArrowSchema::new(vec![Field::new(
            DATA_FILES_COL_NAME,
            DataType::Utf8,
            false,
        )]))
    }
}

impl Debug for IcebergWriteExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "IcebergWriteExec")
    }
}

impl DisplayAs for IcebergWriteExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => {
                write!(f, "IcebergWriteExec: table={}", self.table.identifier())
            }
            DisplayFormatType::Verbose => {
                write!(
                    f,
                    "IcebergWriteExec: table={}, result_schema={:?}",
                    self.table.identifier(),
                    self.result_schema
                )
            }
            DisplayFormatType::TreeRender => {
                write!(f, "IcebergWriteExec: table={}", self.table.identifier())
            }
        }
    }
}

impl ExecutionPlan for IcebergWriteExec {
    fn name(&self) -> &str {
        "IcebergWriteExec"
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
            return Err(DataFusionError::Internal(
                "IcebergWriteExec expects exactly one child, but provided {} ".to_string(),
            ));
        }

        Ok(Arc::new(Self::new(
            self.table.clone(),
            Arc::clone(&children[0]),
            self.schema(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        // todo non-default partition spec?
        let spec_id = self.table.metadata().default_partition_spec_id();
        let partition_type = self.table.metadata().default_partition_type().clone();
        let format_version = self.table.metadata().format_version();

        // Check data file format
        let file_format = DataFileFormat::from_str(
            self.table
                .metadata()
                .properties()
                .get(PROPERTY_DEFAULT_FILE_FORMAT)
                .unwrap_or(&PROPERTY_DEFAULT_FILE_FORMAT_DEFAULT.to_string()),
        )
        .map_err(to_datafusion_error)?;
        if file_format != DataFileFormat::Parquet {
            return Err(to_datafusion_error(Error::new(
                ErrorKind::FeatureUnsupported,
                format!(
                    "File format {} is not supported for insert_into yet!",
                    file_format
                ),
            )));
        }

        // Create data file writer builder
        let data_file_writer_builder = DataFileWriterBuilder::new(
            ParquetWriterBuilder::new(
                WriterProperties::default(),
                self.table.metadata().current_schema().clone(),
                self.table.file_io().clone(),
                DefaultLocationGenerator::new(self.table.metadata().clone())
                    .map_err(to_datafusion_error)?,
                // todo filename prefix/suffix should be configurable
                DefaultFileNameGenerator::new(
                    "datafusion".to_string(),
                    Some(Uuid::now_v7().to_string()),
                    file_format,
                ),
            ),
            None,
            spec_id,
        );

        // Get input data
        let data = execute_input_stream(
            Arc::clone(&self.input),
            Arc::new(
                schema_to_arrow_schema(self.table.metadata().current_schema())
                    .map_err(to_datafusion_error)?,
            ),
            partition,
            Arc::clone(&context),
        )?;

        // Create write stream
        let stream = futures::stream::once(async move {
            let mut writer = data_file_writer_builder
                .build()
                .await
                .map_err(to_datafusion_error)?;
            let mut input_stream = data;

            while let Some(batch) = input_stream.next().await {
                writer.write(batch?).await.map_err(to_datafusion_error)?;
            }

            let data_file_builders = writer.close().await.map_err(to_datafusion_error)?;

            // Convert builders to data files and then to JSON strings
            let data_files: Vec<String> = data_file_builders
                .into_iter()
                .map(|data_file| -> DFResult<String> {
                    // Serialize to JSON
                    let json =
                        serialize_data_file_to_json(data_file, &partition_type, format_version)
                            .map_err(to_datafusion_error)?;

                    println!("Serialized data file: {}", json); // todo remove log
                    Ok(json)
                })
                .collect::<DFResult<Vec<String>>>()?;

            Self::make_result_batch(data_files)
        })
        .boxed();

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.result_schema),
            stream,
        )))
    }
}
