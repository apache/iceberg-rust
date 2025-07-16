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

use datafusion::arrow::array::{ArrayRef, RecordBatch, StringArray, UInt64Array};
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
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, execute_input_stream,
};
use futures::StreamExt;
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::spec::{DataFileFormat, DataFileSerde, FormatVersion};
use iceberg::table::Table;
use iceberg::writer::CurrentFileStatus;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::{FileWriter, FileWriterBuilder, ParquetWriterBuilder};
use parquet::file::properties::WriterProperties;
use uuid::Uuid;

use crate::to_datafusion_error;

pub(crate) struct IcebergWriteExec {
    table: Table,
    input: Arc<dyn ExecutionPlan>,
    result_schema: ArrowSchemaRef,
    plan_properties: PlanProperties,
}

impl IcebergWriteExec {
    pub fn new(table: Table, input: Arc<dyn ExecutionPlan>, schema: ArrowSchemaRef) -> Self {
        let plan_properties = Self::compute_properties(schema.clone());

        Self {
            table,
            input,
            result_schema: Self::make_result_schema(),
            plan_properties,
        }
    }

    /// todo: Copied from scan.rs
    fn compute_properties(schema: ArrowSchemaRef) -> PlanProperties {
        // TODO:
        // This is more or less a placeholder, to be replaced
        // once we support output-partitioning
        PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        )
    }

    // Create a record batch with count and serialized data files
    fn make_result_batch(count: u64, data_files: Vec<String>) -> DFResult<RecordBatch> {
        let count_array = Arc::new(UInt64Array::from(vec![count])) as ArrayRef;
        let files_array = Arc::new(StringArray::from(data_files)) as ArrayRef;

        RecordBatch::try_from_iter_with_nullable(vec![
            ("count", count_array, false),
            ("data_files", files_array, false),
        ])
        .map_err(|e| {
            DataFusionError::ArrowError(e, Some("Failed to make result batch".to_string()))
        })
    }

    fn make_result_schema() -> ArrowSchemaRef {
        // Define a schema.
        Arc::new(ArrowSchema::new(vec![
            Field::new("data_files", DataType::Utf8, false),
            Field::new("count", DataType::UInt64, false),
        ]))
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
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let parquet_writer_fut = ParquetWriterBuilder::new(
            WriterProperties::default(),
            self.table.metadata().current_schema().clone(),
            self.table.file_io().clone(),
            DefaultLocationGenerator::new(self.table.metadata().clone())
                .map_err(to_datafusion_error)?,
            // todo filename prefix/suffix should be configurable
            DefaultFileNameGenerator::new(
                "datafusion".to_string(),
                Some(Uuid::now_v7().to_string()),
                DataFileFormat::Parquet,
            ),
        )
        .build();

        let data = execute_input_stream(
            Arc::clone(&self.input),
            Arc::new(
                schema_to_arrow_schema(self.table.metadata().current_schema())
                    .map_err(to_datafusion_error)?,
            ),
            partition,
            Arc::clone(&context),
        )?;

        // todo non-default partition spec?
        let spec_id = self.table.metadata().default_partition_spec_id();
        let partition_type = self.table.metadata().default_partition_type().clone();
        let is_version_1 = self.table.metadata().format_version() == FormatVersion::V1;

        let stream = futures::stream::once(async move {
            let mut writer = parquet_writer_fut.await.map_err(to_datafusion_error)?;
            let mut input_stream = data;

            while let Some(batch_res) = input_stream.next().await {
                let batch = batch_res?;
                writer.write(&batch).await.map_err(to_datafusion_error)?;
            }

            let count = writer.current_row_num() as u64;
            let data_file_builders = writer.close().await.map_err(to_datafusion_error)?;

            // Convert builders to data files and then to JSON strings
            let data_files: Vec<String> = data_file_builders
                .into_iter()
                .map(|mut builder| -> DFResult<String> {
                    // Build the data file
                    let data_file = builder.partition_spec_id(spec_id).build().map_err(|e| {
                        DataFusionError::Execution(format!("Failed to build data file: {}", e))
                    })?;

                    // Convert to DataFileSerde
                    let serde = DataFileSerde::try_from(data_file, &partition_type, is_version_1)
                        .map_err(|e| {
                        DataFusionError::Execution(format!(
                            "Failed to convert to DataFileSerde: {}",
                            e
                        ))
                    })?;

                    // Serialize to JSON
                    let json = serde_json::to_string(&serde).map_err(|e| {
                        DataFusionError::Execution(format!("Failed to serialize to JSON: {}", e))
                    })?;

                    println!("Serialized data file: {}", json); // todo remove log
                    Ok(json)
                })
                .collect::<DFResult<Vec<String>>>()?;

            Self::make_result_batch(count, data_files)
        })
        .boxed();

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.result_schema),
            stream,
        )))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use datafusion::arrow::array::StringArray;
    use iceberg::spec::{
        DataFile, DataFileBuilder, DataFileFormat, DataFileSerde, PartitionSpec, PrimitiveType,
        Schema, Struct, Type,
    };

    // todo move this to DataFileSerde?
    #[test]
    fn test_data_file_serialization() {
        // Create a simple schema
        let schema = Schema::builder()
            .with_schema_id(1)
            .with_identifier_field_ids(vec![1])
            .with_fields(vec![
                iceberg::spec::NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long))
                    .into(),
                iceberg::spec::NestedField::required(
                    2,
                    "name",
                    Type::Primitive(PrimitiveType::String),
                )
                .into(),
            ])
            .build()
            .unwrap();

        // Create a partition spec
        let partition_spec = PartitionSpec::builder(schema.clone())
            .with_spec_id(1)
            .add_partition_field("id", "id_partition", iceberg::spec::Transform::Identity)
            .unwrap()
            .build()
            .unwrap();

        // Get partition type from the partition spec
        let partition_type = partition_spec.partition_type(&schema).unwrap();

        // Set version flag
        let is_version_1 = false;

        // Create a vector of DataFile objects
        let data_files = vec![
            DataFileBuilder::default()
                .content(iceberg::spec::DataContentType::Data)
                .file_format(DataFileFormat::Parquet)
                .file_path("path/to/file1.parquet".to_string())
                .file_size_in_bytes(1024)
                .record_count(100)
                .partition_spec_id(1)
                .partition(Struct::empty())
                .column_sizes(HashMap::from([(1, 512), (2, 512)]))
                .value_counts(HashMap::from([(1, 100), (2, 100)]))
                .null_value_counts(HashMap::from([(1, 0), (2, 0)]))
                .build()
                .unwrap(),
            DataFileBuilder::default()
                .content(iceberg::spec::DataContentType::Data)
                .file_format(DataFileFormat::Parquet)
                .file_path("path/to/file2.parquet".to_string())
                .file_size_in_bytes(2048)
                .record_count(200)
                .partition_spec_id(1)
                .partition(Struct::empty())
                .column_sizes(HashMap::from([(1, 1024), (2, 1024)]))
                .value_counts(HashMap::from([(1, 200), (2, 200)]))
                .null_value_counts(HashMap::from([(1, 10), (2, 5)]))
                .build()
                .unwrap(),
        ];

        // Serialize the DataFile objects
        let serialized_files = data_files
            .into_iter()
            .map(|f| {
                let serde = DataFileSerde::try_from(f, &partition_type, is_version_1).unwrap();
                let json = serde_json::to_string(&serde).unwrap();
                println!("Test serialized data file: {}", json);
                json
            })
            .collect::<Vec<String>>();

        // Verify we have the expected number of serialized files
        assert_eq!(serialized_files.len(), 2);

        // Verify each serialized file contains expected data
        for json in &serialized_files {
            assert!(json.contains("path/to/file"));
            assert!(json.contains("parquet"));
            assert!(json.contains("record_count"));
            assert!(json.contains("file_size_in_bytes"));
        }

        // Convert Vec<String> to StringArray and print it
        let string_array = StringArray::from(serialized_files.clone());
        println!("StringArray: {:?}", string_array);

        // Now deserialize the JSON strings back into DataFile objects
        println!("\nDeserializing back to DataFile objects:");
        let deserialized_files: Vec<DataFile> = serialized_files
            .into_iter()
            .map(|json| {
                // First deserialize to DataFileSerde
                let data_file_serde: DataFileSerde =
                    serde_json::from_str(&json).expect("Failed to deserialize to DataFileSerde");

                // Then convert to DataFile
                let data_file = data_file_serde
                    .try_into(partition_spec.spec_id(), &partition_type, &schema)
                    .expect("Failed to convert DataFileSerde to DataFile");

                println!("Deserialized DataFile: {:?}", data_file);
                data_file
            })
            .collect();

        // Verify we have the expected number of deserialized files
        assert_eq!(deserialized_files.len(), 2);

        // Verify the deserialized files have the expected properties
        for file in &deserialized_files {
            assert_eq!(file.content_type(), iceberg::spec::DataContentType::Data);
            assert_eq!(file.file_format(), DataFileFormat::Parquet);
            assert!(file.file_path().contains("path/to/file"));
            assert!(file.record_count() == 100 || file.record_count() == 200);
        }
    }
}
