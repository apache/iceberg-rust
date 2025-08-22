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

use datafusion::arrow::array::{ArrayRef, RecordBatch, StructArray};
use datafusion::arrow::datatypes::{
    DataType, Field, Schema as ArrowSchema, SchemaRef as ArrowSchemaRef,
};
use datafusion::common::Result as DFResult;
use datafusion::error::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    execute_input_stream,
};
use futures::StreamExt;
use iceberg::spec::{PartitionSpec, Schema};

use crate::to_datafusion_error;

/// Column name for the combined partition values struct
const PARTITION_VALUES_COLUMN: &str = "_iceberg_partition_values";

/// An execution plan node that calculates partition values for Iceberg tables.
///
/// This execution plan takes input data from a child execution plan and adds a single struct column
/// containing all partition values based on the table's partition specification. The partition values
/// are computed by applying the appropriate transforms to the source columns.
///
/// The output schema includes all original columns plus a single `_iceberg_partition_values` struct column.
#[derive(Debug, Clone)]
pub(crate) struct IcebergProjectExec {
    input: Arc<dyn ExecutionPlan>,
    partition_spec: Arc<PartitionSpec>,
    table_schema: Arc<Schema>,
    output_schema: ArrowSchemaRef,
    plan_properties: PlanProperties,
}

/// IcebergProjectExec is responsible for calculating partition values for Iceberg tables.
/// It takes input data from a child execution plan and adds a single struct column containing
/// all partition values based on the table's partition specification. The partition values are
/// computed by applying the appropriate transforms to the source columns. The output schema
/// includes all original columns plus a single `_iceberg_partition_values` struct column.
/// This approach simplifies downstream repartitioning operations by providing a single column
/// that can be directly used for sorting and repartitioning.
impl IcebergProjectExec {
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        partition_spec: Arc<PartitionSpec>,
        table_schema: Arc<Schema>,
    ) -> DFResult<Self> {
        let output_schema =
            Self::create_output_schema(&input.schema(), &partition_spec, &table_schema)?;
        let plan_properties = Self::compute_properties(&input, output_schema.clone());

        Ok(Self {
            input,
            partition_spec,
            table_schema,
            output_schema,
            plan_properties,
        })
    }

    /// Compute the plan properties for this execution plan
    fn compute_properties(
        input: &Arc<dyn ExecutionPlan>,
        schema: ArrowSchemaRef,
    ) -> PlanProperties {
        PlanProperties::new(
            EquivalenceProperties::new(schema),
            input.output_partitioning().clone(),
            EmissionType::Incremental,
            Boundedness::Bounded,
        )
    }

    /// Create the output schema by adding a single partition values struct column to the input schema
    fn create_output_schema(
        input_schema: &ArrowSchema,
        partition_spec: &PartitionSpec,
        table_schema: &Schema,
    ) -> DFResult<ArrowSchemaRef> {
        if partition_spec.is_unpartitioned() {
            return Ok(Arc::new(input_schema.clone()));
        }

        let mut fields: Vec<Arc<Field>> = input_schema.fields().to_vec();

        let partition_struct_type = partition_spec
            .partition_type(table_schema)
            .map_err(to_datafusion_error)?;

        // Convert the Iceberg struct type to Arrow struct type
        let arrow_struct_type =
            iceberg::arrow::type_to_arrow_type(&iceberg::spec::Type::Struct(partition_struct_type))
                .map_err(to_datafusion_error)?;

        // Add a single struct column containing all partition values
        fields.push(Arc::new(Field::new(
            PARTITION_VALUES_COLUMN,
            arrow_struct_type,
            false, // Partition values are generally not null
        )));

        Ok(Arc::new(ArrowSchema::new(fields)))
    }

    /// Calculate partition values for a record batch and return as a single struct array
    fn calculate_partition_values(&self, batch: &RecordBatch) -> DFResult<Option<ArrayRef>> {
        if self.partition_spec.is_unpartitioned() {
            return Ok(None);
        }

        let batch_schema = batch.schema();
        let mut partition_values = Vec::with_capacity(self.partition_spec.fields().len());

        // Get the expected struct fields from our output schema
        let partition_column_field = self
            .output_schema
            .field_with_name(PARTITION_VALUES_COLUMN)
            .map_err(|e| {
                DataFusionError::Internal(format!("Partition column not found in schema: {}", e))
            })?;

        let expected_struct_fields = match partition_column_field.data_type() {
            DataType::Struct(fields) => fields.clone(),
            _ => {
                return Err(DataFusionError::Internal(
                    "Partition column is not a struct type".to_string(),
                ));
            }
        };

        for pf in self.partition_spec.fields() {
            // Find the source field in the table schema
            let source_field = self.table_schema.field_by_id(pf.source_id).ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "Source field not found with id {} when calculating partition values",
                    pf.source_id
                ))
            })?;

            let field_path = Self::find_field_path(&self.table_schema, source_field.id)?;
            let index_path = Self::resolve_arrow_index_path(batch_schema.as_ref(), &field_path)?;

            let source_column = Self::extract_column_by_index_path(batch, &index_path)?;

            let transform_fn = iceberg::transform::create_transform_function(&pf.transform)
                .map_err(to_datafusion_error)?;
            let partition_value = transform_fn
                .transform(source_column)
                .map_err(to_datafusion_error)?;

            partition_values.push(partition_value);
        }

        // Create struct array using the expected fields from the schema
        let struct_array = StructArray::try_new(
            expected_struct_fields,
            partition_values,
            None, // No null buffer for the struct array itself
        )
        .map_err(|e| DataFusionError::ArrowError(e, None))?;

        Ok(Some(Arc::new(struct_array)))
    }

    /// Extract a column by an index path
    fn extract_column_by_index_path(
        batch: &RecordBatch,
        index_path: &[usize],
    ) -> DFResult<ArrayRef> {
        if index_path.is_empty() {
            return Err(DataFusionError::Internal(
                "Empty index path when extracting partition column".to_string(),
            ));
        }

        let mut current_column = batch.column(*index_path.first().unwrap()).clone();
        for child_index in &index_path[1..] {
            // We only support traversing nested Structs. Provide explicit errors for unsupported
            // nested container types to fail early and clearly.
            let dt = current_column.data_type();
            match dt {
                datafusion::arrow::datatypes::DataType::Struct(_) => {
                    let struct_array = current_column
                        .as_any()
                        .downcast_ref::<datafusion::arrow::array::StructArray>()
                        .ok_or_else(|| {
                            DataFusionError::Internal(format!(
                                "Failed to downcast to StructArray while traversing index path {:?} for partition column extraction",
                                index_path
                            ))
                        })?;
                    current_column = struct_array.column(*child_index).clone();
                }
                datafusion::arrow::datatypes::DataType::List(_)
                | datafusion::arrow::datatypes::DataType::LargeList(_)
                | datafusion::arrow::datatypes::DataType::FixedSizeList(_, _)
                | datafusion::arrow::datatypes::DataType::Map(_, _) => {
                    return Err(DataFusionError::NotImplemented(format!(
                        "Partitioning on nested list/map types is not supported (encountered {:?}) while traversing index path {:?}",
                        dt, index_path
                    )));
                }
                other => {
                    return Err(DataFusionError::Internal(format!(
                        "Expected struct array while traversing index path {:?} for partition column, got {:?}",
                        index_path, other
                    )));
                }
            }
        }
        Ok(current_column)
    }

    /// Find the path to a field by its ID (e.g., ["address", "city"]) in the Iceberg schema
    fn find_field_path(table_schema: &Schema, field_id: i32) -> DFResult<Vec<String>> {
        let dotted = table_schema.name_by_field_id(field_id).ok_or_else(|| {
            DataFusionError::Internal(format!(
                "Field with ID {} not found in schema when building field path for partition column",
                field_id
            ))
        })?;
        Ok(dotted.split('.').map(|s| s.to_string()).collect())
    }

    fn resolve_arrow_index_path(
        input_schema: &ArrowSchema,
        field_path: &[String],
    ) -> DFResult<Vec<usize>> {
        if field_path.is_empty() {
            return Err(DataFusionError::Internal(
                "Empty field path when resolving arrow index path for partition column".to_string(),
            ));
        }

        let mut indices = Vec::with_capacity(field_path.len());
        let mut current_field = input_schema.field_with_name(&field_path[0]).map_err(|_| {
            DataFusionError::Internal(format!(
                "Top-level column '{}' not found in schema when resolving partition column path",
                field_path[0]
            ))
        })?;
        let top_index = input_schema.index_of(&field_path[0]).map_err(|_| {
            DataFusionError::Internal(format!(
                "Failed to get index of top-level column '{}' when resolving partition column path",
                field_path[0]
            ))
        })?;
        indices.push(top_index);

        for name in &field_path[1..] {
            let dt = current_field.data_type();
            let struct_fields = match dt {
                datafusion::arrow::datatypes::DataType::Struct(fields) => fields,
                datafusion::arrow::datatypes::DataType::List(_)
                | datafusion::arrow::datatypes::DataType::LargeList(_)
                | datafusion::arrow::datatypes::DataType::FixedSizeList(_, _) => {
                    return Err(DataFusionError::NotImplemented(format!(
                        "Partitioning on nested list types is not supported while resolving nested field '{}' for partition column",
                        name
                    )));
                }
                datafusion::arrow::datatypes::DataType::Map(_, _) => {
                    return Err(DataFusionError::NotImplemented(format!(
                        "Partitioning on nested map types is not supported while resolving nested field '{}' for partition column",
                        name
                    )));
                }
                other => {
                    return Err(DataFusionError::Internal(format!(
                        "Expected struct type while resolving nested field '{}' for partition column, got {:?}",
                        name, other
                    )));
                }
            };
            let child_index = struct_fields
                .iter()
                .position(|f| f.name() == name)
                .ok_or_else(|| {
                    DataFusionError::Internal(format!(
                        "Field '{}' not found in struct when resolving partition column path",
                        name
                    ))
                })?;
            indices.push(child_index);
            current_field = &struct_fields[child_index];
        }

        Ok(indices)
    }

    /// Process a single batch by adding a partition values struct column
    fn process_batch(&self, batch: RecordBatch) -> DFResult<RecordBatch> {
        if self.partition_spec.is_unpartitioned() {
            return Ok(batch);
        }

        let partition_array = self.calculate_partition_values(&batch)?;

        let mut all_columns = batch.columns().to_vec();
        if let Some(partition_array) = partition_array {
            all_columns.push(partition_array);
        }

        RecordBatch::try_new(Arc::clone(&self.output_schema), all_columns)
            .map_err(|e| DataFusionError::ArrowError(e, None))
    }
}

impl DisplayAs for IcebergProjectExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => {
                write!(
                    f,
                    "IcebergProjectExec: partition_fields=[{}]",
                    self.partition_spec
                        .fields()
                        .iter()
                        .map(|pf| format!("{}({})", pf.transform, pf.name))
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            }
            DisplayFormatType::Verbose => {
                write!(
                    f,
                    "IcebergProjectExec: partition_fields=[{}], output_schema={:?}",
                    self.partition_spec
                        .fields()
                        .iter()
                        .map(|pf| format!("{}({})", pf.transform, pf.name))
                        .collect::<Vec<_>>()
                        .join(", "),
                    self.output_schema
                )
            }
            DisplayFormatType::TreeRender => {
                write!(
                    f,
                    "IcebergProjectExec: partition_fields=[{}]",
                    self.partition_spec
                        .fields()
                        .iter()
                        .map(|pf| format!("{}({})", pf.transform, pf.name))
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            }
        }
    }
}

impl ExecutionPlan for IcebergProjectExec {
    fn name(&self) -> &str {
        "IcebergProjectExec"
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
                "IcebergProjectExec expects exactly one child, but provided {}",
                children.len()
            )));
        }

        Ok(Arc::new(Self::new(
            Arc::clone(&children[0]),
            Arc::clone(&self.partition_spec),
            Arc::clone(&self.table_schema),
        )?))
    }

    /// Executes the partition value calculation for the given partition.
    ///
    /// This processes input data from the child execution plan and adds calculated
    /// partition columns based on the partition specification.
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        // Get input data stream
        let input_stream = execute_input_stream(
            Arc::clone(&self.input),
            self.input.schema(),
            partition,
            Arc::clone(&context),
        )?;

        if self.partition_spec.is_unpartitioned() {
            return Ok(input_stream);
        }

        let output_schema = Arc::clone(&self.output_schema);
        let project_exec = Arc::new(self.clone());

        let stream = input_stream.map(move |batch_result| {
            let batch = batch_result?;

            project_exec.process_batch(batch)
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            output_schema,
            stream.boxed(),
        )))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use datafusion::arrow::array::{Int32Array, RecordBatch, StructArray};
    use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema as ArrowSchema};
    use iceberg::spec::{NestedField, PrimitiveType, Schema, StructType, Transform, Type};
    use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

    use super::*;

    #[test]
    fn test_create_output_schema_unpartitioned() {
        // Create test schema
        let arrow_schema = ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "1".to_string(),
            )])),
            Field::new("name", DataType::Utf8, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "2".to_string(),
            )])),
        ]);

        let table_schema = Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::required(2, "name", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
            .unwrap();

        // Create unpartitioned spec
        let partition_spec = iceberg::spec::PartitionSpec::unpartition_spec();

        // Test schema creation
        let output_schema =
            IcebergProjectExec::create_output_schema(&arrow_schema, &partition_spec, &table_schema)
                .unwrap();

        // For now, should be identical to input schema (pass-through)
        assert_eq!(output_schema.fields().len(), 2);
        assert_eq!(output_schema.field(0).name(), "id");
        assert_eq!(output_schema.field(1).name(), "name");
    }

    #[test]
    fn test_create_output_schema_partitioned() {
        // Create test schema
        let arrow_schema = ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]);

        let table_schema = Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::required(2, "name", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
            .unwrap();

        // Create partitioned spec
        let partition_spec = iceberg::spec::PartitionSpec::builder(Arc::new(table_schema.clone()))
            .add_partition_field("id", "id_partition", Transform::Identity)
            .unwrap()
            .build()
            .unwrap();

        // Test schema creation
        let output_schema =
            IcebergProjectExec::create_output_schema(&arrow_schema, &partition_spec, &table_schema)
                .unwrap();

        // Should have 3 fields: original 2 + 1 partition values struct
        assert_eq!(output_schema.fields().len(), 3);
        assert_eq!(output_schema.field(0).name(), "id");
        assert_eq!(output_schema.field(1).name(), "name");
        assert_eq!(output_schema.field(2).name(), "_iceberg_partition_values");
    }

    #[test]
    fn test_partition_on_struct_field() {
        // Test partitioning on a nested field within a struct (e.g., address.city)
        let struct_fields = Fields::from(vec![
            Field::new("street", DataType::Utf8, false),
            Field::new("city", DataType::Utf8, false),
        ]);

        let arrow_schema = ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("address", DataType::Struct(struct_fields.clone()), false),
        ]);

        // Create Iceberg schema with struct type and nested field IDs
        let address_struct = StructType::new(vec![
            NestedField::required(3, "street", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::required(4, "city", Type::Primitive(PrimitiveType::String)).into(),
        ]);

        let table_schema = Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::required(2, "address", Type::Struct(address_struct)).into(),
            ])
            .build()
            .unwrap();

        // Create partitioned spec on the nested city field using dot notation
        let partition_spec = iceberg::spec::PartitionSpec::builder(Arc::new(table_schema.clone()))
            .add_partition_field("address.city", "city_partition", Transform::Identity)
            .unwrap()
            .build()
            .unwrap();

        // Test schema creation - should add partition column for the nested field
        let output_schema =
            IcebergProjectExec::create_output_schema(&arrow_schema, &partition_spec, &table_schema)
                .unwrap();

        // Should have 3 fields: id, address, and partition values struct
        assert_eq!(output_schema.fields().len(), 3);
        assert_eq!(output_schema.field(0).name(), "id");
        assert_eq!(output_schema.field(1).name(), "address");
        assert_eq!(output_schema.field(2).name(), "_iceberg_partition_values");
    }

    #[test]
    fn test_process_batch_with_nested_struct_partition() {
        // Test processing actual data with partitioning on nested struct field
        let struct_fields = Fields::from(vec![
            Field::new("street", DataType::Utf8, false),
            Field::new("city", DataType::Utf8, false),
        ]);

        let arrow_schema = ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("address", DataType::Struct(struct_fields.clone()), false),
        ]);

        // Create Iceberg schema with struct type and nested field IDs
        let address_struct = StructType::new(vec![
            NestedField::required(3, "street", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::required(4, "city", Type::Primitive(PrimitiveType::String)).into(),
        ]);

        let table_schema = Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::required(2, "address", Type::Struct(address_struct)).into(),
            ])
            .build()
            .unwrap();

        // Create partitioned spec on the nested city field using dot notation
        let partition_spec = iceberg::spec::PartitionSpec::builder(Arc::new(table_schema.clone()))
            .add_partition_field("address.city", "city_partition", Transform::Identity)
            .unwrap()
            .build()
            .unwrap();

        // Create test data
        let id_array = Arc::new(Int32Array::from(vec![1, 2, 3]));

        // Create struct array for addresses
        let street_array = Arc::new(datafusion::arrow::array::StringArray::from(vec![
            "123 Main St",
            "456 Oak Ave",
            "789 Pine Rd",
        ]));
        let city_array = Arc::new(datafusion::arrow::array::StringArray::from(vec![
            "New York",
            "Los Angeles",
            "Chicago",
        ]));

        let struct_array = StructArray::from(vec![
            (
                Arc::new(Field::new("street", DataType::Utf8, false)),
                street_array as datafusion::arrow::array::ArrayRef,
            ),
            (
                Arc::new(Field::new("city", DataType::Utf8, false)),
                city_array as datafusion::arrow::array::ArrayRef,
            ),
        ]);

        let batch = RecordBatch::try_new(Arc::new(arrow_schema.clone()), vec![
            id_array,
            Arc::new(struct_array),
        ])
        .unwrap();

        // Create project exec
        let project_exec = IcebergProjectExec::new(
            Arc::new(datafusion::physical_plan::empty::EmptyExec::new(Arc::new(
                arrow_schema,
            ))),
            Arc::new(partition_spec),
            Arc::new(table_schema),
        )
        .unwrap();

        // Test processing the batch - this should extract city values from the struct
        let result_batch = project_exec.process_batch(batch).unwrap();

        // Verify the result
        assert_eq!(result_batch.num_columns(), 3); // id, address, partition_values
        assert_eq!(result_batch.num_rows(), 3);

        // Verify column names
        assert_eq!(result_batch.schema().field(0).name(), "id");
        assert_eq!(result_batch.schema().field(1).name(), "address");
        assert_eq!(
            result_batch.schema().field(2).name(),
            "_iceberg_partition_values"
        );

        // Verify that the partition values struct contains the city values extracted from the struct
        let partition_column = result_batch.column(2);
        let partition_struct_array = partition_column
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();

        // Get the city_partition field from the struct
        let city_partition_array = partition_struct_array
            .column_by_name("city_partition")
            .unwrap()
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .unwrap();

        assert_eq!(city_partition_array.value(0), "New York");
        assert_eq!(city_partition_array.value(1), "Los Angeles");
        assert_eq!(city_partition_array.value(2), "Chicago");
    }
}
