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

//! Partition value projection for Iceberg tables.

use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, RecordBatch, StructArray};
use datafusion::arrow::datatypes::{DataType, Schema as ArrowSchema};
use datafusion::common::Result as DFResult;
use datafusion::error::DataFusionError;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::{ColumnarValue, ExecutionPlan};
use iceberg::spec::{PartitionSpec, Schema};
use iceberg::table::Table;

use crate::to_datafusion_error;

/// Column name for the combined partition values struct
const PARTITION_VALUES_COLUMN: &str = "_partition";

/// Extends an ExecutionPlan with partition value calculations for Iceberg tables.
///
/// This function takes an input ExecutionPlan and extends it with an additional column
/// containing calculated partition values based on the table's partition specification.
/// For unpartitioned tables, returns the original plan unchanged.
///
/// # Arguments
/// * `input` - The input ExecutionPlan to extend
/// * `table` - The Iceberg table with partition specification
///
/// # Returns
/// * `Ok(Arc<dyn ExecutionPlan>)` - Extended plan with partition values column
/// * `Err` - If partition spec is not found or transformation fails
#[allow(dead_code)]
pub fn project_with_partition(
    input: Arc<dyn ExecutionPlan>,
    table: &Table,
) -> DFResult<Arc<dyn ExecutionPlan>> {
    let metadata = table.metadata();
    let partition_spec = metadata
        .partition_spec_by_id(metadata.default_partition_spec_id())
        .ok_or_else(|| DataFusionError::Internal("Default partition spec not found".to_string()))?;
    let table_schema = metadata.current_schema();

    if partition_spec.is_unpartitioned() {
        return Ok(input);
    }

    let input_schema = input.schema();
    let partition_type = build_partition_type(partition_spec, table_schema.as_ref())?;
    let calculator = PartitionValueCalculator::new(
        partition_spec.as_ref().clone(),
        table_schema.as_ref().clone(),
        partition_type,
    );

    let mut projection_exprs: Vec<(Arc<dyn PhysicalExpr>, String)> = Vec::new();

    for (index, field) in input_schema.fields().iter().enumerate() {
        let column_expr = Arc::new(Column::new(field.name(), index));
        projection_exprs.push((column_expr, field.name().clone()));
    }

    let partition_expr = Arc::new(PartitionExpr::new(calculator));
    projection_exprs.push((partition_expr, PARTITION_VALUES_COLUMN.to_string()));

    let projection = ProjectionExec::try_new(projection_exprs, input)?;
    Ok(Arc::new(projection))
}

/// PhysicalExpr implementation for partition value calculation
#[derive(Debug, Clone, PartialEq, Eq)]
struct PartitionExpr {
    calculator: PartitionValueCalculator,
}

impl PartitionExpr {
    fn new(calculator: PartitionValueCalculator) -> Self {
        Self { calculator }
    }
}

impl PhysicalExpr for PartitionExpr {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn data_type(&self, _input_schema: &ArrowSchema) -> DFResult<DataType> {
        Ok(self.calculator.partition_type.clone())
    }

    fn nullable(&self, _input_schema: &ArrowSchema) -> DFResult<bool> {
        Ok(false)
    }

    fn evaluate(&self, batch: &RecordBatch) -> DFResult<ColumnarValue> {
        let array = self.calculator.calculate(batch)?;
        Ok(ColumnarValue::Array(array))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> DFResult<Arc<dyn PhysicalExpr>> {
        Ok(self)
    }

    fn fmt_sql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "partition_values")
    }
}

impl std::fmt::Display for PartitionExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "partition_values")
    }
}

impl std::hash::Hash for PartitionExpr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        std::any::TypeId::of::<Self>().hash(state);
    }
}

/// Calculator for partition values in Iceberg tables
#[derive(Debug, Clone, PartialEq, Eq)]
struct PartitionValueCalculator {
    partition_spec: PartitionSpec,
    table_schema: Schema,
    partition_type: DataType,
}

impl PartitionValueCalculator {
    fn new(partition_spec: PartitionSpec, table_schema: Schema, partition_type: DataType) -> Self {
        Self {
            partition_spec,
            table_schema,
            partition_type,
        }
    }

    fn calculate(&self, batch: &RecordBatch) -> DFResult<ArrayRef> {
        if self.partition_spec.is_unpartitioned() {
            return Err(DataFusionError::Internal(
                "Cannot calculate partition values for unpartitioned table".to_string(),
            ));
        }

        let batch_schema = batch.schema();
        let mut partition_values = Vec::with_capacity(self.partition_spec.fields().len());

        let expected_struct_fields = match &self.partition_type {
            DataType::Struct(fields) => fields.clone(),
            _ => {
                return Err(DataFusionError::Internal(
                    "Expected partition type must be a struct".to_string(),
                ));
            }
        };

        for pf in self.partition_spec.fields() {
            let source_field = self.table_schema.field_by_id(pf.source_id).ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "Source field not found with id {} when calculating partition values",
                    pf.source_id
                ))
            })?;

            let field_path = find_field_path(&self.table_schema, source_field.id)?;
            let index_path = resolve_arrow_index_path(batch_schema.as_ref(), &field_path)?;

            let source_column = extract_column_by_index_path(batch, &index_path)?;

            let transform_fn = iceberg::transform::create_transform_function(&pf.transform)
                .map_err(to_datafusion_error)?;
            let partition_value = transform_fn
                .transform(source_column)
                .map_err(to_datafusion_error)?;

            partition_values.push(partition_value);
        }

        let struct_array = StructArray::try_new(
            expected_struct_fields,
            partition_values,
            None, // No null buffer for the struct array itself
        )
        .map_err(|e| DataFusionError::ArrowError(e, None))?;

        Ok(Arc::new(struct_array))
    }
}

fn extract_column_by_index_path(batch: &RecordBatch, index_path: &[usize]) -> DFResult<ArrayRef> {
    if index_path.is_empty() {
        return Err(DataFusionError::Internal(
            "Empty index path when extracting partition column".to_string(),
        ));
    }

    let mut current_column = batch.column(*index_path.first().unwrap()).clone();
    for child_index in &index_path[1..] {
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

fn build_partition_type(
    partition_spec: &PartitionSpec,
    table_schema: &Schema,
) -> DFResult<DataType> {
    let partition_struct_type = partition_spec
        .partition_type(table_schema)
        .map_err(to_datafusion_error)?;

    iceberg::arrow::type_to_arrow_type(&iceberg::spec::Type::Struct(partition_struct_type))
        .map_err(to_datafusion_error)
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::Int32Array;
    use datafusion::arrow::datatypes::{Field, Fields};
    use datafusion::physical_plan::empty::EmptyExec;
    use iceberg::spec::{NestedField, PrimitiveType, StructType, Transform, Type};

    use super::*;

    #[test]
    fn test_partition_calculator_basic() {
        let table_schema = Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::required(2, "name", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
            .unwrap();

        let partition_spec = iceberg::spec::PartitionSpec::builder(Arc::new(table_schema.clone()))
            .add_partition_field("id", "id_partition", Transform::Identity)
            .unwrap()
            .build()
            .unwrap();

        let partition_type = build_partition_type(&partition_spec, &table_schema).unwrap();
        let calculator = PartitionValueCalculator::new(
            partition_spec.clone(),
            table_schema,
            partition_type.clone(),
        );

        assert_eq!(calculator.partition_type, partition_type);
    }

    #[test]
    fn test_partition_expr_with_projection() {
        let table_schema = Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::required(2, "name", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
            .unwrap();

        let partition_spec = iceberg::spec::PartitionSpec::builder(Arc::new(table_schema.clone()))
            .add_partition_field("id", "id_partition", Transform::Identity)
            .unwrap()
            .build()
            .unwrap();

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let input = Arc::new(EmptyExec::new(arrow_schema.clone()));

        let partition_type = build_partition_type(&partition_spec, &table_schema).unwrap();
        let calculator =
            PartitionValueCalculator::new(partition_spec, table_schema, partition_type);

        let mut projection_exprs: Vec<(Arc<dyn PhysicalExpr>, String)> = Vec::new();
        for (i, field) in arrow_schema.fields().iter().enumerate() {
            let column_expr = Arc::new(Column::new(field.name(), i));
            projection_exprs.push((column_expr, field.name().clone()));
        }

        let partition_expr = Arc::new(PartitionExpr::new(calculator));
        projection_exprs.push((partition_expr, PARTITION_VALUES_COLUMN.to_string()));

        let projection = ProjectionExec::try_new(projection_exprs, input).unwrap();
        let result = Arc::new(projection);

        assert_eq!(result.schema().fields().len(), 3);
        assert_eq!(result.schema().field(0).name(), "id");
        assert_eq!(result.schema().field(1).name(), "name");
        assert_eq!(result.schema().field(2).name(), "_partition");
    }

    #[test]
    fn test_partition_expr_evaluate() {
        let table_schema = Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::required(2, "data", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
            .unwrap();

        let partition_spec = iceberg::spec::PartitionSpec::builder(Arc::new(table_schema.clone()))
            .add_partition_field("id", "id_partition", Transform::Identity)
            .unwrap()
            .build()
            .unwrap();

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("data", DataType::Utf8, false),
        ]));

        let batch = RecordBatch::try_new(arrow_schema.clone(), vec![
            Arc::new(Int32Array::from(vec![10, 20, 30])),
            Arc::new(datafusion::arrow::array::StringArray::from(vec![
                "a", "b", "c",
            ])),
        ])
        .unwrap();

        let partition_type = build_partition_type(&partition_spec, &table_schema).unwrap();
        let calculator =
            PartitionValueCalculator::new(partition_spec, table_schema, partition_type.clone());
        let expr = PartitionExpr::new(calculator);

        assert_eq!(expr.data_type(&arrow_schema).unwrap(), partition_type);
        assert!(!expr.nullable(&arrow_schema).unwrap());

        let result = expr.evaluate(&batch).unwrap();
        match result {
            ColumnarValue::Array(array) => {
                let struct_array = array.as_any().downcast_ref::<StructArray>().unwrap();
                let id_partition = struct_array
                    .column_by_name("id_partition")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap();
                assert_eq!(id_partition.value(0), 10);
                assert_eq!(id_partition.value(1), 20);
                assert_eq!(id_partition.value(2), 30);
            }
            _ => panic!("Expected array result"),
        }
    }

    #[test]
    fn test_nested_partition() {
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

        let partition_spec = iceberg::spec::PartitionSpec::builder(Arc::new(table_schema.clone()))
            .add_partition_field("address.city", "city_partition", Transform::Identity)
            .unwrap()
            .build()
            .unwrap();

        let struct_fields = Fields::from(vec![
            Field::new("street", DataType::Utf8, false),
            Field::new("city", DataType::Utf8, false),
        ]);

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("address", DataType::Struct(struct_fields), false),
        ]));

        let street_array = Arc::new(datafusion::arrow::array::StringArray::from(vec![
            "123 Main St",
            "456 Oak Ave",
        ]));
        let city_array = Arc::new(datafusion::arrow::array::StringArray::from(vec![
            "New York",
            "Los Angeles",
        ]));

        let struct_array = StructArray::from(vec![
            (
                Arc::new(Field::new("street", DataType::Utf8, false)),
                street_array as ArrayRef,
            ),
            (
                Arc::new(Field::new("city", DataType::Utf8, false)),
                city_array as ArrayRef,
            ),
        ]);

        let batch = RecordBatch::try_new(arrow_schema.clone(), vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(struct_array),
        ])
        .unwrap();

        let partition_type = build_partition_type(&partition_spec, &table_schema).unwrap();
        let calculator =
            PartitionValueCalculator::new(partition_spec, table_schema, partition_type);
        let array = calculator.calculate(&batch).unwrap();

        let struct_array = array.as_any().downcast_ref::<StructArray>().unwrap();
        let city_partition = struct_array
            .column_by_name("city_partition")
            .unwrap()
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .unwrap();

        assert_eq!(city_partition.value(0), "New York");
        assert_eq!(city_partition.value(1), "Los Angeles");
    }
}
