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

//! Utilities for calculating partition values for Iceberg tables.
//!
//! This module provides functions to calculate partition values from record batches
//! based on Iceberg partition specifications. These utilities are used when writing
//! data to partitioned Iceberg tables.

use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, RecordBatch, StructArray};
use datafusion::arrow::datatypes::{
    DataType, Field, Schema as ArrowSchema, SchemaRef as ArrowSchemaRef,
};
use datafusion::common::Result as DFResult;
use datafusion::error::DataFusionError;
use iceberg::spec::{PartitionSpec, Schema};

use crate::to_datafusion_error;

/// Column name for the combined partition values struct
#[allow(dead_code)]
pub(crate) const PARTITION_VALUES_COLUMN: &str = "_iceberg_partition_values";

/// Create an output schema by adding a single partition values struct column to the input schema.
/// Returns the original schema unchanged if the table is unpartitioned.
#[allow(dead_code)]
pub(crate) fn create_schema_with_partition_columns(
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

    let arrow_struct_type =
        iceberg::arrow::type_to_arrow_type(&iceberg::spec::Type::Struct(partition_struct_type))
            .map_err(to_datafusion_error)?;

    fields.push(Arc::new(Field::new(
        PARTITION_VALUES_COLUMN,
        arrow_struct_type,
        false, // Partition values are generally not null
    )));

    Ok(Arc::new(ArrowSchema::new(fields)))
}

/// Calculate partition values for a record batch and return as a single struct array.
/// Returns None if the table is unpartitioned.
///
/// # Arguments
/// * `batch` - The record batch to calculate partition values for
/// * `partition_spec` - The partition specification defining the partition fields
/// * `table_schema` - The Iceberg table schema
/// * `expected_partition_type` - The expected Arrow struct type for the partition values
#[allow(dead_code)]
pub(crate) fn calculate_partition_values(
    batch: &RecordBatch,
    partition_spec: &PartitionSpec,
    table_schema: &Schema,
    expected_partition_type: &DataType,
) -> DFResult<Option<ArrayRef>> {
    if partition_spec.is_unpartitioned() {
        return Ok(None);
    }

    let batch_schema = batch.schema();
    let mut partition_values = Vec::with_capacity(partition_spec.fields().len());

    let expected_struct_fields = match expected_partition_type {
        DataType::Struct(fields) => fields.clone(),
        _ => {
            return Err(DataFusionError::Internal(
                "Expected partition type must be a struct".to_string(),
            ));
        }
    };

    for pf in partition_spec.fields() {
        let source_field = table_schema.field_by_id(pf.source_id).ok_or_else(|| {
            DataFusionError::Internal(format!(
                "Source field not found with id {} when calculating partition values",
                pf.source_id
            ))
        })?;

        let field_path = find_field_path(table_schema, source_field.id)?;
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

    Ok(Some(Arc::new(struct_array)))
}

/// Extract a column from a record batch by following an index path.
/// The index path specifies the column indices to traverse for nested structures.
#[allow(dead_code)]
fn extract_column_by_index_path(batch: &RecordBatch, index_path: &[usize]) -> DFResult<ArrayRef> {
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
#[allow(dead_code)]
fn find_field_path(table_schema: &Schema, field_id: i32) -> DFResult<Vec<String>> {
    let dotted = table_schema.name_by_field_id(field_id).ok_or_else(|| {
        DataFusionError::Internal(format!(
            "Field with ID {} not found in schema when building field path for partition column",
            field_id
        ))
    })?;
    Ok(dotted.split('.').map(|s| s.to_string()).collect())
}

/// Resolve a field path to an index path in an Arrow schema
#[allow(dead_code)]
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

/// Add partition values struct column to a record batch.
/// Returns the original batch unchanged if the partition spec is unpartitioned.
#[allow(dead_code)]
pub(crate) fn add_partition_columns_to_batch(
    batch: RecordBatch,
    partition_values: Option<ArrayRef>,
    output_schema: ArrowSchemaRef,
) -> DFResult<RecordBatch> {
    if let Some(partition_array) = partition_values {
        let mut all_columns = batch.columns().to_vec();
        all_columns.push(partition_array);
        RecordBatch::try_new(output_schema, all_columns)
            .map_err(|e| DataFusionError::ArrowError(e, None))
    } else {
        Ok(batch)
    }
}

/// Get the expected Arrow DataType for partition values based on the partition spec
#[allow(dead_code)]
pub(crate) fn get_partition_struct_type(
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
    use std::collections::HashMap;

    use datafusion::arrow::array::{Int32Array, RecordBatch, StructArray};
    use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema as ArrowSchema};
    use iceberg::spec::{NestedField, PrimitiveType, Schema, StructType, Transform, Type};
    use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

    use super::*;

    #[test]
    fn test_create_output_schema_unpartitioned() {
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

        let partition_spec = iceberg::spec::PartitionSpec::unpartition_spec();

        let output_schema =
            create_schema_with_partition_columns(&arrow_schema, &partition_spec, &table_schema)
                .unwrap();

        assert_eq!(output_schema.fields().len(), 2);
        assert_eq!(output_schema.field(0).name(), "id");
        assert_eq!(output_schema.field(1).name(), "name");
    }

    #[test]
    fn test_create_output_schema_partitioned() {
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

        let partition_spec = iceberg::spec::PartitionSpec::builder(Arc::new(table_schema.clone()))
            .add_partition_field("id", "id_partition", Transform::Identity)
            .unwrap()
            .build()
            .unwrap();

        let output_schema =
            create_schema_with_partition_columns(&arrow_schema, &partition_spec, &table_schema)
                .unwrap();

        assert_eq!(output_schema.fields().len(), 3);
        assert_eq!(output_schema.field(0).name(), "id");
        assert_eq!(output_schema.field(1).name(), "name");
        assert_eq!(output_schema.field(2).name(), "_iceberg_partition_values");
    }

    #[test]
    fn test_partition_on_struct_field() {
        let struct_fields = Fields::from(vec![
            Field::new("street", DataType::Utf8, false),
            Field::new("city", DataType::Utf8, false),
        ]);

        let arrow_schema = ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("address", DataType::Struct(struct_fields.clone()), false),
        ]);

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

        let output_schema =
            create_schema_with_partition_columns(&arrow_schema, &partition_spec, &table_schema)
                .unwrap();

        assert_eq!(output_schema.fields().len(), 3);
        assert_eq!(output_schema.field(0).name(), "id");
        assert_eq!(output_schema.field(1).name(), "address");
        assert_eq!(output_schema.field(2).name(), "_iceberg_partition_values");
    }

    #[test]
    fn test_process_batch_with_nested_struct_partition() {
        let struct_fields = Fields::from(vec![
            Field::new("street", DataType::Utf8, false),
            Field::new("city", DataType::Utf8, false),
        ]);

        let arrow_schema = ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("address", DataType::Struct(struct_fields.clone()), false),
        ]);

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

        let id_array = Arc::new(Int32Array::from(vec![1, 2, 3]));

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

        let output_schema =
            create_schema_with_partition_columns(&arrow_schema, &partition_spec, &table_schema)
                .unwrap();

        let partition_type = get_partition_struct_type(&partition_spec, &table_schema).unwrap();
        let partition_values =
            calculate_partition_values(&batch, &partition_spec, &table_schema, &partition_type)
                .unwrap();
        let result_batch =
            add_partition_columns_to_batch(batch, partition_values, output_schema).unwrap();

        assert_eq!(result_batch.num_columns(), 3); // id, address, partition_values
        assert_eq!(result_batch.num_rows(), 3);
        assert_eq!(result_batch.schema().field(0).name(), "id");
        assert_eq!(result_batch.schema().field(1).name(), "address");
        assert_eq!(
            result_batch.schema().field(2).name(),
            "_iceberg_partition_values"
        );

        let partition_column = result_batch.column(2);
        let partition_struct_array = partition_column
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();

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
