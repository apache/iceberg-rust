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

use arrow_array::{ArrayRef, BooleanArray, RecordBatch, StructArray};
use arrow_row::{OwnedRow, RowConverter, SortField};
use arrow_schema::{DataType, SchemaRef as ArrowSchemaRef};
use arrow_select::filter::filter_record_batch;
use itertools::Itertools;
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

use super::arrow_struct_to_literal;
use super::record_batch_projector::RecordBatchProjector;
use crate::arrow::type_to_arrow_type;
use crate::spec::{Literal, PartitionSpecRef, SchemaRef, Struct, Type};
use crate::transform::{create_transform_function, BoxedTransformFunction};
use crate::{Error, ErrorKind, Result};

/// The splitter used to split the record batch into multiple record batches by the partition spec.
// # TODO
// Remove this after partition writer supported.
#[allow(dead_code)]
pub struct RecordBatchPartitionSplitter {
    schema: SchemaRef,
    partition_spec: PartitionSpecRef,
    projector: RecordBatchProjector,
    transform_functions: Vec<BoxedTransformFunction>,
    row_converter: RowConverter,
}

// # TODO
// Remove this after partition writer supported.
#[allow(dead_code)]
impl RecordBatchPartitionSplitter {
    pub fn new(
        input_schema: ArrowSchemaRef,
        iceberg_schema: SchemaRef,
        partition_spec: PartitionSpecRef,
    ) -> Result<Self> {
        let projector = RecordBatchProjector::new(
            input_schema,
            &partition_spec
                .fields()
                .iter()
                .map(|field| field.source_id)
                .collect::<Vec<_>>(),
            // The source columns, selected by ids, must be a primitive type and cannot be contained in a map or list, but may be nested in a struct.
            // ref: https://iceberg.apache.org/spec/#partitioning
            |field| {
                if !field.data_type().is_primitive() {
                    return Ok(None);
                }
                field
                    .metadata()
                    .get(PARQUET_FIELD_ID_META_KEY)
                    .map(|s| {
                        s.parse::<i64>()
                            .map_err(|e| Error::new(ErrorKind::Unexpected, e.to_string()))
                    })
                    .transpose()
            },
            |_| true,
        )?;
        let transform_functions = partition_spec
            .fields()
            .iter()
            .map(|field| create_transform_function(&field.transform))
            .collect::<Result<Vec<_>>>()?;
        let row_converter = RowConverter::new(
            projector
                .projected_schema_ref()
                .fields()
                .iter()
                .map(|field| SortField::new(field.data_type().clone()))
                .collect(),
        )?;
        Ok(Self {
            schema: iceberg_schema,
            partition_spec,
            projector,
            transform_functions,
            row_converter,
        })
    }

    /// Split the record batch into multiple record batches according to provided partition columns.
    pub fn split_by_partition(
        &self,
        batch: &RecordBatch,
        partition_columns: &[ArrayRef],
    ) -> Result<Vec<(OwnedRow, RecordBatch)>> {
        let rows = self
            .row_converter
            .convert_columns(partition_columns)
            .map_err(|e| Error::new(ErrorKind::DataInvalid, e.to_string()))?;

        // Group the batch by row value.
        let mut group_ids = HashMap::new();
        rows.into_iter().enumerate().for_each(|(row_id, row)| {
            group_ids.entry(row.owned()).or_insert(vec![]).push(row_id);
        });

        // Partition the batch with same partition partition_values
        let mut partition_batches = Vec::with_capacity(group_ids.len());
        for (row, row_ids) in group_ids.into_iter() {
            // generate the bool filter array from column_ids
            let filter_array: BooleanArray = {
                let mut filter = vec![false; batch.num_rows()];
                row_ids.into_iter().for_each(|row_id| {
                    filter[row_id] = true;
                });
                filter.into()
            };

            // filter the RecordBatch
            partition_batches.push((row, filter_record_batch(batch, &filter_array)?));
        }

        Ok(partition_batches)
    }

    /// Compute the partition for the record batch and split the record batch into multiple record batches by the partition spec.
    pub fn split(&self, batch: &RecordBatch) -> Result<Vec<(OwnedRow, RecordBatch)>> {
        // get array using partition spec
        let source_columns = self.projector.project_column(batch.columns())?;
        let partition_columns = source_columns
            .into_iter()
            .zip_eq(self.transform_functions.iter())
            .map(|(source_column, transform_function)| transform_function.transform(source_column))
            .collect::<Result<Vec<_>>>()?;

        self.split_by_partition(batch, &partition_columns)
    }

    /// Convert row back to iceberg value.
    pub fn convert_row(&self, rows: Vec<OwnedRow>) -> Result<Vec<Struct>> {
        let partition_type = self.partition_spec.partition_type(&self.schema)?;
        let partition_arrow_type = type_to_arrow_type(&Type::Struct(partition_type.clone()))?;
        let arrow_struct_array = {
            let partition_columns = self
                .row_converter
                .convert_rows(rows.iter().map(|row| row.row()))
                .map_err(|e| Error::new(ErrorKind::DataInvalid, format!("{e}")))?;
            let partition_arrow_fields = {
                let DataType::Struct(fields) = partition_arrow_type else {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        "The partition arrow type is not a struct type",
                    ));
                };
                fields
            };
            Arc::new(StructArray::try_new(
                partition_arrow_fields,
                partition_columns,
                None,
            )?) as ArrayRef
        };
        let struct_array = {
            let struct_array = arrow_struct_to_literal(&arrow_struct_array, &partition_type)?;
            struct_array
                .into_iter()
                .map(|s| {
                    if let Some(s) = s {
                        if let Literal::Struct(s) = s {
                            Ok(s)
                        } else {
                            Err(Error::new(
                                ErrorKind::DataInvalid,
                                "The struct is not a struct literal",
                            ))
                        }
                    } else {
                        Err(Error::new(ErrorKind::DataInvalid, "The struct is null"))
                    }
                })
                .collect::<Result<Vec<_>>>()?
        };

        Ok(struct_array)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{Int32Array, RecordBatch, StringArray};

    use super::*;
    use crate::arrow::schema_to_arrow_schema;
    use crate::spec::{
        NestedField, PartitionSpecBuilder, Schema, Transform, UnboundPartitionField,
    };

    #[test]
    fn test_record_batch_partition_split() {
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::required(
                        1,
                        "id",
                        Type::Primitive(crate::spec::PrimitiveType::Int),
                    )
                    .into(),
                    NestedField::required(
                        2,
                        "name",
                        Type::Primitive(crate::spec::PrimitiveType::String),
                    )
                    .into(),
                ])
                .build()
                .unwrap(),
        );
        let partition_spec = Arc::new(
            PartitionSpecBuilder::new(schema.clone())
                .with_spec_id(1)
                .add_unbound_field(UnboundPartitionField {
                    source_id: 1,
                    field_id: None,
                    name: "id_bucket".to_string(),
                    transform: Transform::Identity,
                })
                .unwrap()
                .build()
                .unwrap(),
        );
        let input_schema = Arc::new(schema_to_arrow_schema(&schema).unwrap());
        let partition_splitter =
            RecordBatchPartitionSplitter::new(input_schema.clone(), schema.clone(), partition_spec)
                .expect("Failed to create splitter");

        let id_array = Int32Array::from(vec![1, 2, 1, 3, 2, 3, 1]);
        let data_array = StringArray::from(vec!["a", "b", "c", "d", "e", "f", "g"]);
        let batch = RecordBatch::try_new(input_schema.clone(), vec![
            Arc::new(id_array),
            Arc::new(data_array),
        ])
        .expect("Failed to create RecordBatch");

        let mut partitioned_batches = partition_splitter
            .split(&batch)
            .expect("Failed to split RecordBatch");
        assert_eq!(partitioned_batches.len(), 3);
        partitioned_batches.sort_by_key(|(row, _)| row.clone());
        {
            // check the first partition
            let expected_id_array = Int32Array::from(vec![1, 1, 1]);
            let expected_data_array = StringArray::from(vec!["a", "c", "g"]);
            let expected_batch = RecordBatch::try_new(input_schema.clone(), vec![
                Arc::new(expected_id_array),
                Arc::new(expected_data_array),
            ])
            .expect("Failed to create expected RecordBatch");
            assert_eq!(partitioned_batches[0].1, expected_batch);
        }
        {
            // check the second partition
            let expected_id_array = Int32Array::from(vec![2, 2]);
            let expected_data_array = StringArray::from(vec!["b", "e"]);
            let expected_batch = RecordBatch::try_new(input_schema.clone(), vec![
                Arc::new(expected_id_array),
                Arc::new(expected_data_array),
            ])
            .expect("Failed to create expected RecordBatch");
            assert_eq!(partitioned_batches[1].1, expected_batch);
        }
        {
            // check the third partition
            let expected_id_array = Int32Array::from(vec![3, 3]);
            let expected_data_array = StringArray::from(vec!["d", "f"]);
            let expected_batch = RecordBatch::try_new(input_schema.clone(), vec![
                Arc::new(expected_id_array),
                Arc::new(expected_data_array),
            ])
            .expect("Failed to create expected RecordBatch");
            assert_eq!(partitioned_batches[2].1, expected_batch);
        }

        let partition_values = partition_splitter
            .convert_row(
                partitioned_batches
                    .iter()
                    .map(|(row, _)| row.clone())
                    .collect(),
            )
            .unwrap();
        // check partition value is struct(1), struct(2), struct(3)
        assert_eq!(partition_values, vec![
            Struct::from_iter(vec![Some(Literal::int(1))]),
            Struct::from_iter(vec![Some(Literal::int(2))]),
            Struct::from_iter(vec![Some(Literal::int(3))]),
        ]);
    }

    #[test]
    fn test_record_batch_partition_split_by_partition() {
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::required(
                        1,
                        "id",
                        Type::Primitive(crate::spec::PrimitiveType::Int),
                    )
                    .into(),
                    NestedField::required(
                        2,
                        "name",
                        Type::Primitive(crate::spec::PrimitiveType::String),
                    )
                    .into(),
                ])
                .build()
                .unwrap(),
        );
        let partition_spec = Arc::new(
            PartitionSpecBuilder::new(schema.clone())
                .with_spec_id(1)
                .add_unbound_field(UnboundPartitionField {
                    source_id: 1,
                    field_id: None,
                    name: "id_bucket".to_string(),
                    transform: Transform::Identity,
                })
                .unwrap()
                .build()
                .unwrap(),
        );
        let input_schema = Arc::new(schema_to_arrow_schema(&schema).unwrap());
        let partition_splitter = RecordBatchPartitionSplitter::new(
            input_schema.clone(),
            schema.clone(),
            partition_spec.clone(),
        )
        .expect("Failed to create splitter");

        // Prepare data
        let id_array = Int32Array::from(vec![1, 2, 1, 3, 2, 3, 1]);
        let data_array = StringArray::from(vec!["a", "b", "c", "d", "e", "f", "g"]);
        let batch = RecordBatch::try_new(input_schema.clone(), vec![
            Arc::new(id_array),
            Arc::new(data_array),
        ])
        .expect("Failed to create RecordBatch");
        // Prepare partition columns
        let id_bucket_array = Arc::new(Int32Array::from(vec![1, 1, 2, 2, 3, 3, 1]));

        let mut partitioned_batches = partition_splitter
            .split_by_partition(&batch, &[id_bucket_array])
            .expect("Failed to split RecordBatch");
        assert_eq!(partitioned_batches.len(), 3);
        partitioned_batches.sort_by_key(|(row, _)| row.clone());
        {
            // check the first partition
            let expected_id_array = Int32Array::from(vec![1, 2, 1]);
            let expected_data_array = StringArray::from(vec!["a", "b", "g"]);
            let expected_batch = RecordBatch::try_new(input_schema.clone(), vec![
                Arc::new(expected_id_array),
                Arc::new(expected_data_array),
            ])
            .expect("Failed to create expected RecordBatch");
            assert_eq!(partitioned_batches[0].1, expected_batch);
        }
        {
            // check the second partition
            let expected_id_array = Int32Array::from(vec![1, 3]);
            let expected_data_array = StringArray::from(vec!["c", "d"]);
            let expected_batch = RecordBatch::try_new(input_schema.clone(), vec![
                Arc::new(expected_id_array),
                Arc::new(expected_data_array),
            ])
            .expect("Failed to create expected RecordBatch");
            assert_eq!(partitioned_batches[1].1, expected_batch);
        }
        {
            // check the third partition
            let expected_id_array = Int32Array::from(vec![2, 3]);
            let expected_data_array = StringArray::from(vec!["e", "f"]);
            let expected_batch = RecordBatch::try_new(input_schema.clone(), vec![
                Arc::new(expected_id_array),
                Arc::new(expected_data_array),
            ])
            .expect("Failed to create expected RecordBatch");
            assert_eq!(partitioned_batches[2].1, expected_batch);
        }

        let partition_values = partition_splitter
            .convert_row(
                partitioned_batches
                    .iter()
                    .map(|(row, _)| row.clone())
                    .collect(),
            )
            .unwrap();
        // check partition value is struct(1), struct(2), struct(3)
        assert_eq!(partition_values, vec![
            Struct::from_iter(vec![Some(Literal::int(1))]),
            Struct::from_iter(vec![Some(Literal::int(2))]),
            Struct::from_iter(vec![Some(Literal::int(3))]),
        ]);
    }
}
