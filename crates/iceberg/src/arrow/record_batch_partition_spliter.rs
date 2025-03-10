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
use arrow_schema::{DataType, Schema as ArrowSchema};
use arrow_select::filter::filter_record_batch;
use itertools::Itertools;
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

use super::record_batch_projector::RecordBatchProjector;
use crate::arrow::{arrow_struct_to_literal, type_to_arrow_type};
use crate::spec::{Literal, PartitionSpecRef, SchemaRef, Struct, StructType, Type};
use crate::transform::{create_transform_function, BoxedTransformFunction};
use crate::{Error, ErrorKind, Result};

/// A helper function to split the record batch into multiple record batches using computed partition columns.
pub(crate) fn split_with_partition(
    row_converter: &RowConverter,
    partition_columns: &[ArrayRef],
    batch: &RecordBatch,
) -> Result<Vec<(OwnedRow, RecordBatch)>> {
    let rows = row_converter
        .convert_columns(partition_columns)
        .map_err(|e| Error::new(ErrorKind::DataInvalid, format!("{}", e)))?;

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
        partition_batches.push((
            row,
            filter_record_batch(batch, &filter_array)
                .expect("We should guarantee the filter array is valid"),
        ));
    }

    Ok(partition_batches)
}

pub(crate) fn convert_row_to_struct(
    row_converter: &RowConverter,
    struct_type: &StructType,
    rows: Vec<OwnedRow>,
) -> Result<Vec<Struct>> {
    let arrow_struct_array = {
        let partition_columns = row_converter
            .convert_rows(rows.iter().map(|row| row.row()))
            .map_err(|e| Error::new(ErrorKind::DataInvalid, format!("{e}")))?;
        let partition_arrow_fields = {
            let partition_arrow_type = type_to_arrow_type(&Type::Struct(struct_type.clone()))?;
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
        let struct_array = arrow_struct_to_literal(&arrow_struct_array, struct_type)?;
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

/// The spliter used to split the record batch into multiple record batches by the partition spec.
pub(crate) struct RecordBatchPartitionSpliter {
    partition_spec: PartitionSpecRef,
    schema: SchemaRef,
    projector: RecordBatchProjector,
    transform_functions: Vec<BoxedTransformFunction>,
    row_converter: RowConverter,
}

impl RecordBatchPartitionSpliter {
    pub(crate) fn new(
        arrow_schema: &ArrowSchema,
        table_schema: SchemaRef,
        partition_spec: PartitionSpecRef,
    ) -> Result<Self> {
        if partition_spec.fields().is_empty() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Fail to create partition spliter using empty partition spec",
            ));
        }
        let projector = RecordBatchProjector::new(
            arrow_schema,
            &partition_spec
                .fields()
                .iter()
                .map(|field| field.source_id)
                .collect::<Vec<_>>(),
            // The source columns, selected by ids, must be a primitive type and cannot be contained in a map or list, but may be nested in a struct.
            // ref: https://iceberg.apache.org/spec/#partitioning
            |field| {
                if field.data_type().is_nested() {
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
            partition_spec
                .partition_type(&table_schema)?
                .fields()
                .iter()
                .map(|f| Ok(SortField::new(type_to_arrow_type(&f.field_type)?)))
                .collect::<Result<Vec<_>>>()?,
        )?;
        Ok(Self {
            partition_spec,
            schema: table_schema,
            projector,
            transform_functions,
            row_converter,
        })
    }

    /// Split the record batch into multiple record batches by the partition spec.
    pub(crate) fn split(&self, batch: &RecordBatch) -> Result<Vec<(OwnedRow, RecordBatch)>> {
        // get array using partition spec
        let source_columns = self.projector.project_column(batch.columns())?;
        let partition_columns = source_columns
            .into_iter()
            .zip_eq(self.transform_functions.iter())
            .map(|(source_column, transform_function)| transform_function.transform(source_column))
            .collect::<Result<Vec<_>>>()?;

        split_with_partition(&self.row_converter, &partition_columns, batch)
    }

    pub(crate) fn convert_row(&self, rows: Vec<OwnedRow>) -> Result<Vec<Struct>> {
        convert_row_to_struct(
            &self.row_converter,
            &self.partition_spec.partition_type(&self.schema)?,
            rows,
        )
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{Int32Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema as ArrowSchema};

    use super::*;
    use crate::arrow::schema_to_arrow_schema;
    use crate::spec::{NestedField, PartitionSpec, Schema, Transform, UnboundPartitionField};

    #[test]
    fn test_record_batch_partition_spliter() {
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(crate::spec::PrimitiveType::Int))
                    .into(),
                NestedField::required(
                    2,
                    "name",
                    Type::Primitive(crate::spec::PrimitiveType::String),
                )
                .into(),
            ])
            .build()
            .unwrap();
        let partition_spec = PartitionSpec::builder(schema.clone())
            .with_spec_id(1)
            .add_unbound_field(UnboundPartitionField {
                source_id: 1,
                field_id: None,
                name: "id_bucket".to_string(),
                transform: Transform::Identity,
            })
            .unwrap()
            .build()
            .unwrap();
        let partition_spliter = RecordBatchPartitionSpliter::new(
            &schema_to_arrow_schema(&schema).unwrap(),
            Arc::new(schema),
            Arc::new(partition_spec),
        )
        .expect("Failed to create spliter");

        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("data", DataType::Utf8, false),
        ]));
        let id_array = Int32Array::from(vec![1, 2, 1, 3, 2, 3, 1]);
        let data_array = StringArray::from(vec!["a", "b", "c", "d", "e", "f", "g"]);
        let batch = RecordBatch::try_new(schema.clone(), vec![
            Arc::new(id_array),
            Arc::new(data_array),
        ])
        .expect("Failed to create RecordBatch");

        let mut partitioned_batches = partition_spliter
            .split(&batch)
            .expect("Failed to split RecordBatch");
        assert_eq!(partitioned_batches.len(), 3);
        partitioned_batches.sort_by_key(|(row, _)| row.clone());
        {
            // check the first partition
            let expected_id_array = Int32Array::from(vec![1, 1, 1]);
            let expected_data_array = StringArray::from(vec!["a", "c", "g"]);
            let expected_batch = RecordBatch::try_new(schema.clone(), vec![
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
            let expected_batch = RecordBatch::try_new(schema.clone(), vec![
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
            let expected_batch = RecordBatch::try_new(schema.clone(), vec![
                Arc::new(expected_id_array),
                Arc::new(expected_data_array),
            ])
            .expect("Failed to create expected RecordBatch");
            assert_eq!(partitioned_batches[2].1, expected_batch);
        }

        let partition_values = partition_spliter
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
    fn test_record_batch_partition_spliter_with_extra_columns() {
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(crate::spec::PrimitiveType::Int))
                    .into(),
                NestedField::required(
                    2,
                    "name",
                    Type::Primitive(crate::spec::PrimitiveType::String),
                )
                .into(),
            ])
            .build()
            .unwrap();
        let partition_spec = PartitionSpec::builder(schema.clone())
            .with_spec_id(1)
            .add_unbound_field(UnboundPartitionField {
                source_id: 1,
                field_id: None,
                name: "id_bucket".to_string(),
                transform: Transform::Identity,
            })
            .unwrap()
            .build()
            .unwrap();

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("extra_column1", DataType::Utf8, true),
            Field::new("id", DataType::Int32, true).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "1".to_string(),
            )])),
            Field::new("data", DataType::Utf8, true).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "2".to_string(),
            )])),
            Field::new("extra_column2", DataType::Utf8, true),
        ]));
        let extra_column1_array = StringArray::from(vec![
            "extra1", "extra2", "extra1", "extra3", "extra2", "extra3", "extra1",
        ]);
        let id_array = Int32Array::from(vec![1, 2, 1, 3, 2, 3, 1]);
        let data_array = StringArray::from(vec!["a", "b", "c", "d", "e", "f", "g"]);
        let extra_column2_array = StringArray::from(vec![
            "extra1", "extra2", "extra1", "extra3", "extra2", "extra3", "extra1",
        ]);
        let batch = RecordBatch::try_new(arrow_schema.clone(), vec![
            Arc::new(extra_column1_array),
            Arc::new(id_array),
            Arc::new(data_array),
            Arc::new(extra_column2_array),
        ])
        .expect("Failed to create RecordBatch");
        let partition_spliter = RecordBatchPartitionSpliter::new(
            &arrow_schema,
            Arc::new(schema),
            Arc::new(partition_spec),
        )
        .expect("Failed to create spliter");

        let mut partitioned_batches = partition_spliter
            .split(&batch)
            .expect("Failed to split RecordBatch");
        assert_eq!(partitioned_batches.len(), 3);
        partitioned_batches.sort_by_key(|(row, _)| row.clone());
        {
            // check the first partition
            let expected_extra_column1_array =
                StringArray::from(vec!["extra1", "extra1", "extra1"]);
            let expected_id_array = Int32Array::from(vec![1, 1, 1]);
            let expected_data_array = StringArray::from(vec!["a", "c", "g"]);
            let expected_extra_column2_array =
                StringArray::from(vec!["extra1", "extra1", "extra1"]);
            let expected_batch = RecordBatch::try_new(arrow_schema.clone(), vec![
                Arc::new(expected_extra_column1_array),
                Arc::new(expected_id_array),
                Arc::new(expected_data_array),
                Arc::new(expected_extra_column2_array),
            ])
            .expect("Failed to create expected RecordBatch");
            assert_eq!(partitioned_batches[0].1, expected_batch);
        }
        {
            // check the second partition
            let expected_extra_column1_array = StringArray::from(vec!["extra2", "extra2"]);
            let expected_id_array = Int32Array::from(vec![2, 2]);
            let expected_data_array = StringArray::from(vec!["b", "e"]);
            let expected_extra_column2_array = StringArray::from(vec!["extra2", "extra2"]);
            let expected_batch = RecordBatch::try_new(arrow_schema.clone(), vec![
                Arc::new(expected_extra_column1_array),
                Arc::new(expected_id_array),
                Arc::new(expected_data_array),
                Arc::new(expected_extra_column2_array),
            ])
            .expect("Failed to create expected RecordBatch");
            assert_eq!(partitioned_batches[1].1, expected_batch);
        }
        {
            // check the third partition
            let expected_id_array = Int32Array::from(vec![3, 3]);
            let expected_data_array = StringArray::from(vec!["d", "f"]);
            let expected_extra_column1_array = StringArray::from(vec!["extra3", "extra3"]);
            let expected_extra_column2_array = StringArray::from(vec!["extra3", "extra3"]);
            let expected_batch = RecordBatch::try_new(arrow_schema.clone(), vec![
                Arc::new(expected_extra_column1_array),
                Arc::new(expected_id_array),
                Arc::new(expected_data_array),
                Arc::new(expected_extra_column2_array),
            ])
            .expect("Failed to create expected RecordBatch");
            assert_eq!(partitioned_batches[2].1, expected_batch);
        }

        let partition_values = partition_spliter
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
    fn test_empty_partition() {
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(crate::spec::PrimitiveType::Int))
                    .into(),
                NestedField::required(
                    2,
                    "name",
                    Type::Primitive(crate::spec::PrimitiveType::String),
                )
                .into(),
            ])
            .build()
            .unwrap();
        let partition_spec = PartitionSpec::builder(schema.clone())
            .with_spec_id(1)
            .build()
            .unwrap();
        assert!(RecordBatchPartitionSpliter::new(
            &schema_to_arrow_schema(&schema).unwrap(),
            Arc::new(schema),
            Arc::new(partition_spec),
        )
        .is_err())
    }
}
