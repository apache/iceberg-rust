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

use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch, StructArray};
use arrow_schema::{DataType, Field, FieldRef, Fields, Schema, SchemaRef};

use crate::error::Result;
use crate::{Error, ErrorKind};

/// Help to project specific field from `RecordBatch`` according to the fields id.
#[derive(Clone, Debug)]
pub(crate) struct RecordBatchProjector {
    // A vector of vectors, where each inner vector represents the index path to access a specific field in a nested structure.
    // E.g. [[0], [1, 2]] means the first field is accessed directly from the first column,
    // while the second field is accessed from the second column and then from its third subcolumn (second column must be a struct column).
    field_indices: Vec<Vec<usize>>,
    // The schema reference after projection. This schema is derived from the original schema based on the given field IDs.
    projected_schema: SchemaRef,
}

impl RecordBatchProjector {
    /// Init ArrowFieldProjector
    ///
    /// This function will iterate through the field and fetch the field from the original schema according to the field ids.
    /// The function to fetch the field id from the field is provided by `field_id_fetch_func`, return None if the field need to be skipped.
    /// This function will iterate through the nested fields if the field is a struct, `searchable_field_func` can be used to control whether
    /// iterate into the nested fields.
    pub(crate) fn new<F1, F2>(
        original_schema: SchemaRef,
        field_ids: &[i32],
        field_id_fetch_func: F1,
        searchable_field_func: F2,
    ) -> Result<Self>
    where
        F1: Fn(&Field) -> Result<Option<i64>>,
        F2: Fn(&Field) -> bool,
    {
        let mut field_indices = Vec::with_capacity(field_ids.len());
        let mut fields = Vec::with_capacity(field_ids.len());
        for &id in field_ids {
            let mut field_index = vec![];
            let field = Self::fetch_field_index(
                original_schema.fields(),
                &mut field_index,
                id as i64,
                &field_id_fetch_func,
                &searchable_field_func,
            )?
            .ok_or_else(|| {
                Error::new(ErrorKind::Unexpected, "Field not found")
                    .with_context("field_id", id.to_string())
            })?;
            fields.push(field.clone());
            field_indices.push(field_index);
        }
        let delete_arrow_schema = Arc::new(Schema::new(fields));
        Ok(Self {
            field_indices,
            projected_schema: delete_arrow_schema,
        })
    }

    fn fetch_field_index<F1, F2>(
        fields: &Fields,
        index_vec: &mut Vec<usize>,
        target_field_id: i64,
        field_id_fetch_func: &F1,
        searchable_field_func: &F2,
    ) -> Result<Option<FieldRef>>
    where
        F1: Fn(&Field) -> Result<Option<i64>>,
        F2: Fn(&Field) -> bool,
    {
        for (pos, field) in fields.iter().enumerate() {
            let id = field_id_fetch_func(field)?;
            if let Some(id) = id {
                if target_field_id == id {
                    index_vec.push(pos);
                    return Ok(Some(field.clone()));
                }
            }
            if let DataType::Struct(inner) = field.data_type() {
                if searchable_field_func(field) {
                    if let Some(res) = Self::fetch_field_index(
                        inner,
                        index_vec,
                        target_field_id,
                        field_id_fetch_func,
                        searchable_field_func,
                    )? {
                        index_vec.push(pos);
                        return Ok(Some(res));
                    }
                }
            }
        }
        Ok(None)
    }

    /// Return the reference of projected schema
    pub(crate) fn projected_schema_ref(&self) -> &SchemaRef {
        &self.projected_schema
    }

    /// Do projection with record batch
    pub(crate) fn project_bacth(&self, batch: RecordBatch) -> Result<RecordBatch> {
        RecordBatch::try_new(
            self.projected_schema.clone(),
            self.project_column(batch.columns())?,
        )
        .map_err(|err| Error::new(ErrorKind::DataInvalid, format!("{err}")))
    }

    /// Do projection with columns
    pub(crate) fn project_column(&self, batch: &[ArrayRef]) -> Result<Vec<ArrayRef>> {
        self.field_indices
            .iter()
            .map(|index_vec| Self::get_column_by_field_index(batch, index_vec))
            .collect::<Result<Vec<_>>>()
    }

    fn get_column_by_field_index(batch: &[ArrayRef], field_index: &[usize]) -> Result<ArrayRef> {
        let mut rev_iterator = field_index.iter().rev();
        let mut array = batch[*rev_iterator.next().unwrap()].clone();
        for idx in rev_iterator {
            array = array
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or(Error::new(
                    ErrorKind::Unexpected,
                    "Cannot convert Array to StructArray",
                ))?
                .column(*idx)
                .clone();
        }
        Ok(array)
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow_array::{ArrayRef, Int32Array, RecordBatch, StringArray, StructArray};
    use arrow_schema::{DataType, Field, Fields, Schema};

    use crate::arrow::record_batch_projector::RecordBatchProjector;
    use crate::{Error, ErrorKind};

    #[test]
    fn test_record_batch_projector_nested_level() {
        let inner_fields = vec![
            Field::new("inner_field1", DataType::Int32, false),
            Field::new("inner_field2", DataType::Utf8, false),
        ];
        let fields = vec![
            Field::new("field1", DataType::Int32, false),
            Field::new(
                "field2",
                DataType::Struct(Fields::from(inner_fields.clone())),
                false,
            ),
        ];
        let schema = Arc::new(Schema::new(fields));

        let field_id_fetch_func = |field: &Field| match field.name().as_str() {
            "field1" => Ok(Some(1)),
            "field2" => Ok(Some(2)),
            "inner_field1" => Ok(Some(3)),
            "inner_field2" => Ok(Some(4)),
            _ => Err(Error::new(ErrorKind::Unexpected, "Field id not found")),
        };
        let projector =
            RecordBatchProjector::new(schema.clone(), &[1, 3], field_id_fetch_func, |_| true)
                .unwrap();

        assert!(projector.field_indices.len() == 2);
        assert_eq!(projector.field_indices[0], vec![0]);
        assert_eq!(projector.field_indices[1], vec![0, 1]);

        let int_array = Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef;
        let inner_int_array = Arc::new(Int32Array::from(vec![4, 5, 6])) as ArrayRef;
        let inner_string_array = Arc::new(StringArray::from(vec!["x", "y", "z"])) as ArrayRef;
        let struct_array = Arc::new(StructArray::from(vec![
            (
                Arc::new(inner_fields[0].clone()),
                inner_int_array as ArrayRef,
            ),
            (
                Arc::new(inner_fields[1].clone()),
                inner_string_array as ArrayRef,
            ),
        ])) as ArrayRef;
        let batch = RecordBatch::try_new(schema, vec![int_array, struct_array]).unwrap();

        let projected_batch = projector.project_bacth(batch).unwrap();
        assert_eq!(projected_batch.num_columns(), 2);
        let projected_int_array = projected_batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let projected_inner_int_array = projected_batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();

        assert_eq!(projected_int_array.values(), &[1, 2, 3]);
        assert_eq!(projected_inner_int_array.values(), &[4, 5, 6]);
    }

    #[test]
    fn test_field_not_found() {
        let inner_fields = vec![
            Field::new("inner_field1", DataType::Int32, false),
            Field::new("inner_field2", DataType::Utf8, false),
        ];

        let fields = vec![
            Field::new("field1", DataType::Int32, false),
            Field::new(
                "field2",
                DataType::Struct(Fields::from(inner_fields.clone())),
                false,
            ),
        ];
        let schema = Arc::new(Schema::new(fields));

        let field_id_fetch_func = |field: &Field| match field.name().as_str() {
            "field1" => Ok(Some(1)),
            "field2" => Ok(Some(2)),
            "inner_field1" => Ok(Some(3)),
            "inner_field2" => Ok(Some(4)),
            _ => Err(Error::new(ErrorKind::Unexpected, "Field id not found")),
        };
        let projector =
            RecordBatchProjector::new(schema.clone(), &[1, 5], field_id_fetch_func, |_| true);

        assert!(projector.is_err());
    }

    #[test]
    fn test_field_not_reachable() {
        let inner_fields = vec![
            Field::new("inner_field1", DataType::Int32, false),
            Field::new("inner_field2", DataType::Utf8, false),
        ];

        let fields = vec![
            Field::new("field1", DataType::Int32, false),
            Field::new(
                "field2",
                DataType::Struct(Fields::from(inner_fields.clone())),
                false,
            ),
        ];
        let schema = Arc::new(Schema::new(fields));

        let field_id_fetch_func = |field: &Field| match field.name().as_str() {
            "field1" => Ok(Some(1)),
            "field2" => Ok(Some(2)),
            "inner_field1" => Ok(Some(3)),
            "inner_field2" => Ok(Some(4)),
            _ => Err(Error::new(ErrorKind::Unexpected, "Field id not found")),
        };
        let projector =
            RecordBatchProjector::new(schema.clone(), &[3], field_id_fetch_func, |_| false);
        assert!(projector.is_err());

        let projector =
            RecordBatchProjector::new(schema.clone(), &[3], field_id_fetch_func, |_| true);
        assert!(projector.is_ok());
    }
}
