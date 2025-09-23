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

//! Schema visitor for partition value extraction

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch, StructArray};
use crate::{Error, ErrorKind, Result};
use crate::arrow::{ArrowArrayAccessor, FieldMatchMode};
use crate::spec::{ListType, MapType, NestedFieldRef, PartitionField, PrimitiveType, Schema, SchemaRef, SchemaWithPartnerVisitor, StructType, visit_struct_with_partner};
use crate::transform::{create_transform_function, BoxedTransformFunction};

/// Visitor which extracts partition values from a record batch based on partition fields
pub struct PartitionValueVisitor {
    /// Map from source ids to their fields
    source_id_to_field: HashMap<i32, PartitionField>,
    /// Match mode for finding columns in Arrow struct
    match_mode: FieldMatchMode,
    /// Store the partition values temporarily during computation
    partition_values: Vec<ArrayRef>,
    /// Current field ID being processed
    current_transform_fn: Option<BoxedTransformFunction>,
}

impl PartitionValueVisitor {
    /// Creates new instance of PartitionValueVisitor
    #[allow(dead_code)]
    pub fn new(partition_fields: Vec<PartitionField>) -> Self {
        Self::new_with_match_mode(partition_fields, FieldMatchMode::Name)
    }

    /// Creates new instance of PartitionValueVisitor with explicit match mode
    #[allow(dead_code)]
    pub fn new_with_match_mode(
        partition_fields: Vec<PartitionField>,
        match_mode: FieldMatchMode,
    ) -> Self {
        Self {
            source_id_to_field: partition_fields
                .into_iter()
                .map(|field| (field.source_id, field))
                .collect(),
            match_mode,
            partition_values: vec![],
            current_transform_fn: None,
        }
    }

    /// Compute partition values in given schema and record batch
    #[allow(dead_code)]
    pub fn compute(
        &mut self,
        schema: SchemaRef,
        batch: RecordBatch,
    ) -> Result<Vec<ArrayRef>> {
        self.partition_values = vec![];

        let arrow_accessor = ArrowArrayAccessor::new_with_match_mode(self.match_mode);

        let struct_arr = Arc::new(StructArray::from(batch)) as ArrayRef;
        visit_struct_with_partner(
            schema.as_struct(),
            &struct_arr,
            self,
            &arrow_accessor,
        )?;

        Ok(std::mem::take(&mut self.partition_values))
    }

    /// Check if the current field is a source field, if so, create a transform function for it
    fn check_and_create_transform_fn(&mut self, field: &NestedFieldRef) -> Result<()> {
        self.current_transform_fn = match self.source_id_to_field.get(&field.id) {
            Some(partition_field) => {
                if field.field_type.is_primitive() {
                    Some(create_transform_function(&partition_field.transform)?)
                } else {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!(
                            "Cannot partition by non-primitive source field: '{field}'.",
                        ),
                    ));
                }

            },
            None => None,
        };

        Ok(())
    }
}

impl SchemaWithPartnerVisitor<ArrayRef> for PartitionValueVisitor {
    type T = ();

    fn before_struct_field(&mut self, field: &NestedFieldRef, _partner: &ArrayRef) -> Result<()> {
        self.check_and_create_transform_fn(field)?;
        Ok(())
    }

    fn after_struct_field(&mut self, _field: &NestedFieldRef, _partner: &ArrayRef) -> Result<()> {
        self.current_transform_fn = None;
        Ok(())
    }

    fn before_list_element(&mut self, field: &NestedFieldRef, _partner: &ArrayRef) -> Result<()> {
        self.check_and_create_transform_fn(field)?;
        Ok(())
    }

    fn after_list_element(&mut self, _field: &NestedFieldRef, _partner: &ArrayRef) -> Result<()> {
        self.current_transform_fn = None;
        Ok(())
    }

    fn before_map_key(&mut self, field: &NestedFieldRef, _partner: &ArrayRef) -> Result<()> {
        self.check_and_create_transform_fn(field)?;
        Ok(())
    }

    fn after_map_key(&mut self, _field: &NestedFieldRef, _partner: &ArrayRef) -> Result<()> {
        self.current_transform_fn = None;
        Ok(())
    }

    fn before_map_value(&mut self, field: &NestedFieldRef, _partner: &ArrayRef) -> Result<()> {
        self.check_and_create_transform_fn(field)?;
        Ok(())
    }

    fn after_map_value(&mut self, _field: &NestedFieldRef, _partner: &ArrayRef) -> Result<()> {
        self.current_transform_fn = None;
        Ok(())
    }

    fn schema(
        &mut self,
        _schema: &Schema,
        _partner: &ArrayRef,
        _value: Self::T,
    ) -> Result<Self::T> {
        Ok(())
    }

    fn field(
        &mut self,
        _field: &NestedFieldRef,
        _partner: &ArrayRef,
        _value: Self::T,
    ) -> Result<Self::T> {
        Ok(())
    }

    fn r#struct(
        &mut self,
        _struct: &StructType,
        _partner: &ArrayRef,
        _results: Vec<Self::T>,
    ) -> Result<Self::T> {
        Ok(())
    }

    fn list(&mut self, _list: &ListType, _list_arr: &ArrayRef, _value: Self::T) -> Result<Self::T> {
        Ok(())
    }

    fn map(
        &mut self,
        _map: &MapType,
        _partner: &ArrayRef,
        _key_value: Self::T,
        _value: Self::T,
    ) -> Result<Self::T> {
        Ok(())
    }

    fn primitive(&mut self, _p: &PrimitiveType, col: &ArrayRef) -> Result<Self::T> {
        // If the transform fn is some, then it means we are visiting a source field,
        // and we should apply the current transform fn
        if let Some(transform_fn) = &self.current_transform_fn {
            self.partition_values.push(transform_fn.transform(col.clone())?);
        }

        Ok(())
    }
}
