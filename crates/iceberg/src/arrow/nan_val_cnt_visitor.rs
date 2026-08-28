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

//! The module contains the visitor for calculating NaN values in give arrow record batch.

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::Arc;

use arrow_array::{
    ArrayRef, Float32Array, Float64Array, ListArray, MapArray, RecordBatch, StructArray,
};
use arrow_schema::DataType;

use crate::arrow::FieldMatchMode;
use crate::spec::{
    ListType, MapType, NestedFieldRef, PrimitiveType, Schema, SchemaRef, SchemaWithPartnerVisitor,
    StructType, Type, VariantType,
};
use crate::{Error, ErrorKind, Result};

macro_rules! cast_and_update_cnt_map {
    ($t:ty, $col:ident, $self:ident, $field_id:ident) => {
        let nan_val_cnt = $col
            .as_any()
            .downcast_ref::<$t>()
            .unwrap()
            .iter()
            .filter(|value| value.map_or(false, |v| v.is_nan()))
            .count() as u64;

        match $self.nan_value_counts.entry($field_id) {
            Entry::Occupied(mut ele) => {
                let total_nan_val_cnt = ele.get() + nan_val_cnt;
                ele.insert(total_nan_val_cnt);
            }
            Entry::Vacant(v) => {
                v.insert(nan_val_cnt);
            }
        };
    };
}

macro_rules! count_float_nans {
    ($col:ident, $self:ident, $field_id:ident) => {
        match $col.data_type() {
            DataType::Float32 => {
                cast_and_update_cnt_map!(Float32Array, $col, $self, $field_id);
            }
            DataType::Float64 => {
                cast_and_update_cnt_map!(Float64Array, $col, $self, $field_id);
            }
            _ => {}
        }
    };
}

/// Visitor which counts and keeps track of NaN value counts in given record batch(s)
pub struct NanValueCountVisitor {
    /// Stores field ID to NaN value count mapping
    pub nan_value_counts: HashMap<i32, u64>,
    match_mode: FieldMatchMode,
}

impl SchemaWithPartnerVisitor<ArrayRef> for NanValueCountVisitor {
    type T = ();

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

    fn primitive(&mut self, _p: &PrimitiveType, _col: &ArrayRef) -> Result<Self::T> {
        Ok(())
    }

    fn variant(&mut self, _v: &VariantType, _col: &ArrayRef) -> Result<Self::T> {
        Ok(())
    }

    fn after_struct_field(&mut self, field: &NestedFieldRef, partner: &ArrayRef) -> Result<()> {
        let field_id = field.id;
        count_float_nans!(partner, self, field_id);
        Ok(())
    }

    fn after_list_element(&mut self, field: &NestedFieldRef, partner: &ArrayRef) -> Result<()> {
        let field_id = field.id;
        count_float_nans!(partner, self, field_id);
        Ok(())
    }

    fn after_map_key(&mut self, field: &NestedFieldRef, partner: &ArrayRef) -> Result<()> {
        let field_id = field.id;
        count_float_nans!(partner, self, field_id);
        Ok(())
    }

    fn after_map_value(&mut self, field: &NestedFieldRef, partner: &ArrayRef) -> Result<()> {
        let field_id = field.id;
        count_float_nans!(partner, self, field_id);
        Ok(())
    }
}

impl NanValueCountVisitor {
    fn visit_field(&mut self, field: &NestedFieldRef, array: &ArrayRef) -> Result<()> {
        let field_id = field.id;
        count_float_nans!(array, self, field_id);

        match field.field_type.as_ref() {
            Type::Primitive(_) | Type::Variant(_) => Ok(()),
            Type::Struct(struct_type) => self.visit_struct(struct_type, array),
            Type::List(list_type) => {
                let list_array = array.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        format!(
                            "Expected list array for field {}, got {}",
                            field.id,
                            array.data_type()
                        ),
                    )
                })?;
                self.visit_field(&list_type.element_field, list_array.values())
            }
            Type::Map(map_type) => {
                let map_array = array.as_any().downcast_ref::<MapArray>().ok_or_else(|| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        format!(
                            "Expected map array for field {}, got {}",
                            field.id,
                            array.data_type()
                        ),
                    )
                })?;
                self.visit_field(&map_type.key_field, map_array.keys())?;
                self.visit_field(&map_type.value_field, map_array.values())
            }
        }
    }

    fn visit_struct(&mut self, struct_type: &StructType, array: &ArrayRef) -> Result<()> {
        let struct_array = array
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!("Expected struct array, got {}", array.data_type()),
                )
            })?;

        for field in struct_type.fields() {
            if matches!(
                field.field_type.as_ref(),
                Type::Primitive(PrimitiveType::Unknown)
            ) {
                continue;
            }

            let field_position = struct_array
                .fields()
                .iter()
                .position(|arrow_field| self.match_mode.match_field(arrow_field, field))
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        format!("Field id {} not found in struct array", field.id),
                    )
                })?;
            self.visit_field(field, struct_array.column(field_position))?;
        }

        Ok(())
    }

    /// Creates new instance of NanValueCountVisitor
    pub fn new() -> Self {
        Self::new_with_match_mode(FieldMatchMode::Id)
    }

    /// Creates new instance of NanValueCountVisitor with explicit match mode
    pub fn new_with_match_mode(match_mode: FieldMatchMode) -> Self {
        Self {
            nan_value_counts: HashMap::new(),
            match_mode,
        }
    }

    /// Compute nan value counts in given schema and record batch
    pub fn compute(&mut self, schema: SchemaRef, batch: RecordBatch) -> Result<()> {
        let struct_arr = Arc::new(StructArray::from(batch)) as ArrayRef;
        self.visit_struct(schema.as_struct(), &struct_arr)
    }
}

impl Default for NanValueCountVisitor {
    fn default() -> Self {
        Self::new()
    }
}
