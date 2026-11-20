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

use arrow_array::{
    Array, ArrayRef, Float32Array, Float64Array, ListArray, MapArray, RecordBatch, StructArray,
};
use arrow_schema::{DataType, FieldRef};

use crate::arrow::get_field_id_from_metadata;
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
}

impl NanValueCountVisitor {
    fn visit_field(&mut self, field: &FieldRef, array: &ArrayRef) -> Result<()> {
        if matches!(array.data_type(), DataType::Float32 | DataType::Float64) {
            let field_id = get_field_id_from_metadata(field)?;
            count_float_nans!(array, self, field_id);
        }

        match field.data_type() {
            DataType::Struct(fields) => {
                let struct_array =
                    array
                        .as_any()
                        .downcast_ref::<StructArray>()
                        .ok_or_else(|| {
                            Error::new(
                                ErrorKind::DataInvalid,
                                "Expected struct array for NaN counts",
                            )
                        })?;
                for (field, column) in fields.iter().zip(struct_array.columns()) {
                    self.visit_field(field, column)?;
                }
                Ok(())
            }
            DataType::List(element) => {
                let list_array = array.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
                    Error::new(ErrorKind::DataInvalid, "Expected list array for NaN counts")
                })?;
                self.visit_field(element, list_array.values())
            }
            DataType::Map(entries, _) => {
                let map_array = array.as_any().downcast_ref::<MapArray>().ok_or_else(|| {
                    Error::new(ErrorKind::DataInvalid, "Expected map array for NaN counts")
                })?;
                let DataType::Struct(fields) = entries.data_type() else {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        "Expected map entry struct for NaN counts",
                    ));
                };
                self.visit_field(&fields[0], map_array.keys())?;
                self.visit_field(&fields[1], map_array.values())
            }
            _ => Ok(()),
        }
    }

    /// Creates new instance of NanValueCountVisitor
    pub fn new() -> Self {
        Self {
            nan_value_counts: HashMap::new(),
        }
    }

    /// Compute NaN counts from the validated, projected Arrow batch.
    pub fn compute(&mut self, batch: &RecordBatch) -> Result<()> {
        for (field, column) in batch.schema().fields().iter().zip(batch.columns()) {
            self.visit_field(field, column)?;
        }
        Ok(())
    }
}

impl Default for NanValueCountVisitor {
    fn default() -> Self {
        Self::new()
    }
}
