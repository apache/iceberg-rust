use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{ArrayRef, Float32Array, Float64Array, RecordBatch, StructArray};
use arrow_schema::DataType;

use crate::arrow::ArrowArrayAccessor;
use crate::spec::{
    visit_struct_with_partner, ListType, MapType, NestedFieldRef, PrimitiveType, Schema, SchemaRef,
    SchemaWithPartnerVisitor, StructType,
};
use crate::Result;

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
    /// Creates new instance of NanValueCountVisitor
    pub fn new() -> Self {
        Self {
            nan_value_counts: HashMap::new(),
        }
    }

    /// Compute nan value counts in given schema and record batch
    pub fn compute(&mut self, schema: SchemaRef, batch: RecordBatch) -> Result<()> {
        let arrow_arr_partner_accessor = ArrowArrayAccessor {};

        let struct_arr = Arc::new(StructArray::from(batch)) as ArrayRef;
        visit_struct_with_partner(
            schema.as_struct(),
            &struct_arr,
            self,
            &arrow_arr_partner_accessor,
        )?;

        Ok(())
    }
}

impl Default for NanValueCountVisitor {
    fn default() -> Self {
        Self::new()
    }
}
