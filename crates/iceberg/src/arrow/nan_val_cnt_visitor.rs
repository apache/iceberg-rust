use std::collections::hash_map::Entry;
use std::collections::HashMap;

use arrow_array::{ArrayRef, Float32Array, Float64Array, RecordBatch};

use crate::arrow::ArrowArrayAccessor;
use crate::spec::{
    visit_schema_with_partner, ListType, MapType, NestedFieldRef, SchemaRef,
    PrimitiveType, Schema, SchemaWithPartnerVisitor, StructType,
};
use crate::Result;

macro_rules! count_float_nans {
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

/// TODO(feniljain)
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

    fn primitive(&mut self, p: &PrimitiveType, col: &ArrayRef) -> Result<Self::T> {
        match p {
            PrimitiveType::Float => {
                // let field_id = p.id;
                // TODO(feniljain): fix this
                let field_id = 1;
                count_float_nans!(Float32Array, col, self, field_id);
            }
            PrimitiveType::Double => {
                let field_id = 1;
                count_float_nans!(Float64Array, col, self, field_id);
            }
            _ => {}
        }

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
    pub fn compute(&mut self, schema: SchemaRef, batch: &RecordBatch) -> Result<()> {
        let arrow_arr_partner_accessor = ArrowArrayAccessor{};

        for arr_ref in batch.columns() {
            visit_schema_with_partner(&schema, arr_ref, self, &arrow_arr_partner_accessor)?;
        }

        Ok(())
    }
}

