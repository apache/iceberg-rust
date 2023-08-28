use crate::Result;
use arrow::array::{new_null_array, ArrayRef};

use super::TransformFunction;

pub struct Void {}

impl TransformFunction for Void {
    fn transform(&self, input: ArrayRef) -> Result<ArrayRef> {
        Ok(new_null_array(input.data_type(), input.len()))
    }
}
