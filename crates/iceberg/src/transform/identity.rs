use crate::Result;
use arrow::array::ArrayRef;

use super::TransformFunction;

/// Return identity array.
pub struct Identity {}

impl TransformFunction for Identity {
    fn transform(&self, input: ArrayRef) -> Result<ArrayRef> {
        Ok(input)
    }
}
