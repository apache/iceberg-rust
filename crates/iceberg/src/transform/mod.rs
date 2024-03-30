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

//! Transform function used to compute partition values.
use crate::{
    spec::{Datum, Transform},
    Error, ErrorKind, Result,
};
use arrow_array::ArrayRef;

mod bucket;
mod identity;
mod temporal;
mod truncate;
mod void;

/// TransformFunction is a trait that defines the interface for all transform functions.
pub trait TransformFunction: Send {
    /// transform will take an input array and transform it into a new array.
    /// The implementation of this function will need to check and downcast the input to specific
    /// type.
    fn transform(&self, input: ArrayRef) -> Result<ArrayRef>;
    /// transform_literal will take an input literal and transform it into a new literal.
    fn transform_literal(&self, input: &Datum) -> Result<Option<Datum>>;
    /// A thin wrapper around `transform_literal`
    /// to return an error even when it's `None`.
    fn transform_literal_result(&self, input: &Datum) -> Result<Datum> {
        self.transform_literal(input)?.ok_or_else(|| {
            Error::new(
                ErrorKind::Unexpected,
                format!("Returns 'None' for literal {}", input),
            )
        })
    }
}

/// BoxedTransformFunction is a boxed trait object of TransformFunction.
pub type BoxedTransformFunction = Box<dyn TransformFunction>;

/// create_transform_function creates a boxed trait object of TransformFunction from a Transform.
pub fn create_transform_function(transform: &Transform) -> Result<BoxedTransformFunction> {
    match transform {
        Transform::Identity => Ok(Box::new(identity::Identity {})),
        Transform::Void => Ok(Box::new(void::Void {})),
        Transform::Year => Ok(Box::new(temporal::Year {})),
        Transform::Month => Ok(Box::new(temporal::Month {})),
        Transform::Day => Ok(Box::new(temporal::Day {})),
        Transform::Hour => Ok(Box::new(temporal::Hour {})),
        Transform::Bucket(mod_n) => Ok(Box::new(bucket::Bucket::new(*mod_n))),
        Transform::Truncate(width) => Ok(Box::new(truncate::Truncate::new(*width))),
        Transform::Unknown => Err(crate::error::Error::new(
            crate::ErrorKind::FeatureUnsupported,
            "Transform Unknown is not implemented",
        )),
    }
}
