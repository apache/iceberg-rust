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

use std::{error, fmt};
use iceberg::spec::Transform;
use iceberg::transform::create_transform_function;
use iceberg::Error;

use arrow::{
    array::{make_array, Array, ArrayData},
};
use arrow::pyarrow::{FromPyArrow, ToPyArrow};
use pyo3::{exceptions::PyValueError, prelude::*};

#[derive(Debug)]
enum PyO3IcebergError {
    Error(Error),
}

impl fmt::Display for PyO3IcebergError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            PyO3IcebergError::Error(ref e) => e.fmt(f),
        }
    }
}

impl error::Error for PyO3IcebergError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match *self {
            // The cause is the underlying implementation error type. Is implicitly
            // cast to the trait object `&error::Error`. This works because the
            // underlying type already implements the `Error` trait.
            PyO3IcebergError::Error(ref e) => Some(e),
        }
    }
}

impl From<Error> for PyO3IcebergError {
    fn from(err: Error) -> PyO3IcebergError {
        PyO3IcebergError::Error(err)
    }
}

impl From<PyO3IcebergError> for PyErr {
    fn from(err: PyO3IcebergError) -> PyErr {
        PyValueError::new_err(err.to_string())
    }
}

#[pyfunction]
pub fn bucket_transform(array: PyObject, num_buckets: u32, py: Python) -> PyResult<PyObject> {
    // import
    let array = ArrayData::from_pyarrow_bound(array.bind(py))?;
    let array = make_array(array);
    let bucket = create_transform_function(&Transform::Bucket(num_buckets)).map_err(PyO3IcebergError::from)?;
    let array = bucket.transform(array).map_err(PyO3IcebergError::from)?;
    // export
    let array = array.into_data();
    array.to_pyarrow(py)
}
