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

use std::{error, fmt, sync::Arc};
use iceberg::spec::Transform;
use iceberg::transform::create_transform_function;
use iceberg::Error;

use arrow::{
    array::{make_array, Array, ArrayData, ArrayRef},
    error::ArrowError,
    ffi::{from_ffi, to_ffi},
};
use libc::uintptr_t;
use pyo3::{exceptions::PyOSError, exceptions::PyValueError, prelude::*};

#[derive(Debug)]
enum PyO3ArrowError {
    ArrowError(ArrowError),
}

impl fmt::Display for PyO3ArrowError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            PyO3ArrowError::ArrowError(ref e) => e.fmt(f),
        }
    }
}

impl error::Error for PyO3ArrowError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match *self {
            // The cause is the underlying implementation error type. Is implicitly
            // cast to the trait object `&error::Error`. This works because the
            // underlying type already implements the `Error` trait.
            PyO3ArrowError::ArrowError(ref e) => Some(e),
        }
    }
}

impl From<ArrowError> for PyO3ArrowError {
    fn from(err: ArrowError) -> PyO3ArrowError {
        PyO3ArrowError::ArrowError(err)
    }
}

impl From<PyO3ArrowError> for PyErr {
    fn from(err: PyO3ArrowError) -> PyErr {
        PyOSError::new_err(err.to_string())
    }
}

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

fn to_rust_arrow_array(ob: PyObject, py: Python) -> PyResult<ArrayRef> {
    // prepare a pointer to receive the Array struct
    let (array, schema) = to_ffi(&ArrayData::new_empty(&arrow::datatypes::DataType::Null))
        .map_err(PyO3ArrowError::from)?;
    let array_pointer = &array as *const _ as uintptr_t;
    let schema_pointer = &schema as *const _ as uintptr_t;

    // make the conversion through PyArrow's private API
    // this changes the pointer's memory and is thus unsafe. In particular, `_export_to_c` can go out of bounds
    ob.call_method1(py, "_export_to_c", (array_pointer, schema_pointer))?;

    let array = unsafe { from_ffi(array, &schema) }.map_err(PyO3ArrowError::from)?;
    let array = make_array(array);
    Ok(array)
}

fn to_pyarrow_array(array: ArrayRef, py: Python) -> PyResult<PyObject> {
    let (array, schema) = to_ffi(&array.to_data()).map_err(PyO3ArrowError::from)?;
    let array_pointer = &array as *const _ as uintptr_t;
    let schema_pointer = &schema as *const _ as uintptr_t;

    let pa = py.import_bound("pyarrow")?;

    let array = pa.getattr("Array")?.call_method1(
        "_import_from_c",
        (array_pointer as uintptr_t, schema_pointer as uintptr_t),
    )?;
    Ok(array.to_object(py))
}

#[pyfunction]
pub fn bucket_transform(array: PyObject, num_buckets: u32, py: Python) -> PyResult<PyObject> {
    // import
    let array = to_rust_arrow_array(array, py)?;
    let bucket = create_transform_function(&Transform::Bucket(num_buckets)).map_err(PyO3IcebergError::from)?;
    let array = bucket.transform(array).map_err(PyO3IcebergError::from)?;
    let array = Arc::new(array);
    // export
    to_pyarrow_array(array, py)
}
