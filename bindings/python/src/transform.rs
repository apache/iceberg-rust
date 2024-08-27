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

use iceberg::spec::Transform;
use iceberg::transform::create_transform_function;

use arrow::{
    array::{make_array, Array, ArrayData},
};
use arrow::pyarrow::{FromPyArrow, ToPyArrow};
use pyo3::{exceptions::PyValueError, prelude::*};

fn to_py_err(err: iceberg::Error) -> PyErr {
    PyValueError::new_err(err.to_string())
}

#[pyclass]
pub struct ArrowArrayTransform {
}

fn apply(array: PyObject, transform: Transform, py: Python) -> PyResult<PyObject> {
    // import
    let array = ArrayData::from_pyarrow_bound(array.bind(py))?;
    let array = make_array(array);
    let transform_function = create_transform_function(&transform).map_err(to_py_err)?;
    let array = transform_function.transform(array).map_err(to_py_err)?;
    // export
    let array = array.into_data();
    array.to_pyarrow(py)
}

#[pymethods]
impl ArrowArrayTransform {
    #[staticmethod]
    pub fn identity(array: PyObject, py: Python) -> PyResult<PyObject> {
        apply(array, Transform::Identity, py)
    }

    #[staticmethod]
    pub fn void(array: PyObject, py: Python) -> PyResult<PyObject> {
        apply(array, Transform::Void, py)
    }

    #[staticmethod]
    pub fn year(array: PyObject, py: Python) -> PyResult<PyObject> {
        apply(array, Transform::Year, py)
    }

    #[staticmethod]
    pub fn month(array: PyObject, py: Python) -> PyResult<PyObject> {
        apply(array, Transform::Month, py)
    }

    #[staticmethod]
    pub fn day(array: PyObject, py: Python) -> PyResult<PyObject> {
        apply(array, Transform::Day, py)
    }

    #[staticmethod]
    pub fn hour(array: PyObject, py: Python) -> PyResult<PyObject> {
        apply(array, Transform::Hour, py)
    }

    #[staticmethod]
    pub fn bucket(array: PyObject, num_buckets: u32, py: Python) -> PyResult<PyObject> {
        apply(array, Transform::Bucket(num_buckets), py)
    }

    #[staticmethod]
    pub fn truncate(array: PyObject, width: u32, py: Python) -> PyResult<PyObject> {
        apply(array, Transform::Truncate(width), py)
    }
}
