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

use std::collections::HashMap;

use iceberg::spec::{DataFile, DataFileFormat, PrimitiveLiteral};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use pyo3::IntoPyObjectExt;

#[pyclass()]
pub struct PyPrimitiveLiteral {
    inner: PrimitiveLiteral,
}

#[pymethods]
impl PyPrimitiveLiteral {
    /// Returns the raw value as a Python object.
    ///
    /// For Int128 (Decimal) and UInt128 (UUID), returns Python int.
    /// Use `decimal_value(scale)` for proper Decimal conversion with scale,
    /// or `uuid_value()` for UUID conversion.
    pub fn value(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        match &self.inner {
            PrimitiveLiteral::Boolean(v) => v.into_py_any(py),
            PrimitiveLiteral::Int(v) => v.into_py_any(py),
            PrimitiveLiteral::Long(v) => v.into_py_any(py),
            PrimitiveLiteral::Float(v) => v.0.into_py_any(py), // unwrap OrderedFloat
            PrimitiveLiteral::Double(v) => v.0.into_py_any(py),
            PrimitiveLiteral::String(v) => v.into_py_any(py),
            PrimitiveLiteral::Binary(v) => PyBytes::new(py, v).into_py_any(py),
            PrimitiveLiteral::Int128(v) => v.into_py_any(py), // Python handles big ints
            PrimitiveLiteral::UInt128(v) => v.into_py_any(py),
            PrimitiveLiteral::AboveMax => Err(PyValueError::new_err("AboveMax is not supported")),
            PrimitiveLiteral::BelowMin => Err(PyValueError::new_err("BelowMin is not supported")),
        }
    }

    /// Returns Int128 as a Python `decimal.Decimal` with the given scale.
    ///
    /// Iceberg stores Decimal values as unscaled Int128 integers. This method
    /// converts the raw integer to a properly scaled Python Decimal.
    ///
    /// # Arguments
    /// * `scale` - The number of digits after the decimal point (from schema)
    ///
    /// # Example
    /// ```python
    /// # For a Decimal(10, 2) field with value 123.45 (stored as 12345):
    /// decimal_value = literal.decimal_value(2)  # Returns Decimal("123.45")
    /// ```
    ///
    /// # Errors
    /// Returns `ValueError` if the underlying value is not Int128.
    pub fn decimal_value(&self, py: Python<'_>, scale: i32) -> PyResult<Py<PyAny>> {
        match &self.inner {
            PrimitiveLiteral::Int128(v) => {
                let decimal_mod = py.import("decimal")?;
                let decimal_cls = decimal_mod.getattr("Decimal")?;

                let abs_value = v.unsigned_abs();
                let is_negative = *v < 0;
                let sign = if is_negative { "-" } else { "" };

                let decimal_str = if scale <= 0 {
                    format!("{}{}", sign, abs_value)
                } else {
                    let scale_u = scale as u32;
                    let divisor = 10u128.pow(scale_u);
                    let integer_part = abs_value / divisor;
                    let fractional_part = abs_value % divisor;
                    format!(
                        "{}{}.{:0>width$}",
                        sign,
                        integer_part,
                        fractional_part,
                        width = scale as usize
                    )
                };

                decimal_cls.call1((decimal_str,))?.into_py_any(py)
            }
            _ => Err(PyValueError::new_err(
                "decimal_value() requires Int128 literal (Decimal type)",
            )),
        }
    }

    /// Returns UInt128 as a Python `uuid.UUID`.
    ///
    /// Iceberg stores UUID values as UInt128 integers. This method converts
    /// the raw integer to a Python UUID object.
    ///
    /// # Example
    /// ```python
    /// uuid_value = literal.uuid_value()  # Returns uuid.UUID
    /// ```
    ///
    /// # Errors
    /// Returns `ValueError` if the underlying value is not UInt128.
    pub fn uuid_value(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        match &self.inner {
            PrimitiveLiteral::UInt128(v) => {
                let uuid_mod = py.import("uuid")?;
                let uuid_cls = uuid_mod.getattr("UUID")?;
                let kwargs = pyo3::types::PyDict::new(py);
                kwargs.set_item("int", *v)?;
                uuid_cls.call((), Some(&kwargs))?.into_py_any(py)
            }
            _ => Err(PyValueError::new_err(
                "uuid_value() requires UInt128 literal (UUID type)",
            )),
        }
    }

    /// Returns the type name of the underlying primitive literal.
    ///
    /// Useful for determining which conversion method to call.
    ///
    /// # Returns
    /// One of: "boolean", "int", "long", "float", "double", "string",
    /// "binary", "int128", "uint128", "above_max", "below_min"
    pub fn literal_type(&self) -> &'static str {
        match &self.inner {
            PrimitiveLiteral::Boolean(_) => "boolean",
            PrimitiveLiteral::Int(_) => "int",
            PrimitiveLiteral::Long(_) => "long",
            PrimitiveLiteral::Float(_) => "float",
            PrimitiveLiteral::Double(_) => "double",
            PrimitiveLiteral::String(_) => "string",
            PrimitiveLiteral::Binary(_) => "binary",
            PrimitiveLiteral::Int128(_) => "int128",
            PrimitiveLiteral::UInt128(_) => "uint128",
            PrimitiveLiteral::AboveMax => "above_max",
            PrimitiveLiteral::BelowMin => "below_min",
        }
    }
}

#[pyclass]
pub struct PyDataFile {
    inner: DataFile,
}

#[pymethods]
impl PyDataFile {
    #[getter]
    fn content(&self) -> i32 {
        self.inner.content_type() as i32
    }

    #[getter]
    fn file_path(&self) -> &str {
        self.inner.file_path()
    }

    #[getter]
    fn file_format(&self) -> &str {
        match self.inner.file_format() {
            DataFileFormat::Avro => "avro",
            DataFileFormat::Orc => "orc",
            DataFileFormat::Parquet => "parquet",
            DataFileFormat::Puffin => "puffin",
        }
    }

    #[getter]
    fn partition(&self) -> Vec<Option<PyPrimitiveLiteral>> {
        self.inner
            .partition()
            .iter()
            .map(|lit| {
                lit.and_then(|l| {
                    Some(PyPrimitiveLiteral {
                        inner: l.as_primitive_literal()?,
                    })
                })
            })
            .collect()
    }

    #[getter]
    fn record_count(&self) -> u64 {
        self.inner.record_count()
    }

    #[getter]
    fn file_size_in_bytes(&self) -> u64 {
        self.inner.file_size_in_bytes()
    }

    #[getter]
    fn column_sizes(&self) -> &HashMap<i32, u64> {
        self.inner.column_sizes()
    }

    #[getter]
    fn value_counts(&self) -> &HashMap<i32, u64> {
        self.inner.value_counts()
    }

    #[getter]
    fn null_value_counts(&self) -> &HashMap<i32, u64> {
        self.inner.null_value_counts()
    }

    #[getter]
    fn nan_value_counts(&self) -> &HashMap<i32, u64> {
        self.inner.nan_value_counts()
    }

    #[getter]
    fn upper_bounds(&self) -> HashMap<i32, Vec<u8>> {
        self.inner
            .upper_bounds()
            .iter()
            .map(|(k, v)| (*k, v.to_bytes().unwrap().to_vec()))
            .collect()
    }

    #[getter]
    fn lower_bounds(&self) -> HashMap<i32, Vec<u8>> {
        self.inner
            .lower_bounds()
            .iter()
            .map(|(k, v)| (*k, v.to_bytes().unwrap().to_vec()))
            .collect()
    }

    #[getter]
    fn key_metadata(&self) -> Option<&[u8]> {
        self.inner.key_metadata()
    }

    #[getter]
    fn split_offsets(&self) -> Option<&[i64]> {
        self.inner.split_offsets()
    }

    #[getter]
    fn equality_ids(&self) -> Option<Vec<i32>> {
        self.inner.equality_ids()
    }

    #[getter]
    fn sort_order_id(&self) -> Option<i32> {
        self.inner.sort_order_id()
    }
}

impl PyDataFile {
    pub fn new(inner: DataFile) -> Self {
        Self { inner }
    }
}
