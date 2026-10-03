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

use iceberg::encryption::StandardKeyMetadata;
use pyo3::prelude::*;
use pyo3::types::PyBytes;

use crate::error::to_py_err;

/// The encryption key, AAD prefix and file length needed to decrypt a single file.
#[pyclass(
    frozen,
    name = "StandardKeyMetadata",
    module = "pyiceberg_core.encryption"
)]
pub struct PyStandardKeyMetadata {
    // Python objects, so attribute access increfs rather than copying the key on every read.
    encryption_key: Py<PyBytes>,
    aad_prefix: Option<Py<PyBytes>>,
    file_length: Option<u64>,
}

#[pymethods]
impl PyStandardKeyMetadata {
    #[getter]
    fn encryption_key(&self, py: Python<'_>) -> Py<PyBytes> {
        self.encryption_key.clone_ref(py)
    }

    #[getter]
    fn aad_prefix(&self, py: Python<'_>) -> Option<Py<PyBytes>> {
        self.aad_prefix.as_ref().map(|prefix| prefix.clone_ref(py))
    }

    #[getter]
    fn file_length(&self) -> Option<u64> {
        self.file_length
    }

    /// Redacts the key, so logging an instance cannot leak key material.
    fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        let key_length = self.encryption_key.bind(py).as_bytes().len();
        let aad_prefix = match &self.aad_prefix {
            Some(prefix) => prefix.bind(py).repr()?.to_string(),
            None => "None".to_string(),
        };
        let file_length = match self.file_length {
            Some(file_length) => file_length.to_string(),
            None => "None".to_string(),
        };

        Ok(format!(
            "StandardKeyMetadata(encryption_key=<redacted, {key_length} bytes>, aad_prefix={aad_prefix}, file_length={file_length})"
        ))
    }
}

/// Decode `StandardKeyMetadata` from its wire format.
#[pyfunction]
pub fn decode_standard_key_metadata(
    py: Python<'_>,
    data: &[u8],
) -> PyResult<PyStandardKeyMetadata> {
    let metadata = StandardKeyMetadata::decode(data).map_err(to_py_err)?;

    Ok(PyStandardKeyMetadata {
        encryption_key: PyBytes::new(py, metadata.encryption_key().as_bytes()).unbind(),
        aad_prefix: metadata
            .aad_prefix()
            .map(|aad_prefix| PyBytes::new(py, aad_prefix).unbind()),
        file_length: metadata.file_length(),
    })
}

/// Encode `StandardKeyMetadata` to its wire format.
#[pyfunction]
#[pyo3(signature = (encryption_key, aad_prefix=None, file_length=None))]
pub fn encode_standard_key_metadata<'py>(
    py: Python<'py>,
    encryption_key: &[u8],
    aad_prefix: Option<&[u8]>,
    file_length: Option<u64>,
) -> PyResult<Bound<'py, PyBytes>> {
    let mut metadata = StandardKeyMetadata::try_new(encryption_key).map_err(to_py_err)?;

    if let Some(aad_prefix) = aad_prefix {
        metadata = metadata.with_aad_prefix(aad_prefix);
    }
    if let Some(file_length) = file_length {
        metadata = metadata.with_file_length(file_length);
    }

    Ok(PyBytes::new(py, &metadata.encode().map_err(to_py_err)?))
}

pub fn register_module(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    let this = PyModule::new(py, "encryption")?;

    this.add_class::<PyStandardKeyMetadata>()?;
    this.add_function(wrap_pyfunction!(decode_standard_key_metadata, &this)?)?;
    this.add_function(wrap_pyfunction!(encode_standard_key_metadata, &this)?)?;

    m.add_submodule(&this)?;
    py.import("sys")?
        .getattr("modules")?
        .set_item("pyiceberg_core.encryption", this)
}
