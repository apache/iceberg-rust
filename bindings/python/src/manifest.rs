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

use iceberg::spec::{DataContentType, DataFile, DataFileFormat, FieldSummary, FormatVersion, Literal, Manifest, ManifestContentType, ManifestEntry, ManifestFile, ManifestList, ManifestStatus, PrimitiveLiteral, StructType, Type};
use pyo3::prelude::*;
use std::collections::HashMap;
use std::sync::Arc;
use pyo3::types::PyAny;
use iceberg::{Error, ErrorKind};

#[pyclass]
pub struct PyLiteral {
    inner: Literal,
}


#[pyclass]
pub struct PyPrimitiveLiteral {
    inner: PrimitiveLiteral,
}


#[pyclass]
pub struct PyDataFile {
    inner: DataFile,
}

#[pymethods]
impl PyDataFile {

    #[getter]
    fn content(&self) -> i32 {
        // TODO: Maps the enum back to the int
        // I have a feeling this can be done more intelligently
        match self.inner.content_type() {
            DataContentType::Data => 0,
            DataContentType::PositionDeletes => 1,
            DataContentType::EqualityDeletes => 2,
        }
    }

    #[getter]
    fn file_path(&self) -> &str {
        self.inner.file_path()
    }

    #[getter]
    fn file_format(&self) -> &str {
        // TODO: Maps the enum back to the int
        // I have a feeling this can be done more intelligently
        match self.inner.file_format() {
            DataFileFormat::Avro => "avro",
            DataFileFormat::Orc => "orc",
            DataFileFormat::Parquet => "parquet",
            DataFileFormat::Puffin => "puffin",
        }
    }

    #[getter]
    fn partition(&self) -> Vec<Option<PyLiteral>> {
        self.inner.partition().fields().iter().map(|p| match p {
            Some(lit) => Some(PyLiteral { inner: lit.clone() }),
            _ => None
        } ).collect()
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
        self.inner.upper_bounds().into_iter().map(|(k, v)| (k.clone(), v.to_bytes().unwrap().to_vec())).collect()
    }

    #[getter]
    fn lower_bounds(&self) -> HashMap<i32, Vec<u8>> {
        self.inner.lower_bounds().into_iter().map(|(k, v)| (k.clone(), v.to_bytes().unwrap().to_vec())).collect()
    }

    #[getter]
    fn key_metadata(&self) ->  Option<&[u8]> {
        self.inner.key_metadata()
    }

    #[getter]
    fn split_offsets(&self) -> &[i64] {
        self.inner.split_offsets()
    }

    #[getter]
    fn equality_ids(&self) -> &[i32] {
        self.inner.equality_ids()
    }

    #[getter]
    fn sort_order_id(&self) -> Option<i32> {
        self.inner.sort_order_id()
    }

}

#[pyclass]
pub struct PyManifest {
    inner: Manifest,
}


#[pymethods]
impl PyManifest {
    fn entries(&self) -> Vec<PyManifestEntry> {
        // TODO: Most of the time, we're only interested in 'alive' entries,
        // that are the ones that are either ADDED or EXISTING
        // so we can add a boolean to skip the DELETED entries right away before
        // moving it into the Python world
        self.inner.entries().iter().map(|entry| PyManifestEntry { inner: entry.clone() }).collect()
    }
}


#[pyclass]
pub struct PyFieldSummary {
    inner: FieldSummary,
}


#[pymethods]
impl crate::manifest::PyFieldSummary {

    #[getter]
    fn contains_null(&self) -> bool {
        self.inner.contains_null
    }

    #[getter]
    fn contains_nan(&self) -> Option<bool> {
        self.inner.contains_nan
    }

    #[getter]
    fn lower_bound(&self) -> Option<PyPrimitiveLiteral> {
        self.inner.lower_bound.clone().map(|v| PyPrimitiveLiteral{ inner: v.literal().clone() })
    }

    #[getter]
    fn upper_bound(&self) -> Option<PyPrimitiveLiteral> {
        self.inner.upper_bound.clone().map(|v| PyPrimitiveLiteral{ inner: v.literal().clone() })
    }



}

#[pyclass]
pub struct PyManifestFile {
    inner: ManifestFile,
}


#[pymethods]
impl crate::manifest::PyManifestFile {
    #[getter]
    fn manifest_path(&self) -> &str {
        self.inner.manifest_path.as_str()
    }
    #[getter]
    fn manifest_length(&self) -> i64 {
        self.inner.manifest_length
    }
    #[getter]
    fn partition_spec_id(&self) -> i32 {
        self.inner.partition_spec_id
    }

    #[getter]
    fn content(&self) -> i32 {
        // TODO: Maps the enum back to the int
        // I have a feeling this can be done more intelligently
        match self.inner.content {
            ManifestContentType::Data => 0,
            ManifestContentType::Deletes => 1,
        }
    }

    #[getter]
    fn sequence_number(&self) -> i64 {
        self.inner.sequence_number
    }


    #[getter]
    fn min_sequence_number(&self) -> i64 {
        self.inner.min_sequence_number
    }

    #[getter]
    fn added_snapshot_id(&self) -> i64 {
        self.inner.added_snapshot_id
    }


    #[getter]
    fn added_files_count(&self) -> Option<u32> {
        self.inner.added_files_count
    }

    #[getter]
    fn existing_files_count(&self) -> Option<u32> {
        self.inner.existing_files_count
    }

    #[getter]
    fn deleted_files_count(&self) -> Option<u32> {
        self.inner.deleted_files_count
    }

    #[getter]
    fn added_rows_count(&self) -> Option<u64> {
        self.inner.added_rows_count
    }

    #[getter]
    fn existing_rows_count(&self) -> Option<u64> {
        self.inner.existing_rows_count
    }

    #[getter]
    fn deleted_rows_count(&self) -> Option<u64> {
        self.inner.deleted_rows_count
    }

    #[getter]
    fn partitions(&self) -> Vec<PyFieldSummary> {
        self.inner.partitions.iter().map(|s| PyFieldSummary {
            inner: s.clone()
        }).collect()
    }

    #[getter]
    fn key_metadata(&self) -> Vec<u8> {
        self.inner.key_metadata.clone()
    }

}

#[pyclass]
pub struct PyManifestEntry {
    inner: Arc<ManifestEntry>,
}

#[pymethods]
impl PyManifestEntry {

    #[getter]
    fn status(&self) -> i32 {
        // TODO: Maps the enum back to the int
        // I have a feeling this can be done more intelligently
        match self.inner.status {
            ManifestStatus::Existing => 0,
            ManifestStatus::Added => 1,
            ManifestStatus::Deleted => 2,
        }
    }

    #[getter]
    fn snapshot_id(&self) -> Option<i64> {
        self.inner.snapshot_id
    }

    #[getter]
    fn sequence_number(&self) -> Option<i64> {
        self.inner.sequence_number
    }

    #[getter]
    fn file_sequence_number(&self) -> Option<i64> {
        self.inner.file_sequence_number
    }

    #[getter]
    fn data_file(&self) -> PyDataFile {
        PyDataFile {
            inner: self.inner.data_file.clone()
        }
    }
}


#[pyfunction]
pub fn read_manifest_entries(bs: &[u8]) -> PyManifest {
    // TODO: Some error handling
    PyManifest {
        inner: Manifest::parse_avro(bs).unwrap()
    }
}

#[pyclass]
// #[derive(Clone)]
pub struct PartitionSpecProviderCallbackHolder {
    callback: Py<PyAny>,
}

#[pymethods]
impl PartitionSpecProviderCallbackHolder {
    #[new]
    fn new(callback: Py<PyAny>) -> Self {
        Self { callback }
    }

    fn trigger_from_python(&self, id: i32) -> PyResult<String> {
        Self::do_the_callback(self, id)
    }
}

impl PartitionSpecProviderCallbackHolder {
    /// Simulate calling the Python callback from "pure Rust"
    pub fn do_the_callback(&self, id: i32) -> PyResult<String> {
        Python::with_gil(|py| {
            let result = self.callback.call1(py, (id,))?;         // Call the Python function
            let string = result.extract::<String>(py)?;      // Try converting the result to a Rust String
            Ok(string)
        })
    }
}


#[pyclass]
pub struct PyManifestList {
    inner: ManifestList,
}


#[pymethods]
impl crate::manifest::PyManifestList {
    fn entries(&self) -> Vec<PyManifestFile> {
        self.inner.entries().iter().map(|file| PyManifestFile { inner: file.clone() }).collect()
    }
}


#[pyfunction]
pub fn read_manifest_list(bs: &[u8], cb: &PartitionSpecProviderCallbackHolder) -> PyManifestList {
    // TODO: Some error handling
    let provider = move |_id| {
        let bound = cb.do_the_callback(_id).unwrap();
        let json = bound.as_str();

        // I don't fully comprehend the deserializer here,
        // it works for a Type, but not for a StructType
        // So I had to do some awkward stuff to make it work
        let res: Result<Type, _> = serde_json::from_str(json);

        let result: Result<Option<StructType>, Error> = match res {
            Ok(Type::Struct(s)) => Ok(Some(s)),
            _ => Err(Error::new(
                ErrorKind::DataInvalid,
                format!("Invalid JSON: {}", json),
            ))
        };

        result
    };

    PyManifestList {
        inner: ManifestList::parse_with_version(bs, FormatVersion::V2, provider).unwrap()
    }
}

pub fn register_module(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    let this = PyModule::new(py, "manifest")?;

    this.add_function(wrap_pyfunction!(read_manifest_entries, &this)?)?;
    this.add_function(wrap_pyfunction!(read_manifest_list, &this)?)?;
    this.add_class::<PartitionSpecProviderCallbackHolder>()?;

    m.add_submodule(&this)?;
    py.import("sys")?
        .getattr("modules")?
        .set_item("pyiceberg_core.manifest", this)
}
