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

use iceberg::spec::{DataFile, DataFileFormat, FieldSummary, FormatVersion, Literal, Manifest, ManifestEntry, ManifestFile, ManifestList, ManifestStatus, PrimitiveLiteral, StructType, Type};
use iceberg::{Error, ErrorKind};
use pyo3::prelude::*;
use pyo3::types::PyAny;
use std::collections::HashMap;
use std::sync::Arc;

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
        self.inner.content as i32
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
        ManifestStatus::Existing as i32
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
pub fn read_manifest_list(bs: &[u8]) -> PyManifestList {
    PyManifestList {
        inner: ManifestList::parse_with_version(bs, FormatVersion::V2).unwrap()
    }
}

pub fn register_module(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    let this = PyModule::new(py, "manifest")?;

    this.add_function(wrap_pyfunction!(read_manifest_entries, &this)?)?;
    this.add_function(wrap_pyfunction!(read_manifest_list, &this)?)?;

    m.add_submodule(&this)?;
    py.import("sys")?
        .getattr("modules")?
        .set_item("pyiceberg_core.manifest", this)
}
