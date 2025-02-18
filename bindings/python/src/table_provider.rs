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

use std::ffi::CString;
use std::sync::Arc;

use datafusion_ffi::table_provider::FFI_TableProvider;
use iceberg::io::FileIO;
use iceberg::table::StaticTable;
use iceberg::TableIdent;
use iceberg_datafusion::table::IcebergTableProvider;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyCapsule;
use tokio::runtime::Runtime;

#[pyclass(name = "IcebergTableProvider")]
#[derive(Clone)]
pub struct PyIcebergTableProvider {
    inner: IcebergTableProvider,
    runtime: Arc<Runtime>,
}

#[pymethods]
impl PyIcebergTableProvider {
    #[new]
    fn new(metadata_location: String) -> PyResult<Self> {
        // Create TableIdent
        let table_ident = TableIdent::from_strs(["myschema", "mytable"]).map_err(|e| {
            PyErr::new::<PyRuntimeError, _>(format!("Failed to create table ident: {}", e))
        })?;

        // Create FileIO
        let file_io = FileIO::from_path(&metadata_location)
            .and_then(|builder| builder.build())
            .map_err(|e| {
                PyErr::new::<PyRuntimeError, _>(format!("Failed to create file IO: {}", e))
            })?;

        // Create a runtime for running async code
        let runtime = Runtime::new().map_err(|e| {
            PyErr::new::<PyRuntimeError, _>(format!("Failed to create runtime: {}", e))
        })?;

        // Run the async initialization in the runtime
        let provider = runtime.block_on(async {
            // Load static table
            let static_table =
                StaticTable::from_metadata_file(&metadata_location, table_ident, file_io)
                    .await
                    .map_err(|e| {
                        PyErr::new::<PyRuntimeError, _>(format!(
                            "Failed to load static table: {}",
                            e
                        ))
                    })?;

            // Convert to table and create schema
            let table = static_table.into_table();

            // Use the public try_new_from_table function
            IcebergTableProvider::try_new_from_table(table)
                .await
                .map_err(|e| {
                    PyErr::new::<PyRuntimeError, _>(format!(
                        "Failed to create table provider: {}",
                        e
                    ))
                })
        })?;

        Ok(PyIcebergTableProvider { 
            inner: provider,
            runtime: Arc::new(runtime),
        })
    }

    /// Expose as a DataFusion table provider
    fn __datafusion_table_provider__<'py>(
        &self,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyCapsule>> {        
        let name = CString::new("datafusion_table_provider").unwrap();
        let provider = FFI_TableProvider::new(Arc::new(self.inner.clone()), false, Some(self.runtime.handle().clone()));
        PyCapsule::new_bound(py, provider, Some(name.clone()))
    }
}

/// Standalone function to create a table provider
#[pyfunction]
pub fn create_table_provider(metadata_location: String) -> PyResult<PyIcebergTableProvider> {
    PyIcebergTableProvider::new(metadata_location)
}

/// Register the module
pub fn register_module(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    let submod = PyModule::new_bound(py, "table_provider")?;
    submod.add_function(wrap_pyfunction!(create_table_provider, &submod)?)?;

    // Add as submodule and register in sys.modules
    m.add_submodule(&submod)?;
    py.import_bound("sys")?
        .getattr("modules")?
        .set_item("pyiceberg_core.table_provider", submod)?;

    Ok(())
}
