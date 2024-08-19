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

use iceberg::io::FileIOBuilder;
use pyo3::prelude::*;
use pyo3::wrap_pyfunction;

mod transform;

#[pyfunction]
fn hello_world() -> PyResult<String> {
    let _ = FileIOBuilder::new_fs_io().build().unwrap();
    Ok("Hello, world!".to_string())
}

#[pymodule]
fn submodule(_py: Python, module: &Bound<'_, PyModule>) -> PyResult<()> {
    use transform::bucket_transform;

    module.add_wrapped(wrap_pyfunction!(bucket_transform))?;
    Ok(())
}

#[pymodule]
fn pyiceberg_core_rust(py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(hello_world, m)?)?;

    // https://github.com/PyO3/pyo3/issues/759
    // Submodules added through PyO3 cannot be imported in Python using
    // the syntax: 'from parent.child import function'. 
    // We need to add the submodule in sys.modules manually so that 
    // Python can find it.
    let child_module = PyModule::new_bound(py, "pyiceberg_core.transform")?;
    submodule(py, &child_module)?;
    m.add("transform", child_module.clone())?;
    py.import_bound("sys")?.getattr("modules")?.set_item("pyiceberg_core.transform", child_module)?;
    Ok(())
}
