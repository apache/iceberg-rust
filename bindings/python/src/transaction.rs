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
use std::sync::Arc;

use iceberg::io::FileIOBuilder;
use iceberg::spec::TableMetadata;
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::file_writer::ParquetWriter;
use iceberg::{Error, ErrorKind, Runtime, TableIdent};
use iceberg_storage_opendal::OpenDalResolvingStorageFactory;
use pyo3::prelude::*;

use crate::error::to_py_err;

/// Registers existing Parquet files in a table with one fast append, without rewriting them.
///
/// Reads each file's footer (record count, column sizes, value and null counts, bounds), writes
/// the manifest and the manifest list next to the table, and returns the commit as JSON:
/// `{"requirements": [...], "updates": [...]}`, the body of a REST `CommitTableRequest` without
/// the identifier. Nothing is committed: the caller sends it through its own catalog client and,
/// on a conflict, calls this again with the refreshed table. Works for format versions 1 to 3
/// (on v3 the snapshot carries its `first-row-id`). Unpartitioned tables only.
#[pyfunction]
#[pyo3(signature = (identifier, metadata_location, table_metadata, file_paths, io_properties, snapshot_properties=None))]
fn add_files(
    py: Python<'_>,
    identifier: Vec<String>,
    metadata_location: String,
    table_metadata: &str,
    file_paths: Vec<String>,
    io_properties: HashMap<String, String>,
    snapshot_properties: Option<HashMap<String, String>>,
) -> PyResult<String> {
    let metadata: TableMetadata = serde_json::from_str(table_metadata).map_err(|e| {
        to_py_err(
            Error::new(
                ErrorKind::DataInvalid,
                "table_metadata is not valid table metadata JSON",
            )
            .with_source(e),
        )
    })?;
    if metadata
        .default_partition_spec()
        .fields()
        .iter()
        .next()
        .is_some()
    {
        return Err(to_py_err(Error::new(
            ErrorKind::FeatureUnsupported,
            "add_files supports unpartitioned tables only",
        )));
    }
    let ident = TableIdent::from_strs(identifier).map_err(to_py_err)?;
    py.detach(|| {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| {
                to_py_err(
                    Error::new(ErrorKind::Unexpected, "cannot start a runtime").with_source(e),
                )
            })?;
        runtime
            .block_on(async move {
                let file_io = FileIOBuilder::new(Arc::new(OpenDalResolvingStorageFactory::new()))
                    .with_props(io_properties)
                    .build();
                let table = Table::builder()
                    .identifier(ident)
                    .metadata(metadata)
                    .metadata_location(metadata_location)
                    .file_io(file_io)
                    .runtime(Runtime::current())
                    .build()?;
                let data_files = ParquetWriter::parquet_files_to_data_files(
                    table.file_io(),
                    file_paths,
                    table.metadata(),
                )
                .await?;
                let tx = Transaction::new(&table);
                let tx = tx
                    .fast_append()
                    .add_data_files(data_files)
                    .set_snapshot_properties(snapshot_properties.unwrap_or_default())
                    .apply(tx)?;
                let mut commit = tx.build_commit().await?;
                let body = serde_json::json!({
                    "requirements": commit.take_requirements(),
                    "updates": commit.take_updates(),
                });
                Ok(body.to_string())
            })
            .map_err(to_py_err)
    })
}

pub fn register_module(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    let this = PyModule::new(py, "transaction")?;

    this.add_function(wrap_pyfunction!(add_files, &this)?)?;

    m.add_submodule(&this)?;
    py.import("sys")?
        .getattr("modules")?
        .set_item("pyiceberg_core.transaction", this)
}
