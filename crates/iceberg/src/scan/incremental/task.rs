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

use std::sync::{Arc, Mutex};

use futures::stream::BoxStream;

use crate::Result;
use crate::arrow::delete_filter::DeleteFilter;
use crate::delete_vector::DeleteVector;
use crate::scan::context::ManifestEntryContext;
use crate::spec::{DataFileFormat, Schema, SchemaRef};

/// A file scan task for appended data files in an incremental scan.
#[derive(Debug, Clone)]
pub struct AppendedFileScanTask {
    /// The start offset of the file to scan.
    pub start: u64,
    /// The length of the file to scan.
    pub length: u64,
    /// The number of records in the file.
    pub record_count: Option<u64>,
    /// The path to the data file to scan.
    pub data_file_path: String,
    /// The format of the data file to scan.
    pub data_file_format: DataFileFormat,
    /// The schema of the data file to scan.
    pub schema: crate::spec::SchemaRef,
    /// The field ids to project.
    pub project_field_ids: Vec<i32>,
    /// The optional positional deletes associated with this data file.
    pub positional_deletes: Option<Arc<Mutex<DeleteVector>>>,
}

impl AppendedFileScanTask {
    /// Returns the data file path of this appended file scan task.
    pub fn data_file_path(&self) -> &str {
        &self.data_file_path
    }

    /// Returns the schema of this file scan task as a reference
    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    /// Returns the schema of this file scan task as a SchemaRef
    pub fn schema_ref(&self) -> SchemaRef {
        self.schema.clone()
    }
}

/// The stream of incremental file scan tasks.
pub type IncrementalFileScanTaskStream = BoxStream<'static, Result<IncrementalFileScanTask>>;

/// An incremental file scan task, which can be either an appended data file or positional
/// deletes.
#[derive(Debug, Clone)]
pub enum IncrementalFileScanTask {
    /// An appended data file.
    Append(AppendedFileScanTask),
    /// Deleted records of a data file. First argument is the file path, second the delete
    /// vector.
    Delete(String, DeleteVector),
}

impl IncrementalFileScanTask {
    /// Create an `IncrementalFileScanTask::Append` from a `ManifestEntryContext` and `DeleteFilter`.
    pub(crate) fn append_from_manifest_entry(
        manifest_entry_context: &ManifestEntryContext,
        delete_filter: &DeleteFilter,
    ) -> Self {
        let data_file_path = manifest_entry_context.manifest_entry.file_path();
        IncrementalFileScanTask::Append(AppendedFileScanTask {
            start: 0,
            length: manifest_entry_context.manifest_entry.file_size_in_bytes(),
            record_count: Some(manifest_entry_context.manifest_entry.record_count()),
            data_file_path: data_file_path.to_string(),
            data_file_format: manifest_entry_context.manifest_entry.file_format(),
            schema: manifest_entry_context.snapshot_schema.clone(),
            project_field_ids: manifest_entry_context.field_ids.as_ref().clone(),
            positional_deletes: delete_filter.get_delete_vector_for_path(data_file_path),
        })
    }

    /// Returns the data file path of this incremental file scan task.
    pub fn data_file_path(&self) -> &str {
        match self {
            IncrementalFileScanTask::Append(task) => task.data_file_path(),
            IncrementalFileScanTask::Delete(path, _) => path,
        }
    }
}
