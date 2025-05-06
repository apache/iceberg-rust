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

use futures::stream::BoxStream;
use serde::{Deserialize, Serialize};

use crate::expr::BoundPredicate;
use crate::spec::{DataContentType, DataFileFormat, ManifestEntryRef, Schema, SchemaRef};
use crate::Result;

/// A stream of [`FileScanTask`].
pub type FileScanTaskStream = BoxStream<'static, Result<FileScanTask>>;

/// A task to scan part of file.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FileScanTask {
    /// The start offset of the file to scan.
    pub start: u64,
    /// The length of the file to scan.
    pub length: u64,
    /// The number of records in the file to scan.
    ///
    /// This is an optional field, and only available if we are
    /// reading the entire data file.
    pub record_count: Option<u64>,

    /// The data file path corresponding to the task.
    pub data_file_path: String,

    /// The content type of the file to scan.
    pub data_file_content: DataContentType,

    /// The format of the file to scan.
    pub data_file_format: DataFileFormat,

    /// The schema of the file to scan.
    pub schema: SchemaRef,
    /// The field ids to project.
    pub project_field_ids: Vec<i32>,
    /// The predicate to filter.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub predicate: Option<BoundPredicate>,

    /// The list of delete files that may need to be applied to this data file
    pub deletes: Vec<FileScanTaskDeleteFile>,
}

impl FileScanTask {
    /// Returns the data file path of this file scan task.
    pub fn data_file_path(&self) -> &str {
        &self.data_file_path
    }

    /// Returns the project field id of this file scan task.
    pub fn project_field_ids(&self) -> &[i32] {
        &self.project_field_ids
    }

    /// Returns the predicate of this file scan task.
    pub fn predicate(&self) -> Option<&BoundPredicate> {
        self.predicate.as_ref()
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

#[derive(Debug)]
pub(crate) struct DeleteFileContext {
    pub(crate) manifest_entry: ManifestEntryRef,
    pub(crate) partition_spec_id: i32,
}

impl From<&DeleteFileContext> for FileScanTaskDeleteFile {
    fn from(ctx: &DeleteFileContext) -> Self {
        FileScanTaskDeleteFile {
            file_path: ctx.manifest_entry.file_path().to_string(),
            file_type: ctx.manifest_entry.content_type(),
            partition_spec_id: ctx.partition_spec_id,
            equality_ids: ctx.manifest_entry.data_file.equality_ids.clone(),
        }
    }
}

/// A task to scan part of file.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FileScanTaskDeleteFile {
    /// The delete file path
    pub file_path: String,

    /// delete file type
    pub file_type: DataContentType,

    /// partition id
    pub partition_spec_id: i32,

    /// equality ids for equality deletes (empty for positional deletes)
    pub equality_ids: Vec<i32>,
}
