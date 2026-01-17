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

use std::sync::Arc;

use futures::stream::BoxStream;
use serde::{Deserialize, Serialize, Serializer};

use crate::Result;
use crate::expr::BoundPredicate;
use crate::spec::{
    DataContentType, DataFileFormat, ManifestEntryRef, NameMapping, PartitionSpec, Schema,
    SchemaRef, Struct,
};

/// A stream of [`FileScanTask`].
pub type FileScanTaskStream = BoxStream<'static, Result<FileScanTask>>;

/// Serialization helper that always returns NotImplementedError.
/// Used for fields that should not be serialized but we want to be explicit about it.
fn serialize_not_implemented<S, T>(_: &T, _: S) -> std::result::Result<S::Ok, S::Error>
where S: Serializer {
    Err(serde::ser::Error::custom(
        "Serialization not implemented for this field",
    ))
}

/// Deserialization helper that always returns NotImplementedError.
/// Used for fields that should not be deserialized but we want to be explicit about it.
fn deserialize_not_implemented<'de, D, T>(_: D) -> std::result::Result<T, D::Error>
where D: serde::Deserializer<'de> {
    Err(serde::de::Error::custom(
        "Deserialization not implemented for this field",
    ))
}

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

    /// Partition data from the manifest entry, used to identify which columns can use
    /// constant values from partition metadata vs. reading from the data file.
    /// Per the Iceberg spec, only identity-transformed partition fields should use constants.
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(serialize_with = "serialize_not_implemented")]
    #[serde(deserialize_with = "deserialize_not_implemented")]
    pub partition: Option<Struct>,

    /// The partition spec for this file, used to distinguish identity transforms
    /// (which use partition metadata constants) from non-identity transforms like
    /// bucket/truncate (which must read source columns from the data file).
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(serialize_with = "serialize_not_implemented")]
    #[serde(deserialize_with = "deserialize_not_implemented")]
    pub partition_spec: Option<Arc<PartitionSpec>>,

    /// Name mapping from table metadata (property: schema.name-mapping.default),
    /// used to resolve field IDs from column names when Parquet files lack field IDs
    /// or have field ID conflicts.
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(serialize_with = "serialize_not_implemented")]
    #[serde(deserialize_with = "deserialize_not_implemented")]
    pub name_mapping: Option<Arc<NameMapping>>,

    /// Whether this scan task should treat column names as case-sensitive when binding predicates.
    pub case_sensitive: bool,
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

    /// equality ids for equality deletes (null for anything other than equality-deletes)
    pub equality_ids: Option<Vec<i32>>,
}

/// A combined scan task that groups multiple [`FileScanTask`]s together.
///
/// This is the result of the bin-packing algorithm that combines small file
/// scan tasks into larger groups for more efficient execution. Each combined
/// task represents a unit of work that can be processed together.
///
/// The grouping considers:
/// - Target split size: Tasks are combined until reaching the target size
/// - Open file cost: The overhead of opening each file is factored into sizing
/// - Lookback: Multiple open bins are maintained for better packing
#[derive(Debug, Clone)]
pub struct CombinedScanTask {
    /// The individual file scan tasks in this combined task
    tasks: Vec<FileScanTask>,
    /// Total estimated size in bytes (including open file costs)
    estimated_size: u64,
}

impl CombinedScanTask {
    /// Creates a new empty combined scan task.
    pub fn new() -> Self {
        Self {
            tasks: Vec::new(),
            estimated_size: 0,
        }
    }

    /// Creates a new combined scan task with the given tasks and estimated size.
    pub fn with_tasks(tasks: Vec<FileScanTask>, estimated_size: u64) -> Self {
        Self {
            tasks,
            estimated_size,
        }
    }

    /// Returns the file scan tasks in this combined task.
    pub fn tasks(&self) -> &[FileScanTask] {
        &self.tasks
    }

    /// Consumes this combined task and returns the underlying file scan tasks.
    pub fn into_tasks(self) -> Vec<FileScanTask> {
        self.tasks
    }

    /// Returns the number of file scan tasks in this combined task.
    pub fn len(&self) -> usize {
        self.tasks.len()
    }

    /// Returns true if this combined task has no file scan tasks.
    pub fn is_empty(&self) -> bool {
        self.tasks.is_empty()
    }

    /// Returns the total estimated size in bytes.
    pub fn estimated_size(&self) -> u64 {
        self.estimated_size
    }

    /// Returns the total number of files referenced by tasks in this combined task.
    /// Note: Multiple tasks may reference the same file (splits), so this counts
    /// unique file paths.
    pub fn files_count(&self) -> usize {
        use std::collections::HashSet;
        self.tasks
            .iter()
            .map(|t| &t.data_file_path)
            .collect::<HashSet<_>>()
            .len()
    }

    /// Adds a file scan task to this combined task.
    pub(crate) fn add_task(&mut self, task: FileScanTask, weight: u64) {
        self.tasks.push(task);
        self.estimated_size += weight;
    }

    /// Returns true if adding a task with the given weight would exceed the target size.
    pub(crate) fn would_exceed(&self, weight: u64, target_size: u64) -> bool {
        self.estimated_size + weight > target_size
    }
}

impl Default for CombinedScanTask {
    fn default() -> Self {
        Self::new()
    }
}

/// A stream of [`CombinedScanTask`].
pub type CombinedScanTaskStream = BoxStream<'static, Result<CombinedScanTask>>;
