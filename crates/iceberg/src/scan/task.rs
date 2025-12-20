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
        let data_file = &ctx.manifest_entry.data_file;
        FileScanTaskDeleteFile {
            file_path: ctx.manifest_entry.file_path().to_string(),
            file_type: ctx.manifest_entry.content_type(),
            partition_spec_id: ctx.partition_spec_id,
            equality_ids: data_file.equality_ids.clone(),
            // Deletion vector fields from DataFile
            referenced_data_file: data_file.referenced_data_file.clone(),
            content_offset: data_file.content_offset,
            content_size_in_bytes: data_file.content_size_in_bytes,
        }
    }
}

/// A task to scan part of a delete file.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FileScanTaskDeleteFile {
    /// The delete file path
    pub file_path: String,

    /// Delete file type (PositionDeletes or EqualityDeletes)
    pub file_type: DataContentType,

    /// Partition spec id
    pub partition_spec_id: i32,

    /// Equality ids for equality deletes (None for positional deletes and deletion vectors)
    pub equality_ids: Option<Vec<i32>>,

    /// Referenced data file for deletion vectors.
    /// When set along with content_offset and content_size_in_bytes, indicates this is a deletion vector.
    pub referenced_data_file: Option<String>,

    /// Content offset in the Puffin file for deletion vectors.
    pub content_offset: Option<i64>,

    /// Content size in bytes for deletion vectors.
    pub content_size_in_bytes: Option<i64>,
}

impl FileScanTaskDeleteFile {
    /// Returns true if this delete file is a deletion vector stored in a Puffin file.
    ///
    /// Deletion vectors are identified by having both content_offset and referenced_data_file set.
    pub fn is_deletion_vector(&self) -> bool {
        self.content_offset.is_some() && self.referenced_data_file.is_some()
    }
}
