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
use typed_builder::TypedBuilder;

use crate::expr::BoundPredicate;
use crate::spec::{
    DataContentType, DataFileFormat, ManifestEntryRef, NameMapping, PartitionSpec, Schema,
    SchemaRef, Struct, StructType,
};
use crate::{Error, ErrorKind, Result};

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
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, TypedBuilder)]
#[builder(
    field_defaults(setter(prefix = "with_")),
    build_method(into = Result<FileScanTask>)
)]
pub struct FileScanTask {
    /// The total size of the data file in bytes, from the manifest entry.
    /// Used to skip a stat/HEAD request when reading Parquet footers.
    file_size_in_bytes: u64,
    /// The start offset of the file to scan.
    start: u64,
    /// The length of the file to scan.
    length: u64,
    /// The number of records in the file to scan.
    ///
    /// This is an optional field, and only available if we are
    /// reading the entire data file.
    #[builder(default)]
    record_count: Option<u64>,

    /// The first row id assigned to the data file.
    ///
    /// Used to derive the `_row_id` metadata column: for a row without an
    /// explicit `_row_id`, it is this value plus the row's ordinal position.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    first_row_id: Option<i64>,

    /// The data sequence number of the file, as opposed to its file sequence
    /// number: the sequence number preserved when a file is carried forward
    /// across a rewrite. May be null for an existing entry in a malformed
    /// manifest that lacks one.
    ///
    /// Used to derive the `_last_updated_sequence_number` metadata column.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    data_sequence_number: Option<i64>,

    /// The data file path corresponding to the task.
    data_file_path: String,

    /// The format of the file to scan.
    data_file_format: DataFileFormat,

    /// The schema of the file to scan.
    schema: SchemaRef,
    /// The field ids to project.
    project_field_ids: Vec<i32>,
    /// The predicate to filter.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    predicate: Option<BoundPredicate>,

    /// The list of delete files that may need to be applied to this data file
    #[builder(default)]
    deletes: Vec<FileScanTaskDeleteFile>,

    /// Partition data from the manifest entry, used to identify which columns can use
    /// constant values from partition metadata vs. reading from the data file.
    /// Per the Iceberg spec, only identity-transformed partition fields should use constants.
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(serialize_with = "serialize_not_implemented")]
    #[serde(deserialize_with = "deserialize_not_implemented")]
    #[builder(default)]
    partition: Option<Struct>,

    /// The partition spec for this file, used to distinguish identity transforms
    /// (which use partition metadata constants) from non-identity transforms like
    /// bucket/truncate (which must read source columns from the data file).
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(serialize_with = "serialize_not_implemented")]
    #[serde(deserialize_with = "deserialize_not_implemented")]
    #[builder(default)]
    partition_spec: Option<Arc<PartitionSpec>>,

    /// Name mapping from table metadata (property: schema.name-mapping.default),
    /// used to resolve field IDs from column names when Parquet files lack field IDs
    /// or have field ID conflicts.
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(serialize_with = "serialize_not_implemented")]
    #[serde(deserialize_with = "deserialize_not_implemented")]
    #[builder(default)]
    name_mapping: Option<Arc<NameMapping>>,

    /// The unified partition type across all specs in the table.
    /// When `RESERVED_FIELD_ID_PARTITION` is in the projected field IDs, the reader
    /// uses this type along with the task's partition_spec and partition data to
    /// materialize the `_partition` struct column at read time.
    ///
    /// This is a table-level value (same for all tasks in a scan), stored per-task
    /// so that readers are self-contained without needing back-pointers to table
    /// metadata. The cost is one Arc clone per task.
    /// Serde: not yet implemented (same pattern as partition, partition_spec, name_mapping).
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(serialize_with = "serialize_not_implemented")]
    #[serde(deserialize_with = "deserialize_not_implemented")]
    #[builder(default)]
    unified_partition_type: Option<Arc<StructType>>,

    /// Whether this scan task should treat column names as case-sensitive when binding predicates.
    case_sensitive: bool,

    /// Key metadata for encrypted data files (Parquet Modular Encryption).
    /// When present, the reader uses this to build `FileDecryptionProperties`.
    ///
    /// Note on the trust boundary: for the standard encryption scheme this
    /// carries `StandardKeyMetadata`, whose payload is the *plaintext* DEK.
    /// Because `FileScanTask` derives `Serialize`, that plaintext DEK is part
    /// of the serialized scan plan should these tasks ever be serialized and sent
    /// over the network.
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    key_metadata: Option<Box<[u8]>>,
}

impl FileScanTask {
    /// Returns the total size of the data file in bytes.
    pub fn file_size_in_bytes(&self) -> u64 {
        self.file_size_in_bytes
    }

    /// Returns the start offset of the file to scan.
    pub fn start(&self) -> u64 {
        self.start
    }

    /// Returns the length of the file to scan.
    pub fn length(&self) -> u64 {
        self.length
    }

    /// Returns the number of records in the file when the whole file is scanned.
    pub fn record_count(&self) -> Option<u64> {
        self.record_count
    }

    /// Returns the first row id assigned to the data file.
    pub fn first_row_id(&self) -> Option<i64> {
        self.first_row_id
    }

    /// Returns the data sequence number of the file.
    pub fn data_sequence_number(&self) -> Option<i64> {
        self.data_sequence_number
    }

    /// Returns the data file path of this file scan task.
    pub fn data_file_path(&self) -> &str {
        &self.data_file_path
    }

    /// Returns the format of the data file.
    pub fn data_file_format(&self) -> DataFileFormat {
        self.data_file_format
    }

    /// Returns the schema of this file scan task as a reference.
    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    /// Returns the schema of this file scan task as a [`SchemaRef`].
    pub fn schema_ref(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Returns the project field id of this file scan task.
    pub fn project_field_ids(&self) -> &[i32] {
        &self.project_field_ids
    }

    /// Returns the predicate of this file scan task.
    pub fn predicate(&self) -> Option<&BoundPredicate> {
        self.predicate.as_ref()
    }

    /// Returns the delete files that may need to be applied to the data file.
    pub fn deletes(&self) -> &[FileScanTaskDeleteFile] {
        &self.deletes
    }

    /// Returns the partition data from the manifest entry.
    pub fn partition(&self) -> Option<&Struct> {
        self.partition.as_ref()
    }

    /// Returns the partition spec for the data file.
    pub fn partition_spec(&self) -> Option<&Arc<PartitionSpec>> {
        self.partition_spec.as_ref()
    }

    /// Returns the name mapping used to resolve field ids.
    pub fn name_mapping(&self) -> Option<&Arc<NameMapping>> {
        self.name_mapping.as_ref()
    }

    /// Returns the unified partition type across all table partition specs.
    pub fn unified_partition_type(&self) -> Option<&Arc<StructType>> {
        self.unified_partition_type.as_ref()
    }

    /// Returns whether names are treated as case-sensitive.
    pub fn case_sensitive(&self) -> bool {
        self.case_sensitive
    }

    /// Returns the key metadata for the encrypted data file.
    pub fn key_metadata(&self) -> Option<&[u8]> {
        self.key_metadata.as_deref()
    }

    fn validate(&self) -> Result<()> {
        if self.partition_spec.is_none()
            && self
                .partition
                .as_ref()
                .is_some_and(|partition| !partition.fields().is_empty())
        {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Non-empty FileScanTask partition requires a partition spec",
            ));
        }

        Ok(())
    }
}

impl From<FileScanTask> for Result<FileScanTask> {
    fn from(task: FileScanTask) -> Self {
        task.validate()?;
        Ok(task)
    }
}

#[derive(Debug)]
pub(crate) struct DeleteFileContext {
    pub(crate) manifest_entry: ManifestEntryRef,
    pub(crate) partition_spec_id: i32,
}

impl From<&DeleteFileContext> for FileScanTaskDeleteFile {
    fn from(ctx: &DeleteFileContext) -> Self {
        FileScanTaskDeleteFile::builder()
            .with_file_path(ctx.manifest_entry.file_path().to_string())
            .with_file_size_in_bytes(ctx.manifest_entry.file_size_in_bytes())
            .with_file_type(ctx.manifest_entry.content_type())
            .with_partition_spec_id(ctx.partition_spec_id)
            .with_equality_ids(ctx.manifest_entry.data_file.equality_ids.clone())
            .with_referenced_data_file(ctx.manifest_entry.data_file.referenced_data_file.clone())
            .with_content_offset(ctx.manifest_entry.data_file.content_offset)
            .with_content_size_in_bytes(ctx.manifest_entry.data_file.content_size_in_bytes)
            .with_record_count(Some(ctx.manifest_entry.record_count()))
            .with_key_metadata(
                ctx.manifest_entry
                    .data_file
                    .key_metadata
                    .as_deref()
                    .map(Box::from),
            )
            .build()
    }
}

/// A task to scan part of file.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, TypedBuilder)]
#[builder(field_defaults(setter(prefix = "with_")))]
pub struct FileScanTaskDeleteFile {
    /// The delete file path
    pub file_path: String,

    /// The total size of the delete file in bytes, from the manifest entry.
    pub file_size_in_bytes: u64,

    /// delete file type
    pub file_type: DataContentType,

    /// partition id
    pub partition_spec_id: i32,

    /// equality ids for equality deletes (null for anything other than equality-deletes)
    #[builder(default)]
    pub equality_ids: Option<Vec<i32>>,

    /// For a deletion vector, the location of the data file whose rows it deletes. Required for
    /// deletion vectors, and may also be set on a position delete file scoped to one data file.
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    pub referenced_data_file: Option<String>,

    /// For a deletion vector, the offset of the blob within its Puffin file. Set only for
    /// deletion vectors, where it locates the blob for direct access.
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    pub content_offset: Option<i64>,

    /// For a deletion vector, the length in bytes of the blob within its Puffin file.
    /// Required together with `content_offset`; both are absent for non-DV delete files.
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    pub content_size_in_bytes: Option<i64>,

    /// The number of records in the delete file, from the manifest entry; for a deletion vector,
    /// the cardinality of its bitmap. `None` only for a task not built from a manifest entry.
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    pub record_count: Option<u64>,

    /// Key metadata for encrypted delete files (Parquet Modular Encryption).
    /// When present, the reader uses this to build `FileDecryptionProperties`.
    ///
    /// Same plaintext-DEK trust boundary as [`FileScanTask::key_metadata`]:
    /// this is serialized into the scan plan and crosses the planner -> worker
    /// channel in the clear for the standard encryption scheme.
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    pub key_metadata: Option<Box<[u8]>>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ErrorKind;
    use crate::spec::Literal;

    fn build_file_scan_task_with_partition(partition: Struct) -> Result<FileScanTask> {
        FileScanTask::builder()
            .with_file_size_in_bytes(100)
            .with_start(0)
            .with_length(100)
            .with_data_file_path("data_file_path".to_string())
            .with_data_file_format(DataFileFormat::Parquet)
            .with_schema(Arc::new(Schema::builder().build().unwrap()))
            .with_project_field_ids(vec![])
            .with_partition(Some(partition))
            .with_case_sensitive(false)
            .build()
    }

    #[test]
    fn test_file_scan_task_builder_rejects_non_empty_partition_without_spec() {
        // Regression test for https://github.com/apache/iceberg-rust/issues/3130.
        let err = build_file_scan_task_with_partition(Struct::from_iter([Some(Literal::long(42))]))
            .unwrap_err();

        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert_eq!(
            err.message(),
            "Non-empty FileScanTask partition requires a partition spec"
        );
    }

    #[test]
    fn test_file_scan_task_builder_accepts_empty_partition_without_spec() {
        build_file_scan_task_with_partition(Struct::empty()).unwrap();
    }
}
