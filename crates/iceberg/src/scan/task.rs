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

/// A stream of [`ChangelogScanTask`].
pub type ChangelogScanTaskStream = BoxStream<'static, Result<ChangelogScanTask>>;

/// The kind of row-level change a [`ChangelogScanTask`] produces.
///
/// Ports Java `org.apache.iceberg.ChangelogOperation`. Java's enum also declares
/// `UPDATE_BEFORE` / `UPDATE_AFTER` for change-data-capture *merging* (collapsing a
/// delete+insert at the same row key into an update pair) — those are produced by a
/// higher-level CDC merge step, NOT by the data-file changelog scan, which only
/// distinguishes whole-file additions from whole-file removals. This scan therefore
/// models only the two operations it can emit; the update variants are intentionally
/// omitted until a CDC-merge layer that needs them lands.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChangelogOperation {
    /// Rows were INSERTED — the task's data file was ADDED by its commit snapshot.
    Insert,
    /// Rows were DELETED — the task's data file was REMOVED by its commit snapshot.
    Delete,
}

/// A changelog scan task: the row-level changes (all inserts or all deletes) carried by
/// a single data file that one snapshot in the changelog range added or removed.
///
/// Ports Java `ChangelogScanTask` + its `BaseAddedRowsScanTask` /
/// `BaseDeletedDataFileScanTask` implementations. A task embeds the [`FileScanTask`]
/// that reads the underlying data file (path, schema, projection, residual predicate)
/// and tags it with the change metadata: the [`operation`](Self::operation) (insert vs
/// delete), the [`change_ordinal`](Self::change_ordinal) (0 for the oldest snapshot in
/// the range, incrementing — changes with a lower ordinal must be applied first), and
/// the [`commit_snapshot_id`](Self::commit_snapshot_id) (the snapshot that committed the
/// change).
///
/// Like Java's current data-file changelog, a task carries NO delete files — the scan
/// rejects a range that contains row-level delete manifests (see
/// [`IncrementalChangelogScan`](super::IncrementalChangelogScan)).
#[derive(Debug, Clone, PartialEq)]
pub struct ChangelogScanTask {
    /// The change ordinal: `0` for the oldest snapshot in the range, incrementing for
    /// each newer snapshot. Changes with a lower ordinal must be applied first (Java
    /// `ChangelogScanTask.changeOrdinal()`).
    pub change_ordinal: i32,
    /// The id of the snapshot that committed this change (Java
    /// `ChangelogScanTask.commitSnapshotId()`).
    pub commit_snapshot_id: i64,
    /// Whether this task's rows are insertions or deletions (Java
    /// `ChangelogScanTask.operation()`).
    pub operation: ChangelogOperation,
    /// The underlying file scan task that reads the data file whose rows changed.
    pub file_scan_task: FileScanTask,
}

impl ChangelogScanTask {
    /// Returns the kind of change (insert / delete) this task produces.
    pub fn operation(&self) -> ChangelogOperation {
        self.operation
    }

    /// Returns the change ordinal — changes with a lower ordinal must be applied first.
    pub fn change_ordinal(&self) -> i32 {
        self.change_ordinal
    }

    /// Returns the id of the snapshot that committed this change.
    pub fn commit_snapshot_id(&self) -> i64 {
        self.commit_snapshot_id
    }

    /// Returns the underlying [`FileScanTask`] that reads the changed data file.
    pub fn file_scan_task(&self) -> &FileScanTask {
        &self.file_scan_task
    }

    /// Returns the data file path of the changed file.
    pub fn data_file_path(&self) -> &str {
        &self.file_scan_task.data_file_path
    }
}

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
    /// The total size of the data file in bytes, from the manifest entry.
    /// Used to skip a stat/HEAD request when reading Parquet footers.
    pub file_size_in_bytes: u64,
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
            file_size_in_bytes: ctx.manifest_entry.file_size_in_bytes(),
            file_type: ctx.manifest_entry.content_type(),
            partition_spec_id: ctx.partition_spec_id,
            equality_ids: ctx.manifest_entry.data_file.equality_ids.clone(),
            file_format: ctx.manifest_entry.data_file.file_format,
            referenced_data_file: ctx.manifest_entry.data_file.referenced_data_file.clone(),
            content_offset: ctx.manifest_entry.data_file.content_offset,
            content_size_in_bytes: ctx.manifest_entry.data_file.content_size_in_bytes,
            record_count: Some(ctx.manifest_entry.data_file.record_count),
        }
    }
}

/// The format a [`FileScanTaskDeleteFile`] deserialized from a pre-deletion-vector
/// serialization defaults to: every delete file was a parquet file before Puffin deletion
/// vectors existed, so absent means parquet.
fn default_delete_file_format() -> DataFileFormat {
    DataFileFormat::Parquet
}

/// A task to scan part of file.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
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
    pub equality_ids: Option<Vec<i32>>,

    /// The on-disk format of the delete file. This is the deletion-vector discriminator Java
    /// uses (`ContentFileUtil.isDV`: `deleteFile.format() == FileFormat.PUFFIN`): a
    /// position-delete entry whose format is [`DataFileFormat::Puffin`] is a deletion vector and
    /// must be loaded from its Puffin blob, never the parquet reader.
    #[serde(default = "default_delete_file_format")]
    pub file_format: DataFileFormat,

    /// The data file path a deletion vector (or file-scoped position delete) applies to, from
    /// the manifest entry's `referenced_data_file`. A loaded deletion vector is keyed by THIS
    /// path — required for deletion vectors.
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub referenced_data_file: Option<String>,

    /// Offset of the `deletion-vector-v1` blob within the Puffin file, from the manifest
    /// entry's `content_offset`; required for deletion vectors.
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_offset: Option<i64>,

    /// Length of the `deletion-vector-v1` blob in bytes, from the manifest entry's
    /// `content_size_in_bytes`; required for deletion vectors.
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_size_in_bytes: Option<i64>,

    /// The record count from the manifest entry. For a deletion vector this is its cardinality
    /// (the number of deleted positions) and is validated against the decoded bitmap, mirroring
    /// Java `BitmapPositionDeleteIndex.deserializeBitmap`'s "Invalid cardinality" check.
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub record_count: Option<u64>,
}
