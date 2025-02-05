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

/*!
 * Snapshots
 */
use std::collections::HashMap;
use std::sync::Arc;

use _serde::SnapshotV2;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use typed_builder::TypedBuilder;

use super::table_metadata::SnapshotLog;
use crate::error::{timestamp_ms_to_utc, Result};
use crate::io::FileIO;
use crate::spec::{ManifestList, SchemaId, SchemaRef, StructType, TableMetadata};
use crate::{Error, ErrorKind};

/// The ref name of the main branch of the table.
pub const MAIN_BRANCH: &str = "main";
/// Placeholder for snapshot ID. The field with this value must be replaced with the actual snapshot ID before it is committed.
pub const UNASSIGNED_SNAPSHOT_ID: i64 = -1;

/// Reference to [`Snapshot`].
pub type SnapshotRef = Arc<Snapshot>;
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(rename_all = "lowercase")]
/// The operation field is used by some operations, like snapshot expiration, to skip processing certain snapshots.
pub enum Operation {
    /// Only data files were added and no files were removed.
    Append,
    /// Data and delete files were added and removed without changing table data;
    /// i.e., compaction, changing the data file format, or relocating data files.
    Replace,
    /// Data and delete files were added and removed in a logical overwrite operation.
    Overwrite,
    /// Data files were removed and their contents logically deleted and/or delete files were added to delete rows.
    Delete,
}

impl Operation {
    /// Returns the string representation (lowercase) of the operation.
    pub fn as_str(&self) -> &str {
        match self {
            Operation::Append => "append",
            Operation::Replace => "replace",
            Operation::Overwrite => "overwrite",
            Operation::Delete => "delete",
        }
    }
}

/// Summarizes the changes in the snapshot.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone, Default)]
#[serde(default)]
pub struct SnapshotSummary {
    /// The type of operation in the snapshot
    pub operation: Operation,

    /// Number of added data files
    pub added_data_files: u64,
    /// Number of deleted data files
    pub deleted_data_files: u64,
    /// Total data files
    pub total_data_files: u64,

    /// Added delete files
    pub added_delete_files: u64,
    /// Removed delete files
    pub removed_delete_files: u64,
    /// Total delete files
    pub total_delete_files: u64,

    /// Number of added records
    pub added_records: u64,
    /// Number of deleted records
    pub deleted_records: u64,
    /// Total number of records
    pub total_records: u64,

    /// Total size of added files in bytes
    pub added_file_size: u64,
    /// Total size of removed files in bytes
    pub removed_file_size: u64,
    /// Total file size (bytes)
    pub total_file_size: u64,

    /// Number of added equality delete files
    pub added_eq_delete_files: u64,
    /// Number of removed equality delete files
    pub removed_eq_delete_files: u64,
    /// Total equality delete files
    pub total_eq_deletes: u64,

    /// Number of added position delete files
    pub added_pos_delete_files: u64,
    /// Number of removed position delete files
    pub removed_pos_delete_files: u64,
    /// Total position delete files
    pub total_pos_deletes: u64,

    /// Number of deleted duplicate files
    pub deleted_duplicate_files: u64,

    /// Number of partitions changed
    pub changed_partition_count: u64,
    /// Tracks changes per partition (partition identifier -> summary of changes)
    #[serde(default)]
    pub partitions: HashMap<String, String>,

    /// Other summary data (any custom properties)
    #[serde(flatten)]
    pub additional_properties: HashMap<String, String>,
}

// Update values macros
macro_rules! update_counters {
    (add, $self:ident, $files_field:ident, $total_files_field:ident,
     $added_size_field:ident, $total_size_field:ident, $size:expr) => {
        $self.$files_field = $self.$files_field.saturating_add(1);
        $self.$total_files_field = $self.$total_files_field.saturating_add(1);
        $self.$added_size_field = $self.$added_size_field.saturating_add($size);
        $self.$total_size_field = $self.$total_size_field.saturating_add($size);
    };

    (remove, $self:ident, $files_field:ident, $total_files_field:ident,
     $removed_size_field:ident, $total_size_field:ident, $size:expr) => {
        $self.$files_field = $self.$files_field.saturating_add(1);
        $self.$total_files_field = $self.$total_files_field.saturating_sub(1);
        $self.$removed_size_field = $self.$removed_size_field.saturating_add($size);
        $self.$total_size_field = $self.$total_size_field.saturating_sub($size);
    };
}

impl SnapshotSummary {
    /// Creates a new `SnapshotSummary` instance.
    pub fn new(operation: Operation, properties: HashMap<String, String>) -> Self {
        Self {
            operation,
            added_data_files: 0,
            deleted_data_files: 0,
            total_data_files: 0,
            added_delete_files: 0,
            removed_delete_files: 0,
            total_delete_files: 0,
            added_records: 0,
            deleted_records: 0,
            total_records: 0,
            added_file_size: 0,
            removed_file_size: 0,
            total_file_size: 0,
            added_eq_delete_files: 0,
            removed_eq_delete_files: 0,
            total_eq_deletes: 0,
            added_pos_delete_files: 0,
            removed_pos_delete_files: 0,
            total_pos_deletes: 0,
            deleted_duplicate_files: 0,
            changed_partition_count: 0,
            partitions: HashMap::new(),
            additional_properties: properties,
        }
    }

    /// Adds a new data file and updates relevant counters.
    pub fn add_data_file(&mut self, size: u64) {
        update_counters!(
            add,
            self,
            added_data_files,
            total_data_files,
            added_file_size,
            total_file_size,
            size
        );
    }

    /// Deletes a data file and updates relevant counters.
    pub fn delete_data_file(&mut self, size: u64) {
        update_counters!(
            remove,
            self,
            deleted_data_files,
            total_data_files,
            removed_file_size,
            total_file_size,
            size
        );
    }

    /// Adds a delete file and updates relevant counters.
    pub fn add_delete_file(&mut self, size: u64) {
        update_counters!(
            add,
            self,
            added_delete_files,
            total_delete_files,
            added_file_size,
            total_file_size,
            size
        );
    }

    /// Removes a delete file and updates relevant counters.
    pub fn remove_delete_file(&mut self, size: u64) {
        update_counters!(
            remove,
            self,
            removed_delete_files,
            total_delete_files,
            removed_file_size,
            total_file_size,
            size
        );
    }

    /// Adds an equality delete file and updates relevant counters.
    pub fn add_eq_delete_file(&mut self, size: u64) {
        update_counters!(
            add,
            self,
            added_eq_delete_files,
            total_eq_deletes,
            added_file_size,
            total_file_size,
            size
        );
    }

    /// Removes an equality delete file and updates relevant counters.
    pub fn remove_eq_delete_file(&mut self, size: u64) {
        update_counters!(
            remove,
            self,
            removed_eq_delete_files,
            total_eq_deletes,
            removed_file_size,
            total_file_size,
            size
        );
    }

    /// Adds a positional delete file and updates relevant counters.
    pub fn add_pos_delete_file(&mut self, size: u64) {
        update_counters!(
            add,
            self,
            added_pos_delete_files,
            total_pos_deletes,
            added_file_size,
            total_file_size,
            size
        );
    }

    /// Removes a positional delete file and updates relevant counters.
    pub fn remove_pos_delete_file(&mut self, size: u64) {
        update_counters!(
            remove,
            self,
            removed_pos_delete_files,
            total_pos_deletes,
            removed_file_size,
            total_file_size,
            size
        );
    }

    /// Increments the counter for deleted duplicate files by one.
    pub fn increment_deleted_duplicate_files(&mut self) {
        self.deleted_duplicate_files = self.deleted_duplicate_files.saturating_add(1);
    }

    /// Increments the counter for deleted duplicate files by the specified amount.
    pub fn increment_deleted_duplicate_files_by(&mut self, count: u64) {
        self.deleted_duplicate_files = self.deleted_duplicate_files.saturating_add(count);
    }

    /// Adds records.
    pub fn add_records(&mut self, count: u64) {
        self.added_records = self.added_records.saturating_add(count);
        self.total_records = self.total_records.saturating_add(count);
    }

    /// Deletes records.
    pub fn delete_records(&mut self, count: u64) {
        self.deleted_records = self.deleted_records.saturating_add(count);
        self.total_records = self.total_records.saturating_sub(count);
    }

    /// Sets an additional property.
    pub fn set_property(&mut self, key: &str, value: &str) {
        self.additional_properties
            .insert(key.to_string(), value.to_string());
    }

    /// Tracks a partition change.
    pub fn add_partition_change(&mut self, partition: String, summary: String) {
        self.partitions.insert(partition, summary);
        self.changed_partition_count = self.changed_partition_count.saturating_add(1);
    }
}

impl Default for Operation {
    fn default() -> Operation {
        Self::Append
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize, TypedBuilder)]
#[serde(from = "SnapshotV2", into = "SnapshotV2")]
#[builder(field_defaults(setter(prefix = "with_")))]
/// A snapshot represents the state of a table at some time and is used to access the complete set of data files in the table.
pub struct Snapshot {
    /// A unique long ID
    snapshot_id: i64,
    /// The snapshot ID of the snapshot’s parent.
    /// Omitted for any snapshot with no parent
    #[builder(default = None)]
    parent_snapshot_id: Option<i64>,
    /// A monotonically increasing long that tracks the order of
    /// changes to a table.
    sequence_number: i64,
    /// A timestamp when the snapshot was created, used for garbage
    /// collection and table inspection
    timestamp_ms: i64,
    /// The location of a manifest list for this snapshot that
    /// tracks manifest files with additional metadata.
    /// Currently we only support manifest list file, and manifest files are not supported.
    #[builder(setter(into))]
    manifest_list: String,
    /// A string map that summarizes the snapshot changes, including operation.
    summary: SnapshotSummary,

    /// ID of the table’s current schema when the snapshot was created.
    #[builder(setter(strip_option(fallback = schema_id_opt)), default = None)]
    schema_id: Option<SchemaId>,
}

impl Snapshot {
    /// Get the id of the snapshot
    #[inline]
    pub fn snapshot_id(&self) -> i64 {
        self.snapshot_id
    }

    /// Get parent snapshot id.
    #[inline]
    pub fn parent_snapshot_id(&self) -> Option<i64> {
        self.parent_snapshot_id
    }

    /// Get sequence_number of the snapshot. Is 0 for Iceberg V1 tables.
    #[inline]
    pub fn sequence_number(&self) -> i64 {
        self.sequence_number
    }
    /// Get location of manifest_list file
    #[inline]
    pub fn manifest_list(&self) -> &str {
        &self.manifest_list
    }

    /// Get summary of the snapshot
    #[inline]
    pub fn summary(&self) -> &SnapshotSummary {
        &self.summary
    }
    /// Get the timestamp of when the snapshot was created
    #[inline]
    pub fn timestamp(&self) -> Result<DateTime<Utc>> {
        timestamp_ms_to_utc(self.timestamp_ms)
    }

    /// Get the timestamp of when the snapshot was created in milliseconds
    #[inline]
    pub fn timestamp_ms(&self) -> i64 {
        self.timestamp_ms
    }

    /// Get the schema id of this snapshot.
    #[inline]
    pub fn schema_id(&self) -> Option<SchemaId> {
        self.schema_id
    }

    /// Get the schema of this snapshot.
    pub fn schema(&self, table_metadata: &TableMetadata) -> Result<SchemaRef> {
        Ok(match self.schema_id() {
            Some(schema_id) => table_metadata
                .schema_by_id(schema_id)
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        format!("Schema with id {} not found", schema_id),
                    )
                })?
                .clone(),
            None => table_metadata.current_schema().clone(),
        })
    }

    /// Get parent snapshot.
    #[cfg(test)]
    pub(crate) fn parent_snapshot(&self, table_metadata: &TableMetadata) -> Option<SnapshotRef> {
        match self.parent_snapshot_id {
            Some(id) => table_metadata.snapshot_by_id(id).cloned(),
            None => None,
        }
    }

    /// Load manifest list.
    pub async fn load_manifest_list(
        &self,
        file_io: &FileIO,
        table_metadata: &TableMetadata,
    ) -> Result<ManifestList> {
        let manifest_list_content = file_io.new_input(&self.manifest_list)?.read().await?;

        let schema = self.schema(table_metadata)?;

        let partition_type_provider = |partition_spec_id: i32| -> Result<Option<StructType>> {
            table_metadata
                .partition_spec_by_id(partition_spec_id)
                .map(|partition_spec| partition_spec.partition_type(&schema))
                .transpose()
        };

        ManifestList::parse_with_version(
            &manifest_list_content,
            table_metadata.format_version(),
            partition_type_provider,
        )
    }

    pub(crate) fn log(&self) -> SnapshotLog {
        SnapshotLog {
            timestamp_ms: self.timestamp_ms,
            snapshot_id: self.snapshot_id,
        }
    }
}

pub(super) mod _serde {
    /// This is a helper module that defines types to help with serialization/deserialization.
    /// For deserialization the input first gets read into either the [SnapshotV1] or [SnapshotV2] struct
    /// and then converted into the [Snapshot] struct. Serialization works the other way around.
    /// [SnapshotV1] and [SnapshotV2] are internal struct that are only used for serialization and deserialization.
    use std::collections::HashMap;

    use serde::{Deserialize, Serialize};

    use super::{Operation, Snapshot, SnapshotSummary};
    use crate::spec::SchemaId;
    use crate::Error;

    #[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
    #[serde(rename_all = "kebab-case")]
    /// Defines the structure of a v2 snapshot for serialization/deserialization
    pub(crate) struct SnapshotV2 {
        pub snapshot_id: i64,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub parent_snapshot_id: Option<i64>,
        pub sequence_number: i64,
        pub timestamp_ms: i64,
        pub manifest_list: String,
        pub summary: SnapshotSummary,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub schema_id: Option<SchemaId>,
    }

    #[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
    #[serde(rename_all = "kebab-case")]
    /// Defines the structure of a v1 snapshot for serialization/deserialization
    pub(crate) struct SnapshotV1 {
        pub snapshot_id: i64,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub parent_snapshot_id: Option<i64>,
        pub timestamp_ms: i64,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub manifest_list: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub manifests: Option<Vec<String>>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub summary: Option<SnapshotSummary>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub schema_id: Option<SchemaId>,
    }

    impl From<SnapshotV2> for Snapshot {
        fn from(v2: SnapshotV2) -> Self {
            Snapshot {
                snapshot_id: v2.snapshot_id,
                parent_snapshot_id: v2.parent_snapshot_id,
                sequence_number: v2.sequence_number,
                timestamp_ms: v2.timestamp_ms,
                manifest_list: v2.manifest_list,
                summary: v2.summary,
                schema_id: v2.schema_id,
            }
        }
    }

    impl From<Snapshot> for SnapshotV2 {
        fn from(v2: Snapshot) -> Self {
            SnapshotV2 {
                snapshot_id: v2.snapshot_id,
                parent_snapshot_id: v2.parent_snapshot_id,
                sequence_number: v2.sequence_number,
                timestamp_ms: v2.timestamp_ms,
                manifest_list: v2.manifest_list,
                summary: v2.summary,
                schema_id: v2.schema_id,
            }
        }
    }

    impl TryFrom<SnapshotV1> for Snapshot {
        type Error = Error;

        fn try_from(v1: SnapshotV1) -> Result<Self, Self::Error> {
            Ok(Snapshot {
                snapshot_id: v1.snapshot_id,
                parent_snapshot_id: v1.parent_snapshot_id,
                sequence_number: 0,
                timestamp_ms: v1.timestamp_ms,
                manifest_list: match (v1.manifest_list, v1.manifests) {
                    (Some(file), None) => file,
                    (Some(_), Some(_)) => "Invalid v1 snapshot, when manifest list provided, manifest files should be omitted".to_string(),
                    (None, _) => "Unsupported v1 snapshot, only manifest list is supported".to_string()
                   },
                summary: v1.summary.unwrap_or(SnapshotSummary::new(Operation::default(), HashMap::new())),
                schema_id: v1.schema_id,
            })
        }
    }

    impl From<Snapshot> for SnapshotV1 {
        fn from(v2: Snapshot) -> Self {
            SnapshotV1 {
                snapshot_id: v2.snapshot_id,
                parent_snapshot_id: v2.parent_snapshot_id,
                timestamp_ms: v2.timestamp_ms,
                manifest_list: Some(v2.manifest_list),
                summary: Some(v2.summary),
                schema_id: v2.schema_id,
                manifests: None,
            }
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(rename_all = "kebab-case")]
/// Iceberg tables keep track of branches and tags using snapshot references.
pub struct SnapshotReference {
    /// A reference’s snapshot ID. The tagged snapshot or latest snapshot of a branch.
    pub snapshot_id: i64,
    #[serde(flatten)]
    /// Snapshot retention policy
    pub retention: SnapshotRetention,
}

impl SnapshotReference {
    /// Returns true if the snapshot reference is a branch.
    pub fn is_branch(&self) -> bool {
        matches!(self.retention, SnapshotRetention::Branch { .. })
    }
}

impl SnapshotReference {
    /// Create new snapshot reference
    pub fn new(snapshot_id: i64, retention: SnapshotRetention) -> Self {
        SnapshotReference {
            snapshot_id,
            retention,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(rename_all = "lowercase", tag = "type")]
/// The snapshot expiration procedure removes snapshots from table metadata and applies the table’s retention policy.
pub enum SnapshotRetention {
    #[serde(rename_all = "kebab-case")]
    /// Branches are mutable named references that can be updated by committing a new snapshot as
    /// the branch’s referenced snapshot using the Commit Conflict Resolution and Retry procedures.
    Branch {
        /// A positive number for the minimum number of snapshots to keep in a branch while expiring snapshots.
        /// Defaults to table property history.expire.min-snapshots-to-keep.
        #[serde(skip_serializing_if = "Option::is_none")]
        min_snapshots_to_keep: Option<i32>,
        /// A positive number for the max age of snapshots to keep when expiring, including the latest snapshot.
        /// Defaults to table property history.expire.max-snapshot-age-ms.
        #[serde(skip_serializing_if = "Option::is_none")]
        max_snapshot_age_ms: Option<i64>,
        /// For snapshot references except the main branch, a positive number for the max age of the snapshot reference to keep while expiring snapshots.
        /// Defaults to table property history.expire.max-ref-age-ms. The main branch never expires.
        #[serde(skip_serializing_if = "Option::is_none")]
        max_ref_age_ms: Option<i64>,
    },
    #[serde(rename_all = "kebab-case")]
    /// Tags are labels for individual snapshots.
    Tag {
        /// For snapshot references except the main branch, a positive number for the max age of the snapshot reference to keep while expiring snapshots.
        /// Defaults to table property history.expire.max-ref-age-ms. The main branch never expires.
        #[serde(skip_serializing_if = "Option::is_none")]
        max_ref_age_ms: Option<i64>,
    },
}

impl SnapshotRetention {
    /// Create a new branch retention policy
    pub fn branch(
        min_snapshots_to_keep: Option<i32>,
        max_snapshot_age_ms: Option<i64>,
        max_ref_age_ms: Option<i64>,
    ) -> Self {
        SnapshotRetention::Branch {
            min_snapshots_to_keep,
            max_snapshot_age_ms,
            max_ref_age_ms,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use chrono::{TimeZone, Utc};

    use crate::spec::snapshot::_serde::SnapshotV1;
    use crate::spec::snapshot::{Operation, Snapshot, SnapshotSummary};

    #[test]
    fn schema() {
        let record = r#"
        {
            "snapshot-id": 3051729675574597004,
            "timestamp-ms": 1515100955770,
            "summary": {
                "operation": "append"
            },
            "manifest-list": "s3://b/wh/.../s1.avro",
            "schema-id": 0
        }
        "#;

        let result: Snapshot = serde_json::from_str::<SnapshotV1>(record)
            .unwrap()
            .try_into()
            .unwrap();
        assert_eq!(3051729675574597004, result.snapshot_id());
        assert_eq!(
            Utc.timestamp_millis_opt(1515100955770).unwrap(),
            result.timestamp().unwrap()
        );
        assert_eq!(1515100955770, result.timestamp_ms());
        assert_eq!(
            SnapshotSummary::new(Operation::default(), HashMap::new()),
            *result.summary()
        );
        assert_eq!("s3://b/wh/.../s1.avro".to_string(), *result.manifest_list());
    }

    #[test]
    fn test_add_remove_data_files() {
        let mut summary = SnapshotSummary::new(Operation::Append, HashMap::new());

        assert_eq!(0, summary.added_data_files);
        assert_eq!(0, summary.deleted_data_files);
        assert_eq!(0, summary.total_data_files);

        summary.add_data_file(100);
        summary.add_data_file(200);

        assert_eq!(2, summary.added_data_files);
        assert_eq!(0, summary.deleted_data_files);
        assert_eq!(2, summary.total_data_files);

        assert_eq!(300, summary.added_file_size);
        assert_eq!(0, summary.removed_file_size);
        assert_eq!(300, summary.total_file_size);

        summary.delete_data_file(100);

        assert_eq!(1, summary.deleted_data_files);
        assert_eq!(1, summary.total_data_files);
        assert_eq!(100, summary.removed_file_size);
        assert_eq!(200, summary.total_file_size);
    }

    #[test]
    fn test_add_remove_delete_files() {
        let mut summary = SnapshotSummary::new(Operation::Append, HashMap::new());

        summary.add_delete_file(50);
        summary.add_delete_file(75);

        assert_eq!(2, summary.added_delete_files);
        assert_eq!(2, summary.total_delete_files);
        assert_eq!(125, summary.added_file_size);
        assert_eq!(125, summary.total_file_size);

        summary.remove_delete_file(50);

        assert_eq!(1, summary.removed_delete_files);
        assert_eq!(1, summary.total_delete_files);
        assert_eq!(50, summary.removed_file_size);
        assert_eq!(75, summary.total_file_size);
    }
}
