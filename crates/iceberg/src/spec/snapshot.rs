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
use crate::error::Result;
use chrono::{DateTime, TimeZone, Utc};
use futures::AsyncReadExt;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use typed_builder::TypedBuilder;

use super::table_metadata::SnapshotLog;
use crate::io::FileIO;
use crate::spec::{ManifestList, SchemaId, SchemaRef, StructType, TableMetadata};
use crate::{Error, ErrorKind};
use _serde::SnapshotV2;

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

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
/// Summarises the changes in the snapshot.
pub struct Summary {
    /// The type of operation in the snapshot
    pub operation: Operation,
    /// Other summary data.
    #[serde(flatten)]
    pub other: HashMap<String, String>,
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
    summary: Summary,
    /// ID of the table’s current schema when the snapshot was created.
    #[builder(setter(strip_option), default = None)]
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
    pub fn summary(&self) -> &Summary {
        &self.summary
    }
    /// Get the timestamp of when the snapshot was created
    #[inline]
    pub fn timestamp(&self) -> DateTime<Utc> {
        Utc.timestamp_millis_opt(self.timestamp_ms).unwrap()
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
        let mut manifest_list_content = Vec::new();
        file_io
            .new_input(&self.manifest_list)?
            .reader()
            .await?
            .read_to_end(&mut manifest_list_content)
            .await?;

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

    use crate::spec::SchemaId;
    use crate::Error;

    use super::{Operation, Snapshot, Summary};

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
        pub summary: Summary,
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
        pub summary: Option<Summary>,
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
                summary: v1.summary.unwrap_or(Summary {
                    operation: Operation::default(),
                    other: HashMap::new(),
                }),
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
        max_ref_age_ms: i64,
    },
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};
    use std::collections::HashMap;

    use crate::spec::snapshot::{Operation, Snapshot, Summary, _serde::SnapshotV1};

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
            result.timestamp()
        );
        assert_eq!(
            Summary {
                operation: Operation::Append,
                other: HashMap::new()
            },
            *result.summary()
        );
        assert_eq!("s3://b/wh/.../s1.avro".to_string(), *result.manifest_list());
    }
}
