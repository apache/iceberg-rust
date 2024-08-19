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

//! Defines the [table metadata](https://iceberg.apache.org/spec/#table-metadata).
//! The main struct here is [TableMetadataV2] which defines the data for a table.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

use _serde::TableMetadataEnum;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};
use uuid::Uuid;

use super::snapshot::{Snapshot, SnapshotReference, SnapshotRetention};
use super::{
    PartitionSpec, PartitionSpecRef, SchemaId, SchemaRef, SnapshotRef, SortOrder, SortOrderRef,
    DEFAULT_PARTITION_SPEC_ID,
};
use crate::error::{timestamp_ms_to_utc, Result};
use crate::{Error, ErrorKind, TableCreation};

static MAIN_BRANCH: &str = "main";
static DEFAULT_SORT_ORDER_ID: i64 = 0;

pub(crate) static EMPTY_SNAPSHOT_ID: i64 = -1;
pub(crate) static INITIAL_SEQUENCE_NUMBER: i64 = 0;

/// Reference to [`TableMetadata`].
pub type TableMetadataRef = Arc<TableMetadata>;

#[derive(Debug, PartialEq, Deserialize, Eq, Clone)]
#[serde(try_from = "TableMetadataEnum")]
/// Fields for the version 2 of the table metadata.
///
/// We assume that this data structure is always valid, so we will panic when invalid error happens.
/// We check the validity of this data structure when constructing.
pub struct TableMetadata {
    /// Integer Version for the format.
    pub(crate) format_version: FormatVersion,
    /// A UUID that identifies the table
    pub(crate) table_uuid: Uuid,
    /// Location tables base location
    pub(crate) location: String,
    /// The tables highest sequence number
    pub(crate) last_sequence_number: i64,
    /// Timestamp in milliseconds from the unix epoch when the table was last updated.
    pub(crate) last_updated_ms: i64,
    /// An integer; the highest assigned column ID for the table.
    pub(crate) last_column_id: i32,
    /// A list of schemas, stored as objects with schema-id.
    pub(crate) schemas: HashMap<i32, SchemaRef>,
    /// ID of the table’s current schema.
    pub(crate) current_schema_id: i32,
    /// A list of partition specs, stored as full partition spec objects.
    pub(crate) partition_specs: HashMap<i32, PartitionSpecRef>,
    /// ID of the “current” spec that writers should use by default.
    pub(crate) default_spec_id: i32,
    /// An integer; the highest assigned partition field ID across all partition specs for the table.
    pub(crate) last_partition_id: i32,
    ///A string to string map of table properties. This is used to control settings that
    /// affect reading and writing and is not intended to be used for arbitrary metadata.
    /// For example, commit.retry.num-retries is used to control the number of commit retries.
    pub(crate) properties: HashMap<String, String>,
    /// long ID of the current table snapshot; must be the same as the current
    /// ID of the main branch in refs.
    pub(crate) current_snapshot_id: Option<i64>,
    ///A list of valid snapshots. Valid snapshots are snapshots for which all
    /// data files exist in the file system. A data file must not be deleted
    /// from the file system until the last snapshot in which it was listed is
    /// garbage collected.
    pub(crate) snapshots: HashMap<i64, SnapshotRef>,
    /// A list (optional) of timestamp and snapshot ID pairs that encodes changes
    /// to the current snapshot for the table. Each time the current-snapshot-id
    /// is changed, a new entry should be added with the last-updated-ms
    /// and the new current-snapshot-id. When snapshots are expired from
    /// the list of valid snapshots, all entries before a snapshot that has
    /// expired should be removed.
    pub(crate) snapshot_log: Vec<SnapshotLog>,

    /// A list (optional) of timestamp and metadata file location pairs
    /// that encodes changes to the previous metadata files for the table.
    /// Each time a new metadata file is created, a new entry of the
    /// previous metadata file location should be added to the list.
    /// Tables can be configured to remove oldest metadata log entries and
    /// keep a fixed-size log of the most recent entries after a commit.
    pub(crate) metadata_log: Vec<MetadataLog>,

    /// A list of sort orders, stored as full sort order objects.
    pub(crate) sort_orders: HashMap<i64, SortOrderRef>,
    /// Default sort order id of the table. Note that this could be used by
    /// writers, but is not used when reading because reads use the specs
    /// stored in manifest files.
    pub(crate) default_sort_order_id: i64,
    ///A map of snapshot references. The map keys are the unique snapshot reference
    /// names in the table, and the map values are snapshot reference objects.
    /// There is always a main branch reference pointing to the current-snapshot-id
    /// even if the refs map is null.
    pub(crate) refs: HashMap<String, SnapshotReference>,
}

impl TableMetadata {
    /// Returns format version of this metadata.
    #[inline]
    pub fn format_version(&self) -> FormatVersion {
        self.format_version
    }

    /// Returns uuid of current table.
    #[inline]
    pub fn uuid(&self) -> Uuid {
        self.table_uuid
    }

    /// Returns table location.
    #[inline]
    pub fn location(&self) -> &str {
        self.location.as_str()
    }

    /// Returns last sequence number.
    #[inline]
    pub fn last_sequence_number(&self) -> i64 {
        self.last_sequence_number
    }

    /// Returns last updated time.
    #[inline]
    pub fn last_updated_timestamp(&self) -> Result<DateTime<Utc>> {
        timestamp_ms_to_utc(self.last_updated_ms)
    }

    /// Returns last updated time in milliseconds.
    #[inline]
    pub fn last_updated_ms(&self) -> i64 {
        self.last_updated_ms
    }

    /// Returns schemas
    #[inline]
    pub fn schemas_iter(&self) -> impl Iterator<Item = &SchemaRef> {
        self.schemas.values()
    }

    /// Lookup schema by id.
    #[inline]
    pub fn schema_by_id(&self, schema_id: SchemaId) -> Option<&SchemaRef> {
        self.schemas.get(&schema_id)
    }

    /// Get current schema
    #[inline]
    pub fn current_schema(&self) -> &SchemaRef {
        self.schema_by_id(self.current_schema_id)
            .expect("Current schema id set, but not found in table metadata")
    }

    /// Returns all partition specs.
    #[inline]
    pub fn partition_specs_iter(&self) -> impl Iterator<Item = &PartitionSpecRef> {
        self.partition_specs.values()
    }

    /// Lookup partition spec by id.
    #[inline]
    pub fn partition_spec_by_id(&self, spec_id: i32) -> Option<&PartitionSpecRef> {
        self.partition_specs.get(&spec_id)
    }

    /// Get default partition spec
    #[inline]
    pub fn default_partition_spec(&self) -> Option<&PartitionSpecRef> {
        if self.default_spec_id == DEFAULT_PARTITION_SPEC_ID {
            self.partition_spec_by_id(DEFAULT_PARTITION_SPEC_ID)
        } else {
            Some(
                self.partition_spec_by_id(self.default_spec_id)
                    .expect("Default partition spec id set, but not found in table metadata"),
            )
        }
    }

    /// Returns all snapshots
    #[inline]
    pub fn snapshots(&self) -> impl Iterator<Item = &SnapshotRef> {
        self.snapshots.values()
    }

    /// Lookup snapshot by id.
    #[inline]
    pub fn snapshot_by_id(&self, snapshot_id: i64) -> Option<&SnapshotRef> {
        self.snapshots.get(&snapshot_id)
    }

    /// Returns snapshot history.
    #[inline]
    pub fn history(&self) -> &[SnapshotLog] {
        &self.snapshot_log
    }

    /// Get current snapshot
    #[inline]
    pub fn current_snapshot(&self) -> Option<&SnapshotRef> {
        self.current_snapshot_id.map(|s| {
            self.snapshot_by_id(s)
                .expect("Current snapshot id has been set, but doesn't exist in metadata")
        })
    }

    /// Return all sort orders.
    #[inline]
    pub fn sort_orders_iter(&self) -> impl Iterator<Item = &SortOrderRef> {
        self.sort_orders.values()
    }

    /// Lookup sort order by id.
    #[inline]
    pub fn sort_order_by_id(&self, sort_order_id: i64) -> Option<&SortOrderRef> {
        self.sort_orders.get(&sort_order_id)
    }

    /// Returns default sort order id.
    #[inline]
    pub fn default_sort_order(&self) -> Option<&SortOrderRef> {
        if self.default_sort_order_id == DEFAULT_SORT_ORDER_ID {
            self.sort_orders.get(&DEFAULT_SORT_ORDER_ID)
        } else {
            Some(
                self.sort_orders
                    .get(&self.default_sort_order_id)
                    .expect("Default order id has been set, but not found in table metadata!"),
            )
        }
    }

    /// Returns properties of table.
    #[inline]
    pub fn properties(&self) -> &HashMap<String, String> {
        &self.properties
    }

    /// Append snapshot to table
    pub fn append_snapshot(&mut self, snapshot: Snapshot) {
        self.last_updated_ms = snapshot.timestamp().timestamp_millis();
        self.last_sequence_number = snapshot.sequence_number();

        self.refs
            .entry(MAIN_BRANCH.to_string())
            .and_modify(|s| {
                s.snapshot_id = snapshot.snapshot_id();
            })
            .or_insert_with(|| {
                SnapshotReference::new(snapshot.snapshot_id(), SnapshotRetention::Branch {
                    min_snapshots_to_keep: None,
                    max_snapshot_age_ms: None,
                    max_ref_age_ms: None,
                })
            });

        self.snapshot_log.push(snapshot.log());
        self.snapshots
            .insert(snapshot.snapshot_id(), Arc::new(snapshot));
    }
}

/// Manipulating table metadata.
pub struct TableMetadataBuilder(TableMetadata);

impl TableMetadataBuilder {
    /// Creates a new table metadata builder from the given table metadata.
    pub fn new(origin: TableMetadata) -> Self {
        Self(origin)
    }

    /// Creates a new table metadata builder from the given table creation.
    pub fn from_table_creation(table_creation: TableCreation) -> Result<Self> {
        let TableCreation {
            name: _,
            location,
            schema,
            partition_spec,
            sort_order,
            properties,
        } = table_creation;

        let partition_specs = match partition_spec {
            Some(_) => {
                return Err(Error::new(
                    ErrorKind::FeatureUnsupported,
                    "Can't create table with partition spec now",
                ))
            }
            None => HashMap::from([(
                DEFAULT_PARTITION_SPEC_ID,
                Arc::new(PartitionSpec {
                    spec_id: DEFAULT_PARTITION_SPEC_ID,
                    fields: vec![],
                }),
            )]),
        };

        let sort_orders = match sort_order {
            Some(_) => {
                return Err(Error::new(
                    ErrorKind::FeatureUnsupported,
                    "Can't create table with sort order now",
                ))
            }
            None => HashMap::from([(
                DEFAULT_SORT_ORDER_ID,
                Arc::new(SortOrder {
                    order_id: DEFAULT_SORT_ORDER_ID,
                    fields: vec![],
                }),
            )]),
        };

        let table_metadata = TableMetadata {
            format_version: FormatVersion::V2,
            table_uuid: Uuid::now_v7(),
            location: location.ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    "Can't create table without location",
                )
            })?,
            last_sequence_number: 0,
            last_updated_ms: Utc::now().timestamp_millis(),
            last_column_id: schema.highest_field_id(),
            current_schema_id: schema.schema_id(),
            schemas: HashMap::from([(schema.schema_id(), Arc::new(schema))]),
            partition_specs,
            default_spec_id: DEFAULT_PARTITION_SPEC_ID,
            last_partition_id: 0,
            properties,
            current_snapshot_id: None,
            snapshots: Default::default(),
            snapshot_log: vec![],
            sort_orders,
            metadata_log: vec![],
            default_sort_order_id: DEFAULT_SORT_ORDER_ID,
            refs: Default::default(),
        };

        Ok(Self(table_metadata))
    }

    /// Changes uuid of table metadata.
    pub fn assign_uuid(mut self, uuid: Uuid) -> Result<Self> {
        self.0.table_uuid = uuid;
        Ok(self)
    }

    /// Returns the new table metadata after changes.
    pub fn build(self) -> Result<TableMetadata> {
        Ok(self.0)
    }
}

pub(super) mod _serde {
    /// This is a helper module that defines types to help with serialization/deserialization.
    /// For deserialization the input first gets read into either the [TableMetadataV1] or [TableMetadataV2] struct
    /// and then converted into the [TableMetadata] struct. Serialization works the other way around.
    /// [TableMetadataV1] and [TableMetadataV2] are internal struct that are only used for serialization and deserialization.
    use std::collections::HashMap;
    /// This is a helper module that defines types to help with serialization/deserialization.
    /// For deserialization the input first gets read into either the [TableMetadataV1] or [TableMetadataV2] struct
    /// and then converted into the [TableMetadata] struct. Serialization works the other way around.
    /// [TableMetadataV1] and [TableMetadataV2] are internal struct that are only used for serialization and deserialization.
    use std::sync::Arc;

    use itertools::Itertools;
    use serde::{Deserialize, Serialize};
    use uuid::Uuid;

    use super::{
        FormatVersion, MetadataLog, SnapshotLog, TableMetadata, DEFAULT_PARTITION_SPEC_ID,
        DEFAULT_SORT_ORDER_ID, MAIN_BRANCH,
    };
    use crate::spec::schema::_serde::{SchemaV1, SchemaV2};
    use crate::spec::snapshot::_serde::{SnapshotV1, SnapshotV2};
    use crate::spec::{
        PartitionField, PartitionSpec, Schema, Snapshot, SnapshotReference, SnapshotRetention,
        SortOrder, EMPTY_SNAPSHOT_ID,
    };
    use crate::{Error, ErrorKind};

    #[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
    #[serde(untagged)]
    pub(super) enum TableMetadataEnum {
        V2(TableMetadataV2),
        V1(TableMetadataV1),
    }

    #[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
    #[serde(rename_all = "kebab-case")]
    /// Defines the structure of a v2 table metadata for serialization/deserialization
    pub(super) struct TableMetadataV2 {
        pub format_version: VersionNumber<2>,
        pub table_uuid: Uuid,
        pub location: String,
        pub last_sequence_number: i64,
        pub last_updated_ms: i64,
        pub last_column_id: i32,
        pub schemas: Vec<SchemaV2>,
        pub current_schema_id: i32,
        pub partition_specs: Vec<PartitionSpec>,
        pub default_spec_id: i32,
        pub last_partition_id: i32,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub properties: Option<HashMap<String, String>>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub current_snapshot_id: Option<i64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub snapshots: Option<Vec<SnapshotV2>>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub snapshot_log: Option<Vec<SnapshotLog>>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub metadata_log: Option<Vec<MetadataLog>>,
        pub sort_orders: Vec<SortOrder>,
        pub default_sort_order_id: i64,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub refs: Option<HashMap<String, SnapshotReference>>,
    }

    #[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
    #[serde(rename_all = "kebab-case")]
    /// Defines the structure of a v1 table metadata for serialization/deserialization
    pub(super) struct TableMetadataV1 {
        pub format_version: VersionNumber<1>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub table_uuid: Option<Uuid>,
        pub location: String,
        pub last_updated_ms: i64,
        pub last_column_id: i32,
        pub schema: SchemaV1,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub schemas: Option<Vec<SchemaV1>>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub current_schema_id: Option<i32>,
        pub partition_spec: Vec<PartitionField>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub partition_specs: Option<Vec<PartitionSpec>>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub default_spec_id: Option<i32>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub last_partition_id: Option<i32>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub properties: Option<HashMap<String, String>>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub current_snapshot_id: Option<i64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub snapshots: Option<Vec<SnapshotV1>>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub snapshot_log: Option<Vec<SnapshotLog>>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub metadata_log: Option<Vec<MetadataLog>>,
        pub sort_orders: Option<Vec<SortOrder>>,
        pub default_sort_order_id: Option<i64>,
    }

    /// Helper to serialize and deserialize the format version.
    #[derive(Debug, PartialEq, Eq)]
    pub(crate) struct VersionNumber<const V: u8>;

    impl Serialize for TableMetadata {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where S: serde::Serializer {
            // we must do a clone here
            let table_metadata_enum: TableMetadataEnum =
                self.clone().try_into().map_err(serde::ser::Error::custom)?;

            table_metadata_enum.serialize(serializer)
        }
    }

    impl<const V: u8> Serialize for VersionNumber<V> {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where S: serde::Serializer {
            serializer.serialize_u8(V)
        }
    }

    impl<'de, const V: u8> Deserialize<'de> for VersionNumber<V> {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where D: serde::Deserializer<'de> {
            let value = u8::deserialize(deserializer)?;
            if value == V {
                Ok(VersionNumber::<V>)
            } else {
                Err(serde::de::Error::custom("Invalid Version"))
            }
        }
    }

    impl TryFrom<TableMetadataEnum> for TableMetadata {
        type Error = Error;
        fn try_from(value: TableMetadataEnum) -> Result<Self, Error> {
            match value {
                TableMetadataEnum::V2(value) => value.try_into(),
                TableMetadataEnum::V1(value) => value.try_into(),
            }
        }
    }

    impl TryFrom<TableMetadata> for TableMetadataEnum {
        type Error = Error;
        fn try_from(value: TableMetadata) -> Result<Self, Error> {
            Ok(match value.format_version {
                FormatVersion::V2 => TableMetadataEnum::V2(value.into()),
                FormatVersion::V1 => TableMetadataEnum::V1(value.try_into()?),
            })
        }
    }

    impl TryFrom<TableMetadataV2> for TableMetadata {
        type Error = Error;
        fn try_from(value: TableMetadataV2) -> Result<Self, self::Error> {
            let current_snapshot_id = if let &Some(-1) = &value.current_snapshot_id {
                None
            } else {
                value.current_snapshot_id
            };
            let schemas = HashMap::from_iter(
                value
                    .schemas
                    .into_iter()
                    .map(|schema| Ok((schema.schema_id, Arc::new(schema.try_into()?))))
                    .collect::<Result<Vec<_>, Error>>()?,
            );
            Ok(TableMetadata {
                format_version: FormatVersion::V2,
                table_uuid: value.table_uuid,
                location: value.location,
                last_sequence_number: value.last_sequence_number,
                last_updated_ms: value.last_updated_ms,
                last_column_id: value.last_column_id,
                current_schema_id: if schemas.keys().contains(&value.current_schema_id) {
                    Ok(value.current_schema_id)
                } else {
                    Err(self::Error::new(
                        ErrorKind::DataInvalid,
                        format!(
                            "No schema exists with the current schema id {}.",
                            value.current_schema_id
                        ),
                    ))
                }?,
                schemas,
                partition_specs: HashMap::from_iter(
                    value
                        .partition_specs
                        .into_iter()
                        .map(|x| (x.spec_id(), Arc::new(x))),
                ),
                default_spec_id: value.default_spec_id,
                last_partition_id: value.last_partition_id,
                properties: value.properties.unwrap_or_default(),
                current_snapshot_id,
                snapshots: value
                    .snapshots
                    .map(|snapshots| {
                        HashMap::from_iter(
                            snapshots
                                .into_iter()
                                .map(|x| (x.snapshot_id, Arc::new(x.into()))),
                        )
                    })
                    .unwrap_or_default(),
                snapshot_log: value.snapshot_log.unwrap_or_default(),
                metadata_log: value.metadata_log.unwrap_or_default(),
                sort_orders: HashMap::from_iter(
                    value
                        .sort_orders
                        .into_iter()
                        .map(|x| (x.order_id, Arc::new(x))),
                ),
                default_sort_order_id: value.default_sort_order_id,
                refs: value.refs.unwrap_or_else(|| {
                    if let Some(snapshot_id) = current_snapshot_id {
                        HashMap::from_iter(vec![(MAIN_BRANCH.to_string(), SnapshotReference {
                            snapshot_id,
                            retention: SnapshotRetention::Branch {
                                min_snapshots_to_keep: None,
                                max_snapshot_age_ms: None,
                                max_ref_age_ms: None,
                            },
                        })])
                    } else {
                        HashMap::new()
                    }
                }),
            })
        }
    }

    impl TryFrom<TableMetadataV1> for TableMetadata {
        type Error = Error;
        fn try_from(value: TableMetadataV1) -> Result<Self, Error> {
            let schemas = value
                .schemas
                .map(|schemas| {
                    Ok::<_, Error>(HashMap::from_iter(
                        schemas
                            .into_iter()
                            .enumerate()
                            .map(|(i, schema)| {
                                Ok((
                                    schema.schema_id.unwrap_or(i as i32),
                                    Arc::new(schema.try_into()?),
                                ))
                            })
                            .collect::<Result<Vec<_>, Error>>()?
                            .into_iter(),
                    ))
                })
                .or_else(|| {
                    Some(value.schema.try_into().map(|schema: Schema| {
                        HashMap::from_iter(vec![(schema.schema_id(), Arc::new(schema))])
                    }))
                })
                .transpose()?
                .unwrap_or_default();
            let partition_specs = HashMap::from_iter(
                value
                    .partition_specs
                    .unwrap_or_else(|| {
                        vec![PartitionSpec {
                            spec_id: DEFAULT_PARTITION_SPEC_ID,
                            fields: value.partition_spec,
                        }]
                    })
                    .into_iter()
                    .map(|x| (x.spec_id(), Arc::new(x))),
            );
            Ok(TableMetadata {
                format_version: FormatVersion::V1,
                table_uuid: value.table_uuid.unwrap_or_default(),
                location: value.location,
                last_sequence_number: 0,
                last_updated_ms: value.last_updated_ms,
                last_column_id: value.last_column_id,
                current_schema_id: value
                    .current_schema_id
                    .unwrap_or_else(|| schemas.keys().copied().max().unwrap_or_default()),
                default_spec_id: value
                    .default_spec_id
                    .unwrap_or_else(|| partition_specs.keys().copied().max().unwrap_or_default()),
                last_partition_id: value
                    .last_partition_id
                    .unwrap_or_else(|| partition_specs.keys().copied().max().unwrap_or_default()),
                partition_specs,
                schemas,

                properties: value.properties.unwrap_or_default(),
                current_snapshot_id: if let &Some(id) = &value.current_snapshot_id {
                    if id == EMPTY_SNAPSHOT_ID {
                        None
                    } else {
                        Some(id)
                    }
                } else {
                    value.current_snapshot_id
                },
                snapshots: value
                    .snapshots
                    .map(|snapshots| {
                        Ok::<_, Error>(HashMap::from_iter(
                            snapshots
                                .into_iter()
                                .map(|x| Ok((x.snapshot_id, Arc::new(x.try_into()?))))
                                .collect::<Result<Vec<_>, Error>>()?,
                        ))
                    })
                    .transpose()?
                    .unwrap_or_default(),
                snapshot_log: value.snapshot_log.unwrap_or_default(),
                metadata_log: value.metadata_log.unwrap_or_default(),
                sort_orders: match value.sort_orders {
                    Some(sort_orders) => HashMap::from_iter(
                        sort_orders.into_iter().map(|x| (x.order_id, Arc::new(x))),
                    ),
                    None => HashMap::new(),
                },
                default_sort_order_id: value.default_sort_order_id.unwrap_or(DEFAULT_SORT_ORDER_ID),
                refs: HashMap::from_iter(vec![(MAIN_BRANCH.to_string(), SnapshotReference {
                    snapshot_id: value.current_snapshot_id.unwrap_or_default(),
                    retention: SnapshotRetention::Branch {
                        min_snapshots_to_keep: None,
                        max_snapshot_age_ms: None,
                        max_ref_age_ms: None,
                    },
                })]),
            })
        }
    }

    impl From<TableMetadata> for TableMetadataV2 {
        fn from(v: TableMetadata) -> Self {
            TableMetadataV2 {
                format_version: VersionNumber::<2>,
                table_uuid: v.table_uuid,
                location: v.location,
                last_sequence_number: v.last_sequence_number,
                last_updated_ms: v.last_updated_ms,
                last_column_id: v.last_column_id,
                schemas: v
                    .schemas
                    .into_values()
                    .map(|x| {
                        Arc::try_unwrap(x)
                            .unwrap_or_else(|schema| schema.as_ref().clone())
                            .into()
                    })
                    .collect(),
                current_schema_id: v.current_schema_id,
                partition_specs: v
                    .partition_specs
                    .into_values()
                    .map(|x| Arc::try_unwrap(x).unwrap_or_else(|s| s.as_ref().clone()))
                    .collect(),
                default_spec_id: v.default_spec_id,
                last_partition_id: v.last_partition_id,
                properties: Some(v.properties),
                current_snapshot_id: v.current_snapshot_id.or(Some(-1)),
                snapshots: if v.snapshots.is_empty() {
                    Some(vec![])
                } else {
                    Some(
                        v.snapshots
                            .into_values()
                            .map(|x| {
                                Arc::try_unwrap(x)
                                    .unwrap_or_else(|snapshot| snapshot.as_ref().clone())
                                    .into()
                            })
                            .collect(),
                    )
                },
                snapshot_log: if v.snapshot_log.is_empty() {
                    None
                } else {
                    Some(v.snapshot_log)
                },
                metadata_log: if v.metadata_log.is_empty() {
                    None
                } else {
                    Some(v.metadata_log)
                },
                sort_orders: v
                    .sort_orders
                    .into_values()
                    .map(|x| Arc::try_unwrap(x).unwrap_or_else(|s| s.as_ref().clone()))
                    .collect(),
                default_sort_order_id: v.default_sort_order_id,
                refs: Some(v.refs),
            }
        }
    }

    impl TryFrom<TableMetadata> for TableMetadataV1 {
        type Error = Error;
        fn try_from(v: TableMetadata) -> Result<Self, Error> {
            Ok(TableMetadataV1 {
                format_version: VersionNumber::<1>,
                table_uuid: Some(v.table_uuid),
                location: v.location,
                last_updated_ms: v.last_updated_ms,
                last_column_id: v.last_column_id,
                schema: v
                    .schemas
                    .get(&v.current_schema_id)
                    .ok_or(Error::new(
                        ErrorKind::Unexpected,
                        "current_schema_id not found in schemas",
                    ))?
                    .as_ref()
                    .clone()
                    .into(),
                schemas: Some(
                    v.schemas
                        .into_values()
                        .map(|x| {
                            Arc::try_unwrap(x)
                                .unwrap_or_else(|schema| schema.as_ref().clone())
                                .into()
                        })
                        .collect(),
                ),
                current_schema_id: Some(v.current_schema_id),
                partition_spec: v
                    .partition_specs
                    .get(&v.default_spec_id)
                    .map(|x| x.fields().to_vec())
                    .unwrap_or_default(),
                partition_specs: Some(
                    v.partition_specs
                        .into_values()
                        .map(|x| Arc::try_unwrap(x).unwrap_or_else(|s| s.as_ref().clone()))
                        .collect(),
                ),
                default_spec_id: Some(v.default_spec_id),
                last_partition_id: Some(v.last_partition_id),
                properties: if v.properties.is_empty() {
                    None
                } else {
                    Some(v.properties)
                },
                current_snapshot_id: v.current_snapshot_id.or(Some(-1)),
                snapshots: if v.snapshots.is_empty() {
                    None
                } else {
                    Some(
                        v.snapshots
                            .into_values()
                            .map(|x| Snapshot::clone(&x).into())
                            .collect(),
                    )
                },
                snapshot_log: if v.snapshot_log.is_empty() {
                    None
                } else {
                    Some(v.snapshot_log)
                },
                metadata_log: if v.metadata_log.is_empty() {
                    None
                } else {
                    Some(v.metadata_log)
                },
                sort_orders: Some(
                    v.sort_orders
                        .into_values()
                        .map(|s| Arc::try_unwrap(s).unwrap_or_else(|s| s.as_ref().clone()))
                        .collect(),
                ),
                default_sort_order_id: Some(v.default_sort_order_id),
            })
        }
    }
}

#[derive(Debug, Serialize_repr, Deserialize_repr, PartialEq, Eq, Clone, Copy, Hash)]
#[repr(u8)]
/// Iceberg format version
pub enum FormatVersion {
    /// Iceberg spec version 1
    V1 = 1u8,
    /// Iceberg spec version 2
    V2 = 2u8,
}

impl PartialOrd for FormatVersion {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for FormatVersion {
    fn cmp(&self, other: &Self) -> Ordering {
        (*self as u8).cmp(&(*other as u8))
    }
}

impl Display for FormatVersion {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            FormatVersion::V1 => write!(f, "v1"),
            FormatVersion::V2 => write!(f, "v2"),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(rename_all = "kebab-case")]
/// Encodes changes to the previous metadata files for the table
pub struct MetadataLog {
    /// The file for the log.
    pub metadata_file: String,
    /// Time new metadata was created
    pub timestamp_ms: i64,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(rename_all = "kebab-case")]
/// A log of when each snapshot was made.
pub struct SnapshotLog {
    /// Id of the snapshot.
    pub snapshot_id: i64,
    /// Last updated timestamp
    pub timestamp_ms: i64,
}

impl SnapshotLog {
    /// Returns the last updated timestamp as a DateTime<Utc> with millisecond precision
    pub fn timestamp(self) -> Result<DateTime<Utc>> {
        timestamp_ms_to_utc(self.timestamp_ms)
    }

    /// Returns the timestamp in milliseconds
    #[inline]
    pub fn timestamp_ms(&self) -> i64 {
        self.timestamp_ms
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fs;
    use std::sync::Arc;

    use anyhow::Result;
    use pretty_assertions::assert_eq;
    use uuid::Uuid;

    use super::{FormatVersion, MetadataLog, SnapshotLog, TableMetadataBuilder};
    use crate::spec::table_metadata::TableMetadata;
    use crate::spec::{
        NestedField, NullOrder, Operation, PartitionField, PartitionSpec, PrimitiveType, Schema,
        Snapshot, SnapshotReference, SnapshotRetention, SortDirection, SortField, SortOrder,
        Summary, Transform, Type, UnboundPartitionField,
    };
    use crate::TableCreation;

    fn check_table_metadata_serde(json: &str, expected_type: TableMetadata) {
        let desered_type: TableMetadata = serde_json::from_str(json).unwrap();
        assert_eq!(desered_type, expected_type);

        let sered_json = serde_json::to_string(&expected_type).unwrap();
        let parsed_json_value = serde_json::from_str::<TableMetadata>(&sered_json).unwrap();

        assert_eq!(parsed_json_value, desered_type);
    }

    fn get_test_table_metadata(file_name: &str) -> TableMetadata {
        let path = format!("testdata/table_metadata/{}", file_name);
        let metadata: String = fs::read_to_string(path).unwrap();

        serde_json::from_str(&metadata).unwrap()
    }

    #[test]
    fn test_table_data_v2() {
        let data = r#"
            {
                "format-version" : 2,
                "table-uuid": "fb072c92-a02b-11e9-ae9c-1bb7bc9eca94",
                "location": "s3://b/wh/data.db/table",
                "last-sequence-number" : 1,
                "last-updated-ms": 1515100955770,
                "last-column-id": 1,
                "schemas": [
                    {
                        "schema-id" : 1,
                        "type" : "struct",
                        "fields" :[
                            {
                                "id": 1,
                                "name": "struct_name",
                                "required": true,
                                "type": "fixed[1]"
                            }
                        ]
                    }
                ],
                "current-schema-id" : 1,
                "partition-specs": [
                    {
                        "spec-id": 1,
                        "fields": [
                            {
                                "source-id": 4,
                                "field-id": 1000,
                                "name": "ts_day",
                                "transform": "day"
                            }
                        ]
                    }
                ],
                "default-spec-id": 1,
                "last-partition-id": 1000,
                "properties": {
                    "commit.retry.num-retries": "1"
                },
                "metadata-log": [
                    {
                        "metadata-file": "s3://bucket/.../v1.json",
                        "timestamp-ms": 1515100
                    }
                ],
                "sort-orders": [],
                "default-sort-order-id": 0
            }
        "#;

        let schema = Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![Arc::new(NestedField::required(
                1,
                "struct_name",
                Type::Primitive(PrimitiveType::Fixed(1)),
            ))])
            .build()
            .unwrap();

        let partition_spec = PartitionSpec {
            spec_id: 1,
            fields: vec![PartitionField {
                name: "ts_day".to_string(),
                transform: Transform::Day,
                source_id: 4,
                field_id: 1000,
            }],
        };

        let expected = TableMetadata {
            format_version: FormatVersion::V2,
            table_uuid: Uuid::parse_str("fb072c92-a02b-11e9-ae9c-1bb7bc9eca94").unwrap(),
            location: "s3://b/wh/data.db/table".to_string(),
            last_updated_ms: 1515100955770,
            last_column_id: 1,
            schemas: HashMap::from_iter(vec![(1, Arc::new(schema))]),
            current_schema_id: 1,
            partition_specs: HashMap::from_iter(vec![(1, partition_spec.into())]),
            default_spec_id: 1,
            last_partition_id: 1000,
            default_sort_order_id: 0,
            sort_orders: HashMap::from_iter(vec![]),
            snapshots: HashMap::default(),
            current_snapshot_id: None,
            last_sequence_number: 1,
            properties: HashMap::from_iter(vec![(
                "commit.retry.num-retries".to_string(),
                "1".to_string(),
            )]),
            snapshot_log: Vec::new(),
            metadata_log: vec![MetadataLog {
                metadata_file: "s3://bucket/.../v1.json".to_string(),
                timestamp_ms: 1515100,
            }],
            refs: HashMap::new(),
        };

        check_table_metadata_serde(data, expected);
    }

    #[test]
    fn test_table_data_v1() {
        let data = r#"
        {
            "format-version" : 1,
            "table-uuid" : "df838b92-0b32-465d-a44e-d39936e538b7",
            "location" : "/home/iceberg/warehouse/nyc/taxis",
            "last-updated-ms" : 1662532818843,
            "last-column-id" : 5,
            "schema" : {
              "type" : "struct",
              "schema-id" : 0,
              "fields" : [ {
                "id" : 1,
                "name" : "vendor_id",
                "required" : false,
                "type" : "long"
              }, {
                "id" : 2,
                "name" : "trip_id",
                "required" : false,
                "type" : "long"
              }, {
                "id" : 3,
                "name" : "trip_distance",
                "required" : false,
                "type" : "float"
              }, {
                "id" : 4,
                "name" : "fare_amount",
                "required" : false,
                "type" : "double"
              }, {
                "id" : 5,
                "name" : "store_and_fwd_flag",
                "required" : false,
                "type" : "string"
              } ]
            },
            "partition-spec" : [ {
              "name" : "vendor_id",
              "transform" : "identity",
              "source-id" : 1,
              "field-id" : 1000
            } ],
            "last-partition-id" : 1000,
            "default-sort-order-id" : 0,
            "sort-orders" : [ {
              "order-id" : 0,
              "fields" : [ ]
            } ],
            "properties" : {
              "owner" : "root"
            },
            "current-snapshot-id" : 638933773299822130,
            "refs" : {
              "main" : {
                "snapshot-id" : 638933773299822130,
                "type" : "branch"
              }
            },
            "snapshots" : [ {
              "snapshot-id" : 638933773299822130,
              "timestamp-ms" : 1662532818843,
              "sequence-number" : 0,
              "summary" : {
                "operation" : "append",
                "spark.app.id" : "local-1662532784305",
                "added-data-files" : "4",
                "added-records" : "4",
                "added-files-size" : "6001"
              },
              "manifest-list" : "/home/iceberg/warehouse/nyc/taxis/metadata/snap-638933773299822130-1-7e6760f0-4f6c-4b23-b907-0a5a174e3863.avro",
              "schema-id" : 0
            } ],
            "snapshot-log" : [ {
              "timestamp-ms" : 1662532818843,
              "snapshot-id" : 638933773299822130
            } ],
            "metadata-log" : [ {
              "timestamp-ms" : 1662532805245,
              "metadata-file" : "/home/iceberg/warehouse/nyc/taxis/metadata/00000-8a62c37d-4573-4021-952a-c0baef7d21d0.metadata.json"
            } ]
          }
        "#;

        let schema = Schema::builder()
            .with_fields(vec![
                Arc::new(NestedField::optional(
                    1,
                    "vendor_id",
                    Type::Primitive(PrimitiveType::Long),
                )),
                Arc::new(NestedField::optional(
                    2,
                    "trip_id",
                    Type::Primitive(PrimitiveType::Long),
                )),
                Arc::new(NestedField::optional(
                    3,
                    "trip_distance",
                    Type::Primitive(PrimitiveType::Float),
                )),
                Arc::new(NestedField::optional(
                    4,
                    "fare_amount",
                    Type::Primitive(PrimitiveType::Double),
                )),
                Arc::new(NestedField::optional(
                    5,
                    "store_and_fwd_flag",
                    Type::Primitive(PrimitiveType::String),
                )),
            ])
            .build()
            .unwrap();

        let partition_spec = PartitionSpec::builder(&schema)
            .with_spec_id(0)
            .add_partition_field("vendor_id", "vendor_id", Transform::Identity)
            .unwrap()
            .build()
            .unwrap();

        let sort_order = SortOrder::builder()
            .with_order_id(0)
            .build_unbound()
            .unwrap();

        let snapshot = Snapshot::builder()
            .with_snapshot_id(638933773299822130)
            .with_timestamp_ms(1662532818843)
            .with_sequence_number(0)
            .with_schema_id(0)
            .with_manifest_list("/home/iceberg/warehouse/nyc/taxis/metadata/snap-638933773299822130-1-7e6760f0-4f6c-4b23-b907-0a5a174e3863.avro")
            .with_summary(Summary { operation: Operation::Append, other: HashMap::from_iter(vec![("spark.app.id".to_string(), "local-1662532784305".to_string()), ("added-data-files".to_string(), "4".to_string()), ("added-records".to_string(), "4".to_string()), ("added-files-size".to_string(), "6001".to_string())]) })
            .build();

        let expected = TableMetadata {
            format_version: FormatVersion::V1,
            table_uuid: Uuid::parse_str("df838b92-0b32-465d-a44e-d39936e538b7").unwrap(),
            location: "/home/iceberg/warehouse/nyc/taxis".to_string(),
            last_updated_ms: 1662532818843,
            last_column_id: 5,
            schemas: HashMap::from_iter(vec![(0, Arc::new(schema))]),
            current_schema_id: 0,
            partition_specs: HashMap::from_iter(vec![(0, partition_spec.into())]),
            default_spec_id: 0,
            last_partition_id: 1000,
            default_sort_order_id: 0,
            sort_orders: HashMap::from_iter(vec![(0, sort_order.into())]),
            snapshots: HashMap::from_iter(vec![(638933773299822130, Arc::new(snapshot))]),
            current_snapshot_id: Some(638933773299822130),
            last_sequence_number: 0,
            properties: HashMap::from_iter(vec![("owner".to_string(), "root".to_string())]),
            snapshot_log: vec![SnapshotLog {
                snapshot_id: 638933773299822130,
                timestamp_ms: 1662532818843,
            }],
            metadata_log: vec![MetadataLog { metadata_file: "/home/iceberg/warehouse/nyc/taxis/metadata/00000-8a62c37d-4573-4021-952a-c0baef7d21d0.metadata.json".to_string(), timestamp_ms: 1662532805245 }],
            refs: HashMap::from_iter(vec![("main".to_string(), SnapshotReference { snapshot_id: 638933773299822130, retention: SnapshotRetention::Branch { min_snapshots_to_keep: None, max_snapshot_age_ms: None, max_ref_age_ms: None } })]),
        };

        check_table_metadata_serde(data, expected);
    }

    #[test]
    fn test_invalid_table_uuid() -> Result<()> {
        let data = r#"
            {
                "format-version" : 2,
                "table-uuid": "xxxx"
            }
        "#;
        assert!(serde_json::from_str::<TableMetadata>(data).is_err());
        Ok(())
    }

    #[test]
    fn test_deserialize_table_data_v2_invalid_format_version() -> Result<()> {
        let data = r#"
            {
                "format-version" : 1
            }
        "#;
        assert!(serde_json::from_str::<TableMetadata>(data).is_err());
        Ok(())
    }

    #[test]
    fn test_table_metadata_v2_file_valid() {
        let metadata =
            fs::read_to_string("testdata/table_metadata/TableMetadataV2Valid.json").unwrap();

        let schema1 = Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![Arc::new(NestedField::required(
                1,
                "x",
                Type::Primitive(PrimitiveType::Long),
            ))])
            .build()
            .unwrap();

        let schema2 = Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![
                Arc::new(NestedField::required(
                    1,
                    "x",
                    Type::Primitive(PrimitiveType::Long),
                )),
                Arc::new(
                    NestedField::required(2, "y", Type::Primitive(PrimitiveType::Long))
                        .with_doc("comment"),
                ),
                Arc::new(NestedField::required(
                    3,
                    "z",
                    Type::Primitive(PrimitiveType::Long),
                )),
            ])
            .with_identifier_field_ids(vec![1, 2])
            .build()
            .unwrap();

        let partition_spec = PartitionSpec::builder(&schema1)
            .with_spec_id(0)
            .add_unbound_field(UnboundPartitionField {
                name: "x".to_string(),
                transform: Transform::Identity,
                source_id: 1,
                partition_id: Some(1000),
            })
            .unwrap()
            .build()
            .unwrap();

        let sort_order = SortOrder::builder()
            .with_order_id(3)
            .with_sort_field(SortField {
                source_id: 2,
                transform: Transform::Identity,
                direction: SortDirection::Ascending,
                null_order: NullOrder::First,
            })
            .with_sort_field(SortField {
                source_id: 3,
                transform: Transform::Bucket(4),
                direction: SortDirection::Descending,
                null_order: NullOrder::Last,
            })
            .build_unbound()
            .unwrap();

        let snapshot1 = Snapshot::builder()
            .with_snapshot_id(3051729675574597004)
            .with_timestamp_ms(1515100955770)
            .with_sequence_number(0)
            .with_manifest_list("s3://a/b/1.avro")
            .with_summary(Summary {
                operation: Operation::Append,
                other: HashMap::new(),
            })
            .build();

        let snapshot2 = Snapshot::builder()
            .with_snapshot_id(3055729675574597004)
            .with_parent_snapshot_id(Some(3051729675574597004))
            .with_timestamp_ms(1555100955770)
            .with_sequence_number(1)
            .with_schema_id(1)
            .with_manifest_list("s3://a/b/2.avro")
            .with_summary(Summary {
                operation: Operation::Append,
                other: HashMap::new(),
            })
            .build();

        let expected = TableMetadata {
            format_version: FormatVersion::V2,
            table_uuid: Uuid::parse_str("9c12d441-03fe-4693-9a96-a0705ddf69c1").unwrap(),
            location: "s3://bucket/test/location".to_string(),
            last_updated_ms: 1602638573590,
            last_column_id: 3,
            schemas: HashMap::from_iter(vec![(0, Arc::new(schema1)), (1, Arc::new(schema2))]),
            current_schema_id: 1,
            partition_specs: HashMap::from_iter(vec![(0, partition_spec.into())]),
            default_spec_id: 0,
            last_partition_id: 1000,
            default_sort_order_id: 3,
            sort_orders: HashMap::from_iter(vec![(3, sort_order.into())]),
            snapshots: HashMap::from_iter(vec![
                (3051729675574597004, Arc::new(snapshot1)),
                (3055729675574597004, Arc::new(snapshot2)),
            ]),
            current_snapshot_id: Some(3055729675574597004),
            last_sequence_number: 34,
            properties: HashMap::new(),
            snapshot_log: vec![
                SnapshotLog {
                    snapshot_id: 3051729675574597004,
                    timestamp_ms: 1515100955770,
                },
                SnapshotLog {
                    snapshot_id: 3055729675574597004,
                    timestamp_ms: 1555100955770,
                },
            ],
            metadata_log: Vec::new(),
            refs: HashMap::from_iter(vec![("main".to_string(), SnapshotReference {
                snapshot_id: 3055729675574597004,
                retention: SnapshotRetention::Branch {
                    min_snapshots_to_keep: None,
                    max_snapshot_age_ms: None,
                    max_ref_age_ms: None,
                },
            })]),
        };

        check_table_metadata_serde(&metadata, expected);
    }

    #[test]
    fn test_table_metadata_v2_file_valid_minimal() {
        let metadata =
            fs::read_to_string("testdata/table_metadata/TableMetadataV2ValidMinimal.json").unwrap();

        let schema = Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                Arc::new(NestedField::required(
                    1,
                    "x",
                    Type::Primitive(PrimitiveType::Long),
                )),
                Arc::new(
                    NestedField::required(2, "y", Type::Primitive(PrimitiveType::Long))
                        .with_doc("comment"),
                ),
                Arc::new(NestedField::required(
                    3,
                    "z",
                    Type::Primitive(PrimitiveType::Long),
                )),
            ])
            .build()
            .unwrap();

        let partition_spec = PartitionSpec::builder(&schema)
            .with_spec_id(0)
            .add_unbound_field(UnboundPartitionField {
                name: "x".to_string(),
                transform: Transform::Identity,
                source_id: 1,
                partition_id: Some(1000),
            })
            .unwrap()
            .build()
            .unwrap();

        let sort_order = SortOrder::builder()
            .with_order_id(3)
            .with_sort_field(SortField {
                source_id: 2,
                transform: Transform::Identity,
                direction: SortDirection::Ascending,
                null_order: NullOrder::First,
            })
            .with_sort_field(SortField {
                source_id: 3,
                transform: Transform::Bucket(4),
                direction: SortDirection::Descending,
                null_order: NullOrder::Last,
            })
            .build_unbound()
            .unwrap();

        let expected = TableMetadata {
            format_version: FormatVersion::V2,
            table_uuid: Uuid::parse_str("9c12d441-03fe-4693-9a96-a0705ddf69c1").unwrap(),
            location: "s3://bucket/test/location".to_string(),
            last_updated_ms: 1602638573590,
            last_column_id: 3,
            schemas: HashMap::from_iter(vec![(0, Arc::new(schema))]),
            current_schema_id: 0,
            partition_specs: HashMap::from_iter(vec![(0, partition_spec.into())]),
            default_spec_id: 0,
            last_partition_id: 1000,
            default_sort_order_id: 3,
            sort_orders: HashMap::from_iter(vec![(3, sort_order.into())]),
            snapshots: HashMap::default(),
            current_snapshot_id: None,
            last_sequence_number: 34,
            properties: HashMap::new(),
            snapshot_log: vec![],
            metadata_log: Vec::new(),
            refs: HashMap::new(),
        };

        check_table_metadata_serde(&metadata, expected);
    }

    #[test]
    fn test_table_metadata_v1_file_valid() {
        let metadata =
            fs::read_to_string("testdata/table_metadata/TableMetadataV1Valid.json").unwrap();

        let schema = Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                Arc::new(NestedField::required(
                    1,
                    "x",
                    Type::Primitive(PrimitiveType::Long),
                )),
                Arc::new(
                    NestedField::required(2, "y", Type::Primitive(PrimitiveType::Long))
                        .with_doc("comment"),
                ),
                Arc::new(NestedField::required(
                    3,
                    "z",
                    Type::Primitive(PrimitiveType::Long),
                )),
            ])
            .build()
            .unwrap();

        let partition_spec = PartitionSpec::builder(&schema)
            .with_spec_id(0)
            .add_unbound_field(UnboundPartitionField {
                name: "x".to_string(),
                transform: Transform::Identity,
                source_id: 1,
                partition_id: Some(1000),
            })
            .unwrap()
            .build()
            .unwrap();

        let expected = TableMetadata {
            format_version: FormatVersion::V1,
            table_uuid: Uuid::parse_str("d20125c8-7284-442c-9aea-15fee620737c").unwrap(),
            location: "s3://bucket/test/location".to_string(),
            last_updated_ms: 1602638573874,
            last_column_id: 3,
            schemas: HashMap::from_iter(vec![(0, Arc::new(schema))]),
            current_schema_id: 0,
            partition_specs: HashMap::from_iter(vec![(0, partition_spec.into())]),
            default_spec_id: 0,
            last_partition_id: 0,
            default_sort_order_id: 0,
            sort_orders: HashMap::new(),
            snapshots: HashMap::new(),
            current_snapshot_id: None,
            last_sequence_number: 0,
            properties: HashMap::new(),
            snapshot_log: vec![],
            metadata_log: Vec::new(),
            refs: HashMap::from_iter(vec![("main".to_string(), SnapshotReference {
                snapshot_id: -1,
                retention: SnapshotRetention::Branch {
                    min_snapshots_to_keep: None,
                    max_snapshot_age_ms: None,
                    max_ref_age_ms: None,
                },
            })]),
        };

        check_table_metadata_serde(&metadata, expected);
    }

    #[test]
    fn test_table_metadata_v2_schema_not_found() {
        let metadata =
            fs::read_to_string("testdata/table_metadata/TableMetadataV2CurrentSchemaNotFound.json")
                .unwrap();

        let desered: Result<TableMetadata, serde_json::Error> = serde_json::from_str(&metadata);

        assert_eq!(
            desered.unwrap_err().to_string(),
            "DataInvalid => No schema exists with the current schema id 2."
        )
    }

    #[test]
    fn test_table_metadata_v2_missing_sort_order() {
        let metadata =
            fs::read_to_string("testdata/table_metadata/TableMetadataV2MissingSortOrder.json")
                .unwrap();

        let desered: Result<TableMetadata, serde_json::Error> = serde_json::from_str(&metadata);

        assert_eq!(
            desered.unwrap_err().to_string(),
            "data did not match any variant of untagged enum TableMetadataEnum"
        )
    }

    #[test]
    fn test_table_metadata_v2_missing_partition_specs() {
        let metadata =
            fs::read_to_string("testdata/table_metadata/TableMetadataV2MissingPartitionSpecs.json")
                .unwrap();

        let desered: Result<TableMetadata, serde_json::Error> = serde_json::from_str(&metadata);

        assert_eq!(
            desered.unwrap_err().to_string(),
            "data did not match any variant of untagged enum TableMetadataEnum"
        )
    }

    #[test]
    fn test_table_metadata_v2_missing_last_partition_id() {
        let metadata = fs::read_to_string(
            "testdata/table_metadata/TableMetadataV2MissingLastPartitionId.json",
        )
        .unwrap();

        let desered: Result<TableMetadata, serde_json::Error> = serde_json::from_str(&metadata);

        assert_eq!(
            desered.unwrap_err().to_string(),
            "data did not match any variant of untagged enum TableMetadataEnum"
        )
    }

    #[test]
    fn test_table_metadata_v2_missing_schemas() {
        let metadata =
            fs::read_to_string("testdata/table_metadata/TableMetadataV2MissingSchemas.json")
                .unwrap();

        let desered: Result<TableMetadata, serde_json::Error> = serde_json::from_str(&metadata);

        assert_eq!(
            desered.unwrap_err().to_string(),
            "data did not match any variant of untagged enum TableMetadataEnum"
        )
    }

    #[test]
    fn test_table_metadata_v2_unsupported_version() {
        let metadata =
            fs::read_to_string("testdata/table_metadata/TableMetadataUnsupportedVersion.json")
                .unwrap();

        let desered: Result<TableMetadata, serde_json::Error> = serde_json::from_str(&metadata);

        assert_eq!(
            desered.unwrap_err().to_string(),
            "data did not match any variant of untagged enum TableMetadataEnum"
        )
    }

    #[test]
    fn test_order_of_format_version() {
        assert!(FormatVersion::V1 < FormatVersion::V2);
        assert_eq!(FormatVersion::V1, FormatVersion::V1);
        assert_eq!(FormatVersion::V2, FormatVersion::V2);
    }

    #[test]
    fn test_default_partition_spec() {
        let default_spec_id = 1234;
        let mut table_meta_data = get_test_table_metadata("TableMetadataV2Valid.json");
        table_meta_data.default_spec_id = default_spec_id;
        table_meta_data
            .partition_specs
            .insert(default_spec_id, Arc::new(PartitionSpec::default()));

        assert_eq!(
            table_meta_data.default_partition_spec(),
            table_meta_data.partition_spec_by_id(default_spec_id)
        );
    }
    #[test]
    fn test_default_sort_order() {
        let default_sort_order_id = 1234;
        let mut table_meta_data = get_test_table_metadata("TableMetadataV2Valid.json");
        table_meta_data.default_sort_order_id = default_sort_order_id;
        table_meta_data
            .sort_orders
            .insert(default_sort_order_id, Arc::new(SortOrder::default()));

        assert_eq!(
            table_meta_data.default_sort_order(),
            table_meta_data.sort_orders.get(&default_sort_order_id)
        )
    }

    #[test]
    fn test_table_metadata_builder_from_table_creation() {
        let table_creation = TableCreation::builder()
            .location("s3://db/table".to_string())
            .name("table".to_string())
            .properties(HashMap::new())
            .schema(Schema::builder().build().unwrap())
            .build();
        let table_metadata = TableMetadataBuilder::from_table_creation(table_creation)
            .unwrap()
            .build()
            .unwrap();
        assert_eq!(table_metadata.location, "s3://db/table");
        assert_eq!(table_metadata.schemas.len(), 1);
        assert_eq!(
            table_metadata
                .schemas
                .get(&0)
                .unwrap()
                .as_struct()
                .fields()
                .len(),
            0
        );
        assert_eq!(table_metadata.properties.len(), 0);
        assert_eq!(
            table_metadata.partition_specs,
            HashMap::from([(
                0,
                Arc::new(
                    PartitionSpec::builder(table_metadata.schemas.get(&0).unwrap())
                        .with_spec_id(0)
                        .build()
                        .unwrap()
                )
            )])
        );
        assert_eq!(
            table_metadata.sort_orders,
            HashMap::from([(
                0,
                Arc::new(SortOrder {
                    order_id: 0,
                    fields: vec![]
                })
            )])
        );
    }

    #[test]
    fn test_table_builder_from_table_metadata() {
        let table_metadata = get_test_table_metadata("TableMetadataV2Valid.json");
        let table_metadata_builder = TableMetadataBuilder::new(table_metadata);
        let uuid = Uuid::new_v4();
        let table_metadata = table_metadata_builder
            .assign_uuid(uuid)
            .unwrap()
            .build()
            .unwrap();
        assert_eq!(table_metadata.uuid(), uuid);
    }
}
