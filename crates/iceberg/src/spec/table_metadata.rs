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
Defines the [table metadata](https://iceberg.apache.org/spec/#table-metadata).
The main struct here is [TableMetadataV2] which defines the data for a table.
*/

use std::{collections::HashMap, sync::Arc};

use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};
use uuid::Uuid;

use crate::{Error, ErrorKind};

use super::{
    partition::PartitionSpec,
    schema::Schema,
    snapshot::{Snapshot, SnapshotReference, SnapshotRetention},
    sort::SortOrder,
};

use _serde::TableMetadataEnum;

static MAIN_BRANCH: &str = "main";
static DEFAULT_SPEC_ID: i32 = 0;

#[derive(Debug, PartialEq, Serialize, Deserialize, Eq, Clone)]
#[serde(try_from = "TableMetadataEnum", into = "TableMetadataEnum")]
/// Fields for the version 2 of the table metadata.
pub struct TableMetadata {
    /// Integer Version for the format.
    format_version: FormatVersion,
    /// A UUID that identifies the table
    table_uuid: Uuid,
    /// Location tables base location
    location: String,
    /// The tables highest sequence number
    last_sequence_number: i64,
    /// Timestamp in milliseconds from the unix epoch when the table was last updated.
    last_updated_ms: i64,
    /// An integer; the highest assigned column ID for the table.
    last_column_id: i32,
    /// A list of schemas, stored as objects with schema-id.
    schemas: HashMap<i32, Arc<Schema>>,
    /// ID of the table’s current schema.
    current_schema_id: i32,
    /// A list of partition specs, stored as full partition spec objects.
    partition_specs: HashMap<i32, PartitionSpec>,
    /// ID of the “current” spec that writers should use by default.
    default_spec_id: i32,
    /// An integer; the highest assigned partition field ID across all partition specs for the table.
    last_partition_id: i32,
    ///A string to string map of table properties. This is used to control settings that
    /// affect reading and writing and is not intended to be used for arbitrary metadata.
    /// For example, commit.retry.num-retries is used to control the number of commit retries.
    properties: HashMap<String, String>,
    /// long ID of the current table snapshot; must be the same as the current
    /// ID of the main branch in refs.
    current_snapshot_id: Option<i64>,
    ///A list of valid snapshots. Valid snapshots are snapshots for which all
    /// data files exist in the file system. A data file must not be deleted
    /// from the file system until the last snapshot in which it was listed is
    /// garbage collected.
    snapshots: Option<HashMap<i64, Arc<Snapshot>>>,
    /// A list (optional) of timestamp and snapshot ID pairs that encodes changes
    /// to the current snapshot for the table. Each time the current-snapshot-id
    /// is changed, a new entry should be added with the last-updated-ms
    /// and the new current-snapshot-id. When snapshots are expired from
    /// the list of valid snapshots, all entries before a snapshot that has
    /// expired should be removed.
    snapshot_log: Vec<SnapshotLog>,

    /// A list (optional) of timestamp and metadata file location pairs
    /// that encodes changes to the previous metadata files for the table.
    /// Each time a new metadata file is created, a new entry of the
    /// previous metadata file location should be added to the list.
    /// Tables can be configured to remove oldest metadata log entries and
    /// keep a fixed-size log of the most recent entries after a commit.
    metadata_log: Vec<MetadataLog>,

    /// A list of sort orders, stored as full sort order objects.
    sort_orders: HashMap<i64, SortOrder>,
    /// Default sort order id of the table. Note that this could be used by
    /// writers, but is not used when reading because reads use the specs
    /// stored in manifest files.
    default_sort_order_id: i64,
    ///A map of snapshot references. The map keys are the unique snapshot reference
    /// names in the table, and the map values are snapshot reference objects.
    /// There is always a main branch reference pointing to the current-snapshot-id
    /// even if the refs map is null.
    refs: HashMap<String, SnapshotReference>,
}

impl TableMetadata {
    /// Get current schema
    #[inline]
    pub fn current_schema(&self) -> Result<Arc<Schema>, Error> {
        self.schemas
            .get(&self.current_schema_id)
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!("Schema id {} not found!", self.current_schema_id),
                )
            })
            .cloned()
    }
    /// Get default partition spec
    #[inline]
    pub fn default_partition_spec(&self) -> Result<&PartitionSpec, Error> {
        self.partition_specs
            .get(&self.default_spec_id)
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!("Partition spec id {} not found!", self.default_spec_id),
                )
            })
    }

    /// Get current snapshot
    #[inline]
    pub fn current_snapshot(&self) -> Result<Option<Arc<Snapshot>>, Error> {
        match (&self.current_snapshot_id, &self.snapshots) {
            (Some(snapshot_id), Some(snapshots)) => Ok(snapshots.get(snapshot_id).cloned()),
            (Some(-1), None) => Ok(None),
            (None, None) => Ok(None),
            (Some(_), None) => Err(Error::new(
                ErrorKind::DataInvalid,
                "Snapshot id is provided but there are no snapshots".to_string(),
            )),
            (None, Some(_)) => Err(Error::new(
                ErrorKind::DataInvalid,
                "There are snapshots but no snapshot id is provided".to_string(),
            )),
        }
    }

    /// Append snapshot to table
    pub fn append_snapshot(&mut self, snapshot: Snapshot) -> Result<(), Error> {
        self.last_updated_ms = snapshot.timestamp();
        self.last_sequence_number = snapshot.sequence_number();

        self.refs
            .entry(MAIN_BRANCH.to_string())
            .and_modify(|s| {
                s.snapshot_id = snapshot.snapshot_id();
            })
            .or_insert_with(|| {
                SnapshotReference::new(
                    snapshot.snapshot_id(),
                    SnapshotRetention::Branch {
                        min_snapshots_to_keep: None,
                        max_snapshot_age_ms: None,
                        max_ref_age_ms: None,
                    },
                )
            });

        if let Some(snapshots) = &mut self.snapshots {
            self.snapshot_log.push(snapshot.log());
            snapshots.insert(snapshot.snapshot_id(), Arc::new(snapshot));
        } else {
            if !self.snapshot_log.is_empty() {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    "Snapshot logs is empty while snapshots is not!",
                ));
            }

            self.snapshot_log = vec![snapshot.log()];
            self.snapshots = Some(HashMap::from_iter(vec![(
                snapshot.snapshot_id(),
                Arc::new(snapshot),
            )]));
        }

        Ok(())
    }
}

pub(super) mod _serde {
    /// This is a helper module that defines types to help with serialization/deserialization.
    /// For deserialization the input first gets read into either the [TableMetadataV1] or [TableMetadataV2] struct
    /// and then converted into the [TableMetadata] struct. Serialization works the other way around.
    /// [TableMetadataV1] and [TableMetadataV2] are internal struct that are only used for serialization and deserialization.
    use std::{collections::HashMap, sync::Arc};

    use serde::{Deserialize, Serialize};
    use uuid::Uuid;

    use crate::{
        spec::{
            schema::_serde::{SchemaV1, SchemaV2},
            snapshot::_serde::{SnapshotV1, SnapshotV2},
            PartitionField, PartitionSpec, Schema, SnapshotReference, SnapshotRetention, SortOrder,
        },
        Error,
    };

    use super::{
        FormatVersion, MetadataLog, SnapshotLog, TableMetadata, DEFAULT_SPEC_ID, MAIN_BRANCH,
    };

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
        pub sort_orders: Vec<SortOrder>,
        pub default_sort_order_id: i64,
    }

    /// Helper to serialize and deserialize the format version.
    #[derive(Debug, PartialEq, Eq)]
    pub(super) struct VersionNumber<const V: u8>;

    impl<const V: u8> Serialize for VersionNumber<V> {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            serializer.serialize_u8(V)
        }
    }

    impl<'de, const V: u8> Deserialize<'de> for VersionNumber<V> {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: serde::Deserializer<'de>,
        {
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

    impl From<TableMetadata> for TableMetadataEnum {
        fn from(value: TableMetadata) -> Self {
            match value.format_version {
                FormatVersion::V2 => TableMetadataEnum::V2(value.into()),
                FormatVersion::V1 => TableMetadataEnum::V1(value.into()),
            }
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
            Ok(TableMetadata {
                format_version: FormatVersion::V2,
                table_uuid: value.table_uuid,
                location: value.location,
                last_sequence_number: value.last_sequence_number,
                last_updated_ms: value.last_updated_ms,
                last_column_id: value.last_column_id,
                schemas: HashMap::from_iter(
                    value
                        .schemas
                        .into_iter()
                        .map(|schema| Ok((schema.schema_id, Arc::new(schema.try_into()?))))
                        .collect::<Result<Vec<_>, Error>>()?
                        .into_iter(),
                ),
                current_schema_id: value.current_schema_id,
                partition_specs: HashMap::from_iter(
                    value.partition_specs.into_iter().map(|x| (x.spec_id, x)),
                ),
                default_spec_id: value.default_spec_id,
                last_partition_id: value.last_partition_id,
                properties: value.properties.unwrap_or_default(),
                current_snapshot_id,
                snapshots: value.snapshots.map(|snapshots| {
                    HashMap::from_iter(
                        snapshots
                            .into_iter()
                            .map(|x| (x.snapshot_id, Arc::new(x.into()))),
                    )
                }),
                snapshot_log: value.snapshot_log.unwrap_or_default(),
                metadata_log: value.metadata_log.unwrap_or_default(),
                sort_orders: HashMap::from_iter(
                    value.sort_orders.into_iter().map(|x| (x.order_id, x)),
                ),
                default_sort_order_id: value.default_sort_order_id,
                refs: value.refs.unwrap_or_else(|| {
                    if let Some(snapshot_id) = current_snapshot_id {
                        HashMap::from_iter(vec![(
                            MAIN_BRANCH.to_string(),
                            SnapshotReference {
                                snapshot_id,
                                retention: SnapshotRetention::Branch {
                                    min_snapshots_to_keep: None,
                                    max_snapshot_age_ms: None,
                                    max_ref_age_ms: None,
                                },
                            },
                        )])
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
                            spec_id: DEFAULT_SPEC_ID,
                            fields: value.partition_spec,
                        }]
                    })
                    .into_iter()
                    .map(|x| (x.spec_id, x)),
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
                current_snapshot_id: if let &Some(-1) = &value.current_snapshot_id {
                    None
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
                    .transpose()?,
                snapshot_log: value.snapshot_log.unwrap_or_default(),
                metadata_log: value.metadata_log.unwrap_or_default(),
                sort_orders: HashMap::from_iter(
                    value.sort_orders.into_iter().map(|x| (x.order_id, x)),
                ),
                default_sort_order_id: value.default_sort_order_id,
                refs: HashMap::from_iter(vec![(
                    MAIN_BRANCH.to_string(),
                    SnapshotReference {
                        snapshot_id: value.current_snapshot_id.unwrap_or_default(),
                        retention: SnapshotRetention::Branch {
                            min_snapshots_to_keep: None,
                            max_snapshot_age_ms: None,
                            max_ref_age_ms: None,
                        },
                    },
                )]),
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
                partition_specs: v.partition_specs.into_values().collect(),
                default_spec_id: v.default_spec_id,
                last_partition_id: v.last_partition_id,
                properties: if v.properties.is_empty() {
                    None
                } else {
                    Some(v.properties)
                },
                current_snapshot_id: v.current_snapshot_id.or(Some(-1)),
                snapshots: v.snapshots.map(|snapshots| {
                    snapshots
                        .into_values()
                        .map(|x| {
                            Arc::try_unwrap(x)
                                .unwrap_or_else(|snapshot| snapshot.as_ref().clone())
                                .into()
                        })
                        .collect()
                }),
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
                sort_orders: v.sort_orders.into_values().collect(),
                default_sort_order_id: v.default_sort_order_id,
                refs: Some(v.refs),
            }
        }
    }

    impl From<TableMetadata> for TableMetadataV1 {
        fn from(v: TableMetadata) -> Self {
            TableMetadataV1 {
                format_version: VersionNumber::<1>,
                table_uuid: Some(v.table_uuid),
                location: v.location,
                last_updated_ms: v.last_updated_ms,
                last_column_id: v.last_column_id,
                schema: v
                    .schemas
                    .get(&v.current_schema_id)
                    .unwrap()
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
                    .map(|x| x.fields.clone())
                    .unwrap_or_default(),
                partition_specs: Some(v.partition_specs.into_values().collect()),
                default_spec_id: Some(v.default_spec_id),
                last_partition_id: Some(v.last_partition_id),
                properties: if v.properties.is_empty() {
                    None
                } else {
                    Some(v.properties)
                },
                current_snapshot_id: v.current_snapshot_id.or(Some(-1)),
                snapshots: v.snapshots.map(|snapshots| {
                    snapshots
                        .into_values()
                        .map(|x| {
                            Arc::try_unwrap(x)
                                .unwrap_or_else(|snapshot| snapshot.as_ref().clone())
                                .into()
                        })
                        .collect()
                }),
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
                sort_orders: v.sort_orders.into_values().collect(),
                default_sort_order_id: v.default_sort_order_id,
            }
        }
    }
}

#[derive(Debug, Serialize_repr, Deserialize_repr, PartialEq, Eq, Clone)]
#[repr(u8)]
/// Iceberg format version
pub enum FormatVersion {
    /// Iceberg spec version 1
    V1 = b'1',
    /// Iceberg spec version 2
    V2 = b'2',
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

#[cfg(test)]
mod tests {

    use std::{collections::HashMap, sync::Arc};

    use anyhow::Result;
    use uuid::Uuid;

    use pretty_assertions::assert_eq;

    use crate::spec::{
        table_metadata::TableMetadata, ManifestList, NestedField, Operation, PartitionField,
        PartitionSpec, PrimitiveType, Schema, Snapshot, SnapshotReference, SnapshotRetention,
        SortOrder, Summary, Transform, Type,
    };

    use super::{FormatVersion, MetadataLog, SnapshotLog};

    fn check_table_metadata_serde(json: &str, expected_type: TableMetadata) {
        let desered_type: TableMetadata = serde_json::from_str(json).unwrap();
        assert_eq!(desered_type, expected_type);

        let sered_json = serde_json::to_string(&expected_type).unwrap();
        let parsed_json_value = serde_json::from_str::<TableMetadata>(&sered_json).unwrap();

        assert_eq!(parsed_json_value, desered_type);
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

        let partition_spec = PartitionSpec::builder()
            .with_spec_id(1)
            .with_partition_field(PartitionField {
                name: "ts_day".to_string(),
                transform: Transform::Day,
                source_id: 4,
                field_id: 1000,
            })
            .build()
            .unwrap();

        let expected = TableMetadata {
            format_version: FormatVersion::V2,
            table_uuid: Uuid::parse_str("fb072c92-a02b-11e9-ae9c-1bb7bc9eca94").unwrap(),
            location: "s3://b/wh/data.db/table".to_string(),
            last_updated_ms: 1515100955770,
            last_column_id: 1,
            schemas: HashMap::from_iter(vec![(1, Arc::new(schema))]),
            current_schema_id: 1,
            partition_specs: HashMap::from_iter(vec![(1, partition_spec)]),
            default_spec_id: 1,
            last_partition_id: 1000,
            default_sort_order_id: 0,
            sort_orders: HashMap::from_iter(vec![]),
            snapshots: None,
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

        let partition_spec = PartitionSpec::builder()
            .with_spec_id(0)
            .with_partition_field(PartitionField {
                name: "vendor_id".to_string(),
                transform: Transform::Identity,
                source_id: 1,
                field_id: 1000,
            })
            .build()
            .unwrap();

        let sort_order = SortOrder::builder().with_order_id(0).build().unwrap();

        let snapshot = Snapshot::builder()
            .with_snapshot_id(638933773299822130)
            .with_timestamp_ms(1662532818843)
            .with_sequence_number(0)
            .with_schema_id(0)
            .with_manifest_list(ManifestList::ManifestListFile("/home/iceberg/warehouse/nyc/taxis/metadata/snap-638933773299822130-1-7e6760f0-4f6c-4b23-b907-0a5a174e3863.avro".to_string()))
            .with_summary(Summary{operation: Operation::Append, other: HashMap::from_iter(vec![("spark.app.id".to_string(),"local-1662532784305".to_string()),("added-data-files".to_string(),"4".to_string()),("added-records".to_string(),"4".to_string()),("added-files-size".to_string(),"6001".to_string())])})
            .build().unwrap();

        let expected = TableMetadata {
            format_version: FormatVersion::V1,
            table_uuid: Uuid::parse_str("df838b92-0b32-465d-a44e-d39936e538b7").unwrap(),
            location: "/home/iceberg/warehouse/nyc/taxis".to_string(),
            last_updated_ms: 1662532818843,
            last_column_id: 5,
            schemas: HashMap::from_iter(vec![(0, Arc::new(schema))]),
            current_schema_id: 0,
            partition_specs: HashMap::from_iter(vec![(0, partition_spec)]),
            default_spec_id: 0,
            last_partition_id: 1000,
            default_sort_order_id: 0,
            sort_orders: HashMap::from_iter(vec![(0, sort_order)]),
            snapshots: Some(HashMap::from_iter(vec![(638933773299822130, Arc::new(snapshot))])),
            current_snapshot_id: Some(638933773299822130),
            last_sequence_number: 0,
            properties: HashMap::from_iter(vec![("owner".to_string(),"root".to_string())]),
            snapshot_log: vec![SnapshotLog {
                snapshot_id: 638933773299822130,
                timestamp_ms: 1662532818843,
            }],
            metadata_log: vec![MetadataLog{metadata_file:"/home/iceberg/warehouse/nyc/taxis/metadata/00000-8a62c37d-4573-4021-952a-c0baef7d21d0.metadata.json".to_string(), timestamp_ms: 1662532805245}],
            refs: HashMap::from_iter(vec![("main".to_string(),SnapshotReference{snapshot_id: 638933773299822130, retention: SnapshotRetention::Branch { min_snapshots_to_keep: None, max_snapshot_age_ms: None, max_ref_age_ms: None }})])
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
}
