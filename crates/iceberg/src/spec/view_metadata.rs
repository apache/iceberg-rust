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

//! Defines the [view metadata](https://iceberg.apache.org/view-spec/#view-metadata).
//! The main struct here is [ViewMetadata] which defines the data for a table.

use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};
use std::cmp::Ordering;
use std::fmt::{Display, Formatter};
use std::{collections::HashMap, sync::Arc};
use uuid::Uuid;

use super::{
    view_version::{ViewVersion, ViewVersionRef},
    SchemaId, SchemaRef,
};
use crate::catalog::ViewCreation;
use crate::error::Result;

use _serde::ViewMetadataEnum;

use chrono::{DateTime, TimeZone, Utc};

/// Reference to [`ViewMetadata`].
pub type ViewMetadataRef = Arc<ViewMetadata>;

#[derive(Debug, PartialEq, Deserialize, Eq, Clone)]
#[serde(try_from = "ViewMetadataEnum", into = "ViewMetadataEnum")]
/// Fields for the version 1 of the view metadata.
///
/// We assume that this data structure is always valid, so we will panic when invalid error happens.
/// We check the validity of this data structure when constructing.
pub struct ViewMetadata {
    /// Integer Version for the format.
    pub(crate) format_version: ViewFormatVersion,
    /// A UUID that identifies the view, generated when the view is created.
    pub(crate) view_uuid: Uuid,
    /// The view's base location; used to create metadata file locations
    pub(crate) location: String,
    /// ID of the current version of the view (version-id)
    pub current_version_id: i64,
    /// A list of known versions of the view
    pub versions: HashMap<i64, ViewVersionRef>,
    /// A list of version log entries with the timestamp and version-id for every
    /// change to current-version-id
    pub version_log: Vec<ViewVersionLog>,
    /// A list of schemas, stored as objects with schema-id.
    pub(crate) schemas: HashMap<i32, SchemaRef>,
    /// A string to string map of view properties.
    /// Properties are used for metadata such as comment and for settings that
    /// affect view maintenance. This is not intended to be used for arbitrary metadata.
    pub(crate) properties: HashMap<String, String>,
}

impl ViewMetadata {
    /// Returns format version of this metadata.
    #[inline]
    pub fn format_version(&self) -> ViewFormatVersion {
        self.format_version
    }

    /// Returns uuid of current view.
    #[inline]
    pub fn uuid(&self) -> Uuid {
        self.view_uuid
    }

    /// Returns view location.
    #[inline]
    pub fn location(&self) -> &str {
        self.location.as_str()
    }

    /// Returns the current version id.
    #[inline]
    pub fn current_version_id(&self) -> i64 {
        self.current_version_id
    }

    /// Returns all view versions.
    #[inline]
    pub fn versions(&self) -> impl Iterator<Item = &ViewVersionRef> {
        self.versions.values()
    }

    /// Lookup a view version by id.
    #[inline]
    pub fn version_by_id(&self, version_id: i64) -> Option<&ViewVersionRef> {
        self.versions.get(&version_id)
    }

    /// Returns the current view version.
    #[inline]
    pub fn current_version(&self) -> &ViewVersionRef {
        self.versions
            .get(&self.current_version_id)
            .expect("Current version id set, but not found in view versions")
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
        let schema_id = self.current_version().schema_id();
        self.schema_by_id(schema_id)
            .expect("Current schema id set, but not found in table metadata")
    }

    /// Returns properties of the view.
    #[inline]
    pub fn properties(&self) -> &HashMap<String, String> {
        &self.properties
    }

    /// Append view version to view
    pub fn append_version(&mut self, view_version: ViewVersion) {
        self.current_version_id = view_version.version_id();

        self.version_log.push(view_version.log());
        self.versions
            .insert(view_version.version_id(), Arc::new(view_version));
    }

    /// Returns view history.
    #[inline]
    pub fn history(&self) -> &[ViewVersionLog] {
        &self.version_log
    }
}

/// Manipulating view metadata.
pub struct ViewMetadataBuilder(ViewMetadata);

impl ViewMetadataBuilder {
    /// Creates a new view metadata builder from the given table metadata.
    pub fn new(origin: ViewMetadata) -> Self {
        Self(origin)
    }

    /// Creates a new view metadata builder from the given table creation.
    pub fn from_view_creation(view_creation: ViewCreation) -> Result<Self> {
        let ViewCreation {
            location,
            schema,
            properties,
            name: _,
            representations,
            default_catalog,
            default_namespace,
            summary,
        } = view_creation;
        let initial_version_id = super::INITIAL_SEQUENCE_NUMBER;
        let version = ViewVersion::builder()
            .with_default_catalog(default_catalog)
            .with_default_namespace(default_namespace)
            .with_representations(
                representations
                    .into_iter()
                    .map(|x| x.into())
                    .collect::<Vec<_>>(),
            )
            .with_schema_id(schema.schema_id())
            .with_summary(summary)
            .with_timestamp_ms(Utc::now().timestamp_millis())
            .with_version_id(initial_version_id)
            .build();

        let versions = HashMap::from_iter(vec![(initial_version_id, version.into())]);

        let view_metadata = ViewMetadata {
            format_version: ViewFormatVersion::V1,
            view_uuid: Uuid::now_v7(),
            location,
            current_version_id: initial_version_id,
            versions,
            version_log: Vec::new(),
            schemas: HashMap::from_iter(vec![(schema.schema_id(), Arc::new(schema))]),
            properties,
        };

        Ok(Self(view_metadata))
    }

    /// Changes uuid of view metadata.
    pub fn assign_uuid(mut self, uuid: Uuid) -> Result<Self> {
        self.0.view_uuid = uuid;
        Ok(self)
    }

    /// Returns the new table metadata after changes.
    pub fn build(self) -> Result<ViewMetadata> {
        Ok(self.0)
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(rename_all = "kebab-case")]
/// A log of when each snapshot was made.
pub struct ViewVersionLog {
    /// ID that current-version-id was set to
    pub version_id: i64,
    /// Timestamp when the view's current-version-id was updated (ms from epoch)
    pub timestamp_ms: i64,
}

impl ViewVersionLog {
    /// Returns the last updated timestamp as a DateTime<Utc> with millisecond precision
    pub fn timestamp(self) -> DateTime<Utc> {
        Utc.timestamp_millis_opt(self.timestamp_ms).unwrap()
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

    use crate::spec::table_metadata::_serde::VersionNumber;
    use crate::spec::ViewVersion;
    use crate::{
        spec::{schema::_serde::SchemaV2, view_version::_serde::ViewVersionV1, ViewMetadata},
        Error, ErrorKind,
    };

    use super::{ViewFormatVersion, ViewVersionLog};

    #[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
    #[serde(untagged)]
    pub(super) enum ViewMetadataEnum {
        V1(ViewMetadataV1),
    }

    #[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
    #[serde(rename_all = "kebab-case")]
    /// Defines the structure of a v2 table metadata for serialization/deserialization
    pub(super) struct ViewMetadataV1 {
        pub format_version: VersionNumber<1>,
        pub(super) view_uuid: Uuid,
        pub(super) location: String,
        pub(super) current_version_id: i64,
        pub(super) versions: Vec<ViewVersionV1>,
        pub(super) version_log: Vec<ViewVersionLog>,
        pub(super) schemas: Vec<SchemaV2>,
        pub(super) properties: Option<std::collections::HashMap<String, String>>,
    }

    impl Serialize for ViewMetadata {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            // we must do a clone here
            let metadata_enum: ViewMetadataEnum =
                self.clone().try_into().map_err(serde::ser::Error::custom)?;

            metadata_enum.serialize(serializer)
        }
    }

    impl TryFrom<ViewMetadataEnum> for ViewMetadata {
        type Error = Error;
        fn try_from(value: ViewMetadataEnum) -> Result<Self, Error> {
            match value {
                ViewMetadataEnum::V1(value) => value.try_into(),
            }
        }
    }

    impl TryFrom<ViewMetadata> for ViewMetadataEnum {
        type Error = Error;
        fn try_from(value: ViewMetadata) -> Result<Self, Error> {
            Ok(match value.format_version {
                ViewFormatVersion::V1 => ViewMetadataEnum::V1(value.into()),
            })
        }
    }

    impl TryFrom<ViewMetadataV1> for ViewMetadata {
        type Error = Error;
        fn try_from(value: ViewMetadataV1) -> Result<Self, self::Error> {
            let schemas = HashMap::from_iter(
                value
                    .schemas
                    .into_iter()
                    .map(|schema| Ok((schema.schema_id, Arc::new(schema.try_into()?))))
                    .collect::<Result<Vec<_>, Error>>()?,
            );
            let versions = HashMap::from_iter(
                value
                    .versions
                    .into_iter()
                    .map(|x| Ok((x.version_id, Arc::new(ViewVersion::from(x)))))
                    .collect::<Result<Vec<_>, Error>>()?,
            );
            // Make sure at least the current schema exists
            let current_version =
                versions
                    .get(&value.current_version_id)
                    .ok_or(self::Error::new(
                        ErrorKind::DataInvalid,
                        format!(
                            "No version exists with the current version id {}.",
                            value.current_version_id
                        ),
                    ))?;
            if !schemas.contains_key(&current_version.schema_id()) {
                return Err(self::Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "No schema exists with the schema id {}.",
                        current_version.schema_id()
                    ),
                ));
            }

            Ok(ViewMetadata {
                format_version: ViewFormatVersion::V1,
                view_uuid: value.view_uuid,
                location: value.location,
                schemas,
                properties: value.properties.unwrap_or_default(),
                current_version_id: value.current_version_id,
                versions,
                version_log: value.version_log,
            })
        }
    }

    impl From<ViewMetadata> for ViewMetadataV1 {
        fn from(v: ViewMetadata) -> Self {
            let schemas = v
                .schemas
                .into_values()
                .map(|x| {
                    Arc::try_unwrap(x)
                        .unwrap_or_else(|schema| schema.as_ref().clone())
                        .into()
                })
                .collect();
            let versions = v
                .versions
                .into_values()
                .map(|x| {
                    Arc::try_unwrap(x)
                        .unwrap_or_else(|version| version.as_ref().clone())
                        .into()
                })
                .collect();
            ViewMetadataV1 {
                format_version: VersionNumber::<1>,
                view_uuid: v.view_uuid,
                location: v.location,
                schemas,
                properties: Some(v.properties),
                current_version_id: v.current_version_id,
                versions,
                version_log: v.version_log,
            }
        }
    }
}

#[derive(Debug, Serialize_repr, Deserialize_repr, PartialEq, Eq, Clone, Copy)]
#[repr(u8)]
/// Iceberg format version
pub enum ViewFormatVersion {
    /// Iceberg view spec version 1
    V1 = 1u8,
}

impl PartialOrd for ViewFormatVersion {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ViewFormatVersion {
    fn cmp(&self, other: &Self) -> Ordering {
        (*self as u8).cmp(&(*other as u8))
    }
}

impl Display for ViewFormatVersion {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ViewFormatVersion::V1 => write!(f, "v1"),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, fs, sync::Arc};

    use anyhow::Result;
    use uuid::Uuid;

    use pretty_assertions::assert_eq;

    use crate::{
        spec::{
            NestedField, PrimitiveType, Schema, Type, ViewMetadata, ViewRepresentation,
            ViewRepresentationsBuilder, ViewVersion,
        },
        NamespaceIdent, ViewCreation,
    };

    use super::{ViewFormatVersion, ViewMetadataBuilder, ViewVersionLog};

    fn check_view_metadata_serde(json: &str, expected_type: ViewMetadata) {
        let desered_type: ViewMetadata = serde_json::from_str(json).unwrap();
        assert_eq!(desered_type, expected_type);

        let sered_json = serde_json::to_string(&expected_type).unwrap();
        let parsed_json_value = serde_json::from_str::<ViewMetadata>(&sered_json).unwrap();

        assert_eq!(parsed_json_value, desered_type);
    }

    fn get_test_view_metadata(file_name: &str) -> ViewMetadata {
        let path = format!("testdata/view_metadata/{}", file_name);
        let metadata: String = fs::read_to_string(path).unwrap();

        serde_json::from_str(&metadata).unwrap()
    }

    #[test]
    fn test_view_data_v1() {
        let data = r#"
        {
            "view-uuid": "fa6506c3-7681-40c8-86dc-e36561f83385",
            "format-version" : 1,
            "location" : "s3://bucket/warehouse/default.db/event_agg",
            "current-version-id" : 1,
            "properties" : {
              "comment" : "Daily event counts"
            },
            "versions" : [ {
              "version-id" : 1,
              "timestamp-ms" : 1573518431292,
              "schema-id" : 1,
              "default-catalog" : "prod",
              "default-namespace" : [ "default" ],
              "summary" : {
                "engine-name" : "Spark",
                "engineVersion" : "3.3.2"
              },
              "representations" : [ {
                "type" : "sql",
                "sql" : "SELECT\n    COUNT(1), CAST(event_ts AS DATE)\nFROM events\nGROUP BY 2",
                "dialect" : "spark"
              } ]
            } ],
            "schemas": [ {
              "schema-id": 1,
              "type" : "struct",
              "fields" : [ {
                "id" : 1,
                "name" : "event_count",
                "required" : false,
                "type" : "int",
                "doc" : "Count of events"
              } ]
            } ],
            "version-log" : [ {
              "timestamp-ms" : 1573518431292,
              "version-id" : 1
            } ]
          }
        "#;

        let schema = Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![Arc::new(
                NestedField::optional(1, "event_count", Type::Primitive(PrimitiveType::Int))
                    .with_doc("Count of events"),
            )])
            .build()
            .unwrap();
        let version = ViewVersion::builder()
            .with_version_id(1)
            .with_timestamp_ms(1573518431292)
            .with_schema_id(1)
            .with_default_catalog("prod".to_string().into())
            .with_default_namespace(NamespaceIdent::from_vec(vec!["default".to_string()]).unwrap())
            .with_summary(HashMap::from_iter(vec![
                ("engineVersion".to_string(), "3.3.2".to_string()),
                ("engine-name".to_string(), "Spark".to_string()),
            ]))
            .with_representations(
                vec![ViewRepresentation::SqlViewRepresentation(
                    crate::spec::SqlViewRepresentation {
                        sql:
                            "SELECT\n    COUNT(1), CAST(event_ts AS DATE)\nFROM events\nGROUP BY 2"
                                .to_string(),
                        dialect: "spark".to_string(),
                    },
                )
                .into()]
                .into(),
            )
            .build();

        let expected = ViewMetadata {
            format_version: ViewFormatVersion::V1,
            view_uuid: Uuid::parse_str("fa6506c3-7681-40c8-86dc-e36561f83385").unwrap(),
            location: "s3://bucket/warehouse/default.db/event_agg".to_string(),
            current_version_id: 1,
            versions: HashMap::from_iter(vec![(1, Arc::new(version))]),
            version_log: vec![ViewVersionLog {
                timestamp_ms: 1573518431292,
                version_id: 1,
            }],
            schemas: HashMap::from_iter(vec![(1, Arc::new(schema))]),
            properties: HashMap::from_iter(vec![(
                "comment".to_string(),
                "Daily event counts".to_string(),
            )]),
        };

        check_view_metadata_serde(data, expected);
    }

    #[test]
    fn test_invalid_view_uuid() -> Result<()> {
        let data = r#"
            {
                "format-version" : 1,
                "view-uuid": "xxxx"
            }
        "#;
        assert!(serde_json::from_str::<ViewMetadata>(data).is_err());
        Ok(())
    }

    #[test]
    fn test_view_builder_from_view_creation() {
        let representations = ViewRepresentationsBuilder::new()
            .add_sql("Select 1".to_string(), "spark".to_string())
            .build();
        let creation = ViewCreation::builder()
            .location("s3://bucket/warehouse/default.db/event_agg".to_string())
            .name("view".to_string())
            .schema(Schema::builder().build().unwrap())
            .default_namespace(NamespaceIdent::from_vec(vec!["default".to_string()]).unwrap())
            .representations(representations)
            .build();

        let metadata = ViewMetadataBuilder::from_view_creation(creation)
            .unwrap()
            .build()
            .unwrap();

        assert_eq!(
            metadata.location(),
            "s3://bucket/warehouse/default.db/event_agg"
        );
        assert_eq!(metadata.current_version_id(), 0);
        assert_eq!(metadata.versions().count(), 1);
        assert_eq!(metadata.schemas_iter().count(), 1);
        assert_eq!(metadata.properties().len(), 0);
    }

    #[test]
    fn test_view_builder_from_table_metadata() {
        let metadata = get_test_view_metadata("ViewMetadataV2Valid.json");
        let metadata_builder = ViewMetadataBuilder::new(metadata);
        let uuid = Uuid::new_v4();
        let table_metadata = metadata_builder.assign_uuid(uuid).unwrap().build().unwrap();
        assert_eq!(table_metadata.uuid(), uuid);
    }
}
