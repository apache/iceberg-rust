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
//! The main struct here is [ViewMetadata] which defines the data for a view.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

use _serde::ViewMetadataEnum;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};
use uuid::Uuid;

pub use super::view_metadata_builder::ViewMetadataBuilder;
use super::view_version::{ViewVersionId, ViewVersionRef};
use super::{SchemaId, SchemaRef};
use crate::compression::CompressionCodec;
use crate::error::{Result, timestamp_ms_to_utc};
use crate::io::FileIO;
use crate::{Error, ErrorKind};

/// Reference to [`ViewMetadata`].
pub type ViewMetadataRef = Arc<ViewMetadata>;

// ID of the initial version of views
pub(crate) static INITIAL_VIEW_VERSION_ID: i32 = 1;

/// Property key for allowing to drop dialects when replacing a view.
pub const VIEW_PROPERTY_REPLACE_DROP_DIALECT_ALLOWED: &str = "replace.drop-dialect.allowed";
/// Default value for the property key for allowing to drop dialects when replacing a view.
pub const VIEW_PROPERTY_REPLACE_DROP_DIALECT_ALLOWED_DEFAULT: bool = false;
/// Property key for the number of history entries to keep.
pub const VIEW_PROPERTY_VERSION_HISTORY_SIZE: &str = "version.history.num-entries";
/// Default value for the property key for the number of history entries to keep.
pub const VIEW_PROPERTY_VERSION_HISTORY_SIZE_DEFAULT: usize = 10;

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
    pub(crate) current_version_id: ViewVersionId,
    /// A list of known versions of the view
    pub(crate) versions: HashMap<ViewVersionId, ViewVersionRef>,
    /// A list of version log entries with the timestamp and version-id for every
    /// change to current-version-id
    pub(crate) version_log: Vec<ViewVersionLog>,
    /// A list of schemas, stored as objects with schema-id.
    pub(crate) schemas: HashMap<SchemaId, SchemaRef>,
    /// A string to string map of view properties.
    /// Properties are used for metadata such as comment and for settings that
    /// affect view maintenance. This is not intended to be used for arbitrary metadata.
    pub(crate) properties: HashMap<String, String>,
}

impl ViewMetadata {
    /// Convert this View Metadata into a builder for modification.
    #[must_use]
    pub fn into_builder(self) -> ViewMetadataBuilder {
        ViewMetadataBuilder::new_from_metadata(self)
    }

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
    pub fn current_version_id(&self) -> ViewVersionId {
        self.current_version_id
    }

    /// Returns all view versions.
    #[inline]
    pub fn versions(&self) -> impl ExactSizeIterator<Item = &ViewVersionRef> {
        self.versions.values()
    }

    /// Lookup a view version by id.
    #[inline]
    pub fn version_by_id(&self, version_id: ViewVersionId) -> Option<&ViewVersionRef> {
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
    pub fn schemas_iter(&self) -> impl ExactSizeIterator<Item = &SchemaRef> {
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
            .expect("Current schema id set, but not found in view metadata")
    }

    /// Returns properties of the view.
    #[inline]
    pub fn properties(&self) -> &HashMap<String, String> {
        &self.properties
    }

    /// Returns view history.
    #[inline]
    pub fn history(&self) -> &[ViewVersionLog] {
        &self.version_log
    }

    /// Read view metadata from the given location.
    ///
    /// Mirrors [`crate::spec::TableMetadata::read_from`]: reads the JSON document at
    /// `metadata_location`, transparently decompressing a gzip-encoded file (detected by the
    /// gzip magic number), and deserializes it into a [`ViewMetadata`]. This is the on-disk
    /// contract Java's `ViewMetadataParser.read` produces.
    pub async fn read_from(
        file_io: &FileIO,
        metadata_location: impl AsRef<str>,
    ) -> Result<ViewMetadata> {
        let metadata_location = metadata_location.as_ref();
        let input_file = file_io.new_input(metadata_location)?;
        let metadata_content = input_file.read().await?;

        // Check if the file is compressed by looking for the gzip "magic number".
        let metadata = if metadata_content.len() > 2
            && metadata_content[0] == 0x1F
            && metadata_content[1] == 0x8B
        {
            let decompressed_data = CompressionCodec::Gzip
                .decompress(metadata_content.to_vec())
                .map_err(|error| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        "Trying to read compressed view metadata file",
                    )
                    .with_context("file_path", metadata_location)
                    .with_source(error)
                })?;
            serde_json::from_slice(&decompressed_data)?
        } else {
            serde_json::from_slice(&metadata_content)?
        };

        Ok(metadata)
    }

    /// Write view metadata to the given location.
    ///
    /// Mirrors [`crate::spec::TableMetadata::write_to`]: serializes this metadata to JSON and
    /// writes it to `metadata_location`. Java's `BaseViewOperations` always overwrites because
    /// the location embeds a UUID and is therefore unique per commit.
    pub async fn write_to(
        &self,
        file_io: &FileIO,
        metadata_location: impl AsRef<str>,
    ) -> Result<()> {
        file_io
            .new_output(metadata_location)?
            .write(serde_json::to_vec(self)?.into())
            .await
    }

    /// Validate the view metadata.
    pub(super) fn validate(&self) -> Result<()> {
        self.validate_current_version_id()?;
        self.validate_current_schema_id()?;
        Ok(())
    }

    fn validate_current_version_id(&self) -> Result<()> {
        if !self.versions.contains_key(&self.current_version_id) {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "No version exists with the current version id {}.",
                    self.current_version_id
                ),
            ));
        }
        Ok(())
    }

    fn validate_current_schema_id(&self) -> Result<()> {
        let schema_id = self.current_version().schema_id();
        if !self.schemas.contains_key(&schema_id) {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!("No schema exists with the schema id {schema_id}."),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(rename_all = "kebab-case")]
/// A log of when each snapshot was made.
pub struct ViewVersionLog {
    /// ID that current-version-id was set to
    version_id: ViewVersionId,
    /// Timestamp when the view's current-version-id was updated (ms from epoch)
    timestamp_ms: i64,
}

impl ViewVersionLog {
    #[inline]
    /// Creates a new view version log.
    pub fn new(version_id: ViewVersionId, timestamp: i64) -> Self {
        Self {
            version_id,
            timestamp_ms: timestamp,
        }
    }

    /// Returns the version id.
    #[inline]
    pub fn version_id(&self) -> ViewVersionId {
        self.version_id
    }

    /// Returns the timestamp in milliseconds from epoch.
    #[inline]
    pub fn timestamp_ms(&self) -> i64 {
        self.timestamp_ms
    }

    /// Returns the last updated timestamp as a DateTime<Utc> with millisecond precision.
    pub fn timestamp(&self) -> Result<DateTime<Utc>> {
        timestamp_ms_to_utc(self.timestamp_ms)
    }

    /// Update the timestamp of this version log.
    pub(crate) fn set_timestamp_ms(&mut self, timestamp_ms: i64) -> &mut Self {
        self.timestamp_ms = timestamp_ms;
        self
    }
}

pub(super) mod _serde {
    /// This is a helper module that defines types to help with serialization/deserialization.
    /// For deserialization the input first gets read into either the [ViewMetadataV1] struct
    /// and then converted into the [ViewMetadata] struct. Serialization works the other way around.
    /// [ViewMetadataV1] is an internal struct that are only used for serialization and deserialization.
    use std::{collections::HashMap, sync::Arc};

    use serde::{Deserialize, Serialize};
    use uuid::Uuid;

    use super::{ViewFormatVersion, ViewVersionId, ViewVersionLog};
    use crate::Error;
    use crate::spec::schema::_serde::SchemaV2;
    use crate::spec::table_metadata::_serde::VersionNumber;
    use crate::spec::view_version::_serde::ViewVersionV1;
    use crate::spec::{ViewMetadata, ViewVersion};

    #[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
    #[serde(untagged)]
    pub(super) enum ViewMetadataEnum {
        V1(ViewMetadataV1),
    }

    #[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
    #[serde(rename_all = "kebab-case")]
    /// Defines the structure of a v1 view metadata for serialization/deserialization
    pub(super) struct ViewMetadataV1 {
        pub format_version: VersionNumber<1>,
        pub(super) view_uuid: Uuid,
        pub(super) location: String,
        pub(super) current_version_id: ViewVersionId,
        pub(super) versions: Vec<ViewVersionV1>,
        pub(super) version_log: Vec<ViewVersionLog>,
        pub(super) schemas: Vec<SchemaV2>,
        pub(super) properties: Option<std::collections::HashMap<String, String>>,
    }

    impl Serialize for ViewMetadata {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where S: serde::Serializer {
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

            let view_metadata = ViewMetadata {
                format_version: ViewFormatVersion::V1,
                view_uuid: value.view_uuid,
                location: value.location,
                schemas,
                properties: value.properties.unwrap_or_default(),
                current_version_id: value.current_version_id,
                versions,
                version_log: value.version_log,
            };
            view_metadata.validate()?;
            Ok(view_metadata)
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
pub(crate) mod tests {
    use std::collections::HashMap;
    use std::fs;
    use std::sync::Arc;

    use anyhow::Result;
    use pretty_assertions::assert_eq;
    use uuid::Uuid;

    use super::{ViewFormatVersion, ViewMetadataBuilder, ViewVersionLog};
    use crate::spec::{
        INITIAL_VIEW_VERSION_ID, NestedField, PrimitiveType, Schema, SqlViewRepresentation, Type,
        ViewMetadata, ViewRepresentations, ViewVersion,
    };
    use crate::{NamespaceIdent, ViewCreation};

    fn check_view_metadata_serde(json: &str, expected_type: ViewMetadata) {
        let desered_type: ViewMetadata = serde_json::from_str(json).unwrap();
        assert_eq!(desered_type, expected_type);

        let sered_json = serde_json::to_string(&expected_type).unwrap();
        let parsed_json_value = serde_json::from_str::<ViewMetadata>(&sered_json).unwrap();

        assert_eq!(parsed_json_value, desered_type);
    }

    pub(crate) fn get_test_view_metadata(file_name: &str) -> ViewMetadata {
        let path = format!("testdata/view_metadata/{file_name}");
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
            .with_representations(ViewRepresentations(vec![
                SqlViewRepresentation {
                    sql: "SELECT\n    COUNT(1), CAST(event_ts AS DATE)\nFROM events\nGROUP BY 2"
                        .to_string(),
                    dialect: "spark".to_string(),
                }
                .into(),
            ]))
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
        let representations = ViewRepresentations(vec![
            SqlViewRepresentation {
                sql: "SELECT\n    COUNT(1), CAST(event_ts AS DATE)\nFROM events\nGROUP BY 2"
                    .to_string(),
                dialect: "spark".to_string(),
            }
            .into(),
        ]);
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
            .unwrap()
            .metadata;

        assert_eq!(
            metadata.location(),
            "s3://bucket/warehouse/default.db/event_agg"
        );
        assert_eq!(metadata.current_version_id(), INITIAL_VIEW_VERSION_ID);
        assert_eq!(metadata.versions().count(), 1);
        assert_eq!(metadata.schemas_iter().count(), 1);
        assert_eq!(metadata.properties().len(), 0);
    }

    #[test]
    fn test_view_metadata_v1_file_valid() {
        let metadata =
            fs::read_to_string("testdata/view_metadata/ViewMetadataV1Valid.json").unwrap();

        let schema = Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![
                Arc::new(
                    NestedField::optional(1, "event_count", Type::Primitive(PrimitiveType::Int))
                        .with_doc("Count of events"),
                ),
                Arc::new(NestedField::optional(
                    2,
                    "event_date",
                    Type::Primitive(PrimitiveType::Date),
                )),
            ])
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
            .with_representations(ViewRepresentations(vec![
                SqlViewRepresentation {
                    sql: "SELECT\n    COUNT(1), CAST(event_ts AS DATE)\nFROM events\nGROUP BY 2"
                        .to_string(),
                    dialect: "spark".to_string(),
                }
                .into(),
            ]))
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

        check_view_metadata_serde(&metadata, expected);
    }

    #[test]
    fn test_view_builder_assign_uuid() {
        let metadata = get_test_view_metadata("ViewMetadataV1Valid.json");
        let metadata_builder = metadata.into_builder();
        let uuid = Uuid::new_v4();
        let metadata = metadata_builder.assign_uuid(uuid).build().unwrap().metadata;
        assert_eq!(metadata.uuid(), uuid);
    }

    #[test]
    fn test_view_metadata_v1_unsupported_version() {
        let metadata =
            fs::read_to_string("testdata/view_metadata/ViewMetadataUnsupportedVersion.json")
                .unwrap();

        let desered: Result<ViewMetadata, serde_json::Error> = serde_json::from_str(&metadata);

        assert_eq!(
            desered.unwrap_err().to_string(),
            "data did not match any variant of untagged enum ViewMetadataEnum"
        )
    }

    #[test]
    fn test_view_metadata_v1_version_not_found() {
        let metadata =
            fs::read_to_string("testdata/view_metadata/ViewMetadataV1CurrentVersionNotFound.json")
                .unwrap();

        let desered: Result<ViewMetadata, serde_json::Error> = serde_json::from_str(&metadata);

        assert_eq!(
            desered.unwrap_err().to_string(),
            "DataInvalid => No version exists with the current version id 2."
        )
    }

    #[test]
    fn test_view_metadata_v1_schema_not_found() {
        let metadata =
            fs::read_to_string("testdata/view_metadata/ViewMetadataV1SchemaNotFound.json").unwrap();

        let desered: Result<ViewMetadata, serde_json::Error> = serde_json::from_str(&metadata);

        assert_eq!(
            desered.unwrap_err().to_string(),
            "DataInvalid => No schema exists with the schema id 2."
        )
    }

    #[test]
    fn test_view_metadata_v1_missing_schema_for_version() {
        let metadata =
            fs::read_to_string("testdata/view_metadata/ViewMetadataV1MissingSchema.json").unwrap();

        let desered: Result<ViewMetadata, serde_json::Error> = serde_json::from_str(&metadata);

        assert_eq!(
            desered.unwrap_err().to_string(),
            "data did not match any variant of untagged enum ViewMetadataEnum"
        )
    }

    #[test]
    fn test_view_metadata_v1_missing_current_version() {
        let metadata =
            fs::read_to_string("testdata/view_metadata/ViewMetadataV1MissingCurrentVersion.json")
                .unwrap();

        let desered: Result<ViewMetadata, serde_json::Error> = serde_json::from_str(&metadata);

        assert_eq!(
            desered.unwrap_err().to_string(),
            "data did not match any variant of untagged enum ViewMetadataEnum"
        )
    }

    // RISK: the on-disk JSON FIELD SET must match exactly what Java's `ViewMetadataParser.toJson`
    // writes — a missing or extra field means another engine cannot read what we wrote (and vice
    // versa). Java writes (in this order): view-uuid, format-version, location, [properties only
    // when non-empty], schemas, current-version-id, versions, version-log; each ViewVersion writes
    // version-id, timestamp-ms, schema-id, summary, [default-catalog only when non-null],
    // default-namespace, representations. Field ORDER is irrelevant to a serde round-trip; the
    // FIELD SET is the contract. This document is hand-pinned in Java's exact field order/set.
    #[test]
    fn test_view_metadata_parses_java_wire_field_set() {
        let java_wire = r#"
        {
          "view-uuid" : "fa6506c3-7681-40c8-86dc-e36561f83385",
          "format-version" : 1,
          "location" : "s3://bucket/warehouse/default.db/event_agg",
          "properties" : {
            "comment" : "Daily event counts"
          },
          "schemas" : [ {
            "type" : "struct",
            "schema-id" : 1,
            "fields" : [ {
              "id" : 1,
              "name" : "event_count",
              "required" : false,
              "type" : "int",
              "doc" : "Count of events"
            } ]
          } ],
          "current-version-id" : 1,
          "versions" : [ {
            "version-id" : 1,
            "timestamp-ms" : 1573518431292,
            "schema-id" : 1,
            "summary" : {
              "engine-name" : "Spark",
              "engineVersion" : "3.3.2"
            },
            "default-catalog" : "prod",
            "default-namespace" : [ "default" ],
            "representations" : [ {
              "type" : "sql",
              "sql" : "SELECT 1",
              "dialect" : "spark"
            } ]
          } ],
          "version-log" : [ {
            "timestamp-ms" : 1573518431292,
            "version-id" : 1
          } ]
        }
        "#;

        let metadata: ViewMetadata = serde_json::from_str(java_wire).unwrap();
        assert_eq!(
            metadata.uuid(),
            Uuid::parse_str("fa6506c3-7681-40c8-86dc-e36561f83385").unwrap()
        );
        assert_eq!(metadata.format_version(), ViewFormatVersion::V1);
        assert_eq!(metadata.current_version_id(), 1);
        assert_eq!(metadata.versions().count(), 1);
        assert_eq!(metadata.schemas_iter().count(), 1);
        assert_eq!(metadata.history().len(), 1);
        let version = metadata.current_version();
        assert_eq!(version.default_catalog(), Some(&"prod".to_string()));
        assert_eq!(version.default_namespace().as_ref(), &vec![
            "default".to_string()
        ]);
        assert_eq!(version.representations().len(), 1);

        // Round-trip through our own serializer: the field SET must be preserved.
        let reserialized = serde_json::to_string(&metadata).unwrap();
        let reparsed: ViewMetadata = serde_json::from_str(&reserialized).unwrap();
        assert_eq!(reparsed, metadata);
    }

    // RISK: read_from/write_to must round-trip view metadata through FileIO byte-for-byte at the
    // struct level — a catalog flips its view pointer to the file write_to produced and load_view
    // reads it back; any asymmetry corrupts the view on the next load.
    #[tokio::test]
    async fn test_view_metadata_read_write_round_trip() {
        use tempfile::TempDir;

        use crate::io::FileIO;

        let temp_dir = TempDir::new().unwrap();
        let temp_path = temp_dir.path().to_str().unwrap();
        let file_io = FileIO::new_with_fs();

        let original = get_test_view_metadata("ViewMetadataV1Valid.json");
        let metadata_location = format!("{temp_path}/00000-{}.metadata.json", Uuid::now_v7());

        original
            .write_to(&file_io, &metadata_location)
            .await
            .unwrap();
        assert!(fs::metadata(&metadata_location).is_ok());

        let read_back = ViewMetadata::read_from(&file_io, &metadata_location)
            .await
            .unwrap();
        assert_eq!(read_back, original);
    }
}
