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
 * View Versions!
*/
use crate::error::Result;
use chrono::{DateTime, TimeZone, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use typed_builder::TypedBuilder;

use super::view_metadata::ViewVersionLog;
use crate::catalog::NamespaceIdent;
use crate::spec::{SchemaId, SchemaRef, ViewMetadata};
use crate::{Error, ErrorKind};
use _serde::ViewVersionV1;

/// Reference to [`ViewVersion`].
pub type ViewVersionRef = Arc<ViewVersion>;

#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize, TypedBuilder)]
#[serde(from = "ViewVersionV1", into = "ViewVersionV1")]
#[builder(field_defaults(setter(prefix = "with_")))]
/// A view versions represents the definition of a view at a specific point in time.
pub struct ViewVersion {
    /// A unique long ID
    version_id: i64,
    /// ID of the schema for the view version
    schema_id: SchemaId,
    /// Timestamp when the version was created (ms from epoch)
    timestamp_ms: i64,
    /// A string to string map of summary metadata about the version
    summary: HashMap<String, String>,
    /// A list of representations for the view definition.
    representations: ViewRepresentations,
    /// Catalog name to use when a reference in the SELECT does not contain a catalog
    #[builder(default = None)]
    default_catalog: Option<String>,
    /// Namespace to use when a reference in the SELECT is a single identifier
    default_namespace: NamespaceIdent,
}

impl ViewVersion {
    /// Get the version id of this view version.
    #[inline]
    pub fn version_id(&self) -> i64 {
        self.version_id
    }

    /// Get the schema id of this view version.
    #[inline]
    pub fn schema_id(&self) -> SchemaId {
        self.schema_id
    }

    /// Get the timestamp of when the view version was created
    #[inline]
    pub fn timestamp(&self) -> DateTime<Utc> {
        Utc.timestamp_millis_opt(self.timestamp_ms).unwrap()
    }

    /// Get summary of the view version
    #[inline]
    pub fn summary(&self) -> &HashMap<String, String> {
        &self.summary
    }

    /// Get this views representations
    #[inline]
    pub fn representations(&self) -> &ViewRepresentations {
        &self.representations
    }

    /// Get the default catalog for this view version
    #[inline]
    pub fn default_catalog(&self) -> Option<&String> {
        self.default_catalog.as_ref()
    }

    /// Get the default namespace to use when a reference in the SELECT is a single identifier
    #[inline]
    pub fn default_namespace(&self) -> &NamespaceIdent {
        &self.default_namespace
    }

    /// Get the schema of this snapshot.
    pub fn schema(&self, view_metadata: &ViewMetadata) -> Result<SchemaRef> {
        let r = view_metadata
            .schema_by_id(self.schema_id())
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!("Schema with id {} not found", self.schema_id()),
                )
            })
            .cloned();
        r
    }

    pub(crate) fn log(&self) -> ViewVersionLog {
        ViewVersionLog {
            timestamp_ms: self.timestamp_ms,
            version_id: self.version_id,
        }
    }
}

/// A list of view representations.
pub type ViewRepresentations = Vec<ViewRepresentation>;

/// A builder for [`ViewRepresentations`].
pub struct ViewRepresentationsBuilder(ViewRepresentations);

impl ViewRepresentationsBuilder {
    /// Create a new builder.
    pub fn new() -> Self {
        Self(Vec::new())
    }

    /// Add a representation to the list.
    pub fn add_representation(mut self, representation: ViewRepresentation) -> Self {
        self.0.push(representation);
        self
    }

    /// Add a SQL representation to the list.
    pub fn add_sql_representation(self, sql: String, dialect: String) -> Self {
        self.add_representation(ViewRepresentation::SqlViewRepresentation(
            SqlViewRepresentation { sql, dialect },
        ))
    }

    /// Build the list of representations.
    pub fn build(self) -> ViewRepresentations {
        self.0
    }
}

impl Default for ViewRepresentationsBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(tag = "type")]
/// View definitions can be represented in multiple ways.
/// Representations are documented ways to express a view definition.
pub enum ViewRepresentation {
    #[serde(rename = "sql")]
    /// The SQL representation stores the view definition as a SQL SELECT,
    SqlViewRepresentation(SqlViewRepresentation),
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(rename_all = "kebab-case")]
/// The SQL representation stores the view definition as a SQL SELECT,
/// with metadata such as the SQL dialect.
pub struct SqlViewRepresentation {
    #[serde(rename = "sql")]
    /// The SQL SELECT statement that defines the view.
    pub sql: String,
    #[serde(rename = "dialect")]
    /// The dialect of the sql SELECT statement (e.g., "trino" or "spark")
    pub dialect: String,
}

pub(super) mod _serde {
    /// This is a helper module that defines types to help with serialization/deserialization.
    /// For deserialization the input first gets read into either the [SnapshotV1] or [SnapshotV2] struct
    /// and then converted into the [Snapshot] struct. Serialization works the other way around.
    /// [SnapshotV1] and [SnapshotV2] are internal struct that are only used for serialization and deserialization.
    use serde::{Deserialize, Serialize};

    use crate::catalog::NamespaceIdent;

    use super::{ViewRepresentation, ViewVersion};

    #[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
    #[serde(rename_all = "kebab-case")]
    /// Defines the structure of a v1 view version for serialization/deserialization
    pub(crate) struct ViewVersionV1 {
        pub version_id: i64,
        pub schema_id: i32,
        pub timestamp_ms: i64,
        pub summary: std::collections::HashMap<String, String>,
        pub representations: Vec<ViewRepresentation>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub default_catalog: Option<String>,
        pub default_namespace: NamespaceIdent,
    }

    impl From<ViewVersionV1> for ViewVersion {
        fn from(v1: ViewVersionV1) -> Self {
            ViewVersion {
                version_id: v1.version_id,
                schema_id: v1.schema_id,
                timestamp_ms: v1.timestamp_ms,
                summary: v1.summary,
                representations: v1.representations,
                default_catalog: v1.default_catalog,
                default_namespace: v1.default_namespace,
            }
        }
    }

    impl From<ViewVersion> for ViewVersionV1 {
        fn from(v1: ViewVersion) -> Self {
            ViewVersionV1 {
                version_id: v1.version_id,
                schema_id: v1.schema_id,
                timestamp_ms: v1.timestamp_ms,
                summary: v1.summary,
                representations: v1.representations,
                default_catalog: v1.default_catalog,
                default_namespace: v1.default_namespace,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::spec::view_version::{ViewVersion, _serde::ViewVersionV1};
    use chrono::{TimeZone, Utc};

    #[test]
    fn view_version() {
        let record = serde_json::json!(
        {
            "version-id" : 1,
            "timestamp-ms" : 1573518431292i64,
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
          }
        );

        let result: ViewVersion = serde_json::from_value::<ViewVersionV1>(record.clone())
            .unwrap()
            .into();

        // Roundtrip
        assert_eq!(serde_json::to_value(result.clone()).unwrap(), record);

        assert_eq!(result.version_id(), 1);
        assert_eq!(
            result.timestamp(),
            Utc.timestamp_millis_opt(1573518431292).unwrap()
        );
        assert_eq!(result.schema_id(), 1);
        assert_eq!(result.default_catalog, Some("prod".to_string()));
        assert_eq!(result.summary(), &{
            let mut map = std::collections::HashMap::new();
            map.insert("engine-name".to_string(), "Spark".to_string());
            map.insert("engineVersion".to_string(), "3.3.2".to_string());
            map
        });
        assert_eq!(result.representations(), &{
            vec![super::ViewRepresentation::SqlViewRepresentation(
                super::SqlViewRepresentation {
                    sql: "SELECT\n    COUNT(1), CAST(event_ts AS DATE)\nFROM events\nGROUP BY 2"
                        .to_string(),
                    dialect: "spark".to_string(),
                },
            )]
        });
        assert_eq!(
            result.default_namespace.inner(),
            vec!["default".to_string()]
        );
    }
}
