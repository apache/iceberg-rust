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

//! View API for Apache Iceberg

use std::collections::HashMap;

use crate::spec::{
    SchemaRef, SqlViewRepresentation, ViewMetadata, ViewMetadataRef, ViewRepresentation,
    ViewVersionRef,
};
use crate::{Error, ErrorKind, Result, TableIdent};

/// Builder to create a [`View`].
pub struct ViewBuilder {
    metadata_location: Option<String>,
    metadata: Option<ViewMetadataRef>,
    identifier: Option<TableIdent>,
}

impl ViewBuilder {
    pub(crate) fn new() -> Self {
        Self {
            metadata_location: None,
            metadata: None,
            identifier: None,
        }
    }

    /// required - sets the view's metadata location
    pub fn metadata_location<T: Into<String>>(mut self, metadata_location: T) -> Self {
        self.metadata_location = Some(metadata_location.into());
        self
    }

    /// required - passes in the ViewMetadata to use for the View
    pub fn metadata<T: Into<ViewMetadataRef>>(mut self, metadata: T) -> Self {
        self.metadata = Some(metadata.into());
        self
    }

    /// required - passes in the identifier to use for the View
    pub fn identifier(mut self, identifier: TableIdent) -> Self {
        self.identifier = Some(identifier);
        self
    }

    /// Build the View
    pub fn build(self) -> Result<View> {
        let Self {
            metadata_location,
            metadata,
            identifier,
        } = self;

        let Some(metadata) = metadata else {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "ViewMetadata must be provided with ViewBuilder.metadata()",
            ));
        };

        let Some(identifier) = identifier else {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Identifier must be provided with ViewBuilder.identifier()",
            ));
        };

        Ok(View {
            metadata_location,
            metadata,
            identifier,
        })
    }
}

/// See the [Iceberg View Spec](https://iceberg.apache.org/view-spec/) for details.
#[derive(Debug, Clone)]
pub struct View {
    metadata_location: Option<String>,
    metadata: ViewMetadataRef,
    identifier: TableIdent,
}

impl View {
    /// Sets the [`View`] metadata and returns an updated instance with the new metadata applied.
    pub(crate) fn with_metadata(mut self, metadata: ViewMetadataRef) -> Self {
        self.metadata = metadata;
        self
    }

    /// Sets the [`View`] metadata location and returns an updated instance.
    /// This will be used by catalog implementations when committing view updates.
    #[allow(dead_code)]
    pub(crate) fn with_metadata_location(mut self, metadata_location: String) -> Self {
        self.metadata_location = Some(metadata_location);
        self
    }

    /// Returns a ViewBuilder to build a view.
    pub fn builder() -> ViewBuilder {
        ViewBuilder::new()
    }

    /// Returns the view identifier.
    pub fn identifier(&self) -> &TableIdent {
        &self.identifier
    }

    /// Returns current metadata.
    pub fn metadata(&self) -> &ViewMetadata {
        &self.metadata
    }

    /// Returns current metadata ref.
    pub fn metadata_ref(&self) -> ViewMetadataRef {
        self.metadata.clone()
    }

    /// Returns current metadata location.
    pub fn metadata_location(&self) -> Option<&str> {
        self.metadata_location.as_deref()
    }

    /// Returns current metadata location in a result.
    pub fn metadata_location_result(&self) -> Result<&str> {
        self.metadata_location.as_deref().ok_or(Error::new(
            ErrorKind::DataInvalid,
            format!(
                "Metadata location does not exist for view: {}",
                self.identifier
            ),
        ))
    }

    /// Returns the current schema of the view.
    pub fn current_schema(&self) -> SchemaRef {
        self.metadata.current_schema().clone()
    }

    /// Returns the current version of the view.
    pub fn current_version(&self) -> &ViewVersionRef {
        self.metadata.current_version()
    }

    /// Returns view properties.
    pub fn properties(&self) -> &HashMap<String, String> {
        self.metadata.properties()
    }

    /// Returns the view's location.
    pub fn location(&self) -> &str {
        self.metadata.location()
    }

    /// Resolves the SQL representation for the given dialect.
    /// Returns the first SQL representation matching the dialect (case-insensitive),
    /// or `None` if no matching representation exists.
    pub fn sql_for(&self, dialect: &str) -> Option<&SqlViewRepresentation> {
        let dialect_lower = dialect.to_lowercase();
        self.metadata
            .current_version()
            .representations()
            .iter()
            .find_map(|repr| match repr {
                ViewRepresentation::Sql(sql) if sql.dialect.to_lowercase() == dialect_lower => {
                    Some(sql)
                }
                _ => None,
            })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use uuid::Uuid;

    use super::*;
    use crate::spec::{
        NestedField, PrimitiveType, Schema, SqlViewRepresentation, Type, ViewFormatVersion,
        ViewMetadata, ViewRepresentation, ViewRepresentations, ViewVersion, ViewVersionLog,
    };
    use crate::{NamespaceIdent, TableIdent};

    fn test_view_metadata() -> ViewMetadata {
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
                ("engine-name".to_string(), "Spark".to_string()),
                ("engineVersion".to_string(), "3.3.2".to_string()),
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

        ViewMetadata {
            format_version: ViewFormatVersion::V1,
            view_uuid: Uuid::parse_str("fa6506c3-7681-40c8-86dc-e36561f83385").unwrap(),
            location: "s3://bucket/warehouse/default.db/event_agg".to_string(),
            current_version_id: 1,
            versions: HashMap::from_iter(vec![(1, Arc::new(version))]),
            version_log: vec![ViewVersionLog::new(1, 1573518431292)],
            schemas: HashMap::from_iter(vec![(1, Arc::new(schema))]),
            properties: HashMap::from_iter(vec![(
                "comment".to_string(),
                "Daily event counts".to_string(),
            )]),
        }
    }

    #[test]
    fn test_view_builder() {
        let metadata = test_view_metadata();
        let identifier = TableIdent::from_strs(["ns", "my_view"]).unwrap();
        let view = View::builder()
            .metadata(metadata.clone())
            .identifier(identifier.clone())
            .metadata_location("s3://bucket/metadata/v1.metadata.json")
            .build()
            .unwrap();

        assert_eq!(view.identifier(), &identifier);
        assert_eq!(
            view.metadata_location(),
            Some("s3://bucket/metadata/v1.metadata.json")
        );
        assert_eq!(
            view.location(),
            "s3://bucket/warehouse/default.db/event_agg"
        );
        assert_eq!(view.current_version().version_id(), 1);
        assert_eq!(view.current_schema().schema_id(), 1);
        assert_eq!(
            view.properties().get("comment"),
            Some(&"Daily event counts".to_string())
        );
    }

    #[test]
    fn test_view_builder_missing_metadata() {
        let identifier = TableIdent::from_strs(["ns", "my_view"]).unwrap();
        let result = View::builder().identifier(identifier).build();

        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("ViewMetadata must be provided")
        );
    }

    #[test]
    fn test_view_builder_missing_identifier() {
        let metadata = test_view_metadata();
        let result = View::builder().metadata(metadata).build();

        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Identifier must be provided")
        );
    }

    #[test]
    fn test_view_builder_without_metadata_location() {
        let metadata = test_view_metadata();
        let identifier = TableIdent::from_strs(["ns", "my_view"]).unwrap();
        let view = View::builder()
            .metadata(metadata)
            .identifier(identifier)
            .build()
            .unwrap();

        assert!(view.metadata_location().is_none());
        assert!(view.metadata_location_result().is_err());
    }

    #[test]
    fn test_view_sql_for() {
        let metadata = test_view_metadata();
        let identifier = TableIdent::from_strs(["ns", "my_view"]).unwrap();
        let view = View::builder()
            .metadata(metadata)
            .identifier(identifier)
            .build()
            .unwrap();

        let spark_sql = view.sql_for("spark");
        assert!(spark_sql.is_some());
        assert!(spark_sql.unwrap().sql.contains("COUNT(1)"));

        // Case-insensitive match
        let spark_sql_upper = view.sql_for("SPARK");
        assert!(spark_sql_upper.is_some());
        assert_eq!(spark_sql_upper.unwrap().sql, spark_sql.unwrap().sql);

        // Non-existent dialect
        assert!(view.sql_for("trino").is_none());
    }

    #[test]
    fn test_view_sql_for_multiple_dialects() {
        let schema = Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![Arc::new(NestedField::optional(
                1,
                "count",
                Type::Primitive(PrimitiveType::Long),
            ))])
            .build()
            .unwrap();

        let version = ViewVersion::builder()
            .with_version_id(1)
            .with_timestamp_ms(1573518431292)
            .with_schema_id(1)
            .with_default_namespace(NamespaceIdent::from_vec(vec!["default".to_string()]).unwrap())
            .with_representations(ViewRepresentations(vec![
                ViewRepresentation::Sql(SqlViewRepresentation {
                    sql: "SELECT COUNT(*) FROM events".to_string(),
                    dialect: "spark".to_string(),
                }),
                ViewRepresentation::Sql(SqlViewRepresentation {
                    sql: "SELECT count(*) FROM events".to_string(),
                    dialect: "trino".to_string(),
                }),
            ]))
            .build();

        let metadata = ViewMetadata {
            format_version: ViewFormatVersion::V1,
            view_uuid: Uuid::now_v7(),
            location: "s3://bucket/warehouse/default.db/event_agg".to_string(),
            current_version_id: 1,
            versions: HashMap::from_iter(vec![(1, Arc::new(version))]),
            version_log: vec![ViewVersionLog::new(1, 1573518431292)],
            schemas: HashMap::from_iter(vec![(1, Arc::new(schema))]),
            properties: HashMap::new(),
        };

        let identifier = TableIdent::from_strs(["ns", "my_view"]).unwrap();
        let view = View::builder()
            .metadata(metadata)
            .identifier(identifier)
            .build()
            .unwrap();

        let spark = view.sql_for("spark").unwrap();
        assert_eq!(spark.sql, "SELECT COUNT(*) FROM events");

        let trino = view.sql_for("trino").unwrap();
        assert_eq!(trino.sql, "SELECT count(*) FROM events");
    }

    #[test]
    fn test_view_with_metadata() {
        let metadata = test_view_metadata();
        let identifier = TableIdent::from_strs(["ns", "my_view"]).unwrap();
        let view = View::builder()
            .metadata(metadata.clone())
            .identifier(identifier)
            .build()
            .unwrap();

        // Build a new metadata with different location
        let new_metadata = ViewMetadata {
            location: "s3://bucket/new_location".to_string(),
            ..metadata
        };

        let updated_view = view.with_metadata(Arc::new(new_metadata));
        assert_eq!(updated_view.location(), "s3://bucket/new_location");
    }

    #[test]
    fn test_view_with_metadata_location() {
        let metadata = test_view_metadata();
        let identifier = TableIdent::from_strs(["ns", "my_view"]).unwrap();
        let view = View::builder()
            .metadata(metadata)
            .identifier(identifier)
            .build()
            .unwrap();

        let updated_view = view.with_metadata_location("s3://new/location.json".to_string());
        assert_eq!(
            updated_view.metadata_location(),
            Some("s3://new/location.json")
        );
    }
}
