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

//! Request and response types for the Iceberg REST API.

use std::collections::HashMap;

use iceberg::spec::{
    Schema, SortOrder, TableMetadata, UnboundPartitionSpec, ViewMetadata, ViewVersion,
};
use iceberg::{
    Error, ErrorKind, Namespace, NamespaceIdent, TableIdent, TableRequirement, TableUpdate,
    ViewRequirement, ViewUpdate,
};
use serde_derive::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct CatalogConfig {
    pub(super) overrides: HashMap<String, String>,
    pub(super) defaults: HashMap<String, String>,
}

#[derive(Debug, Serialize, Deserialize)]
/// Wrapper for all non-2xx error responses from the REST API
pub struct ErrorResponse {
    error: ErrorModel,
}

impl From<ErrorResponse> for Error {
    fn from(resp: ErrorResponse) -> Error {
        resp.error.into()
    }
}

#[derive(Debug, Serialize, Deserialize)]
/// Error payload returned in a response with further details on the error
pub struct ErrorModel {
    /// Human-readable error message
    pub message: String,
    /// Internal type definition of the error
    pub r#type: String,
    /// HTTP response code
    pub code: u16,
    /// Optional error stack / context
    pub stack: Option<Vec<String>>,
}

impl From<ErrorModel> for Error {
    fn from(value: ErrorModel) -> Self {
        let mut error = Error::new(ErrorKind::DataInvalid, value.message)
            .with_context("type", value.r#type)
            .with_context("code", format!("{}", value.code));

        if let Some(stack) = value.stack {
            error = error.with_context("stack", stack.join("\n"));
        }

        error
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub(super) struct OAuthError {
    pub(super) error: String,
    pub(super) error_description: Option<String>,
    pub(super) error_uri: Option<String>,
}

impl From<OAuthError> for Error {
    fn from(value: OAuthError) -> Self {
        let mut error = Error::new(
            ErrorKind::DataInvalid,
            format!("OAuthError: {}", value.error),
        );

        if let Some(desc) = value.error_description {
            error = error.with_context("description", desc);
        }

        if let Some(uri) = value.error_uri {
            error = error.with_context("uri", uri);
        }

        error
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub(super) struct TokenResponse {
    pub(super) access_token: String,
    pub(super) token_type: String,
    pub(super) expires_in: Option<u64>,
    pub(super) issued_token_type: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
/// Namespace response
pub struct NamespaceResponse {
    /// Namespace identifier
    pub namespace: NamespaceIdent,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    /// Properties stored on the namespace, if supported by the server.
    pub properties: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
/// Create namespace request
pub struct CreateNamespaceRequest {
    /// Name of the namespace to create
    pub namespace: NamespaceIdent,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    /// Properties to set on the namespace
    pub properties: HashMap<String, String>,
}

impl From<&Namespace> for NamespaceResponse {
    fn from(value: &Namespace) -> Self {
        Self {
            namespace: value.name().clone(),
            properties: value.properties().clone(),
        }
    }
}

impl From<NamespaceResponse> for Namespace {
    fn from(value: NamespaceResponse) -> Self {
        Namespace::with_properties(value.namespace, value.properties)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
/// Response containing a list of namespace identifiers, with optional pagination support.
pub struct ListNamespaceResponse {
    /// List of namespace identifiers returned by the server
    pub namespaces: Vec<NamespaceIdent>,
    /// Opaque token for pagination. If present, indicates there are more results available.
    /// Use this value in subsequent requests to retrieve the next page.
    pub next_page_token: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
/// Request to update properties on a namespace.
///
/// Properties that are not in the request are not modified or removed by this call.
/// Server implementations are not required to support namespace properties.
pub struct UpdateNamespacePropertiesRequest {
    /// List of property keys to remove from the namespace
    pub removals: Option<Vec<String>>,
    /// Map of property keys to values to set or update on the namespace
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub updates: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
/// Response from updating namespace properties, indicating which properties were changed.
pub struct UpdateNamespacePropertiesResponse {
    /// List of property keys that were added or updated
    pub updated: Vec<String>,
    /// List of properties that were removed
    pub removed: Vec<String>,
    /// List of properties requested for removal that were not found in the namespace's properties.
    /// Represents a partial success response. Servers do not need to implement this.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub missing: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
/// Response containing a list of table identifiers, with optional pagination support.
pub struct ListTablesResponse {
    /// List of table identifiers under the requested namespace
    pub identifiers: Vec<TableIdent>,
    /// Opaque token for pagination. If present, indicates there are more results available.
    /// Use this value in subsequent requests to retrieve the next page.
    #[serde(default)]
    pub next_page_token: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
/// Request to rename a table from one identifier to another.
///
/// It's valid to move a table across namespaces, but the server implementation
/// is not required to support it.
pub struct RenameTableRequest {
    /// Current table identifier to rename
    pub source: TableIdent,
    /// New table identifier to rename to
    pub destination: TableIdent,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
/// Result returned when a table is successfully loaded or created.
///
/// The table metadata JSON is returned in the `metadata` field. The corresponding file location
/// of table metadata should be returned in the `metadata_location` field, unless the metadata
/// is not yet committed. For example, a create transaction may return metadata that is staged
/// but not committed.
///
/// The `config` map returns table-specific configuration for the table's resources, including
/// its HTTP client and FileIO. For example, config may contain a specific FileIO implementation
/// class for the table depending on its underlying storage.
pub struct LoadTableResult {
    /// May be null if the table is staged as part of a transaction
    pub metadata_location: Option<String>,
    /// The table's full metadata
    pub metadata: TableMetadata,
    /// Table-specific configuration overriding catalog configuration
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub config: HashMap<String, String>,
    /// Storage credentials for accessing table data. Clients should check this field
    /// before falling back to credentials in the `config` field.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub storage_credentials: Option<Vec<StorageCredential>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
/// Storage credential for a specific location prefix.
///
/// Indicates a storage location prefix where the credential is relevant. Clients should
/// choose the most specific prefix (by selecting the longest prefix) if several credentials
/// of the same type are available.
pub struct StorageCredential {
    /// Storage location prefix where this credential is relevant
    pub prefix: String,
    /// Configuration map containing credential information
    pub config: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
/// Request to create a new table in a namespace.
///
/// If `stage_create` is false, the table is created immediately.
/// If `stage_create` is true, the table is not created, but table metadata is initialized
/// and returned. The service should prepare as needed for a commit to the table commit
/// endpoint to complete the create transaction.
pub struct CreateTableRequest {
    /// Name of the table to create
    pub name: String,
    /// Optional table location. If not provided, the server will choose a location.
    pub location: Option<String>,
    /// Table schema
    pub schema: Schema,
    /// Optional partition specification. If not provided, the table will be unpartitioned.
    pub partition_spec: Option<UnboundPartitionSpec>,
    /// Optional sort order for the table
    pub write_order: Option<SortOrder>,
    /// Whether to stage the create for a transaction (true) or create immediately (false)
    pub stage_create: Option<bool>,
    /// Optional properties to set on the table
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub properties: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
/// Request to commit updates to a table.
///
/// Commits have two parts: requirements and updates. Requirements are assertions that will
/// be validated before attempting to make and commit changes. Updates are changes to make
/// to table metadata.
///
/// Create table transactions that are started by createTable with `stage-create` set to true
/// are committed using this request. Transactions should include all changes to the table,
/// including table initialization, like AddSchemaUpdate and SetCurrentSchemaUpdate.
pub struct CommitTableRequest {
    /// Table identifier to update; must be present for CommitTransactionRequest
    #[serde(skip_serializing_if = "Option::is_none")]
    pub identifier: Option<TableIdent>,
    /// List of requirements that must be satisfied before committing changes
    pub requirements: Vec<TableRequirement>,
    /// List of updates to apply to the table metadata
    pub updates: Vec<TableUpdate>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
/// Response returned when a table is successfully updated.
///
/// The table metadata JSON is returned in the metadata field. The corresponding file location
/// of table metadata must be returned in the metadata-location field. Clients can check whether
/// metadata has changed by comparing metadata locations.
pub struct CommitTableResponse {
    /// Location of the updated table metadata file
    pub metadata_location: String,
    /// The table's updated metadata
    pub metadata: TableMetadata,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
/// Request to register a table using an existing metadata file location.
pub struct RegisterTableRequest {
    /// Name of the table to register
    pub name: String,
    /// Location of the metadata file for the table
    pub metadata_location: String,
    /// Whether to overwrite table metadata if the table already exists
    pub overwrite: Option<bool>,
}

// ============================================================================
// View request/response shapes — mirror the Iceberg REST OpenAPI view routes
// (`POST/GET /namespaces/{ns}/views`, `GET/POST/DELETE/HEAD .../views/{view}`,
// `POST /views/rename`) and Java's `CreateViewRequest` / `LoadViewResponse` /
// `UpdateTableRequest` (reused for the view replace/commit) wire formats.
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
/// Request to create a new view in a namespace.
///
/// Mirrors Java `org.apache.iceberg.rest.requests.CreateViewRequest` — the wire fields are
/// `name`, `location`, `view-version` (the initial [`ViewVersion`]), `schema`, and `properties`.
pub struct CreateViewRequest {
    /// Name of the view to create
    pub name: String,
    /// Optional view location. If not provided, the server will choose a location.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub location: Option<String>,
    /// The initial view version (one representation per SQL dialect, schema id, default namespace).
    pub view_version: ViewVersion,
    /// View schema
    pub schema: Schema,
    /// Optional properties to set on the view
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub properties: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
/// Result returned when a view is successfully loaded or created.
///
/// Mirrors Java `org.apache.iceberg.rest.responses.LoadViewResponse` — `metadata-location`,
/// `metadata` (the [`ViewMetadata`] JSON), and a view-specific `config` map.
pub struct LoadViewResult {
    /// Location of the view metadata file
    pub metadata_location: String,
    /// The view's full metadata
    pub metadata: ViewMetadata,
    /// View-specific configuration overriding catalog configuration
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub config: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "kebab-case")]
/// Request to commit updates to a view (the replace/update-properties path).
///
/// Java reuses `UpdateTableRequest.create(identifier, requirements, updates)` for the view commit,
/// so the wire shape is `identifier`, `requirements`, `updates` — but carrying VIEW requirements
/// and VIEW updates (from `UpdateRequirements.forReplaceView`), not table ones.
pub struct CommitViewRequest {
    /// View identifier to update
    #[serde(skip_serializing_if = "Option::is_none")]
    pub identifier: Option<TableIdent>,
    /// List of requirements that must be satisfied before committing changes
    pub requirements: Vec<ViewRequirement>,
    /// List of updates to apply to the view metadata
    pub updates: Vec<ViewUpdate>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_namespace_response_serde() {
        let json = serde_json::json!({
            "namespace": ["nested", "ns"],
            "properties": {
                "key1": "value1",
                "key2": "value2"
            }
        });
        let ns_response: NamespaceResponse =
            serde_json::from_value(json.clone()).expect("Deserialization failed");
        assert_eq!(ns_response, NamespaceResponse {
            namespace: NamespaceIdent::from_vec(vec!["nested".to_string(), "ns".to_string()])
                .unwrap(),
            properties: HashMap::from([
                ("key1".to_string(), "value1".to_string()),
                ("key2".to_string(), "value2".to_string()),
            ]),
        });
        assert_eq!(
            serde_json::to_value(&ns_response).expect("Serialization failed"),
            json
        );

        // Without properties
        let json_no_props = serde_json::json!({
            "namespace": ["db", "schema"]
        });
        let ns_response_no_props: NamespaceResponse =
            serde_json::from_value(json_no_props.clone()).expect("Deserialization failed");
        assert_eq!(ns_response_no_props, NamespaceResponse {
            namespace: NamespaceIdent::from_vec(vec!["db".to_string(), "schema".to_string()])
                .unwrap(),
            properties: HashMap::new(),
        });
        assert_eq!(
            serde_json::to_value(&ns_response_no_props).expect("Serialization failed"),
            json_no_props
        );
    }

    // RISK: the LoadViewResult wire shape must match Java's `LoadViewResponse`
    // (`metadata-location` / `metadata` / `config`). A wrong key (e.g. snake_case `metadata_location`)
    // would silently fail to deserialize a real server's load-view / create-view response, and the
    // embedded `metadata` must parse as a full `ViewMetadata` (versions, schemas, version-log).
    #[test]
    fn test_load_view_result_serde() {
        let json = serde_json::json!({
            "metadata-location": "s3://bucket/warehouse/default.db/event_agg/metadata/00001-abc.metadata.json",
            "metadata": {
                "view-uuid": "fa6506c3-7681-40c8-86dc-e36561f83385",
                "format-version": 1,
                "location": "s3://bucket/warehouse/default.db/event_agg",
                "current-version-id": 1,
                "properties": { "comment": "Daily event counts" },
                "versions": [ {
                    "version-id": 1,
                    "timestamp-ms": 1573518431292i64,
                    "schema-id": 1,
                    "default-namespace": [ "default" ],
                    "summary": { "engine-name": "Spark" },
                    "representations": [ {
                        "type": "sql",
                        "sql": "SELECT 1 AS event_count",
                        "dialect": "spark"
                    } ]
                } ],
                "schemas": [ {
                    "schema-id": 1,
                    "type": "struct",
                    "fields": [ {
                        "id": 1,
                        "name": "event_count",
                        "required": false,
                        "type": "int"
                    } ]
                } ],
                "version-log": [ {
                    "timestamp-ms": 1573518431292i64,
                    "version-id": 1
                } ]
            },
            "config": { "key": "value" }
        });

        let result: LoadViewResult =
            serde_json::from_value(json).expect("LoadViewResult deserialization failed");
        assert_eq!(
            result.metadata_location,
            "s3://bucket/warehouse/default.db/event_agg/metadata/00001-abc.metadata.json"
        );
        assert_eq!(
            result.metadata.uuid().to_string(),
            "fa6506c3-7681-40c8-86dc-e36561f83385"
        );
        assert_eq!(result.metadata.current_version_id(), 1);
        assert_eq!(result.metadata.versions().count(), 1);
        assert_eq!(result.config.get("key"), Some(&"value".to_string()));

        // Round-trips back to a value with the same kebab-case keys.
        let reserialized =
            serde_json::to_value(&result).expect("LoadViewResult serialization failed");
        assert!(reserialized.get("metadata-location").is_some());
        assert!(reserialized.get("metadata").is_some());
        assert!(reserialized.get("config").is_some());
    }

    // RISK: the CreateViewRequest wire shape must match Java's `CreateViewRequest`
    // (`name` / `location` / `view-version` / `schema` / `properties`). The `view-version` field
    // (kebab-case) carries a full `ViewVersion`; a snake_case `view_version` or a missing field
    // would be rejected by a real server.
    #[test]
    fn test_create_view_request_serde() {
        let json = serde_json::json!({
            "name": "event_agg",
            "location": "s3://bucket/warehouse/default.db/event_agg",
            "view-version": {
                "version-id": 1,
                "timestamp-ms": 1573518431292i64,
                "schema-id": 1,
                "default-namespace": [ "default" ],
                "summary": {},
                "representations": [ {
                    "type": "sql",
                    "sql": "SELECT 1 AS event_count",
                    "dialect": "spark"
                } ]
            },
            "schema": {
                "schema-id": 1,
                "type": "struct",
                "fields": [ {
                    "id": 1,
                    "name": "event_count",
                    "required": false,
                    "type": "int"
                } ]
            },
            "properties": { "comment": "daily" }
        });

        let request: CreateViewRequest =
            serde_json::from_value(json.clone()).expect("CreateViewRequest deserialization failed");
        assert_eq!(request.name, "event_agg");
        assert_eq!(
            request.location.as_deref(),
            Some("s3://bucket/warehouse/default.db/event_agg")
        );
        assert_eq!(request.view_version.version_id(), 1);
        assert_eq!(request.schema.schema_id(), 1);
        assert_eq!(
            request.properties.get("comment"),
            Some(&"daily".to_string())
        );

        // Round-trip is value-stable (kebab-case `view-version` survives).
        let reserialized =
            serde_json::to_value(&request).expect("CreateViewRequest serialization failed");
        assert_eq!(reserialized, json);
    }

    // RISK: the CommitViewRequest reuses Java's `UpdateTableRequest` shape
    // (`identifier` / `requirements` / `updates`) but carries VIEW requirements/updates. The view
    // requirement tag (`assert-view-uuid`) and the view update action tags must serialize per the
    // REST spec; a table-requirement leak would make a real server reject the commit.
    #[test]
    fn test_commit_view_request_serde() {
        let json = serde_json::json!({
            "identifier": { "namespace": ["default"], "name": "event_agg" },
            "requirements": [ {
                "type": "assert-view-uuid",
                "uuid": "fa6506c3-7681-40c8-86dc-e36561f83385"
            } ],
            "updates": [ {
                "action": "set-properties",
                "updates": { "comment": "daily counts" }
            } ]
        });

        let request: CommitViewRequest =
            serde_json::from_value(json.clone()).expect("CommitViewRequest deserialization failed");
        assert_eq!(request.requirements.len(), 1);
        assert!(matches!(
            request.requirements[0],
            ViewRequirement::UuidMatch { .. }
        ));
        assert_eq!(request.updates.len(), 1);
        assert!(matches!(
            request.updates[0],
            ViewUpdate::SetProperties { .. }
        ));

        // Round-trip is value-stable (view tags `assert-view-uuid` / `set-properties` survive).
        let reserialized =
            serde_json::to_value(&request).expect("CommitViewRequest serialization failed");
        assert_eq!(reserialized, json);
    }
}
