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

//! This module contains the iceberg REST catalog types related to the Events API.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use iceberg::{Error, NamespaceIdent, TableIdent, TableRequirement, TableUpdate, ViewUpdate};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::types::{NamespaceResponse, UpdateNamespacePropertiesResponse};

/// Reference to a named object in the catalog (namespace, table, or view).
#[derive(Debug, Hash, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd)]
#[serde(transparent)]
pub struct CatalogObjectIdentifier(Vec<String>);

impl CatalogObjectIdentifier {
    /// Try to convert into a NamespaceIdent.
    /// Errors if the identifier has zero parts.
    pub fn into_namespace_ident(self) -> Result<NamespaceIdent, Error> {
        NamespaceIdent::from_vec(self.0)
    }

    /// Try to convert into a TableIdent.
    /// Errors if the identifier has fewer than two parts.
    pub fn into_table_ident(self) -> Result<TableIdent, Error> {
        TableIdent::from_strs(self.0)
    }

    /// Returns inner strings.
    pub fn inner(self) -> Vec<String> {
        self.0
    }

    /// Get the parent of this object.
    /// Returns None if this object only has a single element and thus has no parent.
    pub fn parent(&self) -> Option<Self> {
        self.0.split_last().and_then(|(_, parent)| {
            if parent.is_empty() {
                None
            } else {
                Some(Self(parent.to_vec()))
            }
        })
    }

    /// Number of parts in the identifier.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Check if the identifier is empty.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// Identify a table or view by its uuid.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CatalogObjectUuid {
    /// The UUID of the catalog object
    pub uuid: Uuid,
    /// The type of the catalog object
    #[serde(rename = "type")]
    pub object_type: CatalogObjectType,
}

/// The type of a catalog object identified by UUID.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum CatalogObjectType {
    /// Table object
    Table,
    /// View object
    View,
}

/// Type of operation in the catalog.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(untagged)]
#[serde(bound(deserialize = "T: for<'a> Deserialize<'a>"))]
pub enum OperationType<T = String>
where T: CustomOperationType
{
    /// Standard operation types defined by the Iceberg REST API specification
    Standard(StandardOperationType),
    /// Custom operation type for catalog-specific extensions.
    Custom(T),
}

/// Trait for types that can be used as custom operation types.
pub trait CustomOperationType:
    Clone + Serialize + for<'de> Deserialize<'de> + PartialEq + Eq
{
    /// Create a custom operation type, ensuring proper formatting (e.g., "x-" prefix for strings).
    fn normalize(self) -> Self {
        self
    }
}

impl CustomOperationType for String {
    fn normalize(self) -> Self {
        if self.starts_with("x-") {
            self
        } else {
            format!("x-{}", self)
        }
    }
}

impl<T: CustomOperationType> OperationType<T> {
    /// Create a standard operation type.
    pub fn new_standard(op: StandardOperationType) -> Self {
        Self::Standard(op)
    }

    /// Create a custom operation type.
    /// For string-based types, the value will be automatically prefixed with "x-" if not already present.
    pub fn new_custom(value: T) -> Self {
        Self::Custom(value.normalize())
    }

    /// Check if the operation type is a standard well-known type.
    pub fn is_standard(&self) -> bool {
        matches!(self, OperationType::Standard(_))
    }

    /// Check if the operation type is custom.
    pub fn is_custom(&self) -> bool {
        matches!(self, OperationType::Custom(_))
    }
}

impl From<StandardOperationType> for OperationType {
    fn from(op: StandardOperationType) -> Self {
        OperationType::Standard(op)
    }
}

/// Standard operation types defined by the Iceberg REST API specification.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum StandardOperationType {
    /// Create table operation
    CreateTable,
    /// Register table operation
    RegisterTable,
    /// Drop table operation
    DropTable,
    /// Update table operation
    UpdateTable,
    /// Rename table operation
    RenameTable,
    /// Create view operation
    CreateView,
    /// Drop view operation
    DropView,
    /// Update view operation
    UpdateView,
    /// Rename view operation
    RenameView,
    /// Create namespace operation
    CreateNamespace,
    /// Update namespace properties operation
    UpdateNamespaceProperties,
    /// Drop namespace operation
    DropNamespace,
}

/// Type of catalog object for filtering events.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum QueryEventsRequestObjectType {
    /// Namespace object
    Namespace,
    /// Table object
    Table,
    /// View object
    View,
}

/// Trait for custom filter collections in event queries.
///
/// Enables catalog implementations to extend filtering beyond standard Iceberg REST API filters.
/// Types must convert to/from a flat map for REST API serialization.
pub trait CustomFilterCollection: Default + Clone + PartialEq + Eq {
    /// Convert into a filter map, which is used for serialization in
    /// the REST API.
    fn try_as_filter_map(
        &self,
    ) -> Result<HashMap<impl AsRef<str> + Serialize, impl Serialize>, Error>;

    /// Create from a filter map, which is used for deserialization in
    /// the REST API.
    fn try_from_filter_map(map: HashMap<String, serde_json::Value>) -> Result<Self, Error>;

    /// Returns the number of custom filters.
    fn len(&self) -> usize;

    /// Check if there are no custom filters.
    fn is_empty(&self) -> bool;
}

impl CustomFilterCollection for HashMap<String, serde_json::Value> {
    fn try_as_filter_map(
        &self,
    ) -> Result<HashMap<impl AsRef<str> + Serialize, impl Serialize>, Error> {
        Ok(self.clone())
    }

    fn try_from_filter_map(map: HashMap<String, serde_json::Value>) -> Result<Self, Error> {
        Ok(map)
    }

    fn len(&self) -> usize {
        HashMap::len(self)
    }

    fn is_empty(&self) -> bool {
        HashMap::is_empty(self)
    }
}

/// Request to query catalog events.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, typed_builder::TypedBuilder)]
#[serde(rename_all = "kebab-case")]
pub struct QueryEventsRequest<F = HashMap<String, serde_json::Value>>
where F: CustomFilterCollection
{
    /// Opaque pagination token
    #[builder(default)]
    pub continuation_token: Option<String>,

    /// The maximum number of events to return in a single response
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    pub page_size: Option<i32>,

    /// The (server) timestamp to start consuming events from (inclusive).
    /// During serialization/deserialization, this is represented as milliseconds since epoch.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        serialize_with = "serialize_optional_datetime_ms",
        deserialize_with = "deserialize_optional_datetime_ms"
    )]
    #[serde(rename = "after-timestamp-ms")]
    #[builder(default)]
    pub after_timestamp: Option<DateTime<Utc>>,

    /// Filter events by the type of operation
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    pub operation_types: Option<Vec<OperationType>>,

    /// Filter events by catalog objects referenced by name
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    pub catalog_objects_by_name: Option<Vec<CatalogObjectIdentifier>>,

    /// Filter events by catalog objects referenced by UUID
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    pub catalog_objects_by_id: Option<Vec<CatalogObjectUuid>>,

    /// Filter events by the type of catalog object
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    pub object_types: Option<Vec<QueryEventsRequestObjectType>>,

    /// Implementation-specific filter extensions
    #[serde(
        default,
        skip_serializing_if = "CustomFilterCollection::is_empty",
        serialize_with = "serialize_custom_filters::<F, _>",
        deserialize_with = "deserialize_custom_filters::<F, _>"
    )]
    #[builder(default)]
    pub custom_filters: F,
}

/// Response to a query for catalog events.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub struct QueryEventsResponse<C = RawCustomOperation, A = HashMap<String, serde_json::Value>>
where
    C: CustomOperation,
    A: Actor,
{
    /// Opaque pagination token for retrieving the next page of results
    pub continuation_token: String,

    /// The highest event timestamp processed when generating this response.
    /// This may not necessarily appear in the returned events if it was filtered out.
    #[serde(
        serialize_with = "serialize_datetime_ms",
        deserialize_with = "deserialize_datetime_ms"
    )]
    #[serde(rename = "highest-processed-timestamp-ms")]
    pub highest_processed_timestamp: DateTime<Utc>,

    /// List of events matching the query criteria
    pub events: Vec<Event<C, A>>,
}

/// Trait for actor representation in events.
/// Enables catalog implementations to define custom actor types.
pub trait Actor: Default + Clone + PartialEq + Eq {
    /// Convert into a property map, which is used for serialization in
    /// the REST API.
    fn try_as_property_map(
        &self,
    ) -> Result<HashMap<impl AsRef<str> + Serialize, impl Serialize>, Error>;

    /// Create from a property map, which is used for deserialization in
    /// the REST API.
    fn try_from_property_map(map: HashMap<String, serde_json::Value>) -> Result<Self, Error>;
}

impl Actor for HashMap<String, serde_json::Value> {
    fn try_as_property_map(
        &self,
    ) -> Result<HashMap<impl AsRef<str> + Serialize, impl Serialize>, Error> {
        Ok(self.clone())
    }

    fn try_from_property_map(map: HashMap<String, serde_json::Value>) -> Result<Self, Error> {
        Ok(map)
    }
}

/// An event representing a change to a catalog object.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, typed_builder::TypedBuilder)]
#[serde(rename_all = "kebab-case")]
pub struct Event<C = RawCustomOperation, A = HashMap<String, serde_json::Value>>
where
    C: CustomOperation,
    A: Actor,
{
    /// Unique ID of this event. Clients should perform deduplication based on this ID.
    pub event_id: String,

    /// Opaque ID of the request this event belongs to.
    /// This ID can be used to identify events that were part of the same request.
    /// Servers generate this ID randomly.
    pub request_id: String,

    /// Total number of events in this batch or request.
    /// Some endpoints, such as "updateTable" and "commitTransaction", can perform multiple updates in a single atomic request.
    /// Each update is modeled as a separate event. All events generated by the same request share the same `request-id`.
    /// The `request-event-count` field indicates the total number of events generated by that request.
    pub request_event_count: i32,

    /// Timestamp when this event occurred (epoch milliseconds).
    /// Timestamps are not guaranteed to be unique. Typically all events in
    /// a transaction will have the same timestamp.
    #[serde(
        serialize_with = "serialize_datetime_ms",
        deserialize_with = "deserialize_datetime_ms"
    )]
    #[serde(rename = "timestamp-ms")]
    pub timestamp: DateTime<Utc>,

    /// The actor who performed the operation, such as a user or service account.
    /// The content of this field is implementation specific.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        serialize_with = "serialize_custom_actor::<A, _>",
        deserialize_with = "deserialize_custom_actor::<A, _>"
    )]
    #[builder(default)]
    pub actor: Option<A>,

    /// The operation that was performed.
    /// Clients should discard events with unknown operation types.
    #[serde(flatten)]
    pub operation: Operation<C>,
}

/// Operation that was performed on a catalog object.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "operation-type", rename_all = "kebab-case")]
pub enum Operation<C = RawCustomOperation>
where C: CustomOperation
{
    /// Create table operation
    CreateTable(CreateTableOperation),
    /// Register table operation
    RegisterTable(RegisterTableOperation),
    /// Drop table operation
    DropTable(DropTableOperation),
    /// Update table operation
    UpdateTable(UpdateTableOperation),
    /// Rename table operation
    RenameTable(RenameTableOperation),
    /// Create view operation
    CreateView(CreateViewOperation),
    /// Drop view operation
    DropView(DropViewOperation),
    /// Update view operation
    UpdateView(UpdateViewOperation),
    /// Rename view operation
    RenameView(RenameViewOperation),
    /// Create namespace operation
    CreateNamespace(CreateNamespaceOperation),
    /// Update namespace properties operation
    UpdateNamespaceProperties(UpdateNamespacePropertiesOperation),
    /// Drop namespace operation
    DropNamespace(DropNamespaceOperation),
    /// Custom operation for catalog-specific extensions
    Custom(C),
}

/// Operation to create a new table in the catalog.
/// Events for this operation must be issued when the create is finalized and committed,
/// not when the create is staged.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, typed_builder::TypedBuilder)]
#[serde(rename_all = "kebab-case")]
pub struct CreateTableOperation {
    /// Table identifier
    pub identifier: TableIdent,
    /// UUID of the table
    pub table_uuid: Uuid,
    /// Table updates applied during creation
    #[builder(default)]
    pub updates: Vec<TableUpdate>,
}

/// Operation to register an existing table in the catalog.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, typed_builder::TypedBuilder)]
#[serde(rename_all = "kebab-case")]
pub struct RegisterTableOperation {
    /// Table identifier
    pub identifier: TableIdent,
    /// UUID of the table
    pub table_uuid: Uuid,
    /// Optional table updates applied during registration
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[builder(default)]
    pub updates: Vec<TableUpdate>,
}

/// Operation to drop a table from the catalog.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, typed_builder::TypedBuilder)]
#[serde(rename_all = "kebab-case")]
pub struct DropTableOperation {
    /// Table identifier
    pub identifier: TableIdent,
    /// UUID of the table
    pub table_uuid: Uuid,
    /// Whether the purge flag was set
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    pub purge: Option<bool>,
}

/// Operation to update a table in the catalog.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, typed_builder::TypedBuilder)]
#[serde(rename_all = "kebab-case")]
pub struct UpdateTableOperation {
    /// Table identifier
    pub identifier: TableIdent,
    /// UUID of the table
    pub table_uuid: Uuid,
    /// Table updates to apply
    pub updates: Vec<TableUpdate>,
    /// Requirements that must be met for the update to succeed
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[builder(default)]
    pub requirements: Vec<TableRequirement>,
}

/// Operation to rename a table.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, typed_builder::TypedBuilder)]
#[serde(rename_all = "kebab-case")]
pub struct RenameTableOperation {
    /// Current table identifier to rename
    pub source: TableIdent,
    /// New table identifier to rename to
    pub destination: TableIdent,
    /// UUID of the table
    pub table_uuid: Uuid,
}

/// Operation to rename a view.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, typed_builder::TypedBuilder)]
#[serde(rename_all = "kebab-case")]
pub struct RenameViewOperation {
    /// UUID of the view
    pub view_uuid: Uuid,
    /// Current view identifier to rename
    pub source: TableIdent,
    /// New view identifier to rename to
    pub destination: TableIdent,
}

/// Operation to create a view in the catalog.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, typed_builder::TypedBuilder)]
#[serde(rename_all = "kebab-case")]
pub struct CreateViewOperation {
    /// View identifier
    pub identifier: TableIdent,
    /// UUID of the view
    pub view_uuid: Uuid,
    /// View updates applied during creation
    pub updates: Vec<ViewUpdate>,
}

/// Operation to drop a view from the catalog.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, typed_builder::TypedBuilder)]
#[serde(rename_all = "kebab-case")]
pub struct DropViewOperation {
    /// View identifier
    pub identifier: TableIdent,
    /// UUID of the view
    pub view_uuid: Uuid,
}

/// Operation to update a view in the catalog.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, typed_builder::TypedBuilder)]
#[serde(rename_all = "kebab-case")]
pub struct UpdateViewOperation {
    /// View identifier
    pub identifier: TableIdent,
    /// UUID of the view
    pub view_uuid: Uuid,
    /// View updates to apply
    pub updates: Vec<ViewUpdate>,
    /// Requirements that must be met for the update to succeed
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[builder(default)]
    pub requirements: Vec<serde_json::Value>,
}

/// Operation to create a namespace in the catalog.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, typed_builder::TypedBuilder)]
#[serde(rename_all = "kebab-case")]
pub struct CreateNamespaceOperation {
    #[serde(flatten)]
    /// Response containing the created namespace details
    pub namespace_response: NamespaceResponse,
}

/// Operation to update namespace properties.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, typed_builder::TypedBuilder)]
#[serde(rename_all = "kebab-case")]
pub struct UpdateNamespacePropertiesOperation {
    /// Namespace identifier
    pub namespace: NamespaceIdent,
    #[serde(flatten)]
    /// Response containing the updated namespace properties
    pub update_namespace_properties_response: UpdateNamespacePropertiesResponse,
}

/// Operation to drop a namespace from the catalog.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, typed_builder::TypedBuilder)]
#[serde(rename_all = "kebab-case")]
pub struct DropNamespaceOperation {
    /// Namespace identifier
    pub namespace: NamespaceIdent,
}

/// Trait for custom operations in catalog events.
pub trait CustomOperation: Clone + PartialEq + Eq {
    /// The type used for the custom operation type, typically an enum
    /// with the supported custom operation variants.
    type CustomOperationType: CustomOperationType;

    /// Convert to a generic custom operation representation.
    /// This is used for serialization in the REST API.
    fn try_into_raw(
        &self,
    ) -> Result<
        RawCustomOperationSer<
            '_,
            Self::CustomOperationType,
            impl AsRef<str> + Serialize,
            impl Serialize,
        >,
        Error,
    >;

    /// Create from a generic custom operation representation.
    /// This is used for deserialization in the REST API.
    fn try_from_raw(raw: RawCustomOperation<Self::CustomOperationType>) -> Result<Self, Error>;
}

/// Custom operation for catalog-specific extensions not defined in the standard.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(bound(deserialize = "T: for<'a> Deserialize<'a>"))]
#[serde(rename_all = "kebab-case")]
pub struct RawCustomOperation<T = String>
where T: CustomOperationType
{
    /// Custom operation type identifier
    pub custom_type: T,
    /// Optional table/view identifier
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "identifier")]
    pub tabular_ident: Option<TableIdent>,
    /// Optional table UUID
    #[serde(skip_serializing_if = "Option::is_none")]
    pub table_uuid: Option<Uuid>,
    /// Optional view UUID
    #[serde(skip_serializing_if = "Option::is_none")]
    pub view_uuid: Option<Uuid>,
    /// Optional namespace identifier
    #[serde(skip_serializing_if = "Option::is_none")]
    pub namespace: Option<NamespaceIdent>,
    /// Additional custom properties
    #[serde(flatten)]
    pub properties: HashMap<String, serde_json::Value>,
}

impl CustomOperation for RawCustomOperation {
    type CustomOperationType = String;

    fn try_into_raw(
        &self,
    ) -> Result<
        RawCustomOperationSer<
            '_,
            <Self as CustomOperation>::CustomOperationType,
            impl AsRef<str> + Serialize,
            impl Serialize,
        >,
        iceberg::Error,
    > {
        Ok(RawCustomOperationSer {
            custom_type: &self.custom_type,
            tabular_ident: self.tabular_ident.as_ref(),
            table_uuid: self.table_uuid,
            view_uuid: self.view_uuid,
            namespace: self.namespace.as_ref(),
            properties: self.properties.clone(),
        })
    }

    fn try_from_raw(generic: RawCustomOperation<Self::CustomOperationType>) -> Result<Self, Error> {
        Ok(Self {
            custom_type: generic.custom_type,
            tabular_ident: generic.tabular_ident,
            table_uuid: generic.table_uuid,
            view_uuid: generic.view_uuid,
            namespace: generic.namespace,
            properties: generic.properties,
        })
    }
}

/// Custom operation for catalog-specific extensions not defined in the standard.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct RawCustomOperationSer<'s, T, PK, PV>
where
    T: CustomOperationType,
    PK: AsRef<str> + Serialize,
    PV: Serialize,
{
    /// Custom operation type identifier
    pub custom_type: &'s T,
    /// Optional table/view identifier
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "identifier")]
    pub tabular_ident: Option<&'s TableIdent>,
    /// Optional table UUID
    // UUIDs are Copy, so we don't need a reference here
    #[serde(skip_serializing_if = "Option::is_none")]
    pub table_uuid: Option<Uuid>,
    /// Optional view UUID
    #[serde(skip_serializing_if = "Option::is_none")]
    pub view_uuid: Option<Uuid>,
    /// Optional namespace identifier
    #[serde(skip_serializing_if = "Option::is_none")]
    pub namespace: Option<&'s NamespaceIdent>,
    /// Additional custom properties
    // Custom Operation enums typically hold their filters typesafe,
    // thus we must assume the HashMap is constructed just for serialization.
    // Hence it is owned, but keys & values can be references.
    #[serde(flatten)]
    pub properties: HashMap<PK, PV>,
}

fn serialize_datetime_ms<S>(dt: &DateTime<Utc>, serializer: S) -> Result<S::Ok, S::Error>
where S: serde::Serializer {
    serializer.serialize_i64(dt.timestamp_millis())
}

fn deserialize_datetime_ms<'de, D>(deserializer: D) -> Result<DateTime<Utc>, D::Error>
where D: serde::Deserializer<'de> {
    let ms = i64::deserialize(deserializer)?;
    DateTime::from_timestamp_millis(ms).ok_or_else(|| {
        serde::de::Error::custom("invalid timestamp, expected milliseconds since epoch")
    })
}

fn serialize_optional_datetime_ms<S>(
    dt: &Option<DateTime<Utc>>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    match dt {
        Some(dt) => serializer.serialize_some(&dt.timestamp_millis()),
        None => serializer.serialize_none(),
    }
}

fn deserialize_optional_datetime_ms<'de, D>(
    deserializer: D,
) -> Result<Option<DateTime<Utc>>, D::Error>
where D: serde::Deserializer<'de> {
    Option::<i64>::deserialize(deserializer)?
        .map(|ms| {
            DateTime::from_timestamp_millis(ms).ok_or_else(|| {
                serde::de::Error::custom("invalid timestamp, expected milliseconds since epoch")
            })
        })
        .transpose()
}

fn serialize_custom_filters<F, S>(filters: &F, serializer: S) -> Result<S::Ok, S::Error>
where
    F: CustomFilterCollection,
    S: serde::Serializer,
{
    filters
        .try_as_filter_map()
        .map_err(serde::ser::Error::custom)?
        .serialize(serializer)
}

fn deserialize_custom_filters<'de, F, D>(deserializer: D) -> Result<F, D::Error>
where
    F: CustomFilterCollection,
    D: serde::Deserializer<'de>,
{
    let map = HashMap::<String, serde_json::Value>::deserialize(deserializer)?;
    F::try_from_filter_map(map).map_err(serde::de::Error::custom)
}

fn serialize_custom_actor<A, S>(actor: &Option<A>, serializer: S) -> Result<S::Ok, S::Error>
where
    A: Actor,
    S: serde::Serializer,
{
    match actor {
        Some(actor) => actor
            .try_as_property_map()
            .map_err(serde::ser::Error::custom)?
            .serialize(serializer),
        None => serializer.serialize_none(),
    }
}

fn deserialize_custom_actor<'de, A, D>(deserializer: D) -> Result<Option<A>, D::Error>
where
    A: Actor,
    D: serde::Deserializer<'de>,
{
    let opt_map = Option::<HashMap<String, serde_json::Value>>::deserialize(deserializer)?;
    match opt_map {
        Some(map) => Ok(Some(
            A::try_from_property_map(map).map_err(serde::de::Error::custom)?,
        )),
        None => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use iceberg::ErrorKind;

    use super::*;

    #[test]
    fn test_query_events_request_serde() {
        let request = QueryEventsRequest {
            continuation_token: Some("token123".to_string()),
            page_size: Some(100),
            after_timestamp: Some(DateTime::from_timestamp_millis(1625079600000).unwrap()),
            operation_types: Some(vec![
                OperationType::Standard(StandardOperationType::CreateTable),
                OperationType::Standard(StandardOperationType::DropTable),
            ]),
            catalog_objects_by_name: Some(vec![CatalogObjectIdentifier(vec![
                "namespace".to_string(),
                "table_name".to_string(),
            ])]),
            catalog_objects_by_id: Some(vec![CatalogObjectUuid {
                uuid: Uuid::parse_str("123e4567-e89b-12d3-a456-426614174000").unwrap(),
                object_type: CatalogObjectType::Table,
            }]),
            object_types: Some(vec![
                QueryEventsRequestObjectType::Table,
                QueryEventsRequestObjectType::View,
            ]),
            custom_filters: HashMap::from([(
                "custom-key".to_string(),
                serde_json::json!("custom-value"),
            )]),
        };

        let json = serde_json::to_value(&request).unwrap();
        assert_eq!(
            json,
            serde_json::json!({
                "continuation-token": "token123",
                "page-size": 100,
                "after-timestamp-ms": 1625079600000i64,
                "operation-types": ["create-table", "drop-table"],
                "catalog-objects-by-name": [["namespace", "table_name"]],
                "catalog-objects-by-id": [{
                    "uuid": "123e4567-e89b-12d3-a456-426614174000",
                    "type": "table"
                }],
                "object-types": ["table", "view"],
                "custom-filters": {
                    "custom-key": "custom-value"
                }
            })
        );

        let deserialized: QueryEventsRequest = serde_json::from_value(json).unwrap();
        assert_eq!(request, deserialized);
    }

    #[test]
    fn test_query_events_request_minimal() {
        let request = QueryEventsRequest::builder().build();
        let json = serde_json::to_value(&request).unwrap();
        // Page token should be serialized to show that client supports
        // pagination, even if it's None.
        assert_eq!(
            json,
            serde_json::json!({
                "continuation-token": null,
            })
        );

        let deserialized: QueryEventsRequest =
            serde_json::from_value(serde_json::json!({})).unwrap();
        assert_eq!(request, deserialized);
    }

    #[test]
    fn test_operation_type_serialization() {
        let standard = OperationType::from(StandardOperationType::CreateTable);
        let json = serde_json::to_string(&standard).unwrap();
        assert_eq!(json, r#""create-table""#);

        let custom = OperationType::new_custom("custom-op".to_string());
        let json = serde_json::to_string(&custom).unwrap();
        assert_eq!(json, r#""x-custom-op""#);
    }

    #[test]
    fn test_operation_type_deserialization() {
        let standard_json = r#""drop-view""#;
        let standard: OperationType = serde_json::from_str(standard_json).unwrap();
        assert_eq!(
            standard,
            OperationType::from(StandardOperationType::DropView)
        );
        let custom_json = r#""x-my-custom-op""#;
        let custom: OperationType = serde_json::from_str(custom_json).unwrap();
        assert_eq!(
            custom,
            OperationType::new_custom("x-my-custom-op".to_string())
        );
    }

    #[test]
    fn test_custom_operation_type_serde() {
        #[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
        #[serde(rename_all = "kebab-case")]
        pub enum MyCustomOp {
            #[serde(rename = "x-grant-select")]
            GrantSelect,
        }

        impl CustomOperationType for MyCustomOp {}

        let custom = OperationType::new_custom(MyCustomOp::GrantSelect);
        let json = serde_json::to_string(&custom).unwrap();
        assert_eq!(json, r#""x-grant-select""#);
        let deserialized: OperationType<MyCustomOp> = serde_json::from_str(&json).unwrap();
        assert_eq!(custom, deserialized);
    }

    #[test]
    fn test_custom_raw_operation_serde() {
        let raw_op = RawCustomOperation {
            custom_type: "x-custom-op".to_string(),
            tabular_ident: Some(
                TableIdent::from_strs(vec!["namespace".to_string(), "table".to_string()]).unwrap(),
            ),
            table_uuid: Some(Uuid::parse_str("123e4567-e89b-12d3-a456-426614174000").unwrap()),
            view_uuid: None,
            namespace: None,
            properties: HashMap::from([(
                "extra-info".to_string(),
                serde_json::json!("some value"),
            )]),
        };

        let json = serde_json::to_value(&raw_op).unwrap();
        assert_eq!(
            json,
            serde_json::json!({
                "custom-type": "x-custom-op",
                "identifier": {
                    "namespace": ["namespace"],
                    "name": "table"
                },
                "table-uuid": "123e4567-e89b-12d3-a456-426614174000",
                "extra-info": "some value"
            })
        );

        let deserialized: RawCustomOperation = serde_json::from_value(json).unwrap();
        assert_eq!(raw_op, deserialized);
    }

    #[test]
    fn test_custom_operation_serde() {
        #[derive(Clone, Hash, Debug, Serialize, Deserialize, PartialEq, Eq)]
        pub enum MyCustomOperationTypes {
            GrantModifyOnTable,
        }

        impl CustomOperationType for MyCustomOperationTypes {}

        #[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
        pub enum MyCustomOperations {
            GrantModifyOnTable {
                table_ident: TableIdent,
                table_uuid: Uuid,
                to_principal: String,
            },
        }

        impl CustomOperation for MyCustomOperations {
            type CustomOperationType = MyCustomOperationTypes;

            fn try_into_raw(
                &self,
            ) -> Result<
                RawCustomOperationSer<
                    '_,
                    Self::CustomOperationType,
                    impl AsRef<str> + Serialize,
                    impl Serialize,
                >,
                Error,
            > {
                match self {
                    MyCustomOperations::GrantModifyOnTable {
                        table_ident,
                        table_uuid,
                        to_principal,
                    } => {
                        let mut properties = HashMap::new();
                        properties.insert("to-principal".to_string(), to_principal.clone());
                        Ok(RawCustomOperationSer {
                            custom_type: &MyCustomOperationTypes::GrantModifyOnTable,
                            tabular_ident: Some(table_ident),
                            table_uuid: Some(*table_uuid),
                            view_uuid: None,
                            namespace: None,
                            properties,
                        })
                    }
                }
            }

            fn try_from_raw(
                raw: RawCustomOperation<Self::CustomOperationType>,
            ) -> Result<Self, Error> {
                match raw.custom_type {
                    MyCustomOperationTypes::GrantModifyOnTable => {
                        let table_ident = raw.tabular_ident.ok_or_else(|| {
                            Error::new(ErrorKind::DataInvalid, "missing table identifier")
                        })?;
                        let table_uuid = raw.table_uuid.ok_or_else(|| {
                            Error::new(ErrorKind::DataInvalid, "missing table UUID")
                        })?;
                        let to_principal = raw
                            .properties
                            .get("to-principal")
                            .and_then(|v| v.as_str())
                            .ok_or_else(|| {
                                Error::new(ErrorKind::DataInvalid, "missing to-principal property")
                            })?
                            .to_string();
                        Ok(MyCustomOperations::GrantModifyOnTable {
                            table_ident,
                            table_uuid,
                            to_principal,
                        })
                    }
                }
            }
        }

        let custom_op = MyCustomOperations::GrantModifyOnTable {
            table_ident: TableIdent::from_strs(vec!["namespace", "my_table"]).unwrap(),
            table_uuid: Uuid::parse_str("123e4567-e89b-12d3-a456-426614174000").unwrap(),
            to_principal: "user@example.com".to_string(),
        };

        // Test serialization via the trait
        let raw_ser = custom_op.try_into_raw().unwrap();
        let json = serde_json::to_value(&raw_ser).unwrap();

        assert_eq!(
            json,
            serde_json::json!({
                "custom-type": "GrantModifyOnTable",
                "identifier": {
                    "namespace": ["namespace"],
                    "name": "my_table"
                },
                "table-uuid": "123e4567-e89b-12d3-a456-426614174000",
                "to-principal": "user@example.com"
            })
        );

        // Test deserialization via the trait
        let raw_custom_op: RawCustomOperation<MyCustomOperationTypes> = RawCustomOperation {
            custom_type: MyCustomOperationTypes::GrantModifyOnTable,
            tabular_ident: Some(TableIdent::from_strs(vec!["namespace", "my_table"]).unwrap()),
            table_uuid: Some(Uuid::parse_str("123e4567-e89b-12d3-a456-426614174000").unwrap()),
            view_uuid: None,
            namespace: None,
            properties: HashMap::from([(
                "to-principal".to_string(),
                serde_json::json!("user@example.com"),
            )]),
        };

        let deserialized = MyCustomOperations::try_from_raw(raw_custom_op).unwrap();
        assert_eq!(custom_op, deserialized);
    }

    #[test]
    fn test_query_events_response_serde() {
        let response = QueryEventsResponse {
            continuation_token: "next-page-token".to_string(),
            highest_processed_timestamp: DateTime::from_timestamp_millis(1625079600000).unwrap(),
            events: vec![
                Event::builder()
                    .event_id("event-1".to_string())
                    .request_id("req-123".to_string())
                    .request_event_count(2)
                    .timestamp(DateTime::from_timestamp_millis(1625079600000).unwrap())
                    .operation(Operation::CreateTable(CreateTableOperation {
                        identifier: TableIdent::from_strs(vec!["namespace", "table1"]).unwrap(),
                        table_uuid: Uuid::parse_str("123e4567-e89b-12d3-a456-426614174000")
                            .unwrap(),
                        updates: vec![],
                    }))
                    .build(),
                Event::builder()
                    .event_id("event-2".to_string())
                    .request_id("req-123".to_string())
                    .request_event_count(2)
                    .timestamp(DateTime::from_timestamp_millis(1625079600000).unwrap())
                    .actor(Some(HashMap::from([(
                        "user".to_string(),
                        serde_json::json!("alice@example.com"),
                    )])))
                    .operation(Operation::DropTable(DropTableOperation {
                        identifier: TableIdent::from_strs(vec!["namespace", "table2"]).unwrap(),
                        table_uuid: Uuid::parse_str("223e4567-e89b-12d3-a456-426614174000")
                            .unwrap(),
                        purge: Some(true),
                    }))
                    .build(),
            ],
        };

        let json = serde_json::to_value(&response).unwrap();
        assert_eq!(
            json,
            serde_json::json!({
                "continuation-token": "next-page-token",
                "highest-processed-timestamp-ms": 1625079600000i64,
                "events": [
                    {
                        "event-id": "event-1",
                        "request-id": "req-123",
                        "request-event-count": 2,
                        "timestamp-ms": 1625079600000i64,
                        "operation-type": "create-table",
                        "identifier": {
                            "namespace": ["namespace"],
                            "name": "table1"
                        },
                        "table-uuid": "123e4567-e89b-12d3-a456-426614174000",
                        "updates": []
                    },
                    {
                        "event-id": "event-2",
                        "request-id": "req-123",
                        "request-event-count": 2,
                        "timestamp-ms": 1625079600000i64,
                        "actor": {
                            "user": "alice@example.com"
                        },
                        "operation-type": "drop-table",
                        "identifier": {
                            "namespace": ["namespace"],
                            "name": "table2"
                        },
                        "table-uuid": "223e4567-e89b-12d3-a456-426614174000",
                        "purge": true
                    }
                ]
            })
        );

        let deserialized: QueryEventsResponse = serde_json::from_value(json).unwrap();
        assert_eq!(response, deserialized);
    }

    #[test]
    fn test_query_events_response_empty_events() {
        let response = QueryEventsResponse {
            continuation_token: "token".to_string(),
            highest_processed_timestamp: DateTime::from_timestamp_millis(1625079600000).unwrap(),
            events: vec![],
        };

        let json = serde_json::to_value(&response).unwrap();
        assert_eq!(
            json,
            serde_json::json!({
                "continuation-token": "token",
                "highest-processed-timestamp-ms": 1625079600000i64,
                "events": []
            })
        );

        let deserialized: QueryEventsResponse = serde_json::from_value(json).unwrap();
        assert_eq!(response, deserialized);
    }

    #[test]
    fn test_query_events_response_with_namespace_operations() {
        let response = QueryEventsResponse {
            continuation_token: "page2".to_string(),
            highest_processed_timestamp: DateTime::from_timestamp_millis(1625079700000).unwrap(),
            events: vec![
                Event::builder()
                    .event_id("event-ns-1".to_string())
                    .request_id("req-ns-1".to_string())
                    .request_event_count(1)
                    .timestamp(DateTime::from_timestamp_millis(1625079700000).unwrap())
                    .operation(Operation::CreateNamespace(CreateNamespaceOperation {
                        namespace_response: NamespaceResponse {
                            namespace: NamespaceIdent::from_vec(vec!["new_namespace".to_string()])
                                .unwrap(),
                            properties: HashMap::from([(
                                "owner".to_string(),
                                "team-a".to_string(),
                            )]),
                        },
                    }))
                    .build(),
                Event::builder()
                    .event_id("event-ns-2".to_string())
                    .request_id("req-ns-2".to_string())
                    .request_event_count(1)
                    .timestamp(DateTime::from_timestamp_millis(1625079700000).unwrap())
                    .operation(Operation::DropNamespace(DropNamespaceOperation {
                        namespace: NamespaceIdent::from_vec(vec!["old_namespace".to_string()])
                            .unwrap(),
                    }))
                    .build(),
            ],
        };

        let json = serde_json::to_value(&response).unwrap();
        let deserialized: QueryEventsResponse = serde_json::from_value(json).unwrap();
        assert_eq!(response, deserialized);
    }

    #[test]
    fn test_query_events_response_with_view_operations() {
        let response = QueryEventsResponse {
            continuation_token: "view-page".to_string(),
            highest_processed_timestamp: DateTime::from_timestamp_millis(1625079800000).unwrap(),
            events: vec![
                Event::builder()
                    .event_id("event-view-1".to_string())
                    .request_id("req-view-1".to_string())
                    .request_event_count(1)
                    .timestamp(DateTime::from_timestamp_millis(1625079800000).unwrap())
                    .operation(Operation::CreateView(CreateViewOperation {
                        identifier: TableIdent::from_strs(vec!["namespace", "view1"]).unwrap(),
                        view_uuid: Uuid::parse_str("323e4567-e89b-12d3-a456-426614174000").unwrap(),
                        updates: vec![],
                    }))
                    .build(),
                Event::builder()
                    .event_id("event-view-2".to_string())
                    .request_id("req-view-2".to_string())
                    .request_event_count(1)
                    .timestamp(DateTime::from_timestamp_millis(1625079800000).unwrap())
                    .operation(Operation::DropView(DropViewOperation {
                        identifier: TableIdent::from_strs(vec!["namespace", "view2"]).unwrap(),
                        view_uuid: Uuid::parse_str("423e4567-e89b-12d3-a456-426614174000").unwrap(),
                    }))
                    .build(),
            ],
        };

        let json = serde_json::to_value(&response).unwrap();
        let deserialized: QueryEventsResponse = serde_json::from_value(json).unwrap();
        assert_eq!(response, deserialized);
    }

    #[test]
    fn test_query_events_response_with_custom_operation() {
        let response: QueryEventsResponse<RawCustomOperation> = QueryEventsResponse {
            continuation_token: "custom-page".to_string(),
            highest_processed_timestamp: DateTime::from_timestamp_millis(1625079900000).unwrap(),
            events: vec![
                Event::builder()
                    .event_id("event-custom-1".to_string())
                    .request_id("req-custom-1".to_string())
                    .request_event_count(1)
                    .timestamp(DateTime::from_timestamp_millis(1625079900000).unwrap())
                    .operation(Operation::Custom(RawCustomOperation {
                        custom_type: "x-custom-op".to_string(),
                        tabular_ident: Some(
                            TableIdent::from_strs(vec!["namespace", "table"]).unwrap(),
                        ),
                        table_uuid: Some(
                            Uuid::parse_str("523e4567-e89b-12d3-a456-426614174000").unwrap(),
                        ),
                        view_uuid: None,
                        namespace: None,
                        properties: HashMap::from([(
                            "custom-property".to_string(),
                            serde_json::json!("custom-value"),
                        )]),
                    }))
                    .build(),
            ],
        };

        let json = serde_json::to_value(&response).unwrap();
        assert_eq!(
            json["events"][0]["operation-type"],
            serde_json::json!("custom")
        );
        assert_eq!(
            json["events"][0]["custom-type"],
            serde_json::json!("x-custom-op")
        );

        let deserialized: QueryEventsResponse = serde_json::from_value(json).unwrap();
        assert_eq!(response, deserialized);
    }
}
