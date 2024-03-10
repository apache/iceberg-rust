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

//! Catalog API for Apache Iceberg

use crate::spec::{
    FormatVersion, Schema, Snapshot, SnapshotReference, SortOrder, UnboundPartitionSpec,
};
use crate::table::Table;
use crate::{Error, ErrorKind, Result};
use async_trait::async_trait;
use serde_derive::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Debug;
use std::mem::take;
use std::ops::Deref;
use typed_builder::TypedBuilder;
use urlencoding::encode;
use uuid::Uuid;

/// The catalog API for Iceberg Rust.
#[async_trait]
pub trait Catalog: Debug + Sync + Send {
    /// List namespaces from table.
    async fn list_namespaces(&self, parent: Option<&NamespaceIdent>)
        -> Result<Vec<NamespaceIdent>>;

    /// Create a new namespace inside the catalog.
    async fn create_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> Result<Namespace>;

    /// Get a namespace information from the catalog.
    async fn get_namespace(&self, namespace: &NamespaceIdent) -> Result<Namespace>;

    /// Check if namespace exists in catalog.
    async fn namespace_exists(&self, namespace: &NamespaceIdent) -> Result<bool>;

    /// Update a namespace inside the catalog.
    ///
    /// # Behavior
    ///
    /// The properties must be the full set of namespace.
    async fn update_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> Result<()>;

    /// Drop a namespace from the catalog.
    async fn drop_namespace(&self, namespace: &NamespaceIdent) -> Result<()>;

    /// List tables from namespace.
    async fn list_tables(&self, namespace: &NamespaceIdent) -> Result<Vec<TableIdent>>;

    /// Create a new table inside the namespace.
    async fn create_table(
        &self,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> Result<Table>;

    /// Load table from the catalog.
    async fn load_table(&self, table: &TableIdent) -> Result<Table>;

    /// Drop a table from the catalog.
    async fn drop_table(&self, table: &TableIdent) -> Result<()>;

    /// Check if a table exists in the catalog.
    async fn stat_table(&self, table: &TableIdent) -> Result<bool>;

    /// Rename a table in the catalog.
    async fn rename_table(&self, src: &TableIdent, dest: &TableIdent) -> Result<()>;

    /// Update a table to the catalog.
    async fn update_table(&self, commit: TableCommit) -> Result<Table>;
}

/// NamespaceIdent represents the identifier of a namespace in the catalog.
///
/// The namespace identifier is a list of strings, where each string is a
/// component of the namespace. It's catalog implementer's responsibility to
/// handle the namespace identifier correctly.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NamespaceIdent(Vec<String>);

impl NamespaceIdent {
    /// Create a new namespace identifier with only one level.
    pub fn new(name: String) -> Self {
        Self(vec![name])
    }

    /// Create a multi-level namespace identifier from vector.
    pub fn from_vec(names: Vec<String>) -> Result<Self> {
        if names.is_empty() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Namespace identifier can't be empty!",
            ));
        }
        Ok(Self(names))
    }

    /// Try to create namespace identifier from an iterator of string.
    pub fn from_strs(iter: impl IntoIterator<Item = impl ToString>) -> Result<Self> {
        Self::from_vec(iter.into_iter().map(|s| s.to_string()).collect())
    }

    /// Returns url encoded format.
    pub fn encode_in_url(&self) -> String {
        encode(&self.as_ref().join("\u{1F}")).to_string()
    }

    /// Returns inner strings.
    pub fn inner(self) -> Vec<String> {
        self.0
    }
}

impl AsRef<Vec<String>> for NamespaceIdent {
    fn as_ref(&self) -> &Vec<String> {
        &self.0
    }
}

impl Deref for NamespaceIdent {
    type Target = [String];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Namespace represents a namespace in the catalog.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Namespace {
    name: NamespaceIdent,
    properties: HashMap<String, String>,
}

impl Namespace {
    /// Create a new namespace.
    pub fn new(name: NamespaceIdent) -> Self {
        Self::with_properties(name, HashMap::default())
    }

    /// Create a new namespace with properties.
    pub fn with_properties(name: NamespaceIdent, properties: HashMap<String, String>) -> Self {
        Self { name, properties }
    }

    /// Get the name of the namespace.
    pub fn name(&self) -> &NamespaceIdent {
        &self.name
    }

    /// Get the properties of the namespace.
    pub fn properties(&self) -> &HashMap<String, String> {
        &self.properties
    }
}

/// TableIdent represents the identifier of a table in the catalog.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
pub struct TableIdent {
    /// Namespace of the table.
    pub namespace: NamespaceIdent,
    /// Table name.
    pub name: String,
}

impl TableIdent {
    /// Create a new table identifier.
    pub fn new(namespace: NamespaceIdent, name: String) -> Self {
        Self { namespace, name }
    }

    /// Get the namespace of the table.
    pub fn namespace(&self) -> &NamespaceIdent {
        &self.namespace
    }

    /// Get the name of the table.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Try to create table identifier from an iterator of string.
    pub fn from_strs(iter: impl IntoIterator<Item = impl ToString>) -> Result<Self> {
        let mut vec: Vec<String> = iter.into_iter().map(|s| s.to_string()).collect();
        let table_name = vec.pop().ok_or_else(|| {
            Error::new(ErrorKind::DataInvalid, "Table identifier can't be empty!")
        })?;
        let namespace_ident = NamespaceIdent::from_vec(vec)?;

        Ok(Self {
            namespace: namespace_ident,
            name: table_name,
        })
    }
}

/// TableCreation represents the creation of a table in the catalog.
#[derive(Debug, TypedBuilder)]
pub struct TableCreation {
    /// The name of the table.
    pub name: String,
    /// The location of the table.
    #[builder(default, setter(strip_option))]
    pub location: Option<String>,
    /// The schema of the table.
    pub schema: Schema,
    /// The partition spec of the table, could be None.
    #[builder(default, setter(strip_option))]
    pub partition_spec: Option<UnboundPartitionSpec>,
    /// The sort order of the table.
    #[builder(default, setter(strip_option))]
    pub sort_order: Option<SortOrder>,
    /// The properties of the table.
    #[builder(default)]
    pub properties: HashMap<String, String>,
}

/// TableCommit represents the commit of a table in the catalog.
#[derive(Debug, TypedBuilder)]
#[builder(build_method(vis = "pub(crate)"))]
pub struct TableCommit {
    /// The table ident.
    ident: TableIdent,
    /// The requirements of the table.
    ///
    /// Commit will fail if the requirements are not met.
    requirements: Vec<TableRequirement>,
    /// The updates of the table.
    updates: Vec<TableUpdate>,
}

impl TableCommit {
    /// Return the table identifier.
    pub fn identifier(&self) -> &TableIdent {
        &self.ident
    }

    /// Take all requirements.
    pub fn take_requirements(&mut self) -> Vec<TableRequirement> {
        take(&mut self.requirements)
    }

    /// Take all updates.
    pub fn take_updates(&mut self) -> Vec<TableUpdate> {
        take(&mut self.updates)
    }
}

/// TableRequirement represents a requirement for a table in the catalog.
#[derive(Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type")]
pub enum TableRequirement {
    /// The table must not already exist; used for create transactions
    #[serde(rename = "assert-create")]
    NotExist,
    /// The table UUID must match the requirement.
    #[serde(rename = "assert-table-uuid")]
    UuidMatch {
        /// Uuid of original table.
        uuid: Uuid,
    },
    /// The table branch or tag identified by the requirement's `reference` must
    /// reference the requirement's `snapshot-id`.
    #[serde(rename = "assert-ref-snapshot-id")]
    RefSnapshotIdMatch {
        /// The reference of the table to assert.
        r#ref: String,
        /// The snapshot id of the table to assert.
        /// If the id is `None`, the ref must not already exist.
        #[serde(rename = "snapshot-id")]
        snapshot_id: Option<i64>,
    },
    /// The table's last assigned column id must match the requirement.
    #[serde(rename = "assert-last-assigned-field-id")]
    LastAssignedFieldIdMatch {
        /// The last assigned field id of the table to assert.
        #[serde(rename = "last-assigned-field-id")]
        last_assigned_field_id: i64,
    },
    /// The table's current schema id must match the requirement.
    #[serde(rename = "assert-current-schema-id")]
    CurrentSchemaIdMatch {
        /// Current schema id of the table to assert.
        #[serde(rename = "current-schema-id")]
        current_schema_id: i64,
    },
    /// The table's last assigned partition id must match the
    /// requirement.
    #[serde(rename = "assert-last-assigned-partition-id")]
    LastAssignedPartitionIdMatch {
        /// Last assigned partition id of the table to assert.
        #[serde(rename = "last-assigned-partition-id")]
        last_assigned_partition_id: i64,
    },
    /// The table's default spec id must match the requirement.
    #[serde(rename = "assert-default-spec-id")]
    DefaultSpecIdMatch {
        /// Default spec id of the table to assert.
        #[serde(rename = "default-spec-id")]
        default_spec_id: i64,
    },
    /// The table's default sort order id must match the requirement.
    #[serde(rename = "assert-default-sort-order-id")]
    DefaultSortOrderIdMatch {
        /// Default sort order id of the table to assert.
        #[serde(rename = "default-sort-order-id")]
        default_sort_order_id: i64,
    },
}

/// TableUpdate represents an update to a table in the catalog.
#[derive(Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "action", rename_all = "kebab-case")]
pub enum TableUpdate {
    /// Upgrade table's format version
    #[serde(rename_all = "kebab-case")]
    UpgradeFormatVersion {
        /// Target format upgrade to.
        format_version: FormatVersion,
    },
    /// Assign a new UUID to the table
    #[serde(rename_all = "kebab-case")]
    AssignUuid {
        /// The new UUID to assign.
        uuid: Uuid,
    },
    /// Add a new schema to the table
    #[serde(rename_all = "kebab-case")]
    AddSchema {
        /// The schema to add.
        schema: Schema,
        /// The last column id of the table.
        last_column_id: Option<i32>,
    },
    /// Set table's current schema
    #[serde(rename_all = "kebab-case")]
    SetCurrentSchema {
        /// Schema ID to set as current, or -1 to set last added schema
        schema_id: i32,
    },
    /// Add a new partition spec to the table
    AddSpec {
        /// The partition spec to add.
        spec: UnboundPartitionSpec,
    },
    /// Set table's default spec
    #[serde(rename_all = "kebab-case")]
    SetDefaultSpec {
        /// Partition spec ID to set as the default, or -1 to set last added spec
        spec_id: i32,
    },
    /// Add sort order to table.
    #[serde(rename_all = "kebab-case")]
    AddSortOrder {
        /// Sort order to add.
        sort_order: SortOrder,
    },
    /// Set table's default sort order
    #[serde(rename_all = "kebab-case")]
    SetDefaultSortOrder {
        /// Sort order ID to set as the default, or -1 to set last added sort order
        sort_order_id: i32,
    },
    /// Add snapshot to table.
    #[serde(rename_all = "kebab-case")]
    AddSnapshot {
        /// Snapshot to add.
        snapshot: Snapshot,
    },
    /// Set table's snapshot ref.
    #[serde(rename_all = "kebab-case")]
    SetSnapshotRef {
        /// Name of snapshot reference to set.
        ref_name: String,
        /// Snapshot reference to set.
        #[serde(flatten)]
        reference: SnapshotReference,
    },
    /// Remove table's snapshots
    #[serde(rename_all = "kebab-case")]
    RemoveSnapshots {
        /// Snapshot ids to remove.
        snapshot_ids: Vec<i64>,
    },
    /// Remove snapshot reference
    #[serde(rename_all = "kebab-case")]
    RemoveSnapshotRef {
        /// Name of snapshot reference to remove.
        ref_name: String,
    },
    /// Update table's location
    SetLocation {
        /// New location for table.
        location: String,
    },
    /// Update table's properties
    SetProperties {
        /// Properties to update for table.
        updates: HashMap<String, String>,
    },
    /// Remove table's properties
    RemoveProperties {
        /// Properties to remove
        removals: Vec<String>,
    },
}

#[cfg(test)]
mod tests {
    use crate::spec::{
        FormatVersion, NestedField, NullOrder, Operation, PrimitiveType, Schema, Snapshot,
        SnapshotReference, SnapshotRetention, SortDirection, SortField, SortOrder, Summary,
        Transform, Type, UnboundPartitionField, UnboundPartitionSpec,
    };
    use crate::{NamespaceIdent, TableIdent, TableRequirement, TableUpdate};
    use serde::de::DeserializeOwned;
    use serde::Serialize;
    use std::collections::HashMap;
    use std::fmt::Debug;
    use uuid::uuid;

    #[test]
    fn test_create_table_id() {
        let table_id = TableIdent {
            namespace: NamespaceIdent::from_strs(vec!["ns1"]).unwrap(),
            name: "t1".to_string(),
        };

        assert_eq!(table_id, TableIdent::from_strs(vec!["ns1", "t1"]).unwrap());
    }

    fn test_serde_json<T: Serialize + DeserializeOwned + PartialEq + Debug>(
        json: impl ToString,
        expected: T,
    ) {
        let json_str = json.to_string();
        let actual: T = serde_json::from_str(&json_str).expect("Failed to parse from json");
        assert_eq!(actual, expected, "Parsed value is not equal to expected");

        let restored: T = serde_json::from_str(
            &serde_json::to_string(&actual).expect("Failed to serialize to json"),
        )
        .expect("Failed to parse from serialized json");

        assert_eq!(
            restored, expected,
            "Parsed restored value is not equal to expected"
        );
    }

    #[test]
    fn test_table_uuid() {
        test_serde_json(
            r#"
{
    "type": "assert-table-uuid",
    "uuid": "2cc52516-5e73-41f2-b139-545d41a4e151"
}
        "#,
            TableRequirement::UuidMatch {
                uuid: uuid!("2cc52516-5e73-41f2-b139-545d41a4e151"),
            },
        );
    }

    #[test]
    fn test_assert_table_not_exists() {
        test_serde_json(
            r#"
{
    "type": "assert-create"
}
        "#,
            TableRequirement::NotExist,
        );
    }

    #[test]
    fn test_assert_ref_snapshot_id() {
        test_serde_json(
            r#"
{
    "type": "assert-ref-snapshot-id",
    "ref": "snapshot-name",
    "snapshot-id": null 
}
        "#,
            TableRequirement::RefSnapshotIdMatch {
                r#ref: "snapshot-name".to_string(),
                snapshot_id: None,
            },
        );

        test_serde_json(
            r#"
{
    "type": "assert-ref-snapshot-id",
    "ref": "snapshot-name",
    "snapshot-id": 1
}
        "#,
            TableRequirement::RefSnapshotIdMatch {
                r#ref: "snapshot-name".to_string(),
                snapshot_id: Some(1),
            },
        );
    }

    #[test]
    fn test_assert_last_assigned_field_id() {
        test_serde_json(
            r#"
{
    "type": "assert-last-assigned-field-id",
    "last-assigned-field-id": 12
}
        "#,
            TableRequirement::LastAssignedFieldIdMatch {
                last_assigned_field_id: 12,
            },
        );
    }

    #[test]
    fn test_assert_current_schema_id() {
        test_serde_json(
            r#"
{
    "type": "assert-current-schema-id",
    "current-schema-id": 4
}
        "#,
            TableRequirement::CurrentSchemaIdMatch {
                current_schema_id: 4,
            },
        );
    }

    #[test]
    fn test_assert_last_assigned_partition_id() {
        test_serde_json(
            r#"
{
    "type": "assert-last-assigned-partition-id",
    "last-assigned-partition-id": 1004
}
        "#,
            TableRequirement::LastAssignedPartitionIdMatch {
                last_assigned_partition_id: 1004,
            },
        );
    }

    #[test]
    fn test_assert_default_spec_id() {
        test_serde_json(
            r#"
{
    "type": "assert-default-spec-id",
    "default-spec-id": 5
}
        "#,
            TableRequirement::DefaultSpecIdMatch { default_spec_id: 5 },
        );
    }

    #[test]
    fn test_assert_default_sort_order() {
        let json = r#"
{
    "type": "assert-default-sort-order-id",
    "default-sort-order-id": 10
}
        "#;

        let update = TableRequirement::DefaultSortOrderIdMatch {
            default_sort_order_id: 10,
        };

        test_serde_json(json, update);
    }

    #[test]
    fn test_parse_assert_invalid() {
        assert!(
            serde_json::from_str::<TableRequirement>(
                r#"
{
    "default-sort-order-id": 10
}
"#
            )
            .is_err(),
            "Table requirements should not be parsed without type."
        );
    }

    #[test]
    fn test_assign_uuid() {
        test_serde_json(
            r#"
{
    "action": "assign-uuid",
    "uuid": "2cc52516-5e73-41f2-b139-545d41a4e151"
}        
        "#,
            TableUpdate::AssignUuid {
                uuid: uuid!("2cc52516-5e73-41f2-b139-545d41a4e151"),
            },
        );
    }

    #[test]
    fn test_upgrade_format_version() {
        test_serde_json(
            r#"
{
    "action": "upgrade-format-version",
    "format-version": 2
}        
        "#,
            TableUpdate::UpgradeFormatVersion {
                format_version: FormatVersion::V2,
            },
        );
    }

    #[test]
    fn test_add_schema() {
        let test_schema = Schema::builder()
            .with_schema_id(1)
            .with_identifier_field_ids(vec![2])
            .with_fields(vec![
                NestedField::optional(1, "foo", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::required(2, "bar", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::optional(3, "baz", Type::Primitive(PrimitiveType::Boolean)).into(),
            ])
            .build()
            .unwrap();
        test_serde_json(
            r#"
{
    "action": "add-schema",
    "schema": {
        "type": "struct",
        "schema-id": 1,
        "fields": [
            {
                "id": 1,
                "name": "foo",
                "required": false,
                "type": "string"
            },
            {
                "id": 2,
                "name": "bar",
                "required": true,
                "type": "int"
            },
            {
                "id": 3,
                "name": "baz",
                "required": false,
                "type": "boolean"
            }
        ],
        "identifier-field-ids": [
            2
        ]
    },
    "last-column-id": 3
}
        "#,
            TableUpdate::AddSchema {
                schema: test_schema.clone(),
                last_column_id: Some(3),
            },
        );

        test_serde_json(
            r#"
{
    "action": "add-schema",
    "schema": {
        "type": "struct",
        "schema-id": 1,
        "fields": [
            {
                "id": 1,
                "name": "foo",
                "required": false,
                "type": "string"
            },
            {
                "id": 2,
                "name": "bar",
                "required": true,
                "type": "int"
            },
            {
                "id": 3,
                "name": "baz",
                "required": false,
                "type": "boolean"
            }
        ],
        "identifier-field-ids": [
            2
        ]
    }
}
        "#,
            TableUpdate::AddSchema {
                schema: test_schema.clone(),
                last_column_id: None,
            },
        );
    }

    #[test]
    fn test_set_current_schema() {
        test_serde_json(
            r#"
{
   "action": "set-current-schema",
   "schema-id": 23
}
        "#,
            TableUpdate::SetCurrentSchema { schema_id: 23 },
        );
    }

    #[test]
    fn test_add_spec() {
        test_serde_json(
            r#"
{
    "action": "add-spec",
    "spec": {
        "fields": [
            {
                "source-id": 4,
                "name": "ts_day",
                "transform": "day"
            },
            {
                "source-id": 1,
                "name": "id_bucket",
                "transform": "bucket[16]"
            },
            {
                "source-id": 2,
                "name": "id_truncate",
                "transform": "truncate[4]"
            }
        ]
    }
}
        "#,
            TableUpdate::AddSpec {
                spec: UnboundPartitionSpec::builder()
                    .with_unbound_partition_field(
                        UnboundPartitionField::builder()
                            .source_id(4)
                            .name("ts_day".to_string())
                            .transform(Transform::Day)
                            .build(),
                    )
                    .with_unbound_partition_field(
                        UnboundPartitionField::builder()
                            .source_id(1)
                            .name("id_bucket".to_string())
                            .transform(Transform::Bucket(16))
                            .build(),
                    )
                    .with_unbound_partition_field(
                        UnboundPartitionField::builder()
                            .source_id(2)
                            .name("id_truncate".to_string())
                            .transform(Transform::Truncate(4))
                            .build(),
                    )
                    .build()
                    .unwrap(),
            },
        );
    }

    #[test]
    fn test_set_default_spec() {
        test_serde_json(
            r#"
{
    "action": "set-default-spec",
    "spec-id": 1
}
        "#,
            TableUpdate::SetDefaultSpec { spec_id: 1 },
        )
    }

    #[test]
    fn test_add_sort_order() {
        let json = r#"
{
    "action": "add-sort-order",
    "sort-order": {
        "order-id": 1,
        "fields": [
            {
                "transform": "identity",
                "source-id": 2,
                "direction": "asc",
                "null-order": "nulls-first"
            },
            {
                "transform": "bucket[4]",
                "source-id": 3,
                "direction": "desc",
                "null-order": "nulls-last"
            }
        ]
    }
}
        "#;

        let update = TableUpdate::AddSortOrder {
            sort_order: SortOrder::builder()
                .with_order_id(1)
                .with_sort_field(
                    SortField::builder()
                        .source_id(2)
                        .direction(SortDirection::Ascending)
                        .null_order(NullOrder::First)
                        .transform(Transform::Identity)
                        .build(),
                )
                .with_sort_field(
                    SortField::builder()
                        .source_id(3)
                        .direction(SortDirection::Descending)
                        .null_order(NullOrder::Last)
                        .transform(Transform::Bucket(4))
                        .build(),
                )
                .build_unbound()
                .unwrap(),
        };

        test_serde_json(json, update);
    }

    #[test]
    fn test_set_default_order() {
        let json = r#"
{
    "action": "set-default-sort-order",
    "sort-order-id": 2
}
        "#;
        let update = TableUpdate::SetDefaultSortOrder { sort_order_id: 2 };

        test_serde_json(json, update);
    }

    #[test]
    fn test_add_snapshot() {
        let json = r#"
{
    "action": "add-snapshot",
    "snapshot": {
        "snapshot-id": 3055729675574597000,
        "parent-snapshot-id": 3051729675574597000,
        "timestamp-ms": 1555100955770,
        "sequence-number": 1,
        "summary": {
            "operation": "append"
        },
        "manifest-list": "s3://a/b/2.avro",
        "schema-id": 1
    }
}
        "#;

        let update = TableUpdate::AddSnapshot {
            snapshot: Snapshot::builder()
                .with_snapshot_id(3055729675574597000)
                .with_parent_snapshot_id(Some(3051729675574597000))
                .with_timestamp_ms(1555100955770)
                .with_sequence_number(1)
                .with_manifest_list("s3://a/b/2.avro")
                .with_schema_id(1)
                .with_summary(Summary {
                    operation: Operation::Append,
                    other: HashMap::default(),
                })
                .build(),
        };

        test_serde_json(json, update);
    }

    #[test]
    fn test_remove_snapshots() {
        let json = r#"
{
    "action": "remove-snapshots",
    "snapshot-ids": [
        1,
        2
    ]
}  
        "#;

        let update = TableUpdate::RemoveSnapshots {
            snapshot_ids: vec![1, 2],
        };
        test_serde_json(json, update);
    }

    #[test]
    fn test_remove_snapshot_ref() {
        let json = r#"
{
    "action": "remove-snapshot-ref",
    "ref-name": "snapshot-ref"
}
        "#;

        let update = TableUpdate::RemoveSnapshotRef {
            ref_name: "snapshot-ref".to_string(),
        };
        test_serde_json(json, update);
    }

    #[test]
    fn test_set_snapshot_ref_tag() {
        let json = r#"
{
    "action": "set-snapshot-ref",
    "type": "tag",
    "ref-name": "hank",
    "snapshot-id": 1,
    "max-ref-age-ms": 1
}
        "#;

        let update = TableUpdate::SetSnapshotRef {
            ref_name: "hank".to_string(),
            reference: SnapshotReference {
                snapshot_id: 1,
                retention: SnapshotRetention::Tag { max_ref_age_ms: 1 },
            },
        };

        test_serde_json(json, update);
    }

    #[test]
    fn test_set_snapshot_ref_branch() {
        let json = r#"
{
    "action": "set-snapshot-ref",
    "type": "branch",
    "ref-name": "hank",
    "snapshot-id": 1,
    "min-snapshots-to-keep": 2,
    "max-snapshot-age-ms": 3,
    "max-ref-age-ms": 4
}        
        "#;

        let update = TableUpdate::SetSnapshotRef {
            ref_name: "hank".to_string(),
            reference: SnapshotReference {
                snapshot_id: 1,
                retention: SnapshotRetention::Branch {
                    min_snapshots_to_keep: Some(2),
                    max_snapshot_age_ms: Some(3),
                    max_ref_age_ms: Some(4),
                },
            },
        };

        test_serde_json(json, update);
    }

    #[test]
    fn test_set_properties() {
        let json = r#"
{
    "action": "set-properties",
    "updates": {
        "prop1": "v1",
        "prop2": "v2"
    }
}        
        "#;

        let update = TableUpdate::SetProperties {
            updates: vec![
                ("prop1".to_string(), "v1".to_string()),
                ("prop2".to_string(), "v2".to_string()),
            ]
            .into_iter()
            .collect(),
        };

        test_serde_json(json, update);
    }

    #[test]
    fn test_remove_properties() {
        let json = r#"
{
    "action": "remove-properties",
    "removals": [
        "prop1",
        "prop2"
    ]
}        
        "#;

        let update = TableUpdate::RemoveProperties {
            removals: vec!["prop1".to_string(), "prop2".to_string()],
        };

        test_serde_json(json, update);
    }

    #[test]
    fn test_set_location() {
        let json = r#"
{
    "action": "set-location",
    "location": "s3://bucket/warehouse/tbl_location"
}
    "#;

        let update = TableUpdate::SetLocation {
            location: "s3://bucket/warehouse/tbl_location".to_string(),
        };

        test_serde_json(json, update);
    }
}
