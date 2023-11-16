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

use serde_derive::{Deserialize, Serialize};
use urlencoding::encode;

use crate::spec::{FormatVersion, PartitionSpec, Schema, Snapshot, SnapshotReference, SortOrder};
use crate::table::Table;
use crate::{Error, ErrorKind, Result};
use async_trait::async_trait;
use std::collections::HashMap;
use std::mem::take;
use std::ops::Deref;
use typed_builder::TypedBuilder;

/// The catalog API for Iceberg Rust.
#[async_trait]
pub trait Catalog: std::fmt::Debug {
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
    async fn namespace_exists(&self, namesace: &NamespaceIdent) -> Result<bool>;

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
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
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
    pub fn from_strs(iter: impl IntoIterator<Item=impl ToString>) -> Result<Self> {
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
    pub fn from_strs(iter: impl IntoIterator<Item=impl ToString>) -> Result<Self> {
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
    pub partition_spec: Option<PartitionSpec>,
    /// The sort order of the table.
    #[builder(default, setter(strip_option))]
    pub sort_order: Option<SortOrder>,
    /// The properties of the table.
    #[builder(default)]
    pub properties: HashMap<String, String>,
}

/// TableCommit represents the commit of a table in the catalog.
#[derive(Debug, TypedBuilder)]
#[builder(build_method(vis="pub(crate)"))]
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
#[derive(Debug, Serialize, Deserialize)]
pub enum TableRequirement {
    /// The table must not already exist; used for create transactions
    NotExist,
    /// The table UUID must match the requirement.
    UuidMatch(String),
    /// The table branch or tag identified by the requirement's `reference` must
    /// reference the requirement's `snapshot-id`.
    RefSnapshotIdMatch {
        /// The reference of the table to assert.
        reference: String,
        /// The snapshot id of the table to assert.
        /// If the id is `None`, the ref must not already exist.
        snapshot_id: Option<i64>,
    },
    /// The table's last assigned column id must match the requirement.
    LastAssignedFieldIdMatch(i64),
    /// The table's current schema id must match the requirement.
    CurrentSchemaIdMatch(i64),
    /// The table's last assigned partition id must match the
    /// requirement.
    LastAssignedPartitionIdMatch(i64),
    /// The table's default spec id must match the requirement.
    DefaultSpecIdMatch(i64),
    /// The table's default sort order id must match the requirement.
    DefaultSortOrderIdMatch(i64),
}

/// TableUpdate represents an update to a table in the catalog.
#[derive(Debug, Serialize, Deserialize)]
pub enum TableUpdate {
    /// Upgrade table's format version
    UpgradeFormatVersion {
        /// Target format upgrade to.
        format_version: FormatVersion,
    },
    /// Add a new schema to the table
    AddSchema {
        schema: Schema,
        last_column_id: i32,
    },
    /// Set table's current schema
    SetCurrentSchema {
        schema_id: i32
    },
    /// Add a new partition spec to the table
    AddPartitionSpec {
        spec: PartitionSpec,
    },
    /// Set table's default spec
    SetDefaultSpec {
        spec_id: i32,
    },
    /// Add sort order to table.
    AddSortOrder {
        sort_order: SortOrder,
    },
    /// Set table's default sort order
    SetDefaultSortOrder {
        sort_order_id: i32
    },
    /// Add snapshot to table.
    AddSnapshot {
        snapshot: Snapshot,
    },
    /// Set table's snapshot ref.
    SetSnapshotRef {
        ref_name: String,
        reference: SnapshotReference,
    },
    /// Remove table's snapshots
    RemoveSnapshots {
        snapshot_ids: Vec<i64>,
    },
    /// Remove snapshot reference
    RemoveSnapshotRef {
        ref_name: String,
    },
    /// Update table's location
    SetLocation {
        location: String,
    },
    /// Update table's properties
    SetProperties {
        updates: HashMap<String, String>,
    },
    /// Remove table's properties
    RemoveProperties {
        removals: Vec<String>,
    },
}

#[cfg(test)]
mod tests {
    use crate::{NamespaceIdent, TableIdent};

    #[test]
    fn test_create_table_id() {
        let table_id = TableIdent {
            namespace: NamespaceIdent::from_strs(vec!["ns1"]).unwrap(),
            name: "t1".to_string(),
        };

        assert_eq!(table_id, TableIdent::from_strs(vec!["ns1", "t1"]).unwrap());
    }
}
