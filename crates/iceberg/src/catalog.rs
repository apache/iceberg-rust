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

use crate::spec::{PartitionSpec, Schema, SortOrder};
use crate::table::Table;
use crate::Result;
use async_trait::async_trait;
use std::collections::HashMap;

/// The catalog API for Iceberg Rust.
#[async_trait]
pub trait Catalog {
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
    async fn update_table(&self, table: &TableIdent, commit: TableCommit) -> Result<Table>;

    /// Update multiple tables to the catalog as an atomic operation.
    async fn update_tables(&self, tables: &[(TableIdent, TableCommit)]) -> Result<()>;
}

/// NamespaceIdent represents the identifier of a namespace in the catalog.
pub struct NamespaceIdent(Vec<String>);

/// Namespace represents a namespace in the catalog.
pub struct Namespace {
    name: NamespaceIdent,
    properties: HashMap<String, String>,
}

/// TableIdent represents the identifier of a table in the catalog.
pub struct TableIdent {
    namespace: NamespaceIdent,
    name: String,
}

/// TableCreation represents the creation of a table in the catalog.
pub struct TableCreation {
    name: String,
    location: String,
    schema: Schema,
    partition_spec: Option<PartitionSpec>,
    sort_order: SortOrder,
    properties: HashMap<String, String>,
}

/// TableCommit represents the commit of a table in the catalog.
pub struct TableCommit {
    ident: TableIdent,
    requirements: Vec<TableRequirement>,
    updates: Vec<TableUpdate>,
}

/// TableRequirement represents a requirement for a table in the catalog.
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
///
/// TODO: we should fill with UpgradeFormatVersionUpdate, AddSchemaUpdate and so on.
pub enum TableUpdate {}
