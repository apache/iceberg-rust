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

//! A static catalog provider that implements the [`Catalog`] trait for
//! use in constructing an [`IcebergTableProvider`] from a static table.

use std::collections::HashMap;

use async_trait::async_trait;
use iceberg::table::Table;
use iceberg::{
    Catalog, Error, ErrorKind, Namespace, NamespaceIdent, Result, TableCommit, TableCreation,
    TableIdent,
};

/// Represents a static catalog that contains a single table.
#[derive(Debug)]
pub struct StaticCatalog {
    table: Table,
}

impl StaticCatalog {
    pub fn new(table: Table) -> Self {
        Self { table }
    }
}

#[async_trait]
impl Catalog for StaticCatalog {
    async fn load_table(&self, table_identifier: &TableIdent) -> Result<Table> {
        if self.table.identifier() != table_identifier {
            return Err(Error::new(
                ErrorKind::TableNotFound,
                format!(
                    "Table with identifier {} not found in static catalog",
                    table_identifier
                ),
            ));
        }

        Ok(self.table.clone())
    }

    async fn list_namespaces(
        &self,
        _parent: Option<&NamespaceIdent>,
    ) -> Result<Vec<NamespaceIdent>> {
        Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "Listing namespaces is not supported in static catalog",
        ))
    }

    async fn create_namespace(
        &self,
        _namespace: &NamespaceIdent,
        _properties: HashMap<String, String>,
    ) -> Result<Namespace> {
        Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "Creating namespaces is not supported in static catalog",
        ))
    }

    async fn get_namespace(&self, _namespace: &NamespaceIdent) -> Result<Namespace> {
        Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "Getting namespaces is not supported in static catalog",
        ))
    }

    async fn namespace_exists(&self, _namespace: &NamespaceIdent) -> Result<bool> {
        Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "Checking namespace existence is not supported in static catalog",
        ))
    }

    async fn update_namespace(
        &self,
        _namespace: &NamespaceIdent,
        _properties: HashMap<String, String>,
    ) -> Result<()> {
        Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "Updating namespaces is not supported in static catalog",
        ))
    }

    async fn drop_namespace(&self, _namespace: &NamespaceIdent) -> Result<()> {
        Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "Dropping namespaces is not supported in static catalog",
        ))
    }

    async fn list_tables(&self, namespace: &NamespaceIdent) -> Result<Vec<TableIdent>> {
        if self.table.identifier().namespace() == namespace {
            return Ok(vec![self.table.identifier().clone()]);
        }
        Err(Error::new(
            ErrorKind::NamespaceNotFound,
            format!("Namespace {} not found in static catalog", namespace),
        ))
    }

    async fn create_table(
        &self,
        _namespace: &NamespaceIdent,
        _creation: TableCreation,
    ) -> Result<Table> {
        Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "Creating tables is not supported in static catalog",
        ))
    }

    async fn drop_table(&self, _table: &TableIdent) -> Result<()> {
        Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "Dropping tables is not supported in static catalog",
        ))
    }

    async fn table_exists(&self, table: &TableIdent) -> Result<bool> {
        if self.table.identifier() == table {
            return Ok(true);
        }
        Ok(false)
    }

    async fn rename_table(&self, _src: &TableIdent, _dest: &TableIdent) -> Result<()> {
        Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "Renaming tables is not supported in static catalog",
        ))
    }

    async fn update_table(&self, _commit: TableCommit) -> Result<Table> {
        Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "Updating tables is not supported in static catalog",
        ))
    }

    async fn register_table(
        &self,
        _table: &TableIdent,
        _metadata_location: String,
    ) -> Result<Table> {
        Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "Registering tables is not supported in static catalog",
        ))
    }
}
