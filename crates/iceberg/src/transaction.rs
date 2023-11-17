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

//! This module contains transaction api.

use crate::error::Result;
use crate::spec::{FormatVersion, NullOrder, SortDirection, SortField, SortOrder, Transform};
use crate::table::Table;
use crate::TableUpdate::UpgradeFormatVersion;
use crate::{Catalog, Error, ErrorKind, TableCommit, TableRequirement, TableUpdate};
use std::collections::HashMap;

/// Table transaction.
pub struct Transaction<'a> {
    table: &'a Table,
    updates: Vec<TableUpdate>,
    requirements: Vec<TableRequirement>,
}

impl<'a> Transaction<'a> {
    /// Creates a new transaction.
    pub fn new(table: &'a Table) -> Self {
        Self {
            table,
            updates: vec![],
            requirements: vec![],
        }
    }

    fn append_updates(&mut self, updates: Vec<TableUpdate>) -> Result<()> {
        self.updates.extend(updates);
        Ok(())
    }

    fn append_requirements(&mut self, requirements: Vec<TableRequirement>) -> Result<()> {
        self.requirements.extend(requirements);
        Ok(())
    }

    /// Sets table to a new version.
    pub fn upgrade_table_version(mut self, format_version: FormatVersion) -> Result<Self> {
        let current_version = self.table.metadata().format_version();
        if current_version > format_version {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Cannot downgrade table version from {} to {}",
                    current_version, format_version
                ),
            ));
        } else if current_version < format_version {
            self.append_updates(vec![UpgradeFormatVersion { format_version }])?;
        }
        Ok(self)
    }

    /// Update table's property.
    pub fn set_properties(mut self, props: HashMap<String, String>) -> Result<Self> {
        self.append_updates(vec![TableUpdate::SetProperties { updates: props }])?;
        Ok(self)
    }

    /// Creates replace sort order action.
    pub fn replace_sort_order(mut self) -> ReplaceSortOrderAction<'a> {
        ReplaceSortOrderAction {
            tx: self,
            sort_fields: vec![],
        }
    }

    /// Remove properties in table.
    pub fn remove_properties(mut self, keys: Vec<String>) -> Result<Self> {
        self.append_updates(vec![TableUpdate::RemoveProperties { removals: keys }])?;
        Ok(self)
    }

    /// Commit transaction.
    pub async fn commit(self, catalog: &impl Catalog) -> Result<Table> {
        let table_commit = TableCommit::builder()
            .ident(self.table.identifier().clone())
            .updates(self.updates)
            .requirements(self.requirements)
            .build();

        catalog.update_table(table_commit).await
    }
}

/// Transaction action for replacing sort order.
pub struct ReplaceSortOrderAction<'a> {
    tx: Transaction<'a>,
    sort_fields: Vec<SortField>,
}

impl<'a> ReplaceSortOrderAction<'a> {
    /// Adds a field for sorting in ascending order.
    pub fn asc(mut self, name: &str, null_order: NullOrder) -> Result<Self> {
        self.add_sort_field(name, SortDirection::Ascending, null_order)
    }

    /// Adds a field for sorting in descending order.
    pub fn desc(mut self, name: &str, null_order: NullOrder) -> Result<Self> {
        self.add_sort_field(name, SortDirection::Descending, null_order)
    }

    /// Finished building the action and apply it to the transaction.
    pub fn apply(mut self) -> Result<Transaction<'a>> {
        let updates = vec![
            TableUpdate::AddSortOrder {
                sort_order: SortOrder {
                    fields: self.sort_fields,
                    ..SortOrder::default()
                },
            },
            TableUpdate::SetDefaultSortOrder { sort_order_id: -1 },
        ];

        let requirements = vec![
            TableRequirement::CurrentSchemaIdMatch {
                current_schema_id: self.tx.table.metadata().current_schema().schema_id() as i64,
            },
            TableRequirement::DefaultSortOrderIdMatch {
                default_sort_order_id: self
                    .tx
                    .table
                    .metadata()
                    .default_sort_order()
                    .unwrap()
                    .order_id,
            },
        ];

        self.tx.append_requirements(requirements)?;
        self.tx.append_updates(updates)?;
        Ok(self.tx)
    }

    fn add_sort_field(
        mut self,
        name: &str,
        sort_direction: SortDirection,
        null_order: NullOrder,
    ) -> Result<Self> {
        let field_id = self
            .tx
            .table
            .metadata()
            .current_schema()
            .field_id_by_name(name)
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!("Cannot find field {} in table schema", name),
                )
            })?;

        let sort_field = SortField::builder()
            .source_id(field_id)
            .transform(Transform::Identity)
            .direction(sort_direction)
            .null_order(null_order)
            .build();

        self.sort_fields.push(sort_field);
        Ok(self)
    }
}
