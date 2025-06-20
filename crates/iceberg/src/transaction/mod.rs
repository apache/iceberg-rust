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

/// The `ApplyTransactionAction` trait provides an `apply` method
/// that allows users to apply a transaction action to a `Transaction`.
mod action;
pub use action::*;
mod append;
mod snapshot;
mod sort_order;
mod update_location;
mod update_properties;
mod upgrade_format_version;

use std::sync::Arc;

use crate::error::Result;
use crate::table::Table;
use crate::transaction::action::BoxedTransactionAction;
use crate::transaction::append::FastAppendAction;
use crate::transaction::sort_order::ReplaceSortOrderAction;
use crate::transaction::update_location::UpdateLocationAction;
use crate::transaction::update_properties::UpdatePropertiesAction;
use crate::transaction::upgrade_format_version::UpgradeFormatVersionAction;
use crate::{Catalog, TableCommit, TableRequirement, TableUpdate};

/// Table transaction.
pub struct Transaction {
    table: Table,
    actions: Vec<BoxedTransactionAction>,
}

impl Transaction {
    /// Creates a new transaction.
    pub fn new(table: &Table) -> Self {
        Self {
            table: table.clone(),
            actions: vec![],
        }
    }

    fn update_table_metadata(table: &mut Table, updates: &[TableUpdate]) -> Result<()> {
        let mut metadata_builder = table.metadata().clone().into_builder(None);
        for update in updates {
            metadata_builder = update.clone().apply(metadata_builder)?;
        }

        table.with_metadata(Arc::new(metadata_builder.build()?.metadata));

        Ok(())
    }

    fn apply(
        table: &mut Table,
        mut action_commit: ActionCommit,
        existing_updates: &mut Vec<TableUpdate>,
        existing_requirements: &mut Vec<TableRequirement>,
    ) -> Result<()> {
        let updates = action_commit.take_updates();
        let requirements = action_commit.take_requirements();
        
        for requirement in &requirements {
            requirement.check(Some(table.metadata()))?;
        }

        Self::update_table_metadata(table, &updates)?;

        existing_updates.extend(updates);
        existing_requirements.extend(requirements);

        Ok(())
    }

    /// Sets table to a new version.
    pub fn upgrade_table_version(&self) -> UpgradeFormatVersionAction {
        UpgradeFormatVersionAction::new()
    }

    /// Update table's property.
    pub fn update_table_properties(&self) -> UpdatePropertiesAction {
        UpdatePropertiesAction::new()
    }

    /// Creates a fast append action.
    pub fn fast_append(&self) -> FastAppendAction {
        FastAppendAction::new()
    }

    /// Creates replace sort order action.
    pub fn replace_sort_order(&self) -> ReplaceSortOrderAction {
        ReplaceSortOrderAction::new()
    }

    /// Set the location of table
    pub fn update_location(&self) -> UpdateLocationAction {
        UpdateLocationAction::new()
    }

    /// Commit transaction.
    pub async fn commit(mut self, catalog: &dyn Catalog) -> Result<Table> {
        if self.actions.is_empty() {
            // nothing to commit
            return Ok(self.table.clone());
        }

        self.do_commit(catalog).await
    }

    async fn do_commit(&mut self, catalog: &dyn Catalog) -> Result<Table> {
        let base_table_identifier = self.table.identifier().to_owned();

        let refreshed = catalog.load_table(&base_table_identifier.clone()).await?;

        let mut existing_updates: Vec<TableUpdate> = vec![];
        let mut existing_requirements: Vec<TableRequirement> = vec![];

        if self.table.metadata() != refreshed.metadata()
            || self.table.metadata_location() != refreshed.metadata_location()
        {
            // current base is stale, use refreshed as base and re-apply transaction actions
            self.table = refreshed.clone();
        }

        let mut current_table = self.table.clone();

        for action in &self.actions {
            let action_commit = Arc::clone(action).commit(&current_table).await?;
            // apply action commit to current_table
            Self::apply(
                &mut current_table,
                action_commit,
                &mut existing_updates,
                &mut existing_requirements,
            )?;
        }

        let table_commit = TableCommit::builder()
            .ident(base_table_identifier)
            .updates(existing_updates)
            .requirements(existing_requirements)
            .build();

        catalog.update_table(table_commit).await
    }
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::io::BufReader;

    use crate::TableIdent;
    use crate::io::FileIOBuilder;
    use crate::spec::TableMetadata;
    use crate::table::Table;

    pub fn make_v1_table() -> Table {
        let file = File::open(format!(
            "{}/testdata/table_metadata/{}",
            env!("CARGO_MANIFEST_DIR"),
            "TableMetadataV1Valid.json"
        ))
        .unwrap();
        let reader = BufReader::new(file);
        let resp = serde_json::from_reader::<_, TableMetadata>(reader).unwrap();

        Table::builder()
            .metadata(resp)
            .metadata_location("s3://bucket/test/location/metadata/v1.json".to_string())
            .identifier(TableIdent::from_strs(["ns1", "test1"]).unwrap())
            .file_io(FileIOBuilder::new("memory").build().unwrap())
            .build()
            .unwrap()
    }

    pub fn make_v2_table() -> Table {
        let file = File::open(format!(
            "{}/testdata/table_metadata/{}",
            env!("CARGO_MANIFEST_DIR"),
            "TableMetadataV2Valid.json"
        ))
        .unwrap();
        let reader = BufReader::new(file);
        let resp = serde_json::from_reader::<_, TableMetadata>(reader).unwrap();

        Table::builder()
            .metadata(resp)
            .metadata_location("s3://bucket/test/location/metadata/v1.json".to_string())
            .identifier(TableIdent::from_strs(["ns1", "test1"]).unwrap())
            .file_io(FileIOBuilder::new("memory").build().unwrap())
            .build()
            .unwrap()
    }

    pub fn make_v2_minimal_table() -> Table {
        let file = File::open(format!(
            "{}/testdata/table_metadata/{}",
            env!("CARGO_MANIFEST_DIR"),
            "TableMetadataV2ValidMinimal.json"
        ))
        .unwrap();
        let reader = BufReader::new(file);
        let resp = serde_json::from_reader::<_, TableMetadata>(reader).unwrap();

        Table::builder()
            .metadata(resp)
            .metadata_location("s3://bucket/test/location/metadata/v1.json".to_string())
            .identifier(TableIdent::from_strs(["ns1", "test1"]).unwrap())
            .file_io(FileIOBuilder::new("memory").build().unwrap())
            .build()
            .unwrap()
    }
}
