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

mod action;
mod append;
mod snapshot;
mod sort_order;
mod update_location;
mod update_properties;
mod upgrade_format_version;

use std::cmp::Ordering;
use std::collections::HashMap;
use std::mem::discriminant;
use std::sync::Arc;

use uuid::Uuid;

use crate::TableUpdate::UpgradeFormatVersion;
use crate::error::Result;
use crate::spec::FormatVersion;
use crate::table::Table;
use crate::transaction::action::BoxedTransactionAction;
use crate::transaction::append::FastAppendAction;
use crate::transaction::sort_order::ReplaceSortOrderAction;
use crate::transaction::update_location::UpdateLocationAction;
use crate::transaction::update_properties::UpdatePropertiesAction;
use crate::transaction::upgrade_format_version::UpgradeFormatVersionAction;
use crate::{Catalog, Error, ErrorKind, TableCommit, TableRequirement, TableUpdate};

/// Table transaction.
pub struct Transaction {
    base_table: Table,
    current_table: Table,
    actions: Vec<BoxedTransactionAction>,
    updates: Vec<TableUpdate>,
    requirements: Vec<TableRequirement>,
}

impl Transaction {
    /// Creates a new transaction.
    pub fn new(table: &Table) -> Self {
        Self {
            base_table: table.clone(),
            current_table: table.clone(),
            actions: vec![],
            updates: vec![],
            requirements: vec![],
        }
    }

    fn update_table_metadata(&mut self, updates: &[TableUpdate]) -> Result<()> {
        let mut metadata_builder = self.current_table.metadata().clone().into_builder(None);
        for update in updates {
            metadata_builder = update.clone().apply(metadata_builder)?;
        }

        self.current_table
            .with_metadata(Arc::new(metadata_builder.build()?.metadata));

        Ok(())
    }

    fn apply(
        &mut self,
        updates: Vec<TableUpdate>,
        requirements: Vec<TableRequirement>,
    ) -> Result<()> {
        for requirement in &requirements {
            requirement.check(Some(self.current_table.metadata()))?;
        }

        self.update_table_metadata(&updates)?;

        self.updates.extend(updates);

        // For the requirements, it does not make sense to add a requirement more than once
        // For example, you cannot assert that the current schema has two different IDs
        for new_requirement in requirements {
            if self
                .requirements
                .iter()
                .map(discriminant)
                .all(|d| d != discriminant(&new_requirement))
            {
                self.requirements.push(new_requirement);
            }
        }

        // # TODO
        // Support auto commit later.

        Ok(())
    }

    /// Sets table to a new version.
    pub fn upgrade_table_version(&self) -> UpgradeFormatVersionAction {
        UpgradeFormatVersionAction::new()
    }

    /// Update table's property.
    pub fn update_properties(&self) -> UpdatePropertiesAction {
        UpdatePropertiesAction::new()
    }

    fn generate_unique_snapshot_id(&self) -> i64 {
        let generate_random_id = || -> i64 {
            let (lhs, rhs) = Uuid::new_v4().as_u64_pair();
            let snapshot_id = (lhs ^ rhs) as i64;
            if snapshot_id < 0 {
                -snapshot_id
            } else {
                snapshot_id
            }
        };
        let mut snapshot_id = generate_random_id();
        while self
            .current_table
            .metadata()
            .snapshots()
            .any(|s| s.snapshot_id() == snapshot_id)
        {
            snapshot_id = generate_random_id();
        }
        snapshot_id
    }

    /// Creates a fast append action.
    pub fn fast_append(
        self,
        commit_uuid: Option<Uuid>,
        key_metadata: Vec<u8>,
    ) -> Result<FastAppendAction> {
        let snapshot_id = self.generate_unique_snapshot_id();
        FastAppendAction::new(
            self,
            snapshot_id,
            commit_uuid.unwrap_or_else(Uuid::now_v7),
            key_metadata,
            HashMap::new(),
        )
    }

    /// Creates replace sort order action.
    pub fn replace_sort_order(self) -> ReplaceSortOrderAction {
        ReplaceSortOrderAction {
            tx: self,
            sort_fields: vec![],
        }
    }

    /// Set the location of table
    pub fn update_location(&self) -> UpdateLocationAction {
        UpdateLocationAction::new()
    }

    /// Commit transaction.
    pub async fn commit(self, catalog: &dyn Catalog) -> Result<Table> {
        let table_commit = TableCommit::builder()
            .ident(self.base_table.identifier().clone())
            .updates(self.updates)
            .requirements(self.requirements)
            .build();

        catalog.update_table(table_commit).await
    }
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::io::BufReader;

    use crate::io::FileIOBuilder;
    use crate::spec::{FormatVersion, TableMetadata};
    use crate::table::Table;
    use crate::transaction::Transaction;
    use crate::{TableIdent, TableUpdate};

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
